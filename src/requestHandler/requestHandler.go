package requesthandler

import (
	"errors"
	"log"
	"net"
	"strconv"
	"strings"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"

	"github.com/CPEN-431-2021/dht-abcpen431/src/chainReplication"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/membership"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
)

/**
* Passes external messages to the appropriate handler function
 */
func InternalReqHandler(addr net.Addr, msg *pb.InternalMsg) (*net.Addr, bool, []byte, int, error) {
	var payload []byte = nil
	var err error = nil
	var fwdAddr *net.Addr = nil
	var respond = true
	var responseType = int(msg.InternalID)

	switch msg.InternalID {
	case requestreply.MEMBERSHIP_REQ:
		membership.MembershipReqHandler(addr, msg)
		respond = false

	case requestreply.HEARTBEAT_MSG:
		membership.HeartbeatHandler(addr, msg)

	case requestreply.TRANSFER_FINISHED_MSG:

		if membership.IsBootstrapping() {
			membership.BootstrapTransferFinishedHandler()
		} else {
			payload = chainReplication.HandleTransferFinishedMsg(msg)
			if payload == nil {
				respond = false
			}
		}

	case requestreply.TRANSFER_REQ:
		chainReplication.HandleTransferReq(&addr, msg)
		respond = false

	case requestreply.DATA_TRANSFER_MSG:
		err = transferService.HandleDataMsg(addr, msg)
		if err != nil {
			log.Println("ERROR: Could not handle data transfer message - ", err)
		}
		respond = false

	case requestreply.PING_MSG:
		// Send nil payload back
		// log.Println("Got PINGed")
		break

	case requestreply.FORWARDED_CHAIN_UPDATE_REQ:
		chainReplication.AddRequest(&addr, msg)
		respond = false

	default:
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
		return nil, false, nil, responseType, errors.New("Invalid InternalID Error")
	}

	return fwdAddr, respond, payload, responseType, err
}

/**
 * Passes external messages to the appropriate handler function if they belong to this node.
 * Forwards them to the correct node otherwise.
 * @return the address to forward the message if applicable, nil otherwise
 * @return true if the forwarded message if of type FORWARDED_CHAIN_UPDATE_REQ, false otherwise
 * @return the payload of reply or forwarded message
 * @return an error in case of failure
 */
func ExternalReqHandler(addr net.Addr, msg *pb.InternalMsg) (*net.Addr, bool, []byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, false, nil, err
	}
	// try to handle the request here at this node
	// fwdAddr, payload, isMine, err := chainReplication.HandleClientRequest(kvRequest)
	// if isMine {
	// 	if kvstore.IsUpdateRequest(kvRequest) {
	// 		// Forward any updates to the bootstrapping predecessor to keep sequential consistency
	// 		transferNodeAddr := membership.GetTransferNodeAddr()
	// 		if transferNodeAddr != nil {
	// 			requestreply.SendDataTransferMessage(msg.GetPayload(), transferNodeAddr)
	// 		}
	// 	}
	// 	// the request was handled by this node
	// 	return fwdAddr, true, payload, err
	// }
	currAddr := *chainReplication.MyAddr
	if !kvstore.IsKVRequest(kvRequest) {
		// Any type of client request besides key-value requests gets handled here
		payload, err, _ := kvstore.RequestHandler(kvRequest, membership.GetMembershipCount(), util.KeyRange{})
		return nil, true, payload, err
	}
	if kvRequest.Key == nil {
		// put the key for WIPEOUT requests
		ipstr, portstr := util.GetIPPort(currAddr.String())
		kvRequest.Key = []byte(ipstr + portstr)
		log.Println("Putting key", util.Hash(kvRequest.Key), "for WIPEOUT Request isWipeout = ", kvRequest.Command == 0x05)
	}
	msg.Payload, err = proto.Marshal(kvRequest)
	msg.CheckSum = uint64(util.ComputeChecksum(msg.MessageID, msg.Payload))
	if err != nil {
		return nil, false, nil, err
	}
	// the request does not belong to this node, route it to the right node
	fwdAddr, err := getTransferAddr(kvRequest)
	if err != nil {
		return nil, false, nil, err
	}
	//the request belongs to this node
	if strings.Compare((*fwdAddr).String(), currAddr.String()) == 0 {
		chainReplication.AddRequest(&addr, msg)
		if kvstore.IsUpdateRequest(kvRequest) {
			transferNodeAddr := membership.GetTransferNodeAddr()
			if transferNodeAddr != nil {
				requestreply.SendDataTransferMessage(msg.GetPayload(), transferNodeAddr)
			}
		}

		fwdAddr = nil
	}
	return fwdAddr, false, nil, err
}

func getTransferAddr(kvRequest *pb.KVRequest) (*net.Addr, error) {
	key := util.Hash(kvRequest.GetKey())

	headAddr, tailAddr, errHead, errTail := membership.GetHeadTailAddr(key)

	var fwdAddr *net.Addr = nil
	var err error = nil

	if kvstore.IsGetRequest(kvRequest) {
		fwdAddr = tailAddr
		err = errTail
		if errTail != nil {
			log.Println("WARN: tail addr for GET request for key", key, "was nil")
		}
	} else if kvstore.IsUpdateRequest(kvRequest) {
		fwdAddr = headAddr
		err = errHead
		if errHead != nil {
			log.Println("WARN: head addr for UPDATE request for key", key, "was nil")
		}
	}
	return fwdAddr, err
}

func InternalResHandler(addr net.Addr, msg *pb.InternalMsg) {
	if msg.InternalID == requestreply.TRANSFER_FINISHED_MSG {
		chainReplication.HandleDataTransferFinishedAck(&addr, msg)
	}
}
