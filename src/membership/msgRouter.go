package membership

import (
	"errors"
	"log"
	"net"
	"strconv"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/chainReplication"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"github.com/golang/protobuf/proto"
)

/**
* Passes external messages to the appropriate handler function
 */
func InternalReqHandler(addr net.Addr, msg *pb.InternalMsg) (*net.Addr, bool, []byte, int, error) {
	var payload []byte = nil
	var err error = nil
	var fwdAddr *net.Addr = nil
	var respond = true
	var responseType = requestreply.GENERIC_RES

	switch msg.InternalID {
	case requestreply.MEMBERSHIP_REQ:
		membershipReqHandler(addr, msg)
		respond = false

	case requestreply.HEARTBEAT_MSG:
		heartbeatHandler(addr, msg)

	case requestreply.TRANSFER_FINISHED_MSG:
		memberStore_.lock.RLock()
		status := memberStore_.members[memberStore_.position].Status
		memberStore_.lock.RUnlock()

		if status == STATUS_BOOTSTRAPPING {
			bootstrapTransferFinishedHandler(memberStore_)
		} else {
			chainReplication.HandleTransferFinishedMsg(msg)
		}

	case requestreply.TRANSFER_REQ:
		payload, respond = chainReplication.HandleTransferReq(msg)
		if respond {
			responseType = requestreply.TRANSFER_RES
		}

	case requestreply.DATA_TRANSFER_MSG:
		err = transferService.HandleDataMsg(addr, msg)
		if err != nil {
			log.Println("ERROR: Could not handle data transfer message - ", err)
		}

	case requestreply.PING_MSG:
		// Send nil payload back
		log.Println("Got PINGed")

	case requestreply.FORWARDED_CHAIN_UPDATE_REQ:
		fwdAddr, payload, err = chainReplication.HandleForwardedChainUpdate(msg)

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
func ExternalReqHandler(msg *pb.InternalMsg) (*net.Addr, bool, []byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, false, nil, err
	}
	// try to handle the request here at this node
	fwdAddr, payload, isMine, err := chainReplication.HandleClientRequest(kvRequest)
	if isMine {
		if kvstore.IsUpdateRequest(kvRequest) {
			// Forward any updates to the bootstrapping predecessor to keep sequential consistency
			memberStore_.lock.RLock()
			transferNodeAddr := memberStore_.transferNodeAddr
			memberStore_.lock.RUnlock()
			if transferNodeAddr != nil {
				requestreply.SendDataTransferMessage(msg.GetPayload(), memberStore_.transferNodeAddr)
			}
		}
		// the request was handled by this node
		return fwdAddr, true, payload, err
	}
	// the request does not belong to this node, route it to the right node

	key := util.Hash(kvRequest.GetKey())

	memberStore_.lock.RLock()
	// transferNdAddr := memberStore_.transferNodeAddr
	transferRcvNdKey := getKeyOfNodeTransferringTo()

	member, pos := searchForSuccessor(key, transferRcvNdKey) //TODO transferRcvNdKey?
	successorIP := member.GetIp()
	successorPort := member.GetPort()

	tail, _ := searchForTail(pos)
	memberStore_.lock.RUnlock()

	fwdAddr = nil

	if kvstore.IsGetRequest(kvRequest) {
		fwdAddr, err = util.GetAddr(string(tail.GetIp()), int(tail.GetPort()))
		if err != nil {
			return nil, false, nil, err
		}
	} else if kvstore.IsUpdateRequest(kvRequest) {
		fwdAddr, err = util.GetAddr(string(successorIP), int(successorPort))
		if err != nil {
			return nil, false, nil, err
		}
	}
	return fwdAddr, false, nil, nil

}

func InternalResHandler(addr net.Addr, msg *pb.InternalMsg) {
	log.Println("InternalResHandler - Received response of type", msg.GetInternalID())
	if msg.InternalID == requestreply.TRANSFER_RES {
		chainReplication.HandleDataTransferRes(&addr, msg)
	}
}
