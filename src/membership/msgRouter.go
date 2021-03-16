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
func InternalReqHandler(addr net.Addr, msg *pb.InternalMsg) (bool, []byte, error) {
	var resPayload []byte = nil
	var err error = nil
	respond := true

	switch msg.InternalID {
	case requestreply.MEMBERSHIP_REQUEST:
		membershipReqHandler(addr, msg)
		respond = false

	case requestreply.HEARTBEAT:
		heartbeatHandler(addr, msg)

	case requestreply.TRANSFER_FINISHED:
		memberStore_.lock.RLock()
		status := memberStore_.members[memberStore_.position].Status
		memberStore_.lock.RUnlock()

		if status == STATUS_BOOTSTRAPPING {
			bootstrapTransferFinishedHandler(memberStore_)
		} else {
			chainReplication.HandleTransferFinishedReq(&addr)
		}

	case requestreply.DATA_TRANSFER:
		err = transferService.HandleDataMsg(addr, msg)
		if err != nil {
			log.Println("ERROR: Could not handle data transfer message - ", err)
		}

	case requestreply.PING:
		// Send nil payload back
		log.Println("Got PINGed")

	default:
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
		return false, nil, errors.New("Invalid InternalID Error")
	}

	return respond, resPayload, err
}

/**
 * Passes external messages to the appropriate handler function if they belong to this node.
 * Forwards them to the correct node otherwise.
 */
func ExternalReqHandler(addr net.Addr, msg *pb.InternalMsg) (net.Addr, net.Addr, []byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, nil, nil, err
	}

	key := util.Hash(kvRequest.GetKey())

	// TODO: handle case when the key is null

	memberStore_.lock.RLock()
	transferNdAddr := memberStore_.transferNodeAddr
	transferRcvNdKey := getKeyOfNodeTransferringTo()

	member, pos := searchForSuccessor(key, transferRcvNdKey)
	successorIP := member.GetIp()
	successorPort := member.GetPort()

	thisMemberPos := memberStore_.position
	memberStore_.lock.RUnlock()

	// TODO(Brennan): ask replition manager if it wants to handle it.
	// This will handle it if it's a GET request that this node is a tail for
	// or it's an PUT or REMOVE request that this node is the head for.

	var forwardAddr *net.Addr

	if transferRcvNdKey != nil && *transferRcvNdKey == member.Key {
		log.Println("transferRcvNdKey: This index: ", thisMemberPos, ", Successor index: ", pos)
		payload, err, isUpdate := kvstore.RequestHandler(kvRequest, GetMembershipCount())

		// Forward any updates to the bootstrapping predecessor to keep sequential consistency
		if isUpdate {
			requestreply.SendDataTransferMessage(msg.GetPayload(), transferNdAddr)
		}

		return nil, addr, payload, err

	} else if pos == thisMemberPos {
		log.Println("This index: ", thisMemberPos, ", Successor index: ", pos)
		payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount()) //TODO change membershipcount
		return nil, addr, payload, err
	} else {
		forwardAddr, err = util.GetAddr(string(successorIP), int(successorPort))
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return *forwardAddr, addr, msg.GetPayload(), nil
}
