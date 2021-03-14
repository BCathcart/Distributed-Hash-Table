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
	case MEMBERSHIP_REQUEST:
		membershipReqHandler(addr, msg)
		respond = false

	case HEARTBEAT:
		heartbeatHandler(addr, msg)

	case TRANSFER_FINISHED:
		transferFinishedHandler(addr, msg)

	case DATA_TRANSFER:
		chainReplication.HandleDataMsg(addr, msg)
		// resPayload, err = transferRequestHandler(addr, msg)

	case PING:
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

	// If this node is bootstrapping and this is FORWARDED_CLIENT_REQ, then it automatically belongs to this node.
	// TODO(Brennan): use DATA_TRANSFER type instead?
	if memberStore_.getCurrMember().Status == STATUS_BOOTSTRAPPING && msg.InternalID == requestreply.FORWARDED_CLIENT_REQ {
		log.Println(key, memberStore_.mykey, "keeping key sent by successor during bootstrap")
		payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount())
		return nil, addr, payload, err
	}

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

	// First try handling the request here. If the key isn't found, forward the request to bootstrapping predecessor
	if transferRcvNdKey != nil && *transferRcvNdKey == member.Key {
		// TODO(Brennan): if the request is an update, also forward the update to the predecessor transferring to

		log.Println("transferRcvNdKey: This index: ", thisMemberPos, ", Successor index: ", pos)
		payload, err, errCode := kvstore.RequestHandler(kvRequest, GetMembershipCount())
		if errCode != kvstore.NOT_FOUND {
			return nil, addr, payload, err
		} else {
			return *transferNdAddr, addr, payload, err
		}
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
