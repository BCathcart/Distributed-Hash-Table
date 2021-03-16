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
func InternalMsgHandler(addr net.Addr, msg *pb.InternalMsg) (*net.Addr, bool, []byte, error) {
	var payload []byte = nil
	var err error = nil
	var fwdAddr *net.Addr = nil
	respond := true

	switch msg.InternalID {
	case requestreply.MEMBERSHIP_REQUEST:
		membershipReqHandler(addr, msg)
		respond = false

	case requestreply.HEARTBEAT:
		heartbeatHandler(addr, msg)

	case requestreply.TRANSFER_FINISHED:
		transferFinishedHandler(addr, msg)

	case requestreply.DATA_TRANSFER:
		chainReplication.HandleDataMsg(addr, msg)

	case requestreply.PING:
		// Send nil payload back
		log.Println("Got PINGed")

	case requestreply.FORWARDED_CHAIN_UPDATE:
		fwdAddr, payload, err = chainReplication.HandleForwardedChainUpdate(msg)

	default:
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
		return nil, false, nil, errors.New("Invalid InternalID Error")
	}

	return fwdAddr, respond, payload, err
}

/**
 * Passes external messages to the appropriate handler function if they belong to this node.
 * Forwards them to the correct node otherwise.
 * @return the address to forward the message if applicable, nil otherwise
 * @return true if the forwarded message if of type FORWARDED_CHAIN_UPDATE, false otherwise
 * @return the payload of reply or forwarded message
 * @return an error in case of failure
 */
func ExternalMsgHandler(msg *pb.InternalMsg) (*net.Addr, bool, []byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, false, nil, err
	}
	fwdAddr, payload, isMine, err := chainReplication.HandleClientRequest(kvRequest)
	if isMine {
		// the request was handled by this node
		return *fwdAddr, true, payload, err
	}
	// the request does not belong to this node, route it to the right node
	key := util.Hash(kvRequest.GetKey())

	/*****************************************   MILESTONE 1 CODE *************************************

	// If this node is bootstrapping and this is FORWARDED_CLIENT_REQ, then it automatically belongs to this node.
	// TODO(Brennan): use DATA_TRANSFER type instead?
	if memberStore_.getCurrMember().Status == STATUS_BOOTSTRAPPING && msg.InternalID == requestreply.FORWARDED_CLIENT_REQ {
		log.Println(key, memberStore_.mykey, "keeping key sent by successor during bootstrap")
		payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount())
		return nil, addr, payload, err
	}
	****************************************************************************************************/

	memberStore_.lock.RLock()
	// transferNdAddr := memberStore_.transferNodeAddr
	transferRcvNdKey := getKeyOfNodeTransferringTo()

	member, pos := searchForSuccessor(key, transferRcvNdKey) //TODO transferRcvNdKey?
	successorIP := member.GetIp()
	successorPort := member.GetPort()

	tail, _ := searchForTail(pos)
	memberStore_.lock.RUnlock()

	fwdAddr = nil

	/*****************************************   MILESTONE 1 CODE *************************************
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
	}
	***************************************************************************************************/

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
