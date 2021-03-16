package membership

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
)

/***** GOSSIP PROTOCOL *****/
const STATUS_NORMAL = 0x1
const STATUS_BOOTSTRAPPING = 0x2
const STATUS_UNAVAILABLE = 0x3

const HEARTBEAT_INTERVAL = 1000 // ms

// Maps msg ID to serialized response
var memberStore_ *MemberStore

/* MUST be holding member store lock */
func getKeyOfNodeTransferringTo() *uint32 {
	// memberStore_.lock.RLock()
	if memberStore_.transferNodeAddr == nil {
		// memberStore_.lock.RUnlock()
		return nil
	} else {
		ret := memberStore_.getKeyFromAddr(memberStore_.transferNodeAddr)
		// memberStore_.lock.RUnlock()
		return ret
	}
}

/**
Membership protocol (bootstrapping process). Sends a request to a random node,
which will eventually get forwarded to our node's successor.

@param otherMembers list of members, given from the initial text file on bootstrap
@param thisIP Current node's ip
@param thisPort Current node's port
*/
func makeMembershipReq(otherMembers []*net.UDPAddr, thisIP string, thisPort int32) {
	// Send request to random node (from list of nodes)
	randIdx := rand.Intn(len(otherMembers))
	randAddr := otherMembers[randIdx]
	localAddrStr := util.CreateAddressString(thisIP, int(thisPort))
	reqPayload := []byte(localAddrStr)
	err := requestreply.SendMembershipRequest(reqPayload, randAddr.IP.String(), randAddr.Port)
	if err != nil {
		log.Println("Error sending membership message ")
	}
	// TODO: For future milestones, Repeat this request periodically until receiving the TRANSFER_FINISHED message
	// this would protect against nodes failing
}

/**
Find successors node: if it is the current node, initiate the transfer process.
Otherwise, forward the membership request there to start the transfer
@param addr the address that sent the message
@param InternalMsg the internal message being sent
*/
func membershipReqHandler(addr net.Addr, msg *pb.InternalMsg) {
	// Send heartbeat to the node requesting
	gossipHeartbeat(&addr)

	ipStr, portStr := util.GetIPPort(string(msg.Payload))
	targetKey := util.GetNodeKey(ipStr, portStr)

	memberStore_.lock.Lock()
	targetMember, targetMemberIdx := searchForSuccessor(targetKey, nil)
	isSuccessor := targetMemberIdx == memberStore_.position

	if isSuccessor {
		memberStore_.transferNodeAddr = &addr
		// curKey := memberStore_.getCurrMember().Key
		memberStore_.lock.Unlock()

		// TODO: Transfer everything between the target key and the previous predecessor
		// TODO: move this funcionality to replicationManager.NewBootstrappingPredecessor
		go transferService.TransferKVStoreData(ipStr, portStr, nil, &targetKey, func() {
			memberStore_.lock.Lock()
			memberStore_.transferNodeAddr = nil
			memberStore_.lock.Unlock()
		})
	} else {
		memberStore_.lock.Unlock()
		err := requestreply.SendMembershipRequest(msg.Payload, string(targetMember.Ip), int(targetMember.Port)) // TODO Don't know about return addr param
		if err != nil {
			log.Println("ERROR Sending membership message to successor") // TODO more error handling
		}
	}

}

/**
Membership protocol: after receiving the transfer finished from the successor node,
sets status to normal
*/
func transferFinishedHandler(addr net.Addr, msg *pb.InternalMsg) {
	log.Println("RECEIVED TRANSFER FINISHED MSG")

	memberStore_.lock.RLock()
	memberStore_.members[memberStore_.position].Status = STATUS_NORMAL
	memberStore_.sortAndUpdateIdx()
	memberStore_.lock.RUnlock()

	// TODO: End timer and set status to "Normal"
	// Nodes will now start sending requests directly to us rather than to our successor.
}

// When a member is found to be unavailable, remove it from the member list
func MemberUnavailableHandler(addr *net.Addr) {
	memberStore_.setStatus(addr, STATUS_UNAVAILABLE)
	log.Println("Finished updating member to UNAVAILABLE: ", *addr)
}

// TODO(Brennan): what is this for????
// func transferRequestHandler(addr net.Addr, msg *pb.InternalMsg) ([]byte, error) {
// 	// Unmarshal KVRequest
// 	kvRequest := &pb.KVRequest{}
// 	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
// 	if err != nil {
// 		return nil, err
// 	}

// 	payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount())
// 	return payload, err
// }

func Init(conn *net.PacketConn, otherMembers []*net.UDPAddr, ip string, port int32) {
	memberStore_ = NewMemberStore()

	key := util.GetNodeKey(ip, strconv.Itoa(int(port)))

	var status int32
	if len(otherMembers) == 0 {
		status = STATUS_NORMAL
	} else {
		status = STATUS_BOOTSTRAPPING
	}

	// Add current node to Member array
	memberStore_.members = append(memberStore_.members, &pb.Member{Ip: []byte(ip), Port: port, Key: key, Heartbeat: 0, Status: status})
	memberStore_.position = 0
	memberStore_.mykey = key
	// Update heartbeat every HEARTBEAT_INTERVAL seconds
	var ticker = time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL)
	go func() {
		for {
			<-ticker.C
			tickHeartbeat()
			if /* memberStore_.getCurrMember().Status != STATUS_BOOTSTRAPPING && */ memberStore_.getLength() != 1 {
				gossipHeartbeat(nil)
			}
		}
	}()

	// Send initial membership request message - this tells receiving node they should try to contact the successor first (as well as
	// respond with this node's IP address if needed)
	// If no other nodes are known, then assume this is the first node
	// and this node simply waits to be contacted
	if len(otherMembers) != 0 {
		makeMembershipReq(otherMembers, ip, port)
	}

}
