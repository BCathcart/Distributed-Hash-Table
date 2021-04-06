package membership

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/CPEN-431-2021/dht-abcpen431/src/chainReplication"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
)

/***** GOSSIP PROTOCOL *****/
const STATUS_NORMAL = 0x1
const STATUS_BOOTSTRAPPING = 0x2
const STATUS_UNAVAILABLE = 0x3

const HEARTBEAT_INTERVAL = 1000 // ms

var memberStore_ *MemberStore /* Structure to hold all membership info */

/* MUST be holding member store lock */
func getKeyOfNodeTransferringTo() *uint32 {
	if memberStore_.transferNodeAddr == nil {
		return nil
	} else {
		ret := memberStore_.getKeyFromAddr(memberStore_.transferNodeAddr)
		return ret
	}
}

/*
 * Gets the address of the head and tail nodes in the chain where the key is replicated.
 * @param key
 * @return The head address
 * @return The tail address
 * @return Error retrieving the head, nil if success
 * @return Error retrieving the tail, nil if success
 */
func GetHeadTailAddr(key uint32) (*net.Addr, *net.Addr, error, error) {
	memberStore_.lock.RLock()
	transferRcvNdKey := getKeyOfNodeTransferringTo()

	head, pos := searchForSuccessor(key, transferRcvNdKey)

	tail, _ := searchForTail(pos)
	memberStore_.lock.RUnlock()

	headAddr, errHead := getMemberAddr(head)
	tailAddr, errTail := getMemberAddr(tail)
	return headAddr, tailAddr, errHead, errTail
}

/**
Membership protocol (bootstrapping process). Sends a request to a random node,
which will eventually get forwarded to our node's successor.

@param otherMembers list of members, given from the initial text file on bootstrap
@param thisIP Current node's ip
@param thisPort Current node's port
*/
func makeMembershipReq(otherMembers []*net.UDPAddr, thisIP string, thisPort int32) {
	var nodeStatus int32 = STATUS_BOOTSTRAPPING
	for nodeStatus != STATUS_NORMAL {
		// Send request to random node (from list of nodes)
		randIdx := rand.Intn(len(otherMembers))
		randAddr := otherMembers[randIdx]
		localAddrStr := util.CreateAddressString(thisIP, int(thisPort))

		var randAddrTmp net.Addr = randAddr
		if util.CreateAddressStringFromAddr(&randAddrTmp) == localAddrStr {
			log.Println("WARN: cannot send membership request to yourself")
			if len(otherMembers) <= 1 {
				log.Println("WARN: We are the first node, waiting to be contacted")
				SetStatusToNormal()
				break
			} else {
				continue
			}
		}

		log.Println("NOT THE LOCAL ADDRESS  ", util.CreateAddressStringFromAddr(&randAddrTmp), "  ", localAddrStr)

		// Send heartbeat to the node to get gossip started of our existence started
		gossipHeartbeat(&randAddrTmp)

		reqPayload := []byte(localAddrStr)
		err := requestreply.SendMembershipRequest(reqPayload, randAddr.IP.String(), randAddr.Port)
		if err != nil {
			log.Println("ERROR sending membership message ")
		}

		// Make a new request every 5 seconds if a transfer hasn't finished
		time.Sleep(5 * time.Second)
		memberStore_.lock.RLock()
		nodeStatus = memberStore_.members[memberStore_.position].Status
		memberStore_.lock.RUnlock()
	}
}

/**
Find successors node: if it is the current node, initiate the transfer process.
Otherwise, forward the membership request there to start the transfer
@param addr the address that sent the message
@param InternalMsg the internal message being sent
*/
func MembershipReqHandler(addr net.Addr, msg *pb.InternalMsg) {
	// Send heartbeat to the node requesting
	gossipHeartbeat(&addr)

	targetIpStr, targetPortStr := util.GetIPPort(string(msg.Payload))
	targetPort, _ := strconv.Atoi(targetPortStr)
	targetKey := util.GetNodeKey(targetIpStr, targetPortStr)

	memberStore_.lock.RLock()
	targetMember, targetMemberIdx := searchForSuccessor(targetKey, nil)

	if targetMemberIdx == memberStore_.position {
		// We don't want to continue if a transfer is already in progress
		if memberStore_.transferNodeAddr != nil {
			memberStore_.lock.RUnlock()
			log.Println("WARN: Ignoring membership request b/c a transfer is already in progress")
			return
		}

		predKey, _ := getPredecessor(targetKey)
		// curKey := memberStore_.getCurrMember().Key
		memberStore_.lock.RUnlock()

		transferAddr, err := util.GetAddr(targetIpStr, targetPort)
		if err != nil {
			log.Println(err)
			return
		}

		// Transfer everything between the new predecessor's key and the previous predecessor's key
		if transferToBootstrappingPred(transferAddr, predKey, targetKey) == false {
			log.Println("WARN: Ignoring membership request b/c a transfer is already in progress")
		}
	} else {
		memberStore_.lock.RUnlock()
		err := requestreply.SendMembershipRequest(msg.Payload, string(targetMember.Ip), int(targetMember.Port)) // TODO Don't know about return addr param
		if err != nil {
			log.Println("ERROR sending membership message to successor") // TODO more error handling
		}
	}
}

/*
 * When a member is found to be unavailable, remove it from the member list
 */
func MemberUnavailableHandler(addr *net.Addr) {
	memberStore_.setStatus(addr, STATUS_UNAVAILABLE)
	memberStore_.lock.Lock()
	updateChain(&memberStore_.lock)
	log.Println("Finished updating member to UNAVAILABLE: ", *addr)
}

/*
 * Sets this member's status to STATUS_NORMAL and updates the key ranges in the replication layer
 */
func SetStatusToNormal() {
	memberStore_.lock.Lock()
	memberStore_.members[memberStore_.position].Status = STATUS_NORMAL

	// Set key range now that the memberstore info will have been gossiped during bootstrap
	myKey := memberStore_.mykey
	var predKey uint32
	pred := searchForPredecessors(memberStore_.position, 1)[0]
	if pred != nil {
		predKey = pred.Key
	} else {
		predKey = myKey
	}
	chainReplication.SetKeyRange(predKey+1, myKey)

	memberStore_.lock.Unlock()
}

/*
 * Initializes the membership layer
 */
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
	localAddr, _ := util.GetAddr(ip, int(port))
	chainReplication.Init(localAddr)
	// Update heartbeat every HEARTBEAT_INTERVAL seconds
	var ticker = time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL)
	go func() {
		for {
			<-ticker.C
			tickHeartbeat()
			gossipHeartbeat(nil)
		}
	}()

	// Send initial membership request message - this tells receiving node they should try to contact the successor first (as well as
	// respond with this node's IP address if needed)
	// If no other nodes are known, then assume this is the first node
	// and this node simply waits to be contacted
	if len(otherMembers) != 0 {
		go makeMembershipReq(otherMembers, ip, port)
	}

}
