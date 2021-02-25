package membership

import (
	"github.com/abcpen431/miniproject/src/util"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
	"github.com/abcpen431/miniproject/src/requestreply"
	"github.com/golang/protobuf/proto"
)

/* Internal Msg IDs */
//TODO: don't copy, create new file?
const MEMBERSHIP_REQUEST = 0x1
const HEARTBEAT = 0x2
const TRANSFER_FINISHED = 0x3

/***** GOSSIP PROTOCOL *****/
const STATUS_NORMAL = 0x1
const STATUS_BOOTSTRAPPING = 0x2
const STATUS_UNAVAILABLE = 0x3

const HEARTBEAT_INTERVAL = 1000 // ms

// Maps msg ID to serialized response
var memberStore_ *MemberStore
var key_ int32

/*
* Updates the heartbeat by one.
 */
func tickHeartbeat() {
	memberStore_.lock.Lock()
	memberStore_.members[memberStore_.position].Heartbeat++
	memberStore_.lock.Unlock()
}

// TOM
// TASK3 (part 1): Membership protocol (bootstrapping process)
func makeMembershipReq() {
	// Send request to random node (from list of nodes)
	randMember := getRandMember()
	//randMemberAddr := util.CreateAddressString(randMember.Ip, randMember.port)
	localAddr := GetOutboundAddress()
	localAddrStr := localAddr.String()
	reqPayload := []byte(localAddrStr)

	err := requestreply.SendMembershipMessage(reqPayload, string(randMember.Ip), int(randMember.Port))
	if err != nil {
		log.Println("Error sending membership message ") // TODO some sort of error handling
	}
	// TODO: This comment below
	// Repeat this request periodically until receive TRANSFER_FINISHED message
	// to protect against nodes failing (this will probably be more important for later milestones)
}

// Source: Sathish's campuswire post #310, slightly modified
func GetOutboundAddress() net.Addr {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr()

	return localAddr
}

// Rozhan
// TASK4 (part 1): Gossip heartbeat (send the entire member array in the MemberStore).
func gossipHeartbeat() {
	// Package MemberStore.members array
	members := &pb.GossipMessage{}
	memberStore_.lock.RLock()
	members.Members = make([]*pb.GossipMessage_Member, len(memberStore_.members))
	for i, member := range memberStore_.members {
		members.Members[i] = member
	}
	memberStore_.lock.RUnlock()
	gspPayload, err := proto.Marshal(members)
	if err != nil {
		log.Println(err)
		return
	}
	//pick a node at random to gossip to
	randi := memberStore_.position
	for randi == memberStore_.position {
		randi = rand.Intn(len(memberStore_.members))
	}
	randMember := memberStore_.members[randi]
	err = requestreply.SendGossipMessage(gspPayload, string(randMember.GetIp()), int(randMember.GetPort()))
	if err != nil {
		//corrupted ip addr/port
		//TODO: discard member from members list?
	}
}

// Rozhan
// TASK4 (part 2): Compare incoming member array with current member array and
// update entries to the one with the larger heartbeat (i.e. newer gossip)
func heartbeatHandler(addr net.Addr, payload []byte) {
	// Compare Members list and update as necessary
	// Need to ignore any statuses of "Unavailable" (or just don't send them)
	// since failure detection is local.

	// (Not a big priority for M1) If we receive a heartbeat update from a predecessor
	// that had status "Unavailable" at this node, then we can transfer any keys we were storing for it
	// - need to check version number before writing

	//assume the incoming member list is in the correct order so no need to
	//reorder it?

	gossipMsg := &pb.GossipMessage{}
	err := proto.Unmarshal(payload, gossipMsg)
	if err != nil {
		log.Println("WARN heartbeat message with invalid format")
		return
	}
	reindex := false
	members := gossipMsg.GetMembers()
	for i := range members {
		//TODO: make more efficient?-- this is terrible-- runs finds for every member received
		localidx := memberStore_.findIPPortIndex(string(members[i].GetIp()), members[i].GetPort())
		if localidx == -1 {
			// member was not in membership list, so add it
			memberStore_.members = append(memberStore_.members, members[i])
			reindex = true
		} else if members[i].GetHeartbeat() > memberStore_.members[localidx].GetHeartbeat() {
			// the incoming member information is newer
			if members[i].Key != memberStore_.members[localidx].GetKey() {
				reindex = true
			}
			memberStore_.members[localidx] = members[i]
		}
	}
	if reindex {
		memberStore_.sortAndUpdateIdx()
	}
}

// Shay
// TASK3 (part 3): Membership protocol - transfers the necessary data to a joined node
// The actual transfer from the succesor to the predecessor
// Send a TRANSFER_FINISHED when it's done
func transferToPredecessor(dummy1 string, dummy2 string, dummy3 int32 /* predecessor key */) {
	// Send all key-value pairs that is the responsibility of the predecessor
	// Use PUT requests (like an external client)
}

// TOM
// TASK3 (part 2): Membership protocol
/**
@param addr the address that sent the message
@param InternalMsg the internal message being sent
*/
func membershipReqHandler(addr net.Addr, msg *pb.InternalMsg) {
	// Find successor node and forward the
	// membership request there to start the transfer

	ipStr, portStr := util.GetIPPort(string(msg.Payload))
	targetKey := int32(util.GetNodeKey(ipStr, portStr))
	nodeIsSuccessor, err := isSuccessor(targetKey)
	if err != nil {
		log.Println("Error finding successor") // TODO actually handle error
	}
	if nodeIsSuccessor {
		go transferToPredecessor(ipStr, portStr, targetKey) // TODO Not sure about how to call this.
	} else {
		targetNodePosition := searchForSuccessor(targetKey)
		targetMember := memberStore_.members[targetNodePosition]
		err = requestreply.SendMembershipMessage(msg.Payload, string(targetMember.Ip), int(targetMember.Port)) // TODO Don't know about return addr param
		if err != nil {
			log.Println("ERROR Sending membership message to successor") // TODO more error handling
		}
	}

	// If this node is the successor, start transferring keys

	// If this node is the successor and already in the process of
	// receiving or sending a transfer, respond with "Busy" to the request.
	// The node sending the request will then re-send it after waiting a bit.
}

/* Ignore this */
// func transferStartedHandler(addr net.Addr, msg *pb.InternalMsg) {
// 	// Register that the transfer is started
// 	// Start timeout, save IP of sender, and reset timer everytime receive a write from
// 	// the IP address
// 	// If timeout hit, set status to "Normal"
// }

// TOM + Shay
// TASK3 (part 4): Membership protocol - transfer to this node is finished
func transferFinishedHandler(addr net.Addr, msg *pb.InternalMsg) {
	memberStore_.members[memberStore_.position].Status = STATUS_NORMAL
	// End timer and set status to "Normal"
	// Nodes will now start sending requests directly to us rather than to our successor.
}

// pass internal messges to the appropriate handler function
func InternalMsgHandler(addr net.Addr, msg *pb.InternalMsg) {
	switch msg.InternalID {
	case MEMBERSHIP_REQUEST:
		membershipReqHandler(addr, msg)

	case HEARTBEAT:
		go heartbeatHandler(addr, msg.GetPayload())

	case TRANSFER_FINISHED:
		transferFinishedHandler(addr, msg)

	default:
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
	}
}

func MembershipLayerInit(conn *net.PacketConn, otherMembers []*net.UDPAddr, ip string, port int32) {
	memberStore_ = NewMemberStore()

	key_ = int32(util.GetNodeKey(string(ip), strconv.Itoa(int(port))))
	// TODO: Get Key here

	var status int32
	if len(otherMembers) == 0 {
		status = STATUS_NORMAL
	} else {
		status = STATUS_BOOTSTRAPPING
	}

	// Add this node to Member array
	memberStore_.members = append(memberStore_.members, &pb.GossipMessage_Member{Ip: []byte(ip), Port: port, Key: key_, Heartbeat: 0, Status: status})
	memberStore_.position = 0

	// Update heartbeat every HEARTBEAT_INTERVAL seconds
	var ticker = time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL)
	go func() {
		for {
			<-ticker.C
			tickHeartbeat()
			gossipHeartbeat()
		}
	}()

	// Send initial membership request message - this tells receiving node they should try to contact the successor first (as well as
	// respond with this node's IP address if needed)

	// If no other nodes are known, then assume this is the first node
	// and this node simply waits to be contacted
	if len(otherMembers) != 0 {
		makeMembershipReq()
	}

}
