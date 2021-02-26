package membership

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	kvstore "github.com/abcpen431/miniproject/src/kvStore"
	"github.com/abcpen431/miniproject/src/util"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
	"github.com/abcpen431/miniproject/src/requestreply"
	"github.com/golang/protobuf/proto"
)

/* Internal Msg IDs */
//TODO: don't copy, create new file?
const MEMBERSHIP_REQUEST = 0x1
const HEARTBEAT = 0x2
const TRANSFER_FINISHED = 0x3
const TRANSFER_REQ = 0x6

/***** GOSSIP PROTOCOL *****/
const STATUS_NORMAL = 0x1
const STATUS_BOOTSTRAPPING = 0x2
const STATUS_UNAVAILABLE = 0x3

const HEARTBEAT_INTERVAL = 1000 // ms

// Maps msg ID to serialized response
var memberStore_ *MemberStore

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
func makeMembershipReq(otherMembers []*net.UDPAddr, thisIP string, thisPort int32) {
	// Send request to random node (from list of nodes)
	randIdx := rand.Intn(len(otherMembers))
	randAddr := otherMembers[randIdx]
	log.Println("RANDOM ADDRESS", randAddr)
	log.Println("RANDOM IP", randAddr.IP)
	log.Println("RANDOM Port", randAddr.Port)
	//randMemberAddr := util.CreateAddressString(randMember.Ip, randMember.port)
	localAddrStr := util.CreateAddressString(thisIP, int(thisPort))
	reqPayload := []byte(localAddrStr)
	log.Println("MY ADDRESS: ", localAddrStr)
	err := requestreply.SendMembershipRequest(reqPayload, randAddr.IP.String(), randAddr.Port)
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
	members := &pb.Members{}
	memberStore_.lock.RLock()
	members.Members = make([]*pb.Member, len(memberStore_.members))
	for i, member := range memberStore_.members {
		members.Members[i] = member
	}
	memberStore_.lock.RUnlock()
	gspPayload, err := proto.Marshal(members)
	if err != nil {
		log.Println(err)
		return
	}

	randMember := memberStore_.getRandMember()
	err = requestreply.SendHeartbeatMessage(gspPayload, string(randMember.GetIp()), int(randMember.GetPort()))
	if err != nil {
		//corrupted ip addr/port
		//TODO: discard member from members list?
	}
}

// Rozhan
// TASK4 (part 2): Compare incoming member array with current member array and
// update entries to the one with the larger heartbeat (i.e. newer gossip)
func heartbeatHandler(addr net.Addr, msg *pb.InternalMsg) {
	// Compare Members list and update as necessary
	// Need to ignore any statuses of "Unavailable" (or just don't send them)
	// since failure detection is local.

	// (Not a big priority for M1) If we receive a heartbeat update from a predecessor
	// that had status "Unavailable" at this node, then we can transfer any keys we were storing for it
	// - need to check version number before writing

	//assume the incoming member list is in the correct order so no need to
	//reorder it?
	payload := msg.GetPayload()

	gossipMsg := &pb.Members{}
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
			status := members[i].GetStatus()
			if status == STATUS_UNAVAILABLE {
				// ignore unavailable status from another node since
				// failure detection is local
				status = memberStore_.members[localidx].GetStatus()
			}
			memberStore_.members[localidx] = members[i]
			memberStore_.members[localidx].Status = status
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
func transferToPredecessor(ipStr string, portStr string, dummy3 uint32 /* predecessor key */) {
	// Send all key-value pairs that is the responsibility of the predecessor
	// Use PUT requests (like an external client)
	log.Println("TRANSFER TO PRED: ", ipStr, portStr)
	portInt, _ := strconv.Atoi(portStr)
	_ = requestreply.SendTransferFinished([]byte(""), ipStr, portInt)

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
	targetKey := util.GetNodeKey(ipStr, portStr)
	nodeIsSuccessor, err := isSuccessor(targetKey)
	if err != nil {
		log.Println("Error finding successor") // TODO actually handle error
	}
	if nodeIsSuccessor {
		go transferToPredecessor(ipStr, portStr, targetKey) // TODO Not sure about how to call this.
	} else {
		targetNodePosition := searchForSuccessor(targetKey)
		targetMember := memberStore_.members[targetNodePosition]
		err = requestreply.SendMembershipRequest(msg.Payload, string(targetMember.Ip), int(targetMember.Port)) // TODO Don't know about return addr param
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
	log.Println("\n===== TRANSFER FINISHED =======\n", addr)
	memberStore_.members[memberStore_.position].Status = STATUS_NORMAL
	ip, portStr := util.GetIPPort(addr.String())
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Println("Invalid port")
	}
	newKey := util.GetNodeKey(ip, portStr)
	newMember := &pb.Member{Ip: []byte(ip), Port: int32(port), Key: newKey, Heartbeat: 0, Status: STATUS_NORMAL}
	memberStore_.members = append(memberStore_.members, newMember)
	memberStore_.sortAndUpdateIdx()

	// End timer and set status to "Normal"
	// Nodes will now start sending requests directly to us rather than to our successor.
}

func MemberUnavailableHandler(addr *net.Addr) {
	memberStore_.lock.Lock()
	ip := (*addr).(*net.UDPAddr).IP.String()
	port := (*addr).(*net.UDPAddr).Port
	memberStore_.members[memberStore_.findIPPortIndex(ip, int32(port))].Status = STATUS_UNAVAILABLE
	memberStore_.lock.Unlock()
}

// pass internal messges to the appropriate handler function
func InternalMsgHandler(addr net.Addr, msg *pb.InternalMsg) (bool, []byte, error) {
	var resPayload []byte = nil
	var err error = nil
	respond := false
	switch msg.InternalID {
	case MEMBERSHIP_REQUEST:
		membershipReqHandler(addr, msg)

	case HEARTBEAT:
		heartbeatHandler(addr, msg)
		respond = true
	case TRANSFER_FINISHED:
		transferFinishedHandler(addr, msg)

	case TRANSFER_REQ:
		resPayload, err = transferRequestHandler(addr, msg)
		respond = true
	default:
		//TODO return err
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
		return false, nil, errors.New("Invalid InternalID Error")
	}
	return respond, resPayload, err
}

func transferRequestHandler(addr net.Addr, msg *pb.InternalMsg) ([]byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, err
	}
	return kvstore.RequestHandler(kvRequest, GetMembershipCount())
}

// pass internal messges to the appropriate handler function
func ExternalMsgHandler(addr net.Addr, msg *pb.InternalMsg) (net.Addr, net.Addr, []byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, nil, nil, err
	}
	key := util.Hash(kvRequest.GetKey())
	idx := searchForSuccessor(key)
	if idx == memberStore_.position { //TODO status bootstaping?
		payload, err := kvstore.RequestHandler(kvRequest, GetMembershipCount()) //TODO change membershipcount
		return nil, addr, payload, err
	}
	member := memberStore_.get(idx)
	forwardAddr, err := util.GetAddr(string(member.GetIp()), int(member.GetPort()))
	if err != nil {
		return nil, nil, nil, err
	}
	return *forwardAddr, addr, msg.GetPayload(), nil

}

func MembershipLayerInit(conn *net.PacketConn, otherMembers []*net.UDPAddr, ip string, port int32) {
	memberStore_ = NewMemberStore()

	key := util.GetNodeKey(ip, strconv.Itoa(int(port)))

	var status int32
	if len(otherMembers) == 0 {
		status = STATUS_NORMAL
	} else {
		status = STATUS_BOOTSTRAPPING
	}

	// Add this node to Member array
	memberStore_.members = append(memberStore_.members, &pb.Member{Ip: []byte(ip), Port: port, Key: key, Heartbeat: 0, Status: status})
	memberStore_.position = 0
	memberStore_.mykey = key

	// Update heartbeat every HEARTBEAT_INTERVAL seconds
	var ticker = time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL)
	go func() {
		for {
			log.Println("MEMBERS", memberStore_.members)
			<-ticker.C
			tickHeartbeat()
			if memberStore_.getCurrMember().Status != STATUS_BOOTSTRAPPING && len(memberStore_.members) != 1 {
				log.Println("TICKED HEARTBEAT")
				gossipHeartbeat()
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
