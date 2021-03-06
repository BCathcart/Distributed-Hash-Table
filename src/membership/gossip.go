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
const MEMBERSHIP_REQUEST = 0x1
const HEARTBEAT = 0x2
const TRANSFER_FINISHED = 0x3
const PING = 0x5
const TRANSFER_REQ = 0x6

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

/*
* Updates the current node's heartbeat by one.
 */
func tickHeartbeat() {
	memberStore_.lock.Lock()
	memberStore_.members[memberStore_.position].Heartbeat++
	memberStore_.lock.Unlock()
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

// Sends the entire member array in the MemberStore.
func gossipHeartbeat(addr *net.Addr) {
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

	// Pick random member if an address is not provided
	var ip string
	var port int
	if addr == nil {
		member := memberStore_.getRandMember()
		ip = string(member.GetIp())
		port = int(member.GetPort())
	} else {
		ip = (*addr).(*net.UDPAddr).IP.String()
		port = (*addr).(*net.UDPAddr).Port
	}

	err = requestreply.SendHeartbeatMessage(gspPayload, ip, port)
	if err != nil {
		//corrupted ip addr/port
		//TODO: Possibly discard member from members list
	}
}

// Compares incoming member array with current member array and
// update entries to the one with the larger heartbeat (i.e. newer gossip)
// TODO: (Not a big priority for M1) If we receive a heartbeat update from a predecessor
// that had status "Unavailable" at this node, then we can transfer any keys we were storing for it
// - need to check version number before writing
func heartbeatHandler(addr net.Addr, msg *pb.InternalMsg) {
	log.Println("RECEIVED HEARTBEAT MSG")

	payload := msg.GetPayload()

	gossipMsg := &pb.Members{}
	err := proto.Unmarshal(payload, gossipMsg)
	if err != nil {
		log.Println("WARN heartbeat message with invalid format")
		return
	}
	reindex := false
	members := gossipMsg.GetMembers()

	// TODO: locking the whole time is inefficient but will prevent runtime errors
	memberStore_.lock.Lock()
	for i := range members {
		//TODO: Make finding IP Port index more efficient: currently runs finds for every member received
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
	memberStore_.lock.Unlock()

	log.Println(memberStore_.members)

}

/* Membership protocol - transfers the necessary data to a joined node

@param ipStr/PortStr address to transfer keys to
@param predecessorKey key to transfer to
Sends a TRANSFER_FINISHED when it's done

*/
func transferToPredecessor(ipStr string, portStr string, predecessorKey uint32) {

	log.Println("TRANSFERRING KEYS TO PREDECESSOR WITH ADDRESS: ", ipStr, ":", portStr)
	portInt, _ := strconv.Atoi(portStr)
	localKeyList := kvstore.GetKeyList()

	// TODO a map to hold the keys that need to be transferred and whether they've been successfully sent to predecessor
	// var keysToTransfer map[int]bool will be added in next milestone

	// a byte array to hold the byte representation of the key
	memberStore_.lock.RLock()
	curKey := memberStore_.getCurrMember().Key
	memberStore_.lock.RUnlock()

	// iterate thru key list, if key should be transferred, add it to transfer map, send it to predecessor
	for _, key := range localKeyList {
		hashVal := util.Hash([]byte(key))

		var shouldTransfer = false
		if predecessorKey > curKey { // wrap around
			shouldTransfer = hashVal >= predecessorKey || curKey >= hashVal
		} else {
			shouldTransfer = hashVal <= predecessorKey
		}

		if shouldTransfer {
			// get the kv from local kvStore, serialized and ready to send
			serPayload, err := kvstore.GetPutRequest(key)
			if err != nil {
				log.Println("WARN: Could not get kv from local kvStore")
				continue
			}

			// send kv to predecessor using requestreply.SendTransferRequest (not sure this is what it's meant for)
			// uses sendUDPRequest under the hood
			err = requestreply.SendTransferRequest(serPayload, ipStr, portInt)

			/* TODO: a receipt confirmation mechanism + retry policy: for now, assumes first transfer request is received successfully,
			doesn't wait for response to delete from local kvStore
			*/
			wasRemoved := kvstore.RemoveKey(key)
			if wasRemoved == kvstore.NOT_FOUND {
				log.Println("key", key, "was not found in local kvStore")
			}

		}
	}

	log.Println("SENDING TRANSFER FINISHED TO PREDECESSOR WITH ADDRESS: ", ipStr, ":", portStr)

	_ = requestreply.SendTransferFinished([]byte(""), ipStr, portInt)

	memberStore_.lock.Lock()
	memberStore_.transferNodeAddr = nil
	memberStore_.lock.Unlock()
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
		memberStore_.lock.Unlock()
		go transferToPredecessor(ipStr, portStr, targetKey)
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
	// ip, portStr := util.GetIPPort(addr.String())
	// port, err := strconv.Atoi(portStr)
	// if err != nil {
	// 	log.Println("Invalid port")
	// }
	// newKey := util.GetNodeKey(ip, portStr)
	// newMember := &pb.Member{Ip: []byte(ip), Port: int32(port), Key: newKey, Heartbeat: 0, Status: STATUS_NORMAL}
	// TODO (Brennan): remove - this is handled by heartbeat send in membership request handler
	// memberStore_.members = append(memberStore_.members, newMember)
	memberStore_.sortAndUpdateIdx()

	memberStore_.lock.RUnlock()

	// End timer and set status to "Normal"
	// Nodes will now start sending requests directly to us rather than to our successor.
}

// When a member is found to be unavailable, remove it from the member list
func MemberUnavailableHandler(addr *net.Addr) {
	ip := (*addr).(*net.UDPAddr).IP.String()
	port := (*addr).(*net.UDPAddr).Port
	memberStore_.Remove(ip, int32(port))
	log.Println("Finished removing member: ", addr)
}

// passes internal messages to the appropriate handler function
func InternalMsgHandler(addr net.Addr, msg *pb.InternalMsg) (bool, []byte, error) {
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
	case TRANSFER_REQ:
		resPayload, err = transferRequestHandler(addr, msg)
	case PING:
		// Send nil payload back
		log.Println("Got PINGed")
	default:
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
		return false, nil, errors.New("Invalid InternalID Error")
	}
	return respond, resPayload, err
}

// TODO(BRENNAN): what is this for????
func transferRequestHandler(addr net.Addr, msg *pb.InternalMsg) ([]byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, err
	}

	payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount())
	return payload, err
}

/**
Passes internal messages to the appropriate handler function
*/
func ExternalMsgHandler(addr net.Addr, msg *pb.InternalMsg) (net.Addr, net.Addr, []byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, nil, nil, err
	}

	key := util.Hash(kvRequest.GetKey())

	// TODO: Just use standard PUT requests with Internal ID of FORWARDED_CLIENT_REQUEST and no return address for transferring.

	// If this node is bootstrapping and this is FORWARDED_CLIENT_REQ, then it automatically belongs to this node.
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

	// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	// os.Exit(1)

	// First try handling the request here. If the key isn't found, forward the request to bootstrapping predecessor
	if transferRcvNdKey != nil && *transferRcvNdKey == member.Key {
		log.Println("transferRcvNdKey: This index: ", thisMemberPos, ", Successor index: ", pos)
		payload, err, errCode := kvstore.RequestHandler(kvRequest, GetMembershipCount())
		if errCode != kvstore.NOT_FOUND {
			return nil, addr, payload, err
		} else {
			return *transferNdAddr, addr, payload, err
		}

	} else if pos == thisMemberPos { //TODO status bootstrapping?
		log.Println("This index: ", thisMemberPos, ", Successor index: ", pos)
		payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount()) //TODO change membershipcount
		return nil, addr, payload, err
	}

	forwardAddr, err := util.GetAddr(string(successorIP), int(successorPort))
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
