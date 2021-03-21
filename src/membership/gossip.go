package membership

import (
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"log"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/chainReplication"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/golang/protobuf/proto"
)

/*
* Updates the current node's heartbeat by one.
 */
func tickHeartbeat() {
	memberStore_.lock.Lock()
	memberStore_.members[memberStore_.position].Heartbeat++
	if memberStore_.members[memberStore_.position].Heartbeat%5 == 0 {
		kvstore.PrintKVStoreSize()
	}
	memberStore_.lock.Unlock()
}

// Sends the entire member array in the MemberStore.
func gossipHeartbeat(addr *net.Addr) {

	// Pick random member if an address is not provided
	var ip string
	var port int
	if addr == nil {
		member := memberStore_.getRandMember()
		if member == nil {
			log.Println("No members to send to")
			return
		}
		ip = string(member.GetIp())
		port = int(member.GetPort())
	} else {
		ip = (*addr).(*net.UDPAddr).IP.String()
		port = (*addr).(*net.UDPAddr).Port
	}

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
func HeartbeatHandler(addr net.Addr, msg *pb.InternalMsg) {
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
	// get the last 3 predecessors
	preds := searchForPredecessors(memberStore_.position, 3)
	addresses, keys := getPredAddresses(preds)
	successor, _ := getSuccessorFromPos(memberStore_.position)
	memberStore_.lock.Unlock()

	if successor != nil {
		successorAddr, _ := getMemberAddr(successor)
		successorKey := successor.GetKey()
		chainReplication.UpdateSuccessor(successorAddr, memberStore_.mykey+1, successorKey)
	} else {
		chainReplication.UpdateSuccessor(nil, 0, 0)
	}
	chainReplication.UpdatePredecessors(addresses, keys, memberStore_.mykey)

	log.Println(memberStore_.members)

}
