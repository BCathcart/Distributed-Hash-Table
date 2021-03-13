package membership

import (
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
)

type MemberStore struct {
	lock             sync.RWMutex
	members          []*pb.Member
	position         int       // this node's position in the memberStore
	mykey            uint32    // this node's key
	transferNodeAddr *net.Addr // Set to current predecessor being transferred to, nil otherwise
}

/**
* Creates and returns a pointer to a new MemberStore
* @return The member store.
 */
func NewMemberStore() *MemberStore {
	memberStore := new(MemberStore)
	return memberStore
}

/**
* Sorts and updates the
* @return The member store.
 */
/* MUST be holding write lock */
func (ms *MemberStore) sortAndUpdateIdx() {
	// ms.lock.Lock()
	sort.SliceStable(ms.members, func(i, j int) bool {
		return ms.members[i].Key < ms.members[j].Key
	})

	// find and update index of the current key
	for i := range ms.members {
		if ms.members[i].Key == ms.mykey {
			ms.position = i
			// ms.lock.Unlock()
			return
		}
	}

	// ms.lock.Unlock()

	// Should never get here!
	log.Println("Error: could not find own key in member array")
}

/* Memberstore lock must be held */
func (ms *MemberStore) getKeyFromAddr(addr *net.Addr) *uint32 {
	ip := (*addr).(*net.UDPAddr).IP.String()
	port := (*addr).(*net.UDPAddr).Port
	position := ms.findIPPortIndex(ip, int32(port))

	if position != -1 {
		key := ms.members[position].Key
		return &key
	} else {
		return nil
	}
}

/* Finds index in membership list of a key
Currently does not lock/unlock, assuming precondition of MemberStore lock being held */
func (ms *MemberStore) findKeyIndex(key uint32) int {
	// ms.lock.RLock()
	for i := range ms.members {
		if ms.members[i].Key == key {
			// memberStore_.lock.RUnlock()
			return i
		}
	}
	// memberStore_.lock.RUnlock()
	return -1
}

/* Memberstore lock must be held */
func (ms *MemberStore) findIPPortIndex(ip string, port int32) int {
	// ms.lock.RLock()
	for i := range ms.members {
		if string(ms.members[i].GetIp()) == ip &&
			ms.members[i].GetPort() == port {
			// memberStore_.lock.RUnlock()
			return i
		}
	}
	// memberStore_.lock.RUnlock()
	return -1
}

func (ms *MemberStore) isFirstNode() bool {
	// Check if any STATUS_NORMAL nodes come before it
	ms.lock.RLock()
	for i := 0; i < ms.position; i++ {
		if ms.members[i].Status == STATUS_NORMAL {
			ms.lock.RUnlock()
			return false
		}
	}
	ms.lock.RUnlock()
	return true
}

func (ms *MemberStore) getCurrMember() *pb.Member {
	ms.lock.RLock()
	member := ms.members[ms.position]
	ms.lock.RUnlock()
	return member
}

func (ms *MemberStore) getRandMember() *pb.Member {
	//pick a node at random to gossip to
	var member *pb.Member
	ms.lock.RLock()
	membersCopy := make([]*pb.Member, len(ms.members))
	copy(membersCopy, ms.members)

	// Remove all members w/o STATUS_NORMAL
	filterForStatusNormal(membersCopy)

	if len(membersCopy) == 1 {
		log.Println("Warn: only one STATUS_NORMAL node")
		ms.lock.RUnlock()
		return nil
	}

	randi := ms.position
	for randi == ms.position {
		randi = rand.Intn(len(membersCopy))
	}

	member = membersCopy[randi]
	ms.lock.RUnlock() // Need to lock whole time b/c members are pointers

	return member
}

func (ms *MemberStore) getLength() int {
	ms.lock.RLock()
	count := len(ms.members)
	ms.lock.RUnlock()
	return count
}

func (ms *MemberStore) get(pos int) *pb.Member {
	ms.lock.RLock()
	member := ms.members[pos]
	ms.lock.RUnlock()
	return member
}

func (ms *MemberStore) removeidx(s int) {
	if ms.position != s { //TODO don't allow the node to remove itself?
		ms.members = append(ms.members[:s], ms.members[s+1:]...)
		if ms.position > s {
			ms.position--
		}
	}
}

func (ms *MemberStore) Remove(ip string, port int32) {
	ms.lock.Lock()
	idx := ms.findIPPortIndex(ip, int32(port))
	if idx != -1 {
		ms.removeidx(idx)
	}
	ms.lock.Unlock()
}

/* Updates status if member found */
func (ms *MemberStore) setStatus(addr *net.Addr, status int) {
	ip := (*addr).(*net.UDPAddr).IP.String()
	port := (*addr).(*net.UDPAddr).Port

	ms.lock.Lock()
	idx := ms.findIPPortIndex(ip, int32(port))
	ms.members[idx].Status = int32(status)
	ms.lock.Unlock()
}

func filterForStatusNormal(members []*pb.Member) {
	var i int
	for i < len(members) {
		if members[i].Status == STATUS_NORMAL {
			i++
		} else {
			// Remove the element
			members = append(members[:i], members[i+1:]...)
		}
	}
}
