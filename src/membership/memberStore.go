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

// get a random STATUS_NORMAL member from the memberstore
// return nil if there is no other valid member
func (ms *MemberStore) getRandMember() *pb.Member {
	//pick a node at random to gossip to
	var member *pb.Member
	ms.lock.RLock()
	membersCopy := make([]*pb.Member, len(ms.members))
	copy(membersCopy, ms.members)

	// Remove all members w/o STATUS_NORMAL
	membersCopy, mypos := filterForStatusNormal(membersCopy, ms.position)

	if len(membersCopy) == 0 || (mypos != -1 && len(membersCopy) == 1) {
		//return nil if there are no valid members (other than this node)
		ms.lock.RUnlock()
		return nil
	}
	randi := mypos
	for randi == mypos {
		randi = rand.Intn(len(membersCopy))
	}

	member = membersCopy[randi]
	ms.lock.RUnlock() // Need to lock whole time b/c members are pointers

	return member
}

func (ms *MemberStore) getCountStatusNormal() int {
	count := 0
	for i := 0; i < len(ms.members); i++ {
		if memberStore_.members[i].Status == STATUS_NORMAL {
			count++
		}
	}
	return count
}

func (ms *MemberStore) get(pos int) *pb.Member {
	ms.lock.RLock()
	member := ms.members[pos]
	ms.lock.RUnlock()
	return member
}

func removeidx(members []*pb.Member, posToRemove int, mypos int) ([]*pb.Member, int) {
	retpos := mypos
	members = append(members[:posToRemove], members[posToRemove+1:]...)
	if mypos > posToRemove {
		retpos--
	} else if mypos == posToRemove {
		retpos = -1
	}
	return members, retpos
}

func (ms *MemberStore) Remove(ip string, port int32) {
	ms.lock.Lock()
	idx := ms.findIPPortIndex(ip, int32(port))
	if idx != -1 {
		ms.members, ms.position = removeidx(ms.members, idx, ms.position)
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

func filterForStatusNormal(members []*pb.Member, mypos int) ([]*pb.Member, int) {
	var i int
	pos := mypos
	for i < len(members) {
		if members[i].Status == STATUS_NORMAL {
			i++
		} else {
			log.Println("Removing ")
			// Remove the element and return our new position in the slice
			members, pos = removeidx(members, i, pos)
		}
	}
	return members, pos
}
