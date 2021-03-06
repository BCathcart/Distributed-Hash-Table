package membership

import (
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"

	pb "github.com/BCathcart/Distributed-Hash-Table/pb/protobuf"
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
	sort.SliceStable(ms.members, func(i, j int) bool {
		return ms.members[i].Key < ms.members[j].Key
	})

	// find and update index of the current key
	for i := range ms.members {
		if ms.members[i].Key == ms.mykey {
			ms.position = i
			return
		}
	}

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
	for i := range ms.members {
		if ms.members[i].Key == key {
			return i
		}
	}
	return -1
}

/* Memberstore lock must be held */
func (ms *MemberStore) findIPPortIndex(ip string, port int32) int {
	// ms.lock.RLock()
	for i := range ms.members {
		if string(ms.members[i].GetIp()) == ip &&
			ms.members[i].GetPort() == port {
			return i
		}
	}
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

/*
 * Returns a random STATUS_NORMAL member from the memberstore
 * @return A member or nil if there is no other valid member
 */
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

func (ms *MemberStore) Get(pos int) *pb.Member {
	ms.lock.RLock()
	member := ms.members[pos]
	ms.lock.RUnlock()
	return member
}

func (ms *MemberStore) GetStatus() int32 {
	ms.lock.RLock()
	status := ms.members[memberStore_.position].Status
	ms.lock.RUnlock()
	return status
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
	defer ms.lock.Unlock()
	idx := ms.findIPPortIndex(ip, int32(port))
	if idx == -1 {
		log.Println((*addr).String(), "was not in member store!")
		return
	}
	if ms.members[idx].Status != int32(status) {
		log.Println("Updating member to UNAVAILABLE: ", *addr)
		ms.members[idx].Status = int32(status)
	}
}

func filterForStatusNormal(members []*pb.Member, mypos int) ([]*pb.Member, int) {
	var i int
	pos := mypos
	for i < len(members) {
		if members[i].Status == STATUS_NORMAL {
			i++
		} else {
			// Remove the element and return our new position in the slice
			members, pos = removeidx(members, i, pos)
		}
	}
	return members, pos
}
