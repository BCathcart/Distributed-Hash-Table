package membership

import (
	"log"
	"math/rand"
	"sort"
	"sync"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
)

type MemberStore struct {
	lock     sync.RWMutex
	members  []*pb.Member
	position int    // this node's position in the memberStore
	mykey    uint32 // this node's key
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
func (ms *MemberStore) sortAndUpdateIdx() {
	ms.lock.Lock()
	sort.SliceStable(ms.members, func(i, j int) bool {
		return ms.members[i].Key < ms.members[j].Key
	})

	// find and update index of the current key

	for i := range ms.members {
		if ms.members[i].Key == ms.mykey {
			ms.position = i
			ms.lock.Unlock()
			return
		}
	}

	ms.lock.Unlock()

	// Should never get here!
	log.Println("Error: could not find own key in member array")
}

func (ms *MemberStore) findKeyIndex(key uint32) int {
	for i := range ms.members {
		if ms.members[i].Key == key {
			return i
		}
	}
	return -1
}

func (ms *MemberStore) findIPPortIndex(ip string, port int32) int {
	for i := range ms.members {
		if string(ms.members[i].GetIp()) == ip &&
			ms.members[i].GetPort() == port {
			return i
		}
	}
	return -1
}

func (ms *MemberStore) isFirstNode() bool {
	return ms.position == 0
}

func (ms *MemberStore) getCurrMember() *pb.Member {
	return ms.members[memberStore_.position]
}

func (ms *MemberStore) getRandMember() *pb.Member {
	//pick a node at random to gossip to
	randi := memberStore_.position
	for randi == memberStore_.position {
		randi = rand.Intn(len(memberStore_.members))
	}
	return ms.members[randi]
}

func (ms *MemberStore) getLength() int {
	ms.lock.RLock()
	count := len(ms.members)
	ms.lock.Unlock()
	return count
}

func (ms *MemberStore) get(pos int) *pb.Member {
	ms.lock.RLock()
	member := ms.members[pos]
	ms.lock.RUnlock()
	return member
}
