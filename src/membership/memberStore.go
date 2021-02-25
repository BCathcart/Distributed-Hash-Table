package membership

import (
	"log"
	"sort"
	"sync"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
)

type MemberStore struct {
	lock     sync.RWMutex
	members  []*pb.GossipMessage_Member
	position int
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
		if ms.members[i].Key == key_ {
			ms.position = i
			ms.lock.Unlock()
			return
		}
	}

	ms.lock.Unlock()

	// Should never get here!
	log.Println("Error: could not find own key in member array")
}

func (ms *MemberStore) findKeyIndex(key int32) int {
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
