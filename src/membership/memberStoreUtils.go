package membership

import (
	"errors"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/*
	Used when a node gets a membership request from a new node, but
	this node is not its successor. It will scan through the list of
	nodes to make it's best guess of where of successor
*/
// TODO: pass in key of a node that is being transferred to exclude it from the filtering
func searchForSuccessor(targetKey uint32, exceptionKey *uint32) (*pb.Member, int) {
	for i := 0; i < len(memberStore_.members); i++ {
		// skip over "Bootstrapping" nodes
		if targetKey <= memberStore_.members[i].Key && (memberStore_.members[i].Status == STATUS_NORMAL || (exceptionKey != nil && memberStore_.members[i].Key == *exceptionKey)) {
			member := memberStore_.members[i]
			return member, i
		}
	}

	// Search for first node with status normal (wrap around)
	for i := 0; i < len(memberStore_.members); i++ {
		// Skip over "Bootstrapping" nodes
		if memberStore_.members[i].Status == STATUS_NORMAL {
			member := memberStore_.members[i]
			return member, i
		}
	}

	// Otherwise, there are no other nodes with status normal
	member := memberStore_.members[memberStore_.position]

	return member, 0
}

/*
* Search for the tail of the chain with the head at the given position
* If the number of STATUS_NORMAL members is less than the default length
* of the chain (3), searchForTail returns the sucessor (if membership count is 2)
* or the head itself (if membership count is 1)
* Assume the head is STATUS_NORMAL
 */
func searchForTail(head int) (*pb.Member, int) {
	chain := make([]int, 3)
	chain[0] = head
	count := 1
	for i := head + 1; i != head; i++ {
		if i == len(memberStore_.members) {
			i = 0
			if i == head {
				break
			}
		}
		member := memberStore_.members[i]
		if member.Status == STATUS_NORMAL {
			chain[count] = i
			count++
			if count == 3 {
				return member, i
			}
		}
	}
	// there were not enough STATUS_NORMAL members in the ring
	idx := chain[count-1]

	return memberStore_.members[idx], idx
}

func searchForPredecessors(pos int, numPredecessors int) []*pb.Member {
	members := make([]*pb.Member, numPredecessors)
	count := 0
	for i := pos - 1; i != pos; i-- {
		if i < 0 {
			i = len(memberStore_.members) - 1
			if i == pos {
				break
			}
		}
		member := memberStore_.members[i]
		if member.Status == STATUS_NORMAL {
			members[count] = member
			count++
			if count == numPredecessors {
				return members
			}
		}
	}

	return members
}

/**
* Based on a target node position in the memberstore, returns the first STATUS_NORMAL node before
 */
func getPredecessorFromPos(pos int) (*pb.Member, int) {
	for i := pos - 1; i != pos; i-- {
		if i < 0 {
			i = len(memberStore_.members) - 1
			if i == pos {
				break
			}
		}
		member := memberStore_.members[i]
		if member.Status == STATUS_NORMAL {
			return member, i
		}
	}
	// no predecessor exists
	return nil, 0
}

/**
* Based on a target node position in the memberstore, returns the first STATUS_NORMAL node after
 */
func getSuccessorFromPos(pos int) (*pb.Member, int) {
	for i := pos + 1; i != pos; i++ {
		if i == len(memberStore_.members) {
			i = 0
			if i == pos {
				break
			}
		}
		member := memberStore_.members[i]
		if member.Status == STATUS_NORMAL {
			return member, i
		}
	}
	// no successor exists
	return nil, 0
}

func getPredAddresses(members []*pb.Member) ([]*net.Addr, []uint32) {
	addresses := make([]*net.Addr, len(members))
	keys := make([]uint32, len(members))

	for i := 0; i < len(members); i++ {
		if members[i] != nil {
			addresses[i], _ = getMemberAddr(members[i])
			keys[i] = members[i].GetKey()
		}
	}
	return addresses, keys
}

/**
Based on a target key, returns the first node before
*/
func getPredecessor(targetKey uint32) (uint32, error) {
	for i := len(memberStore_.members) - 1; i >= 0; i-- {
		// skip over "Bootstrapping" nodes
		if targetKey < memberStore_.members[i].Key && memberStore_.members[i].Status == STATUS_NORMAL {
			return memberStore_.members[i].Key, nil
		}
	}

	// Search for last node with status normal (wrap around)
	for i := len(memberStore_.members) - 1; i >= 0; i-- {
		// Skip over "Bootstrapping" nodes
		if memberStore_.members[i].Status == STATUS_NORMAL {
			return memberStore_.members[i].Key, nil
		}
	}

	return 0, errors.New("can't find predecessor")
}

/* GetMembershipCount returns the number of members in  the membership list with STATUS_NORMAL */
func GetMembershipCount() int {
	memberStore_.lock.RLock()
	defer memberStore_.lock.RUnlock()

	return memberStore_.getCountStatusNormal()
}

func getMemberAddr(member *pb.Member) (*net.Addr, error) {
	return util.GetAddr(string(member.GetIp()), int(member.GetPort()))
}

func IsBootstrapping() bool {
	return memberStore_.GetStatus() == STATUS_BOOTSTRAPPING
}

func GetTransferNodeAddr() *net.Addr {
	memberStore_.lock.RLock()
	transferNodeAddr := memberStore_.transferNodeAddr
	memberStore_.lock.RUnlock()
	return transferNodeAddr
}

func GetPosFromAddr(addr *net.Addr) int {
	for i, member := range memberStore_.members {
		if util.CreateAddressString(string(member.Ip), int(member.Port)) == util.CreateAddressStringFromAddr(addr) {
			return i
		}
	}
	return -1
}
