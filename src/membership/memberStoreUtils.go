package membership

import (
	"errors"
	pb "github.com/abcpen431/miniproject/pb/protobuf"
	"math/rand"
)

/*
	Used when a node gets a membership request from a new node, but
	this node is not its successor. It will scan through the list of
	nodes to make it's best guess of where of successor
*/
func searchForSuccessor(targetKey int32) int {
	for i := 0; i < len(memberStore_.members)-1; i++ {
		if targetKey <= memberStore_.members[i].Key {
			return i
		}
	}
	// If targetKey is bigger than everything, it's successor should
	// Wrap around to the first node
	return 0
}

func isFirstNode() bool {
	return memberStore_.position == 0
}

func getCurrMember() *pb.GossipMessage_Member {
	return memberStore_.members[memberStore_.position]
}

func getRandMember() *pb.GossipMessage_Member {
	return memberStore_.members[rand.Intn(len(memberStore_.members))]
}

func getPredecessor() (int32, error) {
	if len(memberStore_.members) == 1 {
		return -1, errors.New("can't find predecessor")
	}
	numMembers := len(memberStore_.members)
	predecessorPosition := (memberStore_.position + numMembers - 1) % numMembers // Handles wrap around
	predecessor := memberStore_.members[predecessorPosition]
	return predecessor.Key, nil
}

func isSuccessor(targetKey int32) (bool, error) {
	if len(memberStore_.members) <= 1 {
		return true, nil
	}

	predVal, err := getPredecessor()

	if err != nil {
		return false, err
	}

	/*
		Need to check for wraparound cases.
		e.g. if maxRingValue = 100, curNode = 10, predecessor = 90
		(which will be the biggest node), keys will get transferred if node is < curNode (0 to 9)
		or bigger than biggest node (91 to 99)
	*/
	if isFirstNode() {
		return targetKey < key_ || targetKey > predVal, nil
	}
	// Non edge case: just needs to be between current node and its predecessor.
	return targetKey < key_ && targetKey > predVal, nil
}
