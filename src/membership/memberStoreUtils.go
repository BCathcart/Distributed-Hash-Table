package membership

import (
	"errors"
)

/*
	Used when a node gets a membership request from a new node, but
	this node is not its successor. It will scan through the list of
	nodes to make it's best guess of where of successor
*/
func searchForSuccessor(targetKey uint32) int { //TODO no lock?
	for i := 0; i < len(memberStore_.members)-1; i++ {
		if targetKey <= memberStore_.members[i].Key {
			return i
		}
	}
	// If targetKey is bigger than everything, it's successor should
	// Wrap around to the first node
	return 0
}

func getPredecessor() (uint32, error) {
	if len(memberStore_.members) == 1 {
		return 0, errors.New("can't find predecessor")
	}
	numMembers := len(memberStore_.members)
	predecessorPosition := (memberStore_.position + numMembers - 1) % numMembers // Handles wrap around
	predecessor := memberStore_.members[predecessorPosition]
	return predecessor.Key, nil
}

func isSuccessor(targetKey uint32) (bool, error) {
	if memberStore_.getLength() <= 1 {
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
	if memberStore_.isFirstNode() {
		return targetKey < memberStore_.mykey || targetKey > predVal, nil
	}
	// Non edge case: just needs to be between current node and its predecessor.
	return targetKey < memberStore_.mykey && targetKey > predVal, nil
}

func GetMembershipCount() int {
	return memberStore_.getLength()
}

/**
* @param hashed key
* @return true if the key is assigned to the node
 */
func IsMine(key uint32) bool {
	return key <= memberStore_.mykey && (memberStore_.position == 0 || (key <= memberStore_.get(memberStore_.position-1).GetKey()))
}
