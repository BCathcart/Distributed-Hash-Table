package membership

import (
	"errors"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
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

/**
Legacy function, current function (getPredecessor2) handles bootstrapping nodes.
*/
func getPredecessor() (uint32, error) {

	if len(memberStore_.members) == 1 {
		return 0, errors.New("can't find predecessor")
	}

	numMembers := len(memberStore_.members)
	predecessorPosition := (memberStore_.position + numMembers - 1) % numMembers // Handles wrap around
	predecessor := memberStore_.members[predecessorPosition]
	key := predecessor.Key

	return key, nil
}

/**
Based on a target key, returns the first node before
*/
func getPredecessor2(targetKey uint32) (uint32, error) {
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

/**
Legacy function: returns true/false boolean based on whether the current node
is a successor of a key.

Instead, we call searchForSuccessor function and check the index.
*/
func isSuccessor(targetKey uint32) (bool, error) {
	if memberStore_.getLength() <= 1 {
		return true, nil
	}
	memberStore_.lock.RLock()
	defer memberStore_.lock.RUnlock()
	predVal, err := getPredecessor2(targetKey)

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
