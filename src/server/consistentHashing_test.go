package main

import (
	"testing"
)

func valInArray(val uint32, arr []uint32) bool {

	for i := 0; i < len(arr); i++ {
		if arr[i] == val {
			return true
		}

	}
	return false
}

func TestAddNode(t *testing.T) {
	hRing := InitHashingRing()
	var testChecksum = uint32(111)
	member := NewMember("abc", testChecksum)
	err := AddVNodesToRing(member, hRing)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < vNodesPerNode; i++ {
		expectedIndex := uint32(i)*distanceBetweenVNodes + testChecksum
		if _, ok := hRing.memberMap[expectedIndex]; !ok {
			t.Errorf("Expected %d in hashing Ring but not found", expectedIndex)
		}
		if !valInArray(expectedIndex, member.vNodeIndexes) {
			t.Errorf("Expected %d in member indexes but not found", expectedIndex)
		}
	}
}
