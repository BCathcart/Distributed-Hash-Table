package main

import (
	"errors"
	"hash/crc32"
	"sync"
)

const vNodesPerNode = 10

/*
	Not making this the maximum Uint32 value ensures that there will be no overflow issues
	When iterating through the ring. With a maximum of 50 nodes and 10 vNodes per node the
	likelihood of collisions are still extremely small. Making the number of partitions
	a round number (1 billion) will also assist in implementation / testing.
*/
const numPartitions = 1_000_000_000
const distanceBetweenVNodes = uint32(numPartitions / vNodesPerNode)

var hashingError = errors.New("error hashing node")

type HashingRing struct {
	partitions uint32
	numNodes   uint32
	memberMap  map[uint32]*Member2
	sync.RWMutex
}

/**
* Creates and returns a pointer to a new Hashing Ring.
* @return The hashing ring.
 */
func InitHashingRing() *HashingRing {
	ring := new(HashingRing)
	ring.numNodes = 1
	ring.partitions = numPartitions
	ring.memberMap = make(map[uint32]*Member2)
	return ring
}

/**
Members. Using this for development, will change once membership protocol is created.
*/

// Re-named to Member2 for compiling reasons
type Member2 struct {
	name         string
	checkSum     uint32
	vNodeIndexes []uint32
	kvStore_     *KVStore
}

func NewMember2(memberString string, checkSum uint32) *Member2 {
	member := new(Member2)
	member.name = memberString
	member.checkSum = checkSum

	return member
}

/* Adding nodes to ring */
func AddNode(memberString string, ring *HashingRing) error {
	member := NewMember2(memberString, crc32.ChecksumIEEE([]byte(memberString)))
	return AddVNodesToRing(member, ring)
}

func AddVNodesToRing(member *Member2, ring *HashingRing) error {
	baseNodePos := member.checkSum % numPartitions

	for vNode := 0; vNode < vNodesPerNode; vNode++ {
		// Two modulo operations are used to get the vNode's position on the ring.
		// By adding to the checkSum first it could be done in one operation,
		// however there would be a risk of integer overflow.
		adjustedNodePos := (baseNodePos + distanceBetweenVNodes*uint32(vNode)) % numPartitions
		if _, ok := ring.memberMap[adjustedNodePos]; ok {
			return hashingError
		}
		member.vNodeIndexes = append(member.vNodeIndexes, adjustedNodePos)
	}

	for i := 0; i < len(member.vNodeIndexes); i++ {
		ring.memberMap[member.vNodeIndexes[i]] = member
	}

	return nil
}
