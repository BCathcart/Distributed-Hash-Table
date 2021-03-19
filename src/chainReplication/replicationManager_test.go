package chainReplication

import (
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"net"
	"testing"
)

const MOCK_IP = "86.192.0.170"

type transferCall struct {
	addr    *net.Addr
	lowKey  uint32
	highKey uint32
}

type sweeperCall struct {
	lowKey  uint32
	highKey uint32
}

var transferCalls []transferCall
var sweeperCalls []sweeperCall

func mockTransfer(addr *net.Addr, lowKey uint32, highKey uint32) {
	transferCalls = append(transferCalls, transferCall{addr: addr, lowKey: lowKey, highKey: highKey})
	log.Printf("Called Transfer function with range [%v, %v], addr, %v \n", lowKey, highKey, (*addr).String())
}

func mockSweeper(lowKey uint32, highKey uint32) {
	sweeperCalls = append(sweeperCalls, sweeperCall{lowKey: lowKey, highKey: highKey})
	log.Printf("Called Sweeper function with range [%v, %v]\n", lowKey, highKey)
}

func setupPredecessors() [3]*predecessorNode {
	mockAddr1, _ := util.GetAddr(MOCK_IP, 2)
	mockAddr2, _ := util.GetAddr(MOCK_IP, 3)
	mockAddr3, _ := util.GetAddr(MOCK_IP, 4)
	mykeys = keyRange{low: 90, high: 99}
	predecessors[0] = &predecessorNode{
		addr: mockAddr1,
		keys: keyRange{low: 80, high: 89},
	}
	predecessors[1] = &predecessorNode{
		addr: mockAddr2,
		keys: keyRange{low: 70, high: 79},
	}
	predecessors[2] = &predecessorNode{
		addr: mockAddr3,
		keys: keyRange{low: 60, high: 69},
	}
	var newPredecessors [3]*predecessorNode
	newPredecessors[0] = &predecessorNode{
		addr: mockAddr1,
		keys: keyRange{low: 80, high: 89},
	}
	newPredecessors[1] = &predecessorNode{
		addr: mockAddr2,
		keys: keyRange{low: 70, high: 79},
	}
	newPredecessors[2] = &predecessorNode{
		addr: mockAddr3,
		keys: keyRange{low: 60, high: 69},
	}
	return newPredecessors
}

func TestPredecessorsNoChange(t *testing.T) {
	setupPredecessors()
	newPredecessors := predecessors
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)

}

func TestPredecessorsFirst(t *testing.T) {
	newPredecessors := setupPredecessors()

	mykeys = keyRange{low: 90, high: 99}
	newPredecessors[1].keys.low = predecessors[1].keys.low + 2
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	log.Println(sweeperCalls)
}
