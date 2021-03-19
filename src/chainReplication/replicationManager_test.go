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

func mostRecentTransfer() transferCall {
	if len(transferCalls) == 0 {
		return transferCall{}
	}
	return transferCalls[len(transferCalls)-1]
}
func mostRecentSweeper() sweeperCall {
	if len(sweeperCalls) == 0 {
		return sweeperCall{}
	}
	return sweeperCalls[len(sweeperCalls)-1]
}

func newPredecessor(addr *net.Addr, low uint32, high uint32) *predecessorNode {
	return &predecessorNode{
		addr: addr,
		keys: keyRange{low: low, high: high},
	}

}

func setupPredecessors() [3]*predecessorNode {
	mockAddr1, _ := util.GetAddr(MOCK_IP, 2)
	mockAddr2, _ := util.GetAddr(MOCK_IP, 3)
	mockAddr3, _ := util.GetAddr(MOCK_IP, 4)
	mykeys = keyRange{low: 90, high: 99}
	predecessors[0] = newPredecessor(mockAddr1, 80, 89)
	predecessors[1] = newPredecessor(mockAddr2, 70, 79)
	predecessors[2] = newPredecessor(mockAddr3, 60, 69)
	var newPredecessors [3]*predecessorNode
	newPredecessors[0] = newPredecessor(mockAddr1, 80, 89)
	newPredecessors[1] = newPredecessor(mockAddr2, 70, 79)
	newPredecessors[2] = newPredecessor(mockAddr3, 60, 69)
	return newPredecessors
}

func TestComparePredecessorBothNil(t *testing.T) {
	result := comparePredecessors(nil, nil)
	expected := true
	if result != expected {
		t.Errorf("Comparing nil/nil pred failed, expected %v but got %v", expected, result)
	}
}

func TestComparePredecessorOneNil(t *testing.T) {
	result := comparePredecessors(&predecessorNode{keys: keyRange{low: 1, high: 2}}, nil)
	expected := false
	if result != expected {
		t.Errorf("Comparing one nil pred failed, expected %v but got %v", expected, result)
	}
}

// Different low values but same high for keyrange, should still return true
func TestComparePredecessorDiffLows(t *testing.T) {
	result := comparePredecessors(&predecessorNode{keys: keyRange{low: 1, high: 2}}, &predecessorNode{keys: keyRange{low: 0, high: 2}})
	expected := true
	if result != expected {
		t.Errorf("Comparing one nil pred failed, expected %v but got %v", expected, result)
	}
}

// Different high values for keyrange, should return false
func TestComparePredecessorDiffHighs(t *testing.T) {
	result := comparePredecessors(&predecessorNode{keys: keyRange{low: 1, high: 3}}, &predecessorNode{keys: keyRange{low: 1, high: 2}})
	expected := false
	if result != expected {
		t.Errorf("Comparing diff highs failed, expected %v but got %v", expected, result)
	}
}

func TestPredecessorsNoChange(t *testing.T) {
	setupPredecessors()
	newPredecessors := predecessors
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
}

func TestPredecessorsThirdNodeJoined(t *testing.T) {
	newPredecessors := setupPredecessors()
	newPredecessors[1].keys.low = predecessors[1].keys.low + 2
	newPredecessors[2].keys.high = newPredecessors[1].keys.low
	newPredecessors[2].keys.low = predecessors[2].keys.high
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		highKey: newPredecessors[2].keys.high,
		lowKey:  newPredecessors[2].keys.low,
	}
	result := mostRecentSweeper()
	if expected != result {
		t.Errorf("Adding new node failed, expected to call sweeper with %v but got %v", expected, result)
	}
}
