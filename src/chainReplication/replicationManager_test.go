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

// Spies to keep track of function calls during testing
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
	mockSuccAddr, _ := util.GetAddr(MOCK_IP, 5)
	mykeys = keyRange{low: 90, high: 99}
	successor = &successorNode{keys: keyRange{low: 100, high: 109}, addr: mockSuccAddr}
	predecessors[0] = newPredecessor(mockAddr1, 80, 89)
	predecessors[1] = newPredecessor(mockAddr2, 70, 79)
	predecessors[2] = newPredecessor(mockAddr3, 60, 69)
	var newPredecessors [3]*predecessorNode
	newPredecessors[0] = shallowCopy(predecessors[0])
	newPredecessors[1] = shallowCopy(predecessors[1])
	newPredecessors[2] = shallowCopy(predecessors[2])
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

func TestPredecessorsThirdJoined(t *testing.T) {
	newPredecessors := setupPredecessors()
	newPred3KEy := predecessors[1].keys.low + 2
	newPredecessors[1].keys.low = newPred3KEy
	newPredecessors[2].keys.high = newPred3KEy
	newPredecessors[2].keys.low = predecessors[2].keys.high
	beforeLen := len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		highKey: newPredecessors[2].keys.high,
		lowKey:  newPredecessors[2].keys.low,
	}
	if len(sweeperCalls) != beforeLen+1 {
		t.Errorf("Error: did not sweep cache")
	}
	result := mostRecentSweeper()
	if expected != result {
		t.Errorf("Adding new node failed, expected to call sweeper with %v but got %v", expected, result)
	}
}

func TestPredecessorsThirdFailed(t *testing.T) {
	newPredecessors := setupPredecessors()
	newPred3KEy := predecessors[1].keys.low - 2
	newPredecessors[1].keys.low = newPred3KEy
	newPredecessors[2].keys.high = newPred3KEy
	newPredecessors[2].keys.low = predecessors[2].keys.high
	preFunctionLen := len(expectedTransfers)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := preFunctionLen + 1
	result := len(expectedTransfers)
	if expected != result {
		t.Errorf("Expected pendingRcvTransfers to be %v long but got %v", expected, result)
	}
}

// New predecessor joined at index 1 (the second node)
func TestPredecessorsSecondJoined(t *testing.T) {
	newPredecessors := setupPredecessors()
	newPred2KEy := predecessors[1].keys.high + 2

	newPredecessors[1].keys.high = newPred2KEy
	newPredecessors[1].keys.low = predecessors[1].keys.high
	newPredecessors[0].keys.low = newPred2KEy
	newPredecessors[2] = shallowCopy(predecessors[1])

	beforeLen := len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		highKey: predecessors[1].keys.high,
		lowKey:  predecessors[1].keys.low,
	}
	if len(sweeperCalls) != beforeLen+1 {
		t.Errorf("Error: did not sweep cache")
	}
	result := mostRecentSweeper()
	if expected != result {
		t.Errorf("Adding new node at 2nd pos failed, expected to call sweeper with %v but got %v", expected, result)
	}

	// Edge case: second node joining in the chain, should not be calling sweeper
	predecessors[1] = nil
	predecessors[2] = nil
	newPredecessors[2] = nil
	beforeLen = len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	if len(sweeperCalls) != beforeLen {
		t.Errorf("Error: Should not be not sweeping cache as there are not enough nodes in the chain")
	}
}

func TestPredecessorsSecondFailed(t *testing.T) {
	newPredecessors := setupPredecessors()

	newPredecessors[0].keys.low = predecessors[1].keys.low
	newPredecessors[1] = shallowCopy(predecessors[2])
	newPredecessors[2].keys.high = predecessors[2].keys.low
	newPredecessors[2].keys.low = predecessors[2].keys.low - 2

	beforeLen := len(expectedTransfers)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	if len(expectedTransfers) != beforeLen+1 {
		t.Errorf("Error: did not append to receiving transfers")
	}

	expected := transferCall{
		highKey: predecessors[1].keys.high,
		lowKey:  predecessors[1].keys.low,
		addr:    successor.addr,
	}

	result := mostRecentTransfer()
	if expected != result {
		t.Errorf("Removing 2nd node failed, expected to call transfer with %v but got %v", expected, result)
	}
}

// New predecessor joined at index 0 (the first node)
func TestPredecessorsFirstJoined(t *testing.T) {
	newPredecessors := setupPredecessors()
	newPred1KEy := predecessors[0].keys.high + 2

	newPredecessors[0].keys.high = newPred1KEy
	newPredecessors[0].keys.low = predecessors[0].keys.high

	// Rest of predecessors will be shifted by one
	newPredecessors[1] = shallowCopy(predecessors[0])
	newPredecessors[2] = shallowCopy(predecessors[1])

	beforeLen := len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		highKey: predecessors[1].keys.high,
		lowKey:  predecessors[1].keys.low,
	}
	if len(sweeperCalls) != beforeLen+1 {
		t.Errorf("Error: did not sweep cache")
	}
	result := mostRecentSweeper()
	if expected != result {
		t.Errorf("Adding new node at 2nd pos failed, expected to call sweeper with %v but got %v", expected, result)
	}
	// Edge case: very first node joining in the chain, should not be calling sweeper
	predecessors[0] = nil
	predecessors[1] = nil
	predecessors[2] = nil
	newPredecessors[1] = nil
	newPredecessors[2] = nil
	beforeLen = len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	if len(sweeperCalls) != beforeLen {
		t.Errorf("Error: Should not be not sweeping cache as there are not enough nodes in the chain")
	}
}

func TestPredecessorsFirstFailed(t *testing.T) {
	newPredecessors := setupPredecessors()

	newPredecessors[0] = shallowCopy(predecessors[1])
	newPredecessors[1] = shallowCopy(predecessors[2])
	newPredecessors[2].keys.high = predecessors[2].keys.low
	newPredecessors[2].keys.low = predecessors[2].keys.low - 2

	beforeLen := len(expectedTransfers)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	if len(expectedTransfers) != beforeLen+1 {
		t.Errorf("Error: did not append to receiving transfers")
	}

	expected := transferCall{
		highKey: predecessors[1].keys.high,
		lowKey:  predecessors[1].keys.low,
		addr:    successor.addr,
	}

	result := mostRecentTransfer()
	if expected != result {
		t.Errorf("Removing 1st node failed, expected to call transfer with %v but got %v", expected, result)
	}

}
