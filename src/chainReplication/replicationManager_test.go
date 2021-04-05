package chainReplication

import (
	"log"
	"net"
	"testing"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/**
This test suite covers different scenarios for changes to predecessor nodes that the updatePredecessor
function might encounter.
*/
const MOCK_IP = "86.192.0.170"

type transferCall struct {
	addr      *net.Addr
	coordAddr *net.Addr
	keys      util.KeyRange
}

type sweeperCall struct {
	keys util.KeyRange
}

// Spies to keep track of function calls during testing
var transferCalls []transferCall
var sweeperCalls []sweeperCall

func newKR(low uint32, high uint32) util.KeyRange {
	return util.KeyRange{Low: low, High: high}
}

func mockTransfer(addr *net.Addr, coordAddr *net.Addr, keys util.KeyRange) {
	transferCalls = append(transferCalls, transferCall{addr: addr, coordAddr: coordAddr, keys: keys})
	log.Printf("Called Transfer function with range %v, addr, %v\n", keys, (*addr).String())
}

func mockSweeper(keyRange util.KeyRange) {
	sweeperCalls = append(sweeperCalls, sweeperCall{keyRange})
	log.Printf("Called Sweeper function with range [%v, %v]\n", keyRange.Low, keyRange.High)
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
		keys: util.KeyRange{Low: low, High: high},
	}

}

func setupPredecessors() [3]*predecessorNode {
	mockAddr1, _ := util.GetAddr(MOCK_IP, 2)
	mockAddr2, _ := util.GetAddr(MOCK_IP, 3)
	mockAddr3, _ := util.GetAddr(MOCK_IP, 4)
	mockSuccAddr, _ := util.GetAddr(MOCK_IP, 5)
	MyKeys = util.KeyRange{Low: 90, High: 99}
	successor = &successorNode{keys: util.KeyRange{Low: 100, High: 109}, addr: mockSuccAddr}
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
	result := comparePredecessors(&predecessorNode{keys: util.KeyRange{Low: 1, High: 2}}, nil)
	expected := false
	if result != expected {
		t.Errorf("Comparing one nil pred failed, expected %v but got %v", expected, result)
	}
}

// Different low values but same high for util.KeyRange, should still return true
func TestComparePredecessorDiffLows(t *testing.T) {
	result := comparePredecessors(&predecessorNode{keys: util.KeyRange{Low: 1, High: 2}}, &predecessorNode{keys: util.KeyRange{Low: 0, High: 2}})
	expected := true
	if result != expected {
		t.Errorf("Comparing one nil pred failed, expected %v but got %v", expected, result)
	}
}

// Different high values for util.KeyRange, should return false
func TestComparePredecessorDiffHighs(t *testing.T) {
	result := comparePredecessors(&predecessorNode{keys: util.KeyRange{Low: 1, High: 3}}, &predecessorNode{keys: util.KeyRange{Low: 1, High: 2}})
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
	newPred3KEy := predecessors[1].keys.Low + 2
	newPredecessors[1].keys.Low = newPred3KEy
	newPredecessors[2].keys.High = newPred3KEy
	newPredecessors[2].keys.Low = predecessors[2].keys.High
	beforeLen := len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		keys: util.KeyRange{High: newPredecessors[2].keys.High, Low: newPredecessors[2].keys.Low},
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
	newPred3KEy := predecessors[1].keys.Low - 2
	newPredecessors[1].keys.Low = newPred3KEy
	newPredecessors[2].keys.High = newPred3KEy
	newPredecessors[2].keys.Low = predecessors[2].keys.High
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
	newPred2KEy := predecessors[1].keys.High + 2

	newPredecessors[1].keys.High = newPred2KEy
	newPredecessors[1].keys.Low = predecessors[1].keys.High
	newPredecessors[0].keys.Low = newPred2KEy
	newPredecessors[2] = shallowCopy(predecessors[1])

	beforeLen := len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		keys: util.KeyRange{High: predecessors[1].keys.High, Low: predecessors[1].keys.Low},
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

// TODO: Update tests to match new code
func TestPredecessorsSecondFailed(t *testing.T) {
	newPredecessors := setupPredecessors()

	newPredecessors[0].keys.Low = predecessors[1].keys.Low
	newPredecessors[1] = shallowCopy(predecessors[2])
	newPredecessors[2].keys.High = predecessors[2].keys.Low
	newPredecessors[2].keys.Low = predecessors[2].keys.Low - 2

	beforeLen := len(expectedTransfers)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	if len(expectedTransfers) != beforeLen+1 {
		t.Errorf("Error: did not append to receiving transfers")
	}

	expected := transferCall{
		keys:      newKR(predecessors[1].keys.Low, predecessors[1].keys.High),
		addr:      successor.addr,
		coordAddr: newPredecessors[0].addr,
	}

	result := mostRecentTransfer()
	if expected != result {
		t.Errorf("Removing 2nd node failed, expected to call transfer with %v but got %v", expected, result)
	}
}

// New predecessor joined at index 0 (the first node)
func TestPredecessorsFirstJoined(t *testing.T) {
	newPredecessors := setupPredecessors()
	newPred1KEy := predecessors[0].keys.High + 2

	newPredecessors[0].keys.High = newPred1KEy
	newPredecessors[0].keys.Low = predecessors[0].keys.High

	// Rest of predecessors will be shifted by one
	newPredecessors[1] = shallowCopy(predecessors[0])
	newPredecessors[2] = shallowCopy(predecessors[1])

	beforeLen := len(sweeperCalls)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	expected := sweeperCall{
		keys: util.KeyRange{High: predecessors[1].keys.High, Low: predecessors[1].keys.Low},
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

// TODO: Update tests to match new code
func TestPredecessorsFirstFailed(t *testing.T) {
	log.Printf("Before running tests %v", expectedTransfers)
	newPredecessors := setupPredecessors()
	newPredecessors[0] = shallowCopy(predecessors[1])
	newPredecessors[1] = shallowCopy(predecessors[2])
	newPredecessors[2].keys.High = predecessors[2].keys.Low
	newPredecessors[2].keys.Low = predecessors[2].keys.Low - 2

	beforeLen := len(expectedTransfers)
	checkPredecessors(newPredecessors, mockTransfer, mockSweeper)
	log.Println("EXPECTED TRANSFERS")
	log.Println(expectedTransfers)
	if len(expectedTransfers) != beforeLen+1 {
		t.Errorf("Error: incorrect expected transfer len expected %v got %v", beforeLen+1, len(expectedTransfers))
	}

	expected := transferCall{
		keys:      newKR(predecessors[1].keys.Low, predecessors[1].keys.High),
		addr:      successor.addr,
		coordAddr: newPredecessors[0].addr,
	}

	result := mostRecentTransfer()
	if expected != result {
		t.Errorf("Removing 1st node failed, expected to call transfer with %v but got %v", expected, result)
	}

}
