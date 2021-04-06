package chainReplication

import (
	"log"
	"net"
	"sync"

	"time"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

// Only after a transfer is done, start accepting GET requests for those keys

/*
	keyRange struct maintains all of the keys a predecessor/successor node is responsible for.
	The high parameter will be the node's hashed key, and the low parameter
	Will be the previous node's hashed key.
	In the case that there is no predecessor or the node is the third predecessor,
	the low parameter will be set to the current key.
*/

const TRANSFER_TIMEOUT = 10 //sec
const MAX_TRANSFER_REQ_RETRIES = 5

type successorNode struct {
	addr *net.Addr
	keys util.KeyRange
}

type predecessorNode struct {
	addr *net.Addr
	keys util.KeyRange
}

func (p *predecessorNode) getKeys() util.KeyRange {
	if p != nil {
		return p.keys
	}
	return util.KeyRange{}
}

/*** START NEW STUFF ***/
var MyKeys util.KeyRange

// Current range we have keys for
var currentRange util.KeyRange

// Range of keys we are responsible for storing (may not have all the keys yet)
// - if the the key range grows, start requests for the new keys
// - if the key range shrinks, drop all keys outside the range
var responsibleRange util.KeyRange

type expectedTransferInfo struct {
	keys    util.KeyRange
	retries uint8
}

// Expected transfer requests
// We periodically send these to our predecessor until we receive
// a transfer finished
var expectedTransfers []*expectedTransferInfo

var sendingTransfers []util.KeyRange

/*** END NEW STUFF ***/

// type pendingTransferInfo struct {
// 	receiver    *net.Addr
// 	coordinator *net.Addr
// 	keys        util.KeyRange
// 	timer       *time.Timer
// }

// type expectedTransferInfo struct {
// 	coordinator *net.Addr
// 	timer       *time.Timer
// }

// 0 = first, 1 = second, 2 = third (not part of the chain but necessary to get lower bound)
var predecessors [3]*predecessorNode
var successor *successorNode

var MyAddr *net.Addr

// var MyKeys util.KeyRange

// var pendingTransfers []*pendingTransferInfo
// var expectedTransfers []*expectedTransferInfo

var replicaRanges []util.KeyRange // 0 = first predecessor, 1 = second predecessor

var coarseLock sync.RWMutex // TODO: add finer-grained locks for M3

func Init(addr *net.Addr) {
	MyAddr = addr

	// Periodically re-send transfer requests until the receiver is ready
	var ticker = time.NewTicker(time.Millisecond * 2000)
	go func() {
		for {
			<-ticker.C
			// resendPendingTransfers()
			resendExpectedTransfer()
		}
	}()

	reqQueue = make(chan request, 1000)
	go handleRequests(reqQueue)

	// Wait for the first replication event
	go runNextReplicationEvent()
}

func SetKeyRange(keylow uint32, keyhigh uint32) {
	MyKeys.Low = keylow
	MyKeys.High = keyhigh
	currentRange = MyKeys
	responsibleRange = MyKeys

	log.Println("Setting Key Range:")
	log.Println(MyKeys)
	log.Println(responsibleRange)
	log.Println(currentRange)
}

/* Replication Management Helpers */
// func resendPendingTransfers() {
// 	coarseLock.Lock()
// 	defer coarseLock.Unlock()

// 	for _, transfer := range pendingTransfers {
// 		payload := util.SerializeAddr(transfer.coordinator)
// 		log.Println("INFO: SENDING TRANSFER REQUEST FOR", (*transfer.coordinator).String())
// 		requestreply.SendTransferReq(payload, transfer.receiver)
// 	}
// }

func updateCurrentRange(lowKey uint32) {
	currentRange.Low = lowKey // TODO: verify this is always correct (there are never gaps in the key range)
	go runNextReplicationEvent()
}

func resendExpectedTransfer() {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	for _, transfer := range expectedTransfers {
		log.Println("INFO: RE-SENDING TRANSFER REQUEST FOR", transfer.keys)
		if predecessors[0] != nil {
			transfer.retries += 1
			sendDataTransferReq(predecessors[0].addr, transfer.keys, true, transfer.retries >= MAX_TRANSFER_REQ_RETRIES)
		} else {
			log.Println("WARN: No predecessor to send transfer request to")
		}
	}
}

func addExpectedTransfer(keys util.KeyRange) {
	// TODO: check keys already exist (should hopefully not be necessary but is good just in case)
	expectedTransfers = append(expectedTransfers, &expectedTransferInfo{keys, 0})
}

func removeExpectedTransfer(keys util.KeyRange) bool {
	log.Println("removeExpectedtransfer() - 1 ", expectedTransfers)
	log.Println(keys)
	for i, transfer := range expectedTransfers {
		transferKeys := transfer.keys
		if transferKeys.Low == keys.Low && transferKeys.High == keys.High {
			expectedTransfers = removeExpectedTransferFromArr(expectedTransfers, i)
			log.Println("removeExpectedtransfer() -2 ", transferKeys)
			return true
		}
	}
	return false
}

func addSendingTransfer(keys util.KeyRange) {
	sendingTransfers = append(sendingTransfers, keys)
}

func removeSendingTransfer(keys util.KeyRange) bool {
	for i, transfer := range sendingTransfers {
		if transfer.Low == keys.Low && transfer.High == keys.High {
			sendingTransfers = removeKeyRangeFromArr(sendingTransfers, i)
			return true
		}
	}
	return false
}

func delayedRemoveSendingTransfer(keys util.KeyRange, delaySecs time.Duration) {
	timer := time.NewTimer(delaySecs * time.Second)
	go func() {
		<-timer.C
		log.Println("WARN: Delayed removing sending transfer")
		removeSendingTransfer(keys)
	}()
}

func removeKeyRangeFromArr(s []util.KeyRange, i int) []util.KeyRange {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func removeExpectedTransferFromArr(s []*expectedTransferInfo, i int) []*expectedTransferInfo {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func clearSendingTransfers() {
	sendingTransfers = nil // clears the slice
}

func getPredAddrForPrint() []string {
	var addr []string = make([]string, 0)
	for _, p := range predecessors {
		if p != nil {
			addr = append(addr, (*p.addr).String())
		} else {
			addr = append(addr, "<nil>")
		}
	}
	return addr
}

// @return the keyrange for the HEAD of the current chain
func getHead() (*net.Addr, util.KeyRange) {
	head := predecessors[1]
	if head == nil {
		head = predecessors[0]
	}
	if head != nil {
		//DEBUGGING
		// log.Println("the head is", (*head.addr).String())
		return head.addr, head.keys
	}
	return MyAddr, MyKeys
}

func expectingTransferFor(key uint32) bool {
	return util.BetweenKeys(key, responsibleRange.Low, responsibleRange.High) &&
		!util.BetweenKeys(key, currentRange.Low, currentRange.Low)
}

// func expectingTransferFrom(addr *net.Addr) bool {
// 	for _, transfer := range expectedTransfers {
// 		if strings.Compare((*transfer.coordinator).String(), (*addr).String()) == 0 {
// 			return true
// 		}
// 	}

// 	return false
// }

/*
	Helper function for getting predecessor address.
*/
func getPredAddr(predIdx int) *net.Addr {
	if predecessors[predIdx] == nil {
		return nil
	}
	return predecessors[predIdx].addr
}

func getPredKey(predNode *predecessorNode) uint32 {
	if predNode == nil {
		return MyKeys.High
	}
	return predNode.keys.High
}

// func addPendingTransfer(receiver *net.Addr, coordinator *net.Addr, keys util.KeyRange) {
// 	timer := time.NewTimer(time.Duration(TRANSFER_TIMEOUT) * time.Second)
// 	transfer := &pendingTransferInfo{receiver, coordinator, keys, timer}
// 	go func() {
// 		<-timer.C
// 		coarseLock.Lock()
// 		defer coarseLock.Unlock()
// 		if coordinator != nil {
// 			log.Println("WARN: Pending transfer timed out for ", util.CreateAddressStringFromAddr(coordinator))
// 			removePendingTransfer(coordinator)
// 		}
// 	}()
// 	removePendingTransfer(transfer.coordinator)
// 	pendingTransfers = append(pendingTransfers, transfer)
// }

// func addExpectedTransfer(coordinator *net.Addr) {
// 	timer := time.NewTimer(time.Duration(TRANSFER_TIMEOUT) * time.Second)
// 	transfer := &expectedTransferInfo{coordinator, timer}
// 	go func() {
// 		<-timer.C
// 		coarseLock.Lock()
// 		defer coarseLock.Unlock()
// 		if coordinator != nil {
// 			log.Println("WARN: Expected transfer timed out for ", util.CreateAddressStringFromAddr(coordinator))
// 			removeExpectedTransfer(coordinator)
// 		}
// 	}()
// 	removeExpectedTransfer(coordinator)
// 	expectedTransfers = append(expectedTransfers, transfer)
// }

// func removePendingTransfer(coorAddr *net.Addr) {
// 	if coorAddr == nil {
// 		return
// 	}

// 	for i, transfer := range pendingTransfers {
// 		if util.CreateAddressStringFromAddr(transfer.coordinator) == util.CreateAddressStringFromAddr(coorAddr) {
// 			pendingTransfers[i].timer.Stop()
// 			pendingTransfers = removePendingTransferInfoFromArr(pendingTransfers, i)
// 			return
// 		}
// 	}
// }

// func removePendingTransfersToAMember(memAddr *net.Addr) {
// 	for i, transfer := range pendingTransfers {
// 		if util.CreateAddressStringFromAddr(transfer.coordinator) == util.CreateAddressStringFromAddr(memAddr) {
// 			pendingTransfers = removePendingTransferInfoFromArr(pendingTransfers, i)
// 		}
// 	}
// }

// func removeExpectedTransfer(coorAddr *net.Addr) bool {
// 	if coorAddr == nil {
// 		return false
// 	}

// 	for i, transfer := range expectedTransfers {
// 		if util.CreateAddressStringFromAddr(transfer.coordinator) == util.CreateAddressStringFromAddr(coorAddr) {
// 			transfer.timer.Stop()
// 			expectedTransfers = removeExpectedTransferInfoFromArr(expectedTransfers, i)
// 			return true
// 		}
// 	}
// 	return false
// }

// func removePendingTransferInfoFromArr(s []*pendingTransferInfo, i int) []*pendingTransferInfo {
// 	s[len(s)-1], s[i] = s[i], s[len(s)-1]
// 	return s[:len(s)-1]
// }

// func removeExpectedTransferInfoFromArr(s []*expectedTransferInfo, i int) []*expectedTransferInfo {
// 	s[len(s)-1], s[i] = s[i], s[len(s)-1]
// 	return s[:len(s)-1]
// }

// func printState() {
// 	log.Println("Pending transfers to send: ")
// 	for _, transfer := range pendingTransfers {
// 		log.Println((*transfer.coordinator).String())
// 	}
// 	log.Println("Expected transfer to receive: ")
// 	for _, transfer := range expectedTransfers {
// 		log.Println((*transfer.coordinator).String())
// 	}
// 	log.Println("Predecessors:", getPredAddrForPrint())

// 	if successor != nil {
// 		log.Println("Successor: ", (*successor.addr).String())
// 	}
// }
