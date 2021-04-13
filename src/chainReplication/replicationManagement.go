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
	the low parameter will be set to the current key + 1.
*/

const TRANSFER_TIMEOUT = 20 //sec
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

type expectedTransferInfo struct {
	keys    util.KeyRange
	retries uint8
}

/* Key range this node is the coordinator for */
var MyKeys util.KeyRange

/* Current range we have keys for */
var currentRange util.KeyRange

/* Range of keys we are responsible for storing (may not have all the keys yet)
 * - if the the key range grows, start requests for the new keys
 * - if the key range shrinks, drop all keys outside the range
 */
var responsibleRange util.KeyRange

/* Transfers we believe we need. We periodically send these
 * to our predecessor until we receive a transfer finished msg. */
var expectedTransfers []*expectedTransferInfo

/* Transfers we are currently sending to our successor */
var sendingTransfers []util.KeyRange

// 0 = first, 1 = second, 2 = third (not part of the chain but necessary to get lower bound)
var predecessors [3]*predecessorNode
var successor *successorNode

var MyAddr *net.Addr

var coarseLock sync.RWMutex // TODO: add finer-grained locks for M3

func Init(addr *net.Addr) {
	MyAddr = addr

	// Periodically re-send transfer requests until the receiver is ready
	var ticker = time.NewTicker(time.Millisecond * 2000)
	go func() {
		for {
			<-ticker.C
			resendExpectedTransfer()
		}
	}()

	reqQueue = make(chan request, 1000)
	go handleRequests(reqQueue)

	// Wait for the first replication event
	go runNextReplicationEvent()
}

func SetKeyRange(keylow uint32, keyhigh uint32) {
	coarseLock.Lock()
	MyKeys.Low = keylow
	MyKeys.High = keyhigh
	currentRange = MyKeys
	responsibleRange = MyKeys
	coarseLock.Unlock()

	log.Println("INFO: Setting key range")
	printKeyState(nil)
}

func updateCurrentRange(lowKey uint32, runNextEvent bool) {
	currentRange.Low = lowKey // TODO: verify this is always correct (there are never gaps in the key range)
	if runNextEvent {
		go runNextReplicationEvent()
	}
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
	coarseLock.Lock()
	defer coarseLock.Unlock()
	expectedTransfers = append(expectedTransfers, &expectedTransferInfo{keys, 0})
}

func removeExpectedTransfer(keys util.KeyRange) bool {
	for i, transfer := range expectedTransfers {
		transferKeys := transfer.keys
		if transferKeys.Low == keys.Low && transferKeys.High == keys.High {
			expectedTransfers = removeExpectedTransferFromArr(expectedTransfers, i)
			return true
		}
	}
	return false
}

func addSendingTransfer(keys util.KeyRange) {
	var exists = false
	for _, transfer := range sendingTransfers {
		if transfer.Low == keys.Low && transfer.High == keys.High {
			exists = true
			break
		}
	}
	if exists {
		log.Println("ERROR: Transfer already exists")
	} else {
		sendingTransfers = append(sendingTransfers, keys)
	}
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
		if removeSendingTransfer(keys) {
			log.Println("WARN: Transfer finished message timed out (no ACK received), ", keys)
		}
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
	coarseLock.RLock()
	defer coarseLock.RUnlock()

	head := predecessors[1]
	if head == nil {
		head = predecessors[0]
	}
	if head != nil {
		//DEBUGGING
		return head.addr, head.keys
	}
	return MyAddr, MyKeys
}

func expectingTransferFor(key uint32) bool {
	return util.BetweenKeys(key, responsibleRange.Low, responsibleRange.High) &&
		!util.BetweenKeys(key, currentRange.Low, currentRange.High)
}

func printKeyState(newRange *util.KeyRange) {
	log.Println("\nKeys State:")
	log.Println("MyKeys=", MyKeys)
	log.Println("responsibleRange=", responsibleRange)
	log.Println("currentRange=", currentRange)
	if newRange != nil {
		log.Println("newRange=", *newRange)
	}
	log.Println()
}
