package chainReplication

import (
	"log"
	"net"
	"strings"
	"sync"

	"time"

	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

// TODO(Brennan): should add callback to reqreply layer so that
// transfer PUT msgs are handled here. When a transfer is done,
// start accepting GET requests for those keys

/*
	keyRange struct maintains all of the keys a predecessor/successor node is responsible for.
	The high parameter will be the node's hashed key, and the low parameter
	Will be the previous node's hashed key.
	In the case that there is no predecessor or the node is the third predecessor,
	the low parameter will be set to the current key.
*/

var TRANSFER_TIMEOUT = 10 //sec

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

type pendingTransferInfo struct {
	receiver    *net.Addr
	coordinator *net.Addr
	keys        util.KeyRange
	timer       *time.Timer
}

type expectedTransferInfo struct {
	coordinator *net.Addr
	timer       *time.Timer
}

// 0 = first, 1 = second, 2 = third (not part of the chain but necessary to get lower bound)
var predecessors [3]*predecessorNode
var successor *successorNode

var MyAddr *net.Addr
var MyKeys util.KeyRange

// TODO: consider adding a lock to cover these
var pendingTransfers []*pendingTransferInfo
var expectedTransfers []*expectedTransferInfo

// TODO: add finer-grained locks for M3
var coarseLock sync.RWMutex

func Init(addr *net.Addr, keylow uint32, keyhigh uint32) {
	MyKeys.Low = keylow
	MyKeys.High = keyhigh
	MyAddr = addr

	// Periodically re-send transfer requests until the receiver is ready
	var ticker = time.NewTicker(time.Millisecond * 1000)
	go func() {
		for {
			<-ticker.C
			resendPendingTransfers()
		}
	}()

	reqQueue = make(chan request, 1000)
	go handleRequests(reqQueue)
}

/* HELPERS */
func resendPendingTransfers() {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	for _, transfer := range pendingTransfers {
		payload := util.SerializeAddr(transfer.coordinator)
		log.Println("\nSENDING TRANSFER REQUEST FOR", (*transfer.coordinator).String())
		requestreply.SendTransferReq(payload, transfer.receiver)
	}
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

func expectingTransferFrom(addr *net.Addr) bool {
	for _, transfer := range expectedTransfers {
		if strings.Compare((*transfer.coordinator).String(), (*addr).String()) == 0 {
			return true
		}
	}

	return false
}

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

/* Helpers */
func addPendingTransfer(receiver *net.Addr, coordinator *net.Addr, keys util.KeyRange) {
	timer := time.NewTimer(time.Duration(TRANSFER_TIMEOUT) * time.Second)
	transfer := &pendingTransferInfo{receiver, coordinator, keys, timer}
	go func() {
		<-timer.C
		coarseLock.Lock()
		defer coarseLock.Unlock()
		if coordinator != nil {
			log.Println("WARN: Pending transfer timed out for ", util.CreateAddressStringFromAddr(coordinator))
			removePendingTransfer(coordinator)
		}
	}()
	removePendingTransfer(transfer.coordinator)
	pendingTransfers = append(pendingTransfers, transfer)
}

func addExpectedTransfer(coordinator *net.Addr) {
	timer := time.NewTimer(time.Duration(TRANSFER_TIMEOUT) * time.Second)
	transfer := &expectedTransferInfo{coordinator, timer}
	go func() {
		<-timer.C
		coarseLock.Lock()
		defer coarseLock.Unlock()
		if coordinator != nil {
			log.Println("WARN: Expected transfer timed out for ", util.CreateAddressStringFromAddr(coordinator))
			removeExpectedTransfer(coordinator)
		}
	}()
	removeExpectedTransfer(coordinator)
	expectedTransfers = append(expectedTransfers, transfer)
}

func removePendingTransfer(coorAddr *net.Addr) {
	if coorAddr == nil {
		return
	}

	for i, transfer := range pendingTransfers {
		if util.CreateAddressStringFromAddr(transfer.coordinator) == util.CreateAddressStringFromAddr(coorAddr) {
			pendingTransfers[i].timer.Stop()
			pendingTransfers = removePendingTransferInfoFromArr(pendingTransfers, i)
			return
		}
	}
}

func removePendingTransfersToAMember(memAddr *net.Addr) {
	for i := 0; i < len(pendingTransfers); i++ {
		if util.CreateAddressStringFromAddr(pendingTransfers[i].coordinator) == util.CreateAddressStringFromAddr(memAddr) {
			pendingTransfers = removePendingTransferInfoFromArr(pendingTransfers, i)
		} else {
			i++
		}
	}
}

func removeExpectedTransfer(coorAddr *net.Addr) bool {
	if coorAddr == nil {
		return false
	}

	for i, transfer := range expectedTransfers {
		if util.CreateAddressStringFromAddr(transfer.coordinator) == util.CreateAddressStringFromAddr(coorAddr) {
			transfer.timer.Stop()
			expectedTransfers = removeExpectedTransferInfoFromArr(expectedTransfers, i)
			return true
		}
	}
	return false
}

func removePendingTransferInfoFromArr(s []*pendingTransferInfo, i int) []*pendingTransferInfo {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func removeExpectedTransferInfoFromArr(s []*expectedTransferInfo, i int) []*expectedTransferInfo {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}
