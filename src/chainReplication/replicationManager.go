package chainReplication

import (
	"errors"
	"log"
	"net"

	"time"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
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
type keyRange struct {
	low  uint32
	high uint32
}

type successorNode struct {
	addr *net.Addr
	keys keyRange
}

type predecessorNode struct {
	addr        *net.Addr
	keys        keyRange
	transferred bool
}

type transferInfo struct {
	receiver    *net.Addr
	coordinator *net.Addr
	keys        keyRange
}

type transferFunc func(destAddr *net.Addr, coordAddr *net.Addr, keys keyRange)
type sweeperFunc func(lowKey uint32, highKey uint32)

func shallowCopy(orig *predecessorNode) *predecessorNode {
	if orig == nil {
		return nil
	}
	return &predecessorNode{
		keys: orig.keys,
		addr: orig.addr,
	}
}

// Replace with actual transfer / sweeper functions when merging with shay & brennan code
func dummyTransfer(addr *net.Addr, lowKey uint32, highKey uint32) {
	log.Printf("Called Transfer function with range [%v, %v], addr, %v \n", lowKey, highKey, (*addr).String())
}

func dummySweeper(lowKey uint32, highKey uint32) {
	log.Printf("Called Sweeper function with range [%v, %v]\n", lowKey, highKey)
}

// 0 = first, 1 = second, 2 = third (not part of the chain but necessary to get lower bound)
var predecessors [3]*predecessorNode
var successor *successorNode

var myAddr *net.Addr
var mykeys keyRange

// TODO: consider adding a lock to cover these
var pendingTransfers []*transferInfo
var expectedTransfers []*net.Addr

// TODO: remove pending Transfers as needed when the successor updates

func Init(addr *net.Addr, keylow uint32, keyhigh uint32) {
	mykeys.low = keylow
	mykeys.high = keyhigh
	myAddr = addr

	// Periodically re-send transfer requests until the receiver is ready
	var ticker = time.NewTicker(time.Millisecond * 1000)
	go func() {
		for {
			<-ticker.C
			resendPendingTransfers()
		}
	}()
}

func (k keyRange) includesKey(key uint32) bool {
	if k.low < k.high {
		return key <= k.high && key >= k.low
	}
	return key >= k.low || key <= k.high
}

// @return the keyrange for the HEAD of the current chain
func getHeadKeys() keyRange {
	head := predecessors[1]
	if head == nil {
		head = predecessors[0]
	}
	headkeys := mykeys
	if head != nil {
		//DEBUGGING
		log.Println("the head is", (*head.addr).String())
		headkeys = head.keys
	}
	return headkeys
}

func resendPendingTransfers() {
	for _, transfer := range pendingTransfers {
		payload := util.SerializeAddr(transfer.coordinator)
		log.Println("\nSENDING TRANSFER REQUEST FOR", (*transfer.coordinator).String())
		requestreply.SendTransferReq(payload, transfer.receiver)
	}
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
		return mykeys.high
	}
	return predNode.keys.high
}

// TODO: may need to update both predecessors at once
func UpdatePredecessors(addr []*net.Addr, keys []uint32, key uint32) {
	mykeys.high = key
	var newPredecessors [3]*predecessorNode
	for i := 0; i < 3; i++ {
		if addr[i] != nil {
			newPredecessors[i] = &predecessorNode{}
			newPredecessors[i].addr = addr[i]
			newPredecessors[i].keys.high = keys[i]
			if i < 2 && addr[i+1] != nil {
				newPredecessors[i].keys.low = keys[i+1] + 1
			} else {
				newPredecessors[i].keys.low = key + 1
			}
		} else {
			newPredecessors[i] = nil
			break
		}
	}
	// checkAddresses(addr, keys)
	checkPredecessors(newPredecessors, sendDataTransferReq, dummySweeper) // TODO Replace with brennan /shay functions
	copyPredecessors(newPredecessors)                                     // TODO: Not sure if I can do this, seems a bit hacky
	if newPredecessors[0] != nil {
		mykeys.low = newPredecessors[0].keys.high + 1
	}

	// for i := 0; i < 2; i++ {
	// 	if predecessors[i] != nil {
	// 		log.Println((*predecessors[i].addr).String(), predecessors[i].keys.low, predecessors[i].keys.high)
}

func copyPredecessors(newPredecessors [3]*predecessorNode) {
	for i := 0; i < len(newPredecessors); i++ {
		predecessors[i] = shallowCopy(newPredecessors[i])
	}
}

func checkAddresses(addr []*net.Addr, keys []uint32) {
	var addrString = ""
	for i := 0; i < len(addr); i++ {
		if addr[i] == nil {
			addrString = "NIL"
		} else {
			addrString = (*addr[i]).String()
		}
		if predecessors[i] == nil {
			log.Printf("OLD: NIL, NEW: %v\n", addrString)
		} else {
			log.Printf("OLD: %v, NEW: %v\n", (*getPredAddr(i)).String(), addrString)
		}
	}
	log.Printf("Keys %v\n", keys)
}

func comparePredecessors(newPred *predecessorNode, oldPred *predecessorNode) bool {
	if newPred == nil || oldPred == nil {
		return newPred == oldPred
	}
	// Only check the "high" range of the keys. A change of the "low" indicates the node
	// Has a new predecessor, but not necessarily that the node itself has changed.
	return newPred.keys.high == oldPred.keys.high
}

/*
	Compares a nodes current chain to its previous one. If there are any changes, will need to send/receive/drop
	keys.

	This was done through a series of if/else statements.
	Scenarios are listed here: https://app.diagrams.net/#G1MaVQbmbZ6cjkAzkG8zbFdaj9r03HWV5A
	TODO: Currently this code does not handle multiple simultaneous changes,
		such as a new node joining and another node in the chain failing. The assumption
		was that this would be a rare enough event.
	@param newPredecessors are the three nodes previous to the current node in the chain, or
	nil if there are not enough nodes in the chain
	@param transferKeys and sweepCache are used to make this function testable - otherwise it would
	be challenging to keep

	Precondition: if newPredAddrX is nil, all nodes newPredAddr(>X) must also be nil. e.g. if newPredAddr1 is nil,
	newPredAddr2 and newPredAddr3 must also be nil.
*/

func checkPredecessors(newPredecessors [3]*predecessorNode, transferKeys transferFunc, sweepCache sweeperFunc) {
	// Converting addresses to equivalent keys.
	oldPred1, oldPred2, oldPred3 := predecessors[0], predecessors[1], predecessors[2]
	newPred1, newPred2, newPred3 := newPredecessors[0], newPredecessors[1], newPredecessors[2]
	pred1Equal := comparePredecessors(newPred1, oldPred1)
	pred2Equal := comparePredecessors(newPred2, oldPred2)
	pred3Equal := comparePredecessors(newPred3, oldPred3)

	// If none of the previous three have changed, no need to update.
	if pred1Equal && pred2Equal && pred3Equal {
		return
	}
	// PrintKeyChange(newPredecessors)

	// If we newly joined, expect to receive keys
	if newPred1 != nil && oldPred1 == nil && newPred2 != nil && oldPred2 == nil {
		expectedTransfers = append(expectedTransfers, newPred1.addr)
		expectedTransfers = append(expectedTransfers, newPred2.addr)
		return
	}
	/*
		First and second predecessors stay the same (third is different).
		This could mean either the third predecessor has failed, or a new node has joined
		between the second and third predecessor.
	*/
	if pred1Equal && pred2Equal {
		// New node has joined
		if newPred3 != nil && newPred2 != nil && (oldPred3 == nil || util.BetweenKeys(newPred2.keys.low, oldPred2.keys.low, oldPred2.keys.high)) {
			sweepCache(newPred3.keys.low, newPred3.keys.high)
		} else { // P3 failed. Will be receiving P3 keys from P1
			if newPred1 != nil {
				expectedTransfers = append(expectedTransfers, newPred1.addr)
			}
		}
	} else if pred1Equal {
		/*
			First predecessor is the same, second is different. This could mean
			either the second node has failed, or there is a new node between the first and second.
		*/

		// If there's no 2nd predecessor, there can only be 2 nodes in the system - not enough
		// for a full chain so nothing needs to be done in terms of replication.

		// TODO: what if a third node joins?
		if oldPred2 == nil || newPred2 == nil {
			return
		} else if comparePredecessors(newPred3, oldPred2) { // New node joined
			sweepCache(oldPred2.keys.low, oldPred2.keys.high)
		} else if comparePredecessors(newPred2, oldPred3) { // P2 Failed. Will be receiving keys from p1
			if newPred1 != nil {
				expectedTransfers = append(expectedTransfers, newPred1.addr)
			}
			transferKeys(successor.addr, newPredecessors[0].addr, keyRange{low: oldPred2.keys.low, high: oldPred2.keys.high})
		} else {
			UnhandledScenarioError(newPredecessors)
		}
	} else { // First predecessor node has changed.
		// If there's no first predecessor, there can only be one node in the system - not enough
		// for a full chain, so nothing needs to be done in terms of replication.
		if oldPred1 == nil || newPred1 == nil {
			return
		}
		if util.BetweenKeys(newPred1.keys.high, oldPred1.keys.high, mykeys.high) { // New node has joined
			// TODO: bootstrap transfer?
			if oldPred2 != nil {
				sweepCache(oldPred2.keys.low, oldPred2.keys.high)
			}
		} else if comparePredecessors(oldPred2, newPred1) { // Node 1 has failed, node 2 is still running
			expectedTransfers = append(expectedTransfers, newPred1.addr)
			if oldPred2 != nil {
				transferKeys(successor.addr, newPredecessors[0].addr, keyRange{low: oldPred2.keys.low, high: oldPred2.keys.high})
			}
		} else if comparePredecessors(oldPred3, newPred1) { // Both Node 1 and Node 2 have failed.
			if oldPred2 != nil {
				transferKeys(successor.addr, newPredecessors[0].addr, keyRange{low: oldPred2.keys.low, high: oldPred2.keys.high})
			}
			expectedTransfers = append(expectedTransfers, newPred1.addr)
			// TODO: Should also transfer keys between (newPredKey3, oldPredKey2). With
			// 	our current architecture this is not possible since we do not yet have those keys.
			//  This should be very rare so may not need to be handled, as the churn is expected to be low.
			log.Println("TWO NODES FAILED SIMULTANEOUSLY.")
		} else {
			UnhandledScenarioError(newPredecessors)
		}

	}
}

// Helper function for testing / debugging.
func PrintKeyChange(newPredecessors [3]*predecessorNode) {
	log.Printf("OLD KEYS: %v, %v, %v\n", getPredKey(predecessors[0]), getPredKey(predecessors[1]), getPredKey(predecessors[2]))
	log.Printf("NEW KEYS: %v, %v, %v\n", getPredKey(newPredecessors[0]), getPredKey(newPredecessors[1]), getPredKey(newPredecessors[2]))
}

// TODO: This will crash the node and log an error message, should ideally never get called. Regardless,
//  remove this function at the end, only using for debugging to make sure scenarios are properly handled.
func UnhandledScenarioError(newPredecessors [3]*predecessorNode) {
	log.Println("ERROR: UNHANDLED SCENARIO: OLD KEYS")
	for i := 0; i < len(predecessors); i++ {
		if predecessors[i] == nil {
			log.Println(" NIL ")
		} else {
			log.Println(predecessors[i].keys)
		}
	}
	log.Println("NEW KEYS")
	for i := 0; i < len(newPredecessors); i++ {
		if newPredecessors[i] == nil {
			log.Println(" NIL ")
		} else {
			log.Println(newPredecessors[i].keys)
		}
	}
	log.Fatalf("UNHANDLED SCENARIO, INTENTIONALLY CRASHING (REMOVE THIS FUNCTION LATER)")

}

func UpdateSuccessor(succAddr *net.Addr, minKey uint32, maxKey uint32) {
	if succAddr == nil {
		log.Println("ERROR: Successor address cannot be null")
		return
	}

	log.Println("Pending transfers to send: ")
	for _, transfer := range pendingTransfers {
		log.Println((*transfer.coordinator).String())
	}
	log.Println("Expected transfer to receive: ")
	for _, coordinator := range expectedTransfers {
		log.Println((*coordinator).String())
	}

	// No transfer is needed when the node bootstraps (successor will already have a copy of the keys)
	// ASSUMPTION: first node won't receive keys before the second node is launched
	if successor == nil {
		log.Print("\n\n\nUPDATE SUCCESSOR FIRST TIME\n\n\n")
		successor = &successorNode{succAddr, keyRange{minKey, maxKey}}
		return
	}

	// Ignore if information is the same
	if util.CreateAddressStringFromAddr(successor.addr) != util.CreateAddressStringFromAddr(succAddr) {
		log.Print("\n\n\nUPDATE SUCCESSOR\n\n\n")

		// Clear pending transfers to the old successor
		removePendingTransfersToAMember(successor.addr)

		isNewMember := maxKey < successor.keys.high
		successor = &successorNode{succAddr, keyRange{minKey, maxKey}}

		if isNewMember {
			// If the new successor joined, need to transfer your keys and first predecessor's keys

			// Transfer this server's keys to the new successor
			sendDataTransferReq(succAddr, myAddr, mykeys)

			// Transfer predecessor's keys to the new successor
			if predecessors[0].addr != nil {
				sendDataTransferReq(succAddr, predecessors[0].addr, predecessors[0].keys)
			}
		} else {
			// If the new successor already existed (previous successor failed),
			// only need to transfer the first predecessor's keys
			if predecessors[0].addr != nil {
				sendDataTransferReq(succAddr, predecessors[0].addr, predecessors[0].keys)
			}
		}
	}
}

// TRANSFER_REQ internal msg type
func sendDataTransferReq(succAddr *net.Addr, coorAddr *net.Addr, keys keyRange) {
	pendingTransfers = append(pendingTransfers, &transferInfo{succAddr, coorAddr, keys})

	payload := util.SerializeAddr(coorAddr)
	log.Println("\nSENDING TRANSFER REQUEST FOR", (*coorAddr).String())
	requestreply.SendTransferReq(payload, succAddr)
}

// TRANSFER_REQ internal msg type
func HandleTransferReq(msg *pb.InternalMsg) ([]byte, bool) {
	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("\nRECEIVING TRANSFER REQUEST FOR ", (*addr).String())

	// ACK if the address in in expectedTransfers
	// i.e. we are expecting the transfer
	// This ensures the transfer only happens when both parties
	// are ready

	if msg.Payload == nil {
		log.Println("ERROR: HandleTransferReq - Coordinator address can't be null")
		return nil, false
	}

	// Check if the transfer is expected
	for _, coorAddr := range expectedTransfers {
		if util.CreateAddressStringFromAddr(coorAddr) == string(msg.Payload) {
			log.Print("\nTRANSFER IS EXPECTED!\n")
			return msg.Payload, true
		}
	}

	addr, err := util.DeserializeAddr(msg.Payload)
	if err != nil {
		log.Println("ERROR: HandleTransferReq - ", err)
	} else {
		log.Println("ERROR: Not expecting a transfer for keys coordinated by ", util.CreateAddressStringFromAddr(addr))
		log.Println("Expecting ", expectedTransfers)
		log.Println("Predecessors: ", predecessors)
	}

	return nil, false
}

// TRANSFER_RES internal msg
func HandleDataTransferRes(sender *net.Addr, msg *pb.InternalMsg) {
	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("\nRECEIVING TRANSFER ACK FOR ", (*addr).String())

	if msg.Payload == nil {
		log.Println("ERROR: HandleDataTransferRes - Coordinator address can't be null")
		return
	}

	// Check if the transfer is expected
	for i, transfer := range pendingTransfers {
		log.Println("Checking for pending transfers")
		if string(util.SerializeAddr(transfer.coordinator)) == string(msg.Payload) &&
			string(util.SerializeAddr(transfer.receiver)) == string(util.SerializeAddr(sender)) {

			pendingTransfers = removeTransferInfoFromArr(pendingTransfers, i)

			// Start the transfer
			go transferService.TransferKVStoreData(transfer.receiver, transfer.keys.low, transfer.keys.high, func() {
				log.Println("\n SENDING TRANSFER FINISHED FOR ", (*transfer.coordinator).String(), (*transfer.receiver).String())
				requestreply.SendTransferFinished(util.SerializeAddr(transfer.coordinator), transfer.receiver)
			})
			return
		}
	}

	addr, err := util.DeserializeAddr(msg.Payload)
	if err != nil {
		log.Println("ERROR: HandleDataTransferRes - ", err)
	} else {
		log.Println("ERROR: Not expecting a transfer for keys coordinated by ", util.CreateAddressStringFromAddr(addr))
	}
}

// TRANSFER_FINISHED_MSG internal msg type
func HandleTransferFinishedMsg(msg *pb.InternalMsg) {
	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("\nRECEIVING TRANSFER FINISHED MSG FOR ", (*addr).String())

	var removed = false
	for i, coorAddr := range expectedTransfers {
		if string(util.SerializeAddr(coorAddr)) == string(msg.Payload) {
			expectedTransfers = util.RemoveAddrFromArr(expectedTransfers, i)
			removed = true
			return
		}
	}

	if !removed {
		addr, _ := util.DeserializeAddr(msg.Payload)
		log.Println("ERROR: Unexpected HandleTransferFinishedMsg", util.CreateAddressStringFromAddr(addr))
	}
}

// FORWARDED_CHAIN_UPDATE_REQ msg type
func HandleForwardedChainUpdate(msg *pb.InternalMsg) (*net.Addr, []byte, error) {
	log.Println("Received Forwarded Chain update")
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, nil, err
	}
	key := util.Hash(kvRequest.GetKey())
	// Sanity check
	if !predecessors[0].keys.includesKey(key) && !predecessors[1].keys.includesKey(key) {
		log.Println("HandleForwardedChainUpdate: how did we get here?")
		return nil, nil, errors.New("FORWARDED_CHAIN_UPDATE_REQ message received at wrong node")
	}

	payload, err, errcode := kvstore.RequestHandler(kvRequest, 1) //TODO change membershipcount
	if errcode != kvstore.OK || getHeadKeys().includesKey(key) {
		// don't forward if this is the tail or if the request failed
		log.Println("Replying to Forwarded Chain update")

		return nil, payload, err
	}
	// otherwise forward the update to the successor
	log.Println("Forwarding Chain update to", (*successor.addr).String())

	return successor.addr, nil, nil
}

/**
* @return the forward address if the request is to be forwarded to the successor, nil otherwise
* @return The payload of the reply message to be sent
* @return True if the client request belongs to this node, false otherwise
* @return the error in case of failure
 */
func HandleClientRequest(kvRequest *pb.KVRequest) (*net.Addr, []byte, bool, error) {
	keyByte := kvRequest.Key
	if keyByte == nil || !kvstore.IsKVRequest(kvRequest) {
		// Any type of client request besides key-value requests gets handled here
		payload, err, _ := kvstore.RequestHandler(kvRequest, 1) //TODO change membershipcount
		return nil, payload, true, err
	}
	key := util.Hash(keyByte)

	// If this node is the HEAD updates (PUT, REMOVE and WIPEOUT) are performed here and then forwarded
	if mykeys.includesKey(key) && kvstore.IsUpdateRequest(kvRequest) {
		payload, err, errcode := kvstore.RequestHandler(kvRequest, 1) //TODO change membershipcount
		if errcode != kvstore.OK || successor == nil {
			// don't forward invalid/failed requests
			return nil, payload, true, err
		}
		return successor.addr, nil, true, err
	}

	// GET responded to here if they correspond to predecessors[0]
	if getHeadKeys().includesKey(key) && kvstore.IsGetRequest(kvRequest) {
		payload, err, _ := kvstore.RequestHandler(kvRequest, 1) //TODO change membershipcount
		return nil, payload, true, err
	}
	return nil, nil, false, nil
}

/* Helpers */
func removePendingTransfersToAMember(memAddr *net.Addr) {
	log.Println("REMOVE PENDING TRANSFERS")
	for i := 0; i < len(pendingTransfers); {
		if util.CreateAddressStringFromAddr(pendingTransfers[i].receiver) == util.CreateAddressStringFromAddr(memAddr) {
			pendingTransfers = removeTransferInfoFromArr(pendingTransfers, i)
		} else {
			i++
		}
	}
}

func removeTransferInfoFromArr(s []*transferInfo, i int) []*transferInfo {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}
