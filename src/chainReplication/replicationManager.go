package chainReplication

import (
	"errors"
	"log"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
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
	keys keyRange
	addr *net.Addr
}

// TODO: need reference to this nodes KV store?

type predecessorNode struct {
	keys        keyRange
	addr        *net.Addr
	transferred bool
	// TODO: add kvStore instance here?
}

// 0 = first, 1 = second, 2 = third (not part of the chain but necessary to get lower bound)
var predecessors [3]*predecessorNode
var successor *successorNode
var thisKey uint32

var pendingSendingTransfers []*net.Addr
var sendingTransfers []*net.Addr
var pendingRcvingTransfers []*net.Addr
var mykeys keyRange

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
		log.Println("the head is", (*head.addr).String(), "\n")
		headkeys = head.keys
	}
	return headkeys
}

func Init(keylow uint32, keyhigh uint32) {
	mykeys.low = keylow
	mykeys.high = keyhigh
	// Init successor and predecessors
	prepareForBootstrapTransfer(nil)
}

// Membership layer can tell us if update is due to failure or new node joining

func NewBootstrappingPredecessor(addr *net.Addr) {
	startBootstrapTransfer(addr)
	// Will need to drop keys after the transfer is finished
	// - simply call a function to drop all keys outside expected range
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

//func updatePredecessor(newPredAddr *net.Addr, idx int) {
//	if newPredAddr == nil {
//		predecessors[idx] = nil
//	} else {
//		newPredKey := getPredKey(newPredAddr)
//		newPredNode := &predecessorNode{
//			addr: newPredAddr,
//			keys: keyRange{high: newPredKey, low: thisKey},
//		}
//		predecessors[idx] = newPredNode
//		if idx != 0 {
//			predecessors[idx-1].keys.low = newPredKey
//		}
//	}
//}

// TODO: may need to update both predecessors at once
func UpdatePredecessors(addr []*net.Addr, keys []uint32, key uint32) {
	mykeys.high = key
	var newPredecessors [3]*predecessorNode
	for i := 0; i < 2; i++ {
		if addr[i] != nil {
			newPredecessors[i] = &predecessorNode{}
			newPredecessors[i].addr = addr[i]
			newPredecessors[i].keys.high = keys[i]
			if addr[i+1] != nil {
				newPredecessors[i].keys.low = keys[i+1] + 1
			} else {
				newPredecessors[i].keys.low = key + 1
			}
		} else {
			newPredecessors[i] = nil
			break
		}
	}
	checkPredecessors(newPredecessors)
	predecessors = newPredecessors // TODO: Not sure if I can do this, seems a bit hacky
	if newPredecessors[0] != nil {
		mykeys.low = newPredecessors[0].keys.high + 1
	}

	// for i := 0; i < 2; i++ {
	// 	if predecessors[i] != nil {
	// 		log.Println((*predecessors[i].addr).String(), predecessors[i].keys.low, predecessors[i].keys.high)
}

/*
	Compares a nodes current chain to its previous one. If there are any changes, will need to send/receive/drop
	keys.

	This was done through a series of if/else statements.
	Scenarios are listed here: https://app.diagrams.net/#G1MaVQbmbZ6cjkAzkG8zbFdaj9r03HWV5A
	TODO: Currently this code does not handle multiple simultaneous changes,
		such as a new node joining and another node in the chain failing. The assumption
		was that this would be a rare enough event.
	@param newPredAddr1, newPredAddr2, newPredAddr3 are the three nodes previous to the current node in the chain, or
	nil if there are not enough nodes in the chain

	Precondition: if newPredAddrX is nil, all nodes newPredAddr(>X) must also be nil. e.g. if newPredAddr1 is nil,
	newPredAddr2 and newPredAddr3 must also be nil.
*/

func comparePredecessors(newPred *predecessorNode, oldPred *predecessorNode) bool {
	if newPred == nil || oldPred == nil {
		return newPred == oldPred
	}
	// Only check the "high" range of the keys. A change of the "low" indicates the node
	// Has a new predecessor, but not necessarily that the node itself has changed.
	return newPred.keys.high == oldPred.keys.high
}

//func checkPredecessors(newPredAddr1 *net.Addr, newPredAddr2 *net.Addr, newPredAddr3 *net.Addr) {
func checkPredecessors(newPredecessors [3]*predecessorNode) {
	// Converting addresses to equivalent keys.
	//oldPredAddr1, oldPredAddr2, oldPredAddr3 := getPredAddr(0), getPredAddr(1), getPredAddr(2)
	oldPred1, oldPred2, oldPred3 := predecessors[0], predecessors[1], predecessors[2]
	newPred1, newPred2, newPred3 := newPredecessors[0], newPredecessors[1], newPredecessors[2]
	pred1Equal := comparePredecessors(newPred1, oldPred1)
	pred2Equal := comparePredecessors(newPred2, oldPred2)
	pred3Equal := comparePredecessors(newPred3, oldPred3)

	// If none of the previous three have changed, no need to update.
	if pred1Equal && pred2Equal && pred3Equal {
		return
	}
	PrintKeyChange(newPredecessors)
	/*
		First and second predecessors stay the same (third is different).
		This could mean either the third predecessor has failed, or a new node has joined
		between the second and third predecessor.
	*/
	if pred1Equal && pred2Equal {
		// New node has joined
		if newPred3 != nil && (oldPred3 == nil || util.BetweenKeys(newPred2.keys.low, oldPred2.keys.low, oldPred2.keys.high)) {
			// TODO: sweepCache(oldPredKey3, newPredKey3)
		} else { // P3 failed. Will be receiving P3 keys from P1
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPred1.addr)
		}
	} else if pred1Equal {
		/*
			First predecessor is the same, second is different. This could mean
			either the second node has failed, or there is a new node between the first and second.
		*/

		// If there's no 2nd predecessor, there can only be 2 nodes in the system - not enough
		// for a full chain so nothing needs to be done in terms of replication.
		if oldPred2 == nil || newPred2 == nil {
			return
		} else if comparePredecessors(newPred3, oldPred2) { // New node joined
			// TODO: sweepCache(oldPredKey3, oldPredKey2)
		} else if comparePredecessors(newPred2, oldPred3) { // P2 Failed. Will be receiving keys from p1
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPred1.addr)
			// TODO: Transfer keys to successor between (oldPredKey3, oldPredKey2).
			sendDataTransferReq(oldPred2.addr)
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
			// TODO: sweepCache(oldPredKey3, oldPredKey2) + bootstrap transfer?
		} else if comparePredecessors(oldPred2, newPred1) { // Node 1 has failed, node 2 is still running
			// TODO: Transfer keys to successor between (oldPredKey3, oldPredKey2).
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPred1.addr)
		} else if comparePredecessors(oldPred3, newPred1) { // Both Node 1 and Node 2 have failed.
			// TODO: Transfer keys to successor between (oldPredKey3, oldPredKey2).
			// 	Should also transfer keys between (newPredKey3, oldPredKey2). With
			// 	our current architecture this is not possible since we do not yet
			//	have those keys.
			log.Println("TWO NODES FAILED SIMULTANEOUSLY.")
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPred1.addr)
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
			log.Println(predecessors[i].keys.high)
		}
	}
	log.Println("NEW KEYS")
	for i := 0; i < len(newPredecessors); i++ {
		if newPredecessors[i] == nil {
			log.Println(" NIL ")
		} else {
			log.Println(newPredecessors[i].keys.high)
		}
	}
	log.Fatalf("UNHANDLED SCENARIO, INTENTIONALLY CRASHING (REMOVE THIS FUNCTION LATER)")

}

func UpdateSuccessor(succAddr *net.Addr, minKey uint32, maxKey uint32) {
	// isNewMember := false
	// if isNewMember {
	// 	// If the new successor joined, need to transfer first and second predecessor keys
	// 	sendDataTransferReq(succAddr)

	// } else {
	// 	// If the new successor already existed (previous successor failed),
	// 	// only need to transfer the second predecessor's keys
	// 	sendDataTransferReq(succAddr)
	// }

	// Update successor
	if succAddr == nil {
		successor = nil
	}
	successor = &successorNode{addr: succAddr, keys: keyRange{low: minKey, high: maxKey}}
}

func startBootstrapTransfer(predAddr *net.Addr) {
	// Transfer to the newly joined predecessor
	// Note: this is a special case of transferring
	// - node status will need to be set to STATUS_NORMAL when it's done
	// - node isn't an actual successor until its STATUS_NORMAL
}

func prepareForBootstrapTransfer(succAddr *net.Addr) {
	// expect transfer from successor
	pendingRcvingTransfers = append(pendingRcvingTransfers, succAddr)
}

// TRANSFER_REQ internal msg type
func sendDataTransferReq(succAddr *net.Addr) {
	// Keep periodically sending until receive an ACK

	pendingSendingTransfers = append(pendingRcvingTransfers, succAddr)

}

// TRANSFER_REQ internal msg type
func HandleDataTransferReq() {
	// ACK if the address in in pendingRcvingTransfers
	// i.e. we are expecting the transfer
	// This ensures the transfer only happens when both parties
	// are ready
}

// TRANSFER_FINISHED internal msg type
func HandleTransferFinishedReq(addr *net.Addr) {
	// Remove the address from pendingRcvingTransfers
}

// FORWARDED_CHAIN_UPDATE msg type
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
		return nil, nil, errors.New("FORWARDED_CHAIN_UPDATE message received at wrong node")
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
