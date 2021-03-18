package chainReplication

import (
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

// The key field is redundant as our key is calculated by taking the crc32 of the IP/port string, however storing
// simplifies the code.
type predecessorNode struct {
	keys       keyRange
	addr       *net.Addr
	transfered bool
	// TODO: add kvStore instance here?
}

// 0 = first, 1 = second, 2 = third (not part of the chain but necessary to get lower bound)
var predecessors [3]*predecessorNode
var successor *successorNode
var thisKey uint32

var pendingSendingTransfers []*net.Addr
var sendingTransfers []*net.Addr
var pendingRcvingTransfers []*net.Addr

func init() {
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

func getPredKey(addr *net.Addr) uint32 {
	if addr == nil {
		return thisKey
	}
	return util.GetAddrKey(addr)
}

func updatePredecessor(newPredAddr *net.Addr, idx int) {
	if newPredAddr == nil {
		predecessors[idx] = nil
	} else {
		newPredKey := getPredKey(newPredAddr)
		newPredNode := &predecessorNode{
			addr: newPredAddr,
			keys: keyRange{high: newPredKey, low: thisKey},
		}
		predecessors[idx] = newPredNode
		if idx != 0 {
			predecessors[idx-1].keys.low = newPredKey
		}
	}

}

func updatePredecessors(newPredAddr1 *net.Addr, newPredAddr2 *net.Addr, newPredAddr3 *net.Addr) {

	updatePredecessor(newPredAddr1, 0)
	updatePredecessor(newPredAddr2, 1)
	updatePredecessor(newPredAddr3, 2)
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
func checkPredecessors(newPredAddr1 *net.Addr, newPredAddr2 *net.Addr, newPredAddr3 *net.Addr) {

	// Converting addresses to equivalent keys.
	oldPredAddr1, oldPredAddr2, oldPredAddr3 := getPredAddr(0), getPredAddr(1), getPredAddr(2)
	oldPredKey1, oldPredKey2, oldPredKey3 := getPredKey(oldPredAddr1), getPredKey(oldPredAddr2), getPredKey(oldPredAddr3)
	newPredKey1, newPredKey2, newPredKey3 := getPredKey(newPredAddr1), getPredKey(newPredAddr2), getPredKey(newPredAddr3)
	pred1Equal := newPredKey1 == oldPredKey1
	pred2Equal := newPredKey2 == oldPredKey2
	pred3Equal := newPredKey3 == oldPredKey3

	// If none of the previous three have changed, no need to update.
	if pred1Equal && pred2Equal && pred3Equal {
		return
	}
	PrintKeyChange(newPredKey1, newPredKey2, newPredKey3)
	updatePredecessors(newPredAddr1, newPredAddr2, newPredAddr3)

	/*
		First and second predecessors stay the same (third is different).
		This could mean either the third predecessor has failed, or a new node has joined
		between the second and third predecessor.
	*/
	if pred1Equal && pred2Equal {
		// New node has joined
		if newPredAddr3 != nil && (oldPredAddr3 == nil || util.BetweenKeys(newPredKey3, oldPredKey3, oldPredKey2)) {
			// TODO: sweepCache(oldPredKey3, newPredKey3)
		} else { // P3 failed. Will be receiving P3 keys from P1
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr1)
		}
	} else if pred1Equal {
		/*
			First predecessor is the same, second is different. This could mean
			either the second node has failed, or there is a new node between the first and second.
		*/

		// If there's no 2nd predecessor, there can only be 2 nodes in the system - not enough
		// for a full chain so nothing needs to be done in terms of replication.
		if oldPredAddr2 == nil || newPredAddr2 == nil {
			return
		} else if oldPredKey2 == newPredKey3 { // New node joined
			// TODO: sweepCache(oldPredKey3, oldPredKey2)
		} else if oldPredKey3 == newPredKey2 { // P2 Failed. Will be receiving keys from p1
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr1)
			// TODO: Transfer keys to successor between (oldPredKey3, oldPredKey2).
			sendDataTransferReq(oldPredAddr2)
		} else {
			UnhandledScenarioError(newPredKey1, newPredKey2, newPredKey3)
		}
	} else { // First predecessor node has changed.

		// If there's no first predecessor, there can only be one node in the system - not enough
		// for a full chain, so nothing needs to be done in terms of replication.
		if oldPredAddr1 == nil || newPredAddr1 == nil {
			return
		}
		if util.BetweenKeys(newPredKey1, oldPredKey1, thisKey) { // New node has joined
			// TODO: sweepCache(oldPredKey3, oldPredKey2) + bootstrap transfer?
		} else if oldPredKey2 == newPredKey1 { // Node 1 has failed, node 2 is still running
			// TODO: Transfer keys to successor between (oldPredKey3, oldPredKey2).
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr1)
		} else if oldPredKey3 == newPredKey1 { // Both Node 1 and Node 2 have failed.
			// TODO: Transfer keys to successor between (oldPredKey3, oldPredKey2).
			// 	Should also transfer keys between (newPredKey3, oldPredKey2). With
			// 	our current architecture this is not possible since we do not yet
			//	have those keys.
			log.Println("TWO NODES FAILED SIMULTANEOUSLY.")
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr1)
		} else {
			UnhandledScenarioError(newPredKey1, newPredKey2, newPredKey3)
		}

	}
}

// Helper function for testing / debugging.
func PrintKeyChange(predKey1 uint32, predKey2 uint32, predKey3 uint32) {
	log.Printf("OLD KEYS: %v, %v, %v\n", getPredKey(predecessors[0].addr), getPredKey(predecessors[1].addr), getPredKey(predecessors[2].addr))
	log.Printf("NEW KEYS: %v, %v, %v\n", predKey1, predKey2, predKey3)

}

// TODO: This will crash the node and log an error message, should ideally never get called. Regardless,
//  remove this function at the end, only using for debugging to make sure scenarios are properly handled.
func UnhandledScenarioError(predKey1 uint32, predKey2 uint32, predKey3 uint32) {
	log.Println("ERROR: UNHANDLED SCENARIO: OLD KEYS")
	for i := 0; i < len(predecessors); i++ {
		log.Println(getPredKey(predecessors[i].addr))
	}
	log.Fatalf("NEW KEYS:\n %v\n %v\n%v\n", predKey1, predKey2, predKey3)

}

func UpdateSuccessor(succAddr *net.Addr, minKey uint32, maxKey uint32, isNewMember bool) {
	if isNewMember {
		// If the new successor joined, need to transfer first and second predecessor keys
		sendDataTransferReq(succAddr)

	} else {
		// If the new successor already existed (previous successor failed),
		// only need to transfer the second predecessor's keys
		sendDataTransferReq(succAddr)

	}

	// uUpdate successor
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
func HandleTransferFinishedReq() {
	// Remove the address from pendingRcvingTransfers
}

// DATA_TRANSFER internal msg type
func HandleDataMsg(addr net.Addr, msg *pb.InternalMsg) ([]byte, error) {
	return ServiceRequest(msg)
}

// FORWARDED_CHAIN_UPDATE msg type
func forwardUpdateThroughChain() {
	// If this isn't the tail, forward to the successor
}

// FORWARDED_CHAIN_UPDATE msg type
func handleForwardedChainUpdate() {
	// If this is the tail, respond
}

func HandleClientRequest() {
	// If it belongs to this node

	// Updates (PUT and REMOVE) performed here and then forwarded

	// GET responded to here if they correspond to predecessors[0], and

	// Any other type of client request gets handled here
}

func ServiceRequest(msg *pb.InternalMsg) ([]byte, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, err
	}

	payload, err, _ := kvstore.RequestHandler(kvRequest, GetMembershipCount())
	return payload, err
}
