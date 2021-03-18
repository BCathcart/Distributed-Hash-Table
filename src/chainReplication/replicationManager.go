package chainReplication

import (
	"net"
)

// TODO(Brennan): should add callback to reqreply layer so that
// transfer PUT msgs are handled here. When a transfer is done,
// start accepting GET requests for those keys

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

// TODO: may need to update both predecessors at once
func UpdatePredecessors(firstPredAddr *net.Addr, secondPredAddr *net.Addr, thirdPredAddr *net.Addr) {
	// If the new predecessor is not the previous predessor's predecessor,
	// then start the Bootstrap transfer to the newly joined node

	// Not sure if can compare stuff like this.
	pred1Equal := firstPredAddr == predecessors[0].addr
	pred2Equal := secondPredAddr == predecessors[1].addr
	pred3Equal := thirdPredAddr == predecessors[2].addr
	// If none of the previous three have changed, no need to update.
	if pred1Equal && pred2Equal && pred3Equal {
		return
	}
	// Will need to handle case by case basis - lots of different scenarios.

	// Case 1: First and second predecessors stay the same, third predecessor is different
	// Action: Drop appropriate keys
	if pred1Equal && pred2Equal {
		// callSweeper()
	}

	// TODO: handle case second predecessor stays the same but its key range changes
	// - drop keys we are no longer responsible for

	// NOTE: this case handled by having membership layer NewBootstrappingPredecessor
	// if predecessors[0].addr != secondPredAddr {
	// 	startBootstrapTransfer(firstPredAddr)

	// 	// Will need to drop keys after the transfer is finished
	// 	// - simply call a function to drop all keys outside expected range

	// } else

	// TODO(Brennan): do proper comparisons
	if predecessors[0].addr == firstPredAddr && predecessors[1].addr != secondPredAddr {
		// If it's a new member, expect to drop some keys

		// If it's an existing member (second predecessor failed), expect to get transfered keys the second predecessor is responsible for
		pendingRcvingTransfers = append(pendingRcvingTransfers, secondPredAddr)

	} else if predecessors[1].addr == firstPredAddr {
		// First predecessor has failed, expect to get transferred some keys the new second predecessor is responsible for
		pendingRcvingTransfers = append(pendingRcvingTransfers, secondPredAddr)

		// Transfer new first predecessor's keys to successor
		sendDataTransferReq(successor.addr)

	} else {
		// ERROR
	}

	// Update predecessor array
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
func HandleTransferFinishedReq(addr *net.Addr) {
	// Remove the address from pendingRcvingTransfers
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
