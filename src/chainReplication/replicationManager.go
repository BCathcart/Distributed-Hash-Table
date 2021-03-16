package chainReplication

import (
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"google.golang.org/protobuf/proto"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
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

// The key field is redundant as our key is calculated by taking the crc32 of the IP/port string, however storing
// simplifies the code.
type predecessorNode struct {
	keys       keyRange
	addr       *net.Addr
	key uint32
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
func getPredAddr(predIdx int) *net.Addr{
	if predecessors[predIdx] == nil {
		return nil
	}
	return predecessors[predIdx].addr
}

func getPredKey(addr *net.Addr) uint32{
	if addr == nil {
		return thisKey
	}
	return util.GetAddrKey(addr)
}

func updatePredecessors(firstPredAddr *net.Addr, secondPredAddr *net.Addr, thirdPredAddr *net.Addr){

}

// TODO: may need to update both predecessors at once
func checkPredecessors(newPredAddr1 *net.Addr, newPredAddr2 *net.Addr, newPredAddr3 *net.Addr) {
	// If the new predecessor is not the previous predessor's predecessor,
	// then start the Bootstrap transfer to the newly joined node

	// Setup code keys based on addresses
	oldPredAddr1, oldPredAddr2, oldPredAddr3 := getPredAddr(0), getPredAddr(1), getPredAddr(2)
	oldPredKey1, oldPredKey2, oldPredKey3 := getPredKey(oldPredAddr1), getPredKey(oldPredAddr2), getPredKey(oldPredAddr3)
	newPredKey1, newPredKey2, newPredKey3  := getPredKey(newPredAddr1), getPredKey(newPredAddr2), getPredKey(newPredAddr3)
	pred1Equal := newPredKey1 == oldPredKey1
	pred2Equal := newPredKey2 == oldPredKey2
	pred3Equal := newPredKey3 == oldPredKey3

	// If none of the previous three have changed, no need to update.
	if pred1Equal && pred2Equal && pred3Equal {
		return
	}

	updatePredecessors(newPredAddr1, newPredAddr2, newPredAddr3)

	// Will need to handle case by case basis - many different scenarios.
	/*
		Case 1: First and second predecessors stay the same (third is different).
		This could mean either the third predecessor has failed, or a new node has joined
		between the second and third predecessor.
	 */
	if pred1Equal && pred2Equal {
		// New node has joined
		if oldPredAddr3 == nil || util.BetweenKeys(newPredKey3, oldPredKey3, oldPredKey2){
			// TODO: sweepCache(oldPredKey3, oldPredKey2)
		} else{ // P3 failed. Will be receiving P3 keys from P1
			pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr1)
		}
	}else if {

	}



	// TODO: handle case second predecessor stays the same but its key range changes
	// - drop keys we are no longer responsible for

	// NOTE: this case handled by having membership layer NewBootstrappingPredecessor
	// if predecessors[0].addr != newPredAddr2 {
	// 	startBootstrapTransfer(newPredAddr1)

	// 	// Will need to drop keys after the transfer is finished
	// 	// - simply call a function to drop all keys outside expected range

	// } else
	if predecessors[0].addr == newPredAddr1 && predecessors[1].addr != newPredAddr2 {
		// If it's a new member, expect to drop some keys

		// If it's an existing member (second predecessor failed), expect to get transfered keys the second predecessor is responsible for
		pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr2)

	} else if predecessors[1].addr == newPredAddr1 {
		// First predecessor has failed, expect to get transferred some keys the new second predecessor is responsible for
		pendingRcvingTransfers = append(pendingRcvingTransfers, newPredAddr2)

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
