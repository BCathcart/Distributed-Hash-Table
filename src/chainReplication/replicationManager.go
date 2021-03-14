package chainReplication

import (
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"google.golang.org/protobuf/proto"
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

type predecessorNode struct {
	keys       keyRange
	addr       *net.Addr
	transfered bool
}

var predecessors []predecessorNode // 0 = first, 1 = second
var successor successorNode

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
func UpdatePredecessors(firstPredAddr *net.Addr, secondPredAddr *net.Addr, minKey uint32, middleKey uint32, maxKey uint32) {
	// If the new predecessor is not the previous predessor's predecessor,
	// then start the Bootstrap transfer to the newly joined node

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
	//
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
