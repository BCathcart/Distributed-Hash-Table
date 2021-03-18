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

// TODO: may need to update both predecessors at once
func UpdatePredecessors(addr []*net.Addr, keys []uint32, key uint32) {
	for i := 0; i < 2; i++ {
		if addr[i] != nil {
			predecessors[i] = &predecessorNode{}
			predecessors[i].addr = addr[i]
			predecessors[i].keys.high = keys[i]
			if addr[i+1] != nil {
				predecessors[i].keys.low = keys[i+1] + 1
			} else {
				predecessors[i].keys.low = key + 1
			}
		} else {
			predecessors[i] = nil
			break
		}
	}
	if predecessors[0] != nil {
		mykeys.high = key
		mykeys.low = predecessors[0].keys.high + 1
	}

	// for i := 0; i < 2; i++ {
	// 	if predecessors[i] != nil {
	// 		log.Println((*predecessors[i].addr).String(), predecessors[i].keys.low, predecessors[i].keys.high)

	// 	}
	// }

	// log.Println(mykeys.low, mykeys.high)

	// // If the new predecessor is not the previous predessor's predecessor,
	// // then start the Bootstrap transfer to the newly joined node

	// // Not sure if can compare stuff like this.
	// pred1Equal := firstPredAddr == predecessors[0].addr
	// pred2Equal := secondPredAddr == predecessors[1].addr
	// pred3Equal := thirdPredAddr == predecessors[2].addr
	// // If none of the previous three have changed, no need to update.
	// if pred1Equal && pred2Equal && pred3Equal {
	// 	return
	// }
	// // Will need to handle case by case basis - lots of different scenarios.

	// // Case 1: First and second predecessors stay the same, third predecessor is different
	// // Action: Drop appropriate keys
	// if pred1Equal && pred2Equal {
	// 	// callSweeper()
	// }

	// // TODO: handle case second predecessor stays the same but its key range changes
	// // - drop keys we are no longer responsible for

	// // NOTE: this case handled by having membership layer NewBootstrappingPredecessor
	// // if predecessors[0].addr != secondPredAddr {
	// // 	startBootstrapTransfer(firstPredAddr)

	// // 	// Will need to drop keys after the transfer is finished
	// // 	// - simply call a function to drop all keys outside expected range

	// // } else

	// // TODO(Brennan): do proper comparisons
	// if predecessors[0].addr == firstPredAddr && predecessors[1].addr != secondPredAddr {
	// 	// If it's a new member, expect to drop some keys

	// 	// If it's an existing member (second predecessor failed), expect to get transfered keys the second predecessor is responsible for
	// 	pendingRcvingTransfers = append(pendingRcvingTransfers, secondPredAddr)

	// } else if predecessors[1].addr == firstPredAddr {
	// 	// First predecessor has failed, expect to get transferred some keys the new second predecessor is responsible for
	// 	pendingRcvingTransfers = append(pendingRcvingTransfers, secondPredAddr)

	// 	// Transfer new first predecessor's keys to successor
	// 	sendDataTransferReq(successor.addr)

	// } else {
	// 	// ERROR
	// }

	// // Update predecessor array
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
	log.Println("Forwarding Chain update to successor")

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
