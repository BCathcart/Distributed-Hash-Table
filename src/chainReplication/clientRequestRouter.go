package chainReplication

import (
	"errors"
	"log"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"

	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
)

type request struct {
	msg    *pb.InternalMsg
	sender *net.Addr
}

var reqQueue chan request = nil

/**
* Add incoming KVRequests that should be processed in this node to the reqQueue
* Requests are dropped inf the queue gets full
 */
func AddRequest(addr *net.Addr, msg *pb.InternalMsg) {
	if len(reqQueue) < cap(reqQueue) {
		log.Println("Adding request to queue", len(reqQueue))
		reqQueue <- request{msg: msg, sender: addr}
	} else {
		log.Println("WARN: Request queue full --- Dropping request") //TODO reply to node in chain
	}
}

/**
* Handler for messages with of type FORWARDED_CHAIN_UPDATE_REQ
* Performs update operation and return the response payload and address to forward the request
* @return address of successor to forward to or nil if this the tail
* @return true if this is the tail and we need to respond to the client
* @return the payload of the response
* @return false if the key doesn't belong to any of our predecessors (does not perform update in this case)
* @return the error in case of failure
 */
func handleForwardedChainUpdate(kvRequest *pb.KVRequest) (*net.Addr, bool, []byte, bool, error) {
	key := util.Hash(kvRequest.GetKey())

	// Find out where the request originated
	var ownerKeys util.KeyRange
	if predecessors[0].getKeys().IncludesKey(key) {
		ownerKeys = predecessors[0].keys
	} else if predecessors[1].getKeys().IncludesKey(key) {
		ownerKeys = predecessors[1].keys
	} else {
		// log.Println("HandleForwardedChainUpdate: the request for key", key, "is not mine!", predecessors[0].getKeys(), predecessors[1].getKeys())
		return nil, false, nil, false, nil
	}

	payload, err, errcode := kvstore.RequestHandler(kvRequest, 1, ownerKeys)
	_, headKeys := getHead()
	if headKeys.IncludesKey(key) {
		// Reply if this is the tail
		return nil, true, payload, true, err
	}

	if errcode != kvstore.OK {
		// don't forward if the request failed
		log.Println("Replying to Forwarded Chain update REQUEST FAILED")
		return nil, false, payload, true, err
	}
	// otherwise forward the update to the successor
	// log.Println("Forwarding Chain update for key", key, "to", (*successor.addr).String())

	return successor.addr, false, payload, true, nil
}

/**
* Handler for key-value requests for keys that need to be processed at this node
* Either the request is a GET request and this is the tail of the chain or an update request and this
* is the head of the chain
* @param the KVRequest
* @return the forward address if the request is to be forwarded to the successor, nil otherwise
* @return The payload of the reply message to be sent
* @return true if the client request belongs to this node, false otherwise
* @return the error in case of failure
 */
func handleClientRequest(kvRequest *pb.KVRequest) (*net.Addr, []byte, bool, error) {

	key := util.Hash(kvRequest.GetKey())

	// If this node is the HEAD updates (PUT, REMOVE and WIPEOUT) are performed here and then forwarded
	if MyKeys.IncludesKey(key) && kvstore.IsUpdateRequest(kvRequest) {
		payload, err, errcode := kvstore.RequestHandler(kvRequest, 1, MyKeys)
		if errcode != kvstore.OK || successor == nil {
			// don't forward invalid/failed requests
			return nil, payload, true, err
		}

		log.Println("Forwarding Chain update for key", key, "to", (*successor.addr).String())

		return successor.addr, nil, true, err
	}
	_, headKeys := getHead()
	// GET responded to here if they correspond to the HEAD
	if headKeys.IncludesKey(key) && kvstore.IsGetRequest(kvRequest) && !expectingTransferFor(key) {
		payload, err, _ := kvstore.RequestHandler(kvRequest, 1, headKeys)
		return nil, payload, true, err
	}
	return nil, nil, false, nil
}

/**
* Worker function for handling KV requests
* Reads a request from the given requests channel and passes it to the appropriate handler functions
* @param requests the incoming requests channel
 */
func handleRequests(requests <-chan request) {
	for req := range requests {
		reqMsg := req.msg
		var fwdAddr *net.Addr
		var payload []byte
		var isMine bool = false
		var err error
		var isTail bool
		kvRequest, err := unmashalRequest(reqMsg.GetPayload())
		if err != nil {
			log.Println("WARN: error in handleRequests", err)
			break
		}
		switch reqMsg.InternalID {
		case requestreply.EXTERNAL_MSG, requestreply.FORWARDED_CLIENT_REQ:
			fwdAddr, payload, isMine, err = handleClientRequest(kvRequest)
			isTail = fwdAddr == nil

		case requestreply.FORWARDED_CHAIN_UPDATE_REQ:
			fwdAddr, isTail, payload, isMine, err = handleForwardedChainUpdate(kvRequest)

		default:
			err = errors.New("message with invalid internal ID")
		}
		if err != nil {
			log.Println("WARN: error in handleRequests", err)
			break
		}
		if !isMine {
			//this case occurs if there has been a change in the chain from when we first
			//queued the request to when we process it.
			//in this case we simply drop the request and let the client retry
			log.Println("WARN: The request for key", kvRequest.GetKey(), "is no longer mine")
		} else {
			//pass the parameters to the request reply layer to send the messages
			requestreply.RespondToChainRequest(fwdAddr, req.sender, isTail, reqMsg, payload)
		}
	}
}

// Helper function for unmarshaling KVRequest
func unmashalRequest(payload []byte) (*pb.KVRequest, error) {
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(payload, kvRequest)
	if err != nil {
		return nil, err
	}
	return kvRequest, nil
}
