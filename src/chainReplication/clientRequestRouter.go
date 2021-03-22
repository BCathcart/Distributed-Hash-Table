package chainReplication

import (
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

func AddRequest(addr *net.Addr, msg *pb.InternalMsg) {
	if len(reqQueue) < cap(reqQueue) {
		log.Println("Adding request to queue", len(reqQueue))
		reqQueue <- request{msg: msg, sender: addr}
	} else {
		log.Println("WARN: Request queue full --- Dropping request") //TODO reply to node in chain
	}
}

// FORWARDED_CHAIN_UPDATE_REQ msg type
/*
* @return address of successor to forward to
* @return true if this is the tail and we need to respond to the client
* @return the payload of the response
* @return false if the key doesn't belong to any of our predecessors
* @return the error in case of failure
 */
func handleForwardedChainUpdate(msg *pb.InternalMsg) (*net.Addr, bool, []byte, bool, error) {
	log.Println("Received Forwarded Chain update")
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, false, nil, false, err
	}
	key := util.Hash(kvRequest.GetKey())

	// Find out where the request originated
	var ownerKeys util.KeyRange
	if predecessors[0].getKeys().IncludesKey(key) {
		if predecessors[0] == nil {
			log.Println("ERROR: keys predecessor 0 nil", predecessors[0].getKeys(), key, predecessors[0].getKeys().IncludesKey(key))
		}
		ownerKeys = predecessors[0].keys
	} else if predecessors[1].getKeys().IncludesKey(key) {
		if predecessors[1] == nil {
			log.Println("ERROR: keys predecessor 1 nil", predecessors[0].getKeys(), key, predecessors[0].getKeys().IncludesKey(key))
		}
		ownerKeys = predecessors[1].keys
	} else {
		log.Println("HandleForwardedChainUpdate: the request for key", key, "is not mine!", predecessors[0].getKeys(), predecessors[1].getKeys())
		return nil, false, nil, false, nil
	}

	payload, err, errcode := kvstore.RequestHandler(kvRequest, 1, ownerKeys) //TODO change membershipcount
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
	log.Println("Forwarding Chain update for key", key, "to", (*successor.addr).String())

	return successor.addr, false, payload, true, nil
}

/**
* @return the forward address if the request is to be forwarded to the successor, nil otherwise
* @return The payload of the reply message to be sent
* @return True if the client request belongs to this node, false otherwise
* @return the error in case of failure
 */
func handleClientRequest(msg *pb.InternalMsg) (*net.Addr, []byte, bool, error) {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return nil, nil, false, err
	}
	key := util.Hash(kvRequest.GetKey())

	// If this node is the HEAD updates (PUT, REMOVE and WIPEOUT) are performed here and then forwarded
	if MyKeys.IncludesKey(key) && kvstore.IsUpdateRequest(kvRequest) {
		payload, err, errcode := kvstore.RequestHandler(kvRequest, 1, MyKeys) //TODO change membershipcount
		if errcode != kvstore.OK || successor == nil {
			// don't forward invalid/failed requests
			return nil, payload, true, err
		}

		log.Println("Forwarding Chain update for key", key, "to", (*successor.addr).String())

		return successor.addr, nil, true, err
	}
	headAddr, headKeys := getHead()
	// GET responded to here if they correspond to the HEAD
	if headKeys.IncludesKey(key) && kvstore.IsGetRequest(kvRequest) && !expectingTransferFrom(headAddr) {
		payload, err, _ := kvstore.RequestHandler(kvRequest, 1, headKeys) //TODO change membershipcount
		return nil, payload, true, err
	}
	log.Println("handleClientRequest: the request for key", key, "is not mine!", predecessors[0].keys, predecessors[1].keys)
	return nil, nil, false, nil
}

func handleRequests(requests <-chan request) {
	for req := range requests {
		reqMsg := req.msg
		var fwdAddr *net.Addr
		var payload []byte
		var isMine bool = false
		var err error
		var isTail bool
		switch reqMsg.InternalID {
		case requestreply.EXTERNAL_MSG, requestreply.FORWARDED_CLIENT_REQ:
			fwdAddr, payload, isMine, err = handleClientRequest(reqMsg)
			isTail = fwdAddr == nil

		case requestreply.FORWARDED_CHAIN_UPDATE_REQ:
			fwdAddr, isTail, payload, isMine, err = handleForwardedChainUpdate(reqMsg)
		}
		if err != nil {
			log.Println("WARN: error in handleRequests", err)
			break
		}
		if !isMine {
			log.Println("WARN: The request is no longer mine")
			//go requestreply.ProcessExternalRequest(reqMsg, *req.sender) //this request is no longer our responsibility
		} else {
			requestreply.RespondToChainRequest(fwdAddr, req.sender, isTail, reqMsg, payload)
		}

	}
	log.Println("Exiting request handler")
}
