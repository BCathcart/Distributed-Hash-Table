package requestreply

import (
	"log"
	"net"
	"time"
)

// Maps msg ID to serialized response
var reqCache_ *Cache

const REQ_RETRY_TIMEOUT_MS = 250 // ms
const REQ_CACHE_TIMEOUT = 6      // sec

type ReqCacheEntry struct {
	msgType    uint8  // i.e. Internal ID
	msg        []byte // serialized message to re-send
	time       time.Time
	retries    uint8
	addr       *net.Addr
	returnAddr *net.Addr
	isFirstHop bool // Used so we know to remove "internalID" and "isResponse" from response
}

type errRes struct {
	msgId      string
	addr       *net.Addr
	isFirstHop bool
}

/**
* Checks for timed out request cache entries.
 */
func sweepReqCache() {
	var membersToPing []*net.Addr
	var errResponseAddrs []*errRes

	reqCache_.lock.Lock()
	entries := reqCache_.data.Entries()
	for i := 0; i < len(entries); i++ {
		entry := entries[i]

		reqCacheEntry := entry.Value.(ReqCacheEntry)
		elapsedTime := time.Now().Sub(reqCacheEntry.time)

		// Handler timed out request
		if elapsedTime.Milliseconds() > REQ_RETRY_TIMEOUT_MS {
			ndToPing, ndToSndErrRes := handleTimedOutReqCacheEntry(entry.Key.(string), &reqCacheEntry)
			if ndToPing != nil {
				membersToPing = append(membersToPing, ndToPing)
			}
			if ndToSndErrRes != nil {
				errResponseAddrs = append(errResponseAddrs, ndToSndErrRes)
			}

			// Clean up stale client requests
		} else if elapsedTime.Seconds() > REQ_CACHE_TIMEOUT {
			reqCache_.data.Delete(entry.Key)
		}
	}
	reqCache_.lock.Unlock()

	// Send error responses
	// for _, errResponse := range errResponseAddrs {
	// 	sendUDPResponse(*errResponse.addr, []byte(errResponse.msgId), nil, 0, errResponse.isFirstHop == false)
	// }

	// Send ping requests
	for _, addr := range membersToPing {
		sendUDPRequest(addr, nil, PING_MSG)
	}

}

// TODO: change key from string to int

/*
* Handles requests that have not received a response within the timeout period
* @param reqCacheEntry The timed out cache entry
* @return node to Ping, -1 no nodes need to be pinged.
* @return clients/nodes address to send error responses too, nil if no error responses need to be sent.
 */
func handleTimedOutReqCacheEntry(key string, reqCacheEntry *ReqCacheEntry) (*net.Addr, *errRes) {
	isClientReq := reqCacheEntry.msgType == FORWARDED_CLIENT_REQ

	var ndToPing *net.Addr = nil
	var ndToSndErrRes *errRes = nil

	// Return error if this is the clients third retry
	if isClientReq && reqCacheEntry.retries == 3 {
		// NOTE: client is responsible for retries
		// Delete the entry
		reqCache_.data.Delete(key)

		// Send internal communication error response back to original requester
		ndToSndErrRes = &errRes{key, reqCacheEntry.returnAddr, reqCacheEntry.isFirstHop}

		// Send ping message to node we failed to receive a response from
		ndToPing = reqCacheEntry.addr

	} else if !isClientReq {
		// Retry internal requests up to INTERNAL_REQ_RETRIES times
		if reqCacheEntry.retries < INTERNAL_REQ_RETRIES || (reqCacheEntry.msgType == PING_MSG && reqCacheEntry.retries >= 10) {
			log.Println("RE-SENDING MSG")
			reSendMsg(key, reqCacheEntry)

			// Handle expired internal requests
		} else if reqCacheEntry.retries == INTERNAL_REQ_RETRIES || (reqCacheEntry.msgType == PING_MSG && reqCacheEntry.retries >= 10) {
			if reqCacheEntry.msgType == PING_MSG {
				log.Println("PING_MSG TIMED OUT")
				go getNodeUnavailableHandler()(reqCacheEntry.addr)

			} else {
				// Send ping message to node we failed to receive a response from
				ndToPing = reqCacheEntry.addr
			}

			if reqCacheEntry.returnAddr != nil {
				// Send internal communication error if there is a returnAddr
				// (i.e. if the req is forwarded which can only happen for membership request)
				ndToSndErrRes = &errRes{key, reqCacheEntry.returnAddr, reqCacheEntry.isFirstHop}
			}

			// Delete the entry
			reqCache_.data.Delete(key)
		}
	}

	return ndToPing, ndToSndErrRes
}

/**
* Puts a message in the request cache.
* @param id The message id.
* @param msgType The type of message (i.e. the InternalID)
* @param msg The serialized message to cache for retries.
* @param addr The address request is sent to.
* @param returnAddr The address the response should be forwarded to (nil if it shouldn't be forwarded)
* @param isFirstHop True if the request originated from a client and is being forwarded internally for the first time, false otherwise
 */
func putReqCacheEntry(id string, msgType uint8, msg []byte, addr *net.Addr, returnAddr *net.Addr, isFirstHop bool) {
	if isFirstHop && (msgType != FORWARDED_CLIENT_REQ && msgType != FORWARDED_CHAIN_UPDATE_REQ) {
		panic("isFirstHop can only be true for client requests")
	}

	reqCache_.lock.Lock()
	req := reqCache_.data.Get(id)

	// Increment retries if it already exists
	if req != nil {
		reqCacheEntry := req.(ReqCacheEntry)
		reqCacheEntry.retries++
		reqCache_.data.Put(id, reqCacheEntry)

		// Otherwise add a new entry
	} else {
		reqCache_.data.Put(id, ReqCacheEntry{msgType, msg, time.Now(), 0, addr, returnAddr, isFirstHop})
	}
	reqCache_.lock.Unlock()
}

/*
* Re-sends the cached request.
* IMPORTANT: caller must be holding reqCache lock.
* @param reqCacheEntry The cached request to re-send.
 */
func reSendMsg(key string, reqCacheEntry *ReqCacheEntry) {
	// Re-send message
	writeMsg(*reqCacheEntry.addr, reqCacheEntry.msg)

	// Update num retries in
	reqCacheEntry.retries++
	reqCacheEntry.time = time.Now()
	reqCache_.data.Put(string(key), *reqCacheEntry)
}
