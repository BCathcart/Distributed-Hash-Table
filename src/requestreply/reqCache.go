package requestreply

import (
	"net"
	"time"
)

// Maps msg ID to serialized response
var reqCache_ *Cache

type ReqCacheEntry struct {
	msgType       uint8
	msg           []byte // Remove if only retry ping messages (don't need to store if external requests)
	time          time.Time
	retries       uint8
	destMemberKey int
	addr          *net.Addr
	returnAddr    *net.Addr
	isFirstHop    bool // Used so we know to remove "internalID" and "isResponse" from response
}

/**
* Checks for timed out request cache entries.
 */
func sweepReqCache() {
	var membersToPing []int
	var errResponseAddrs []*net.Addr

	reqCache_.lock.Lock()
	entries := reqCache_.data.Entries()
	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		reqCacheEntry := entry.Value.(ReqCacheEntry)
		elapsedTime := time.Now().Sub(reqCacheEntry.time)
		if elapsedTime.Milliseconds() > REQ_RETRY_TIMEOUT_MS {
			ndToPing, ndToSndErrRes := handleTimedOutReqCacheEntry(entry.Key.(string), &reqCacheEntry)
			if ndToPing != -1 {
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

	// TODO: send error responses

	// TODO: send ping requests
}

/*
* Handles requests that have not received a response within the timeout period
* @param reqCacheEntry The timed out cache entry
* @return node to Ping, -1 no nodes need to be pinged.
* @return clients/nodes address to send error responses too, nil if no error responses need to be sent.
 */
func handleTimedOutReqCacheEntry(key string, reqCacheEntry *ReqCacheEntry) (int, *net.Addr) {
	isClientReq := reqCacheEntry.msgType == FORWARDED_CLIENT_REQ

	ndToPing := -1
	var ndToSndErrRes *net.Addr = nil

	// Return error if this is the clients third retry
	if isClientReq && reqCacheEntry.retries == 3 {
		// NOTE: client is responsible for retries
		// Delete the entry
		reqCache_.data.Delete(key)

		// Send internal communication error response back to original requester
		ndToSndErrRes = reqCacheEntry.returnAddr

		// Send ping message to node we failed to receive a response from
		ndToPing = reqCacheEntry.destMemberKey

	} else if !isClientReq {
		// Retry internal requests up to INTERNAL_REQ_RETRIES times
		if reqCacheEntry.retries < INTERNAL_REQ_RETRIES {
			// Re-send message
			writeMsg(*reqCacheEntry.addr, reqCacheEntry.msg)

			// Update num retries in
			reqCacheEntry.retries += 1
			reqCache_.data.Put(string(key), reqCacheEntry.retries)

		} else if reqCacheEntry.retries == INTERNAL_REQ_RETRIES {
			if reqCacheEntry.msgType == PING {
				// TODO: set member's status to unavailable if it's a ping message
				//   - Add function to call here in gossip/membership service

			} else {
				// Send ping message to node we failed to receive a response from
				ndToPing = reqCacheEntry.destMemberKey
			}

			if reqCacheEntry.returnAddr != nil {
				// Send internal communication error if there is a returnAddr
				// (i.e. if the req is forwarded which can only happen for membership request)
				ndToSndErrRes = reqCacheEntry.returnAddr
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
func putReqCacheEntry(id string, msgType uint8, msg []byte, destMemberKey int, addr *net.Addr, returnAddr *net.Addr, isFirstHop bool) {
	if isFirstHop && msgType != FORWARDED_CLIENT_REQ {
		panic("isFirstHop can only be true for client requests")
	}

	reqCache_.lock.Lock()
	res := resCache_.data.Get(id)

	// Increment retries if it already exists
	if res != nil {
		resCacheEntry := res.(ReqCacheEntry)
		resCacheEntry.retries += 1
		resCache_.data.Put(id, resCacheEntry)

		// Otherwise add a new entry
	} else {
		resCache_.data.Put(id, ReqCacheEntry{msgType, msg, time.Now(), 0, destMemberKey, addr, returnAddr, isFirstHop})
	}
	resCache_.lock.Unlock()
}
