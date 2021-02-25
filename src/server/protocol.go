package main

import (
	"encoding/binary"
	"hash/crc32"
	"log"
	"math/rand"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	maps "github.com/ross-oreto/go-list-map"

	pb "github.com/abcpen431/miniproject/pb/protobuf"

	"google.golang.org/protobuf/proto"
)

/************* REQUEST/REPLY PROTOCOL CODE *************/
var conn *net.PacketConn

/* Internal Msg IDs */
const EXTERNAL_REQUEST = 0x0
const MEMBERSHIP_REQUEST = 0x1
const HEARTBEAT = 0x2
const TRANSFER_FINISHED = 0x3
const FORWARDED_CLIENT_REQ = 0x4
const PING = 0x5
const TRANSFER_REQ = 0x5

// Only receive transfer request during bootstrapping

const INTERNAL_REQ_RETRIES = 1

const MAX_BUFFER_SIZE = 11000

type Cache struct {
	lock sync.Mutex
	data *maps.Map
}

/**
* Creates and returns a pointer to a new cache
* @return The cache.
 */
func NewCache() *Cache {
	cache := new(Cache)
	cache.data = maps.New()
	return cache
}

/*** START Req Cache Code ***/
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
		// log.Println(entry.Key)
		// log.Println(entry.Value)
		// time.Sleep(2 * time.Second)

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
	for _, errResponse := range errResponseAddrs {
		sendUDPResponse(*errResponse.addr, []byte(errResponse.msgId), nil, errResponse.isFirstHop == false)
	}

	// Send ping requests
	for _, addr := range membersToPing {
		SendUDPRequest(addr, nil, PING)
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
		if reqCacheEntry.retries < INTERNAL_REQ_RETRIES {
			log.Println("RE-SENDING MSG")

			reSendMsg(key, reqCacheEntry)

			// Handle expired internal requests
		} else if reqCacheEntry.retries == INTERNAL_REQ_RETRIES {
			if reqCacheEntry.msgType == PING {
				log.Println("PING TIMED OUT")
				// TODO: set member's status to unavailable if it's a ping message
				//   - Add function to call here in gossip/membership service

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

/*
* Re-sends the cached request.
* IMPORTANT: caller must be holding reqCache lock.
* @param reqCacheEntry The cached request to re-send.
 */
func reSendMsg(key string, reqCacheEntry *ReqCacheEntry) {
	// Re-send message
	writeMsg(*reqCacheEntry.addr, reqCacheEntry.msg)

	// Update num retries in
	reqCacheEntry.retries += 1
	reqCacheEntry.time = time.Now()
	reqCache_.data.Put(string(key), *reqCacheEntry)
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
	if isFirstHop && msgType != FORWARDED_CLIENT_REQ {
		panic("isFirstHop can only be true for client requests")
	}

	reqCache_.lock.Lock()
	req := reqCache_.data.Get(id)

	// Increment retries if it already exists
	if req != nil {
		reqCacheEntry := req.(ReqCacheEntry)
		reqCacheEntry.retries += 1
		reqCache_.data.Put(id, reqCacheEntry)

		// Otherwise add a new entry
	} else {
		reqCache_.data.Put(id, ReqCacheEntry{msgType, msg, time.Now(), 0, addr, returnAddr, isFirstHop})
	}
	reqCache_.lock.Unlock()
}

/*** END Req Cache Code ***/

/*** START Res Cache Code ***/
const RES_CACHE_TIMEOUT = 6 // Messages only required to live in cache for 5 seconds

/* Note: the cache has a small cap for now due to the nature of only having a small number
of clients sending messages. The cache will need to expanded in the future once we have a
better idea of the likely number of clients and their retry rate. */
const MAX_RES_CACHE_ENTRIES = 50

// Maps msg ID to serialized response
var resCache_ *Cache

type ResCacheEntry struct {
	msg  []byte
	time time.Time
}

/**
* Removes expired entries in the cache every RES_CACHE_TIMEOUT seconds.
 */
func sweepResCache() {
	resCache_.lock.Lock()
	entries := resCache_.data.Entries()
	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		elapsedTime := time.Now().Sub(entry.Value.(ResCacheEntry).time)
		if elapsedTime.Seconds() > RES_CACHE_TIMEOUT {
			resCache_.data.Delete(entry.Key)
		}
	}
	resCache_.lock.Unlock()
	PrintMemStats()

	debug.FreeOSMemory() // Force GO to free unused memory
}

/**
* Puts a message in the cache.
* @param id The message id.
* @param msg The serialized message to cache.
 */
func putResCacheEntry(id string, msg []byte) {
	resCache_.lock.Lock()
	for resCache_.data.Len() >= MAX_RES_CACHE_ENTRIES {
		resCache_.data.Pull()
	}
	resCache_.data.Put(id, ResCacheEntry{msg: msg, time: time.Now()})
	resCache_.lock.Unlock()
}

/*** END Res Cache Code ***/

/**
* Initializes the request/reply layer. Must be called before using
* request/reply layer to get expected functionality.
 */
func requestReplyLayerInit() {
	resCache_ = NewCache()
	reqCache_ = NewCache()

	// Sweep cache every RES_CACHE_TIMEOUT seconds
	var ticker = time.NewTicker(time.Second * RES_CACHE_TIMEOUT)

	go func() {
		for {
			<-ticker.C
			sweepResCache()
		}
	}()

	// Sweep cache every RES_CACHE_TIMEOUT seconds
	var ticker2 = time.NewTicker(time.Millisecond * REQ_RETRY_TIMEOUT_MS)

	go func() {
		for {
			<-ticker2.C
			sweepReqCache()
		}
	}()
}

/**
* Generates a unique 16 byte ID.
* @param clientIP The client's IP address.
* @param port Server port number.
* @return The unique ID as a 16 byte long byte array.
 */
func getmsgID(clientIP string, port uint16) []byte {
	ipArr := strings.Split(clientIP, ".")
	ipBytes := make([]byte, 5)
	for i, s := range ipArr {
		val, _ := strconv.Atoi(s)
		binary.LittleEndian.PutUint16(ipBytes[i:], uint16(val))
	}
	ipBytes = ipBytes[0:4]

	portBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(portBytes, port)
	randBytes := make([]byte, 2)
	rand.Read(randBytes)
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(time.Now().UnixNano()))

	id := append(append(append(ipBytes, portBytes...), randBytes...), timeBytes...)
	return id
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msgID The message ID.
* @param msgPayload The message payload.
* @return The checksum.
 */
func computeChecksum(msgID []byte, msgPayload []byte) uint32 {
	return crc32.ChecksumIEEE(append(msgID, msgPayload...))
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msg The received message.
* @return True if message ID matches the expected ID and checksum is valid, false otherwise.
 */
func verifyChecksum(msg *pb.InternalMsg) bool {
	// Verify MessageID is as expected
	if uint64(computeChecksum((*msg).MessageID, (*msg).Payload)) != (*msg).CheckSum {
		return false
	}
	return true
}

/**
* Writes a message to the connection.
* @param conn The connection object to send the message over.
* @param addr The IP address to send to.
* @param msg The message to send.
 */
func writeMsg(addr net.Addr, msg []byte) {
	// Send msg
	_, err := (*conn).WriteTo(msg, addr)
	if err != nil {
		log.Println(err)
	} else {
		// log.Println("INFO:", n, "bytes written.")
	}
}

/**
* Sends a UDP message responding to a request.
* @param conn The connection object to send messages over.
* @param addr The IP address to send to.
* @param msgID The message id.
* @param payload The message payload.
 */
func sendUDPResponse(addr net.Addr, msgID []byte, payload []byte, isInternal bool) {
	checksum := computeChecksum(msgID, payload)

	resMsg := &pb.InternalMsg{
		MessageID: msgID,
		Payload:   payload,
		CheckSum:  uint64(checksum),
	}

	// Specify it is a response if the destination is another server
	if isInternal {
		resMsg.IsResponse = true
	}

	serMsg, err := proto.Marshal(resMsg)
	if err != nil {
		log.Println(err)
	}

	// Cache message
	putResCacheEntry(string(msgID), serMsg)

	writeMsg(addr, serMsg)
}

/*
* Forwards and caches a response.
* @param conn The connection object to send messages over.
* @param addr The address to send the message to.
* @param resMsg The message to send.
* @param isInternal True if the response is being sent to another server node, false if being sent to a client.
 */
func forwardUDPResponse(addr net.Addr, resMsg *pb.InternalMsg, isInternal bool) {
	// Remove internal fields if forwarding to a client
	if isInternal == false {
		resMsg = &pb.InternalMsg{ // TODO: may need to change this to type pb.Msg
			MessageID: resMsg.MessageID,
			Payload:   resMsg.Payload,
			CheckSum:  resMsg.CheckSum,
		}
	}

	serMsg, err := proto.Marshal(resMsg)
	if err != nil {
		log.Println(err)
	}

	// Cache message
	putResCacheEntry(string(resMsg.MessageID), serMsg)

	writeMsg(addr, serMsg)
}

/*
* Forwards and caches the request
* @param conn The connection object to send messages over.
* @param addr The address to send the message to.
* @param returnAddr The address to forward the response to, nil if the response shouldn't be forwarded.
* @param reqMsg The message to send.
 */
func forwardUDPRequest(addr *net.Addr, returnAddr *net.Addr, reqMsg *pb.InternalMsg) {
	isFirstHop := false

	// Update ID if we are forwarding an external request
	if reqMsg.InternalID == EXTERNAL_REQUEST {
		reqMsg.InternalID = FORWARDED_CLIENT_REQ
		isFirstHop = true
	}

	serMsg, err := proto.Marshal(reqMsg)
	if err != nil {
		log.Println(err)
	}

	// Add to request cache
	if reqMsg.InternalID == FORWARDED_CLIENT_REQ {
		// No need to cache serialized message if is a client request since client responsible for retries
		putReqCacheEntry(string(reqMsg.MessageID), uint8(reqMsg.InternalID), nil, addr, returnAddr, isFirstHop)
	} else {
		putReqCacheEntry(string(reqMsg.MessageID), uint8(reqMsg.InternalID), serMsg, addr, returnAddr, isFirstHop)
	}

	writeMsg(*addr, serMsg)
}

/**
* Processes a request message and either forwards it or handles it at this node.
* @param conn The connection object to send messages over.
* @param returnAddr The IP address to send response to.
* @param reqMsg The incoming message.
* @param externalReqHandler The message handler callback for external messages (msgs passed to app layer).
 */
func processRequest(returnAddr net.Addr, reqMsg *pb.InternalMsg, externalReqHandler func([]byte) ([]byte, error)) {
	log.Println("Received Request of type ", strconv.Itoa(int(reqMsg.InternalID)))
	// Check if response is already cached
	resCache_.lock.Lock()
	res := resCache_.data.Get(string(reqMsg.MessageID))
	if res != nil {
		resCacheEntry := res.(ResCacheEntry)
		writeMsg(returnAddr, resCacheEntry.msg)
		// Reset timeout
		resCacheEntry.time = time.Now()
		resCache_.data.Put(string(reqMsg.MessageID), resCacheEntry) // TODO: does this overwrite?
		resCache_.lock.Unlock()
		return
	}
	resCache_.lock.Unlock()

	// Determine if an internal or external message
	if reqMsg.InternalID != EXTERNAL_REQUEST && reqMsg.InternalID != FORWARDED_CLIENT_REQ {
		// TODO: pass handler as arg?

		// Membership service is reponsible for sending response or forwarding the request
		// TODO: internalReqHandler(conn, returnAddr, reqMsg)

		// FOR TESTING:
		sendUDPResponse(returnAddr, reqMsg.MessageID, nil, true)
	} else {
		// TODO: determine if the key corresponds to this node
		// var addr *net.Addr = nil

		// TODO: If key corresponds to this node:
		// Pass message to handler
		payload, err := externalReqHandler(reqMsg.Payload)
		if err != nil {
			log.Println("WARN could not handle message. Sender = " + returnAddr.String())
			return
		}
		// Send response
		sendUDPResponse(returnAddr, reqMsg.MessageID, payload, reqMsg.InternalID == FORWARDED_CLIENT_REQ)

		// TODO: If key doesn't correspond to this node:
		// forwardUDPRequest(conn, addr, returnAddr, reqMsg)
	}

}

/**
* Processes a response message.
* @param conn The connection object to send messages over.
* @param addr The IP address to send response to.
* @param serialMsg The incoming message.
* @param handler The message handler callback.
 */
func processResponse(resMsg *pb.InternalMsg) {
	log.Println("Received response")
	// Get cached request (ignore if it's not cached)
	reqCache_.lock.Lock()
	req := reqCache_.data.Get(string(resMsg.MessageID))
	if req != nil {
		reqCacheEntry := req.(ReqCacheEntry)

		// If cached request has return address, forward the request
		if reqCacheEntry.returnAddr != nil {
			forwardUDPResponse(*reqCacheEntry.returnAddr, resMsg, !reqCacheEntry.isFirstHop)
		}

		log.Println("Recieved response")

		// TODO: message handler for internal client requests w/o a return address (PUT requests during transfer)

		// Otherwise simply remove the message from the queue
		// Note: We don't need any response handlers for now
		// TODO: what about for TRANSFER_FINISHED and MEMBERSHIP_REQUEST?
		reqCache_.data.Delete(string(resMsg.MessageID))

	} else {
		log.Println("WARN: Received response for unknown request")
	}
	reqCache_.lock.Unlock()
}

/*
* Creates, sends and caches a request.
* @param addr The address to send the message to.
* @param payload The request payload.
* @param internalID The internal message type.
 */
// NOTE: this will be used for sending internal messages
func SendUDPRequest(addr *net.Addr, payload []byte, internalID uint8) {
	ip := (*conn).LocalAddr().(*net.UDPAddr).IP.String()
	port := (*conn).LocalAddr().(*net.UDPAddr).Port

	msgID := getmsgID(ip, uint16(port))

	checksum := computeChecksum(msgID, payload)

	reqMsg := &pb.InternalMsg{
		MessageID:  msgID,
		Payload:    payload,
		CheckSum:   uint64(checksum),
		InternalID: uint32(internalID),
		IsResponse: false,
	}

	serMsg, err := proto.Marshal(reqMsg)
	if err != nil {
		log.Println(err)
	}

	// Add to request cache
	putReqCacheEntry(string(msgID), internalID, serMsg, addr, nil, false)

	writeMsg(*addr, serMsg)
}

/**
* Listens for incoming messages, processes them, and then passes them to the handler callback.
* @param conn The network connection object.
* @param externalReqHandler The external request handler callback.
* @return An error message if failed to read from the connection.
 */
func MsgListener(externalReqHandler func([]byte) ([]byte, error) /*, resHandler func([]byte) ([]byte, error)*/) error {
	buffer := make([]byte, MAX_BUFFER_SIZE)

	// Listen for packets
	for {
		//log.Println("Awaiting packet")
		n, returnAddr, err := (*conn).ReadFrom(buffer)
		if err != nil {
			return err
		}

		// Deserialize message
		msg := &pb.InternalMsg{}
		err = proto.Unmarshal(buffer[0:n], msg)
		if err != nil {
			// Disregard messages with invalid format
			log.Println("WARN msg with invalid format. Sender = " + returnAddr.String())
		}

		// Verify checksum
		if !verifyChecksum(msg) {
			// Disregard messages with invalid checksums
			log.Println("WARN checksum mismatch. Sender = " + returnAddr.String())
			continue
		}

		if msg.IsResponse {
			go processResponse(msg)
		} else {
			go processRequest(returnAddr, msg, externalReqHandler)
		}
	}
}
