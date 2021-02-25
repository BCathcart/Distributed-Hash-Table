package main

import (
	"log"
	"runtime"
)

// import (
// 	"container/list"
// 	"encoding/binary"
// 	"fmt"
// 	"hash/crc32"
// 	"log"
// 	"math/rand"
// 	"net"
// 	"os"
// 	"runtime"
// 	"runtime/debug"
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"

// 	maps "github.com/ross-oreto/go-list-map"

// 	pb "github.com/abcpen431/miniproject/pb/protobuf"

// 	"google.golang.org/protobuf/proto"
// )

// var MAX_MEM_USAGE uint64 = 120 * 1024 * 1024 // Max is actually 128 MB (8MB of buffer)

// /************* REQUEST/REPLY PROTOCOL CODE *************/
// var conn *net.PacketConn

// /* Internal Msg IDs */
// const EXTERNAL_REQUEST = 0x0
// const MEMBERSHIP_REQUEST = 0x1
// const HEARTBEAT = 0x2
// const TRANSFER_FINISHED = 0x3
// const FORWARDED_CLIENT_REQ = 0x4
// const PING = 0x5

// const INTERNAL_REQ_RETRIES = 1

// const MAX_BUFFER_SIZE = 11000

// type Cache struct {
// 	lock sync.Mutex
// 	data *maps.Map
// }

// /**
// * Creates and returns a pointer to a new cache
// * @return The cache.
//  */
// func NewCache() *Cache {
// 	cache := new(Cache)
// 	cache.data = maps.New()
// 	return cache
// }

// /*** START Req Cache Code ***/
// // Maps msg ID to serialized response
// var reqCache_ *Cache

// const REQ_RETRY_TIMEOUT_MS = 250 // ms
// const REQ_CACHE_TIMEOUT = 6      // sec

// type ReqCacheEntry struct {
// 	msgType       uint8
// 	msg           []byte // Remove if only retry ping messages (don't need to store if external requests)
// 	time          time.Time
// 	retries       uint8
// 	destMemberKey int
// 	addr          *net.Addr
// 	returnAddr    *net.Addr
// 	isFirstHop    bool // Used so we know to remove "internalID" and "isResponse" from response
// }

// /**
// * Checks for timed out request cache entries.
//  */
// func sweepReqCache() {
// 	var membersToPing []int
// 	var errResponseAddrs []*net.Addr

// 	reqCache_.lock.Lock()
// 	entries := reqCache_.data.Entries()
// 	for i := 0; i < len(entries); i++ {
// 		entry := entries[i]
// 		reqCacheEntry := entry.Value.(ReqCacheEntry)
// 		elapsedTime := time.Now().Sub(reqCacheEntry.time)
// 		if elapsedTime.Milliseconds() > REQ_RETRY_TIMEOUT_MS {
// 			ndToPing, ndToSndErrRes := handleTimedOutReqCacheEntry(entry.Key.(string), &reqCacheEntry)
// 			if ndToPing != -1 {
// 				membersToPing = append(membersToPing, ndToPing)
// 			}
// 			if ndToSndErrRes != nil {
// 				errResponseAddrs = append(errResponseAddrs, ndToSndErrRes)
// 			}

// 			// Clean up stale client requests
// 		} else if elapsedTime.Seconds() > REQ_CACHE_TIMEOUT {
// 			reqCache_.data.Delete(entry.Key)
// 		}
// 	}
// 	reqCache_.lock.Unlock()

// 	// TODO: send error responses

// 	// TODO: send ping requests
// }

// /*
// * Handles requests that have not received a response within the timeout period
// * @param reqCacheEntry The timed out cache entry
// * @return node to Ping, -1 no nodes need to be pinged.
// * @return clients/nodes address to send error responses too, nil if no error responses need to be sent.
//  */
// func handleTimedOutReqCacheEntry(key string, reqCacheEntry *ReqCacheEntry) (int, *net.Addr) {
// 	isClientReq := reqCacheEntry.msgType == FORWARDED_CLIENT_REQ

// 	ndToPing := -1
// 	var ndToSndErrRes *net.Addr = nil

// 	// Return error if this is the clients third retry
// 	if isClientReq && reqCacheEntry.retries == 3 {
// 		// NOTE: client is responsible for retries
// 		// Delete the entry
// 		reqCache_.data.Delete(key)

// 		// Send internal communication error response back to original requester
// 		ndToSndErrRes = reqCacheEntry.returnAddr

// 		// Send ping message to node we failed to receive a response from
// 		ndToPing = reqCacheEntry.destMemberKey

// 	} else if !isClientReq {
// 		// Retry internal requests up to INTERNAL_REQ_RETRIES times
// 		if reqCacheEntry.retries < INTERNAL_REQ_RETRIES {
// 			// Re-send message
// 			writeMsg(*reqCacheEntry.addr, reqCacheEntry.msg)

// 			// Update num retries in
// 			reqCacheEntry.retries += 1
// 			reqCache_.data.Put(string(key), reqCacheEntry.retries)

// 		} else if reqCacheEntry.retries == INTERNAL_REQ_RETRIES {
// 			if reqCacheEntry.msgType == PING {
// 				// TODO: set member's status to unavailable if it's a ping message
// 				//   - Add function to call here in gossip/membership service

// 			} else {
// 				// Send ping message to node we failed to receive a response from
// 				ndToPing = reqCacheEntry.destMemberKey
// 			}

// 			if reqCacheEntry.returnAddr != nil {
// 				// Send internal communication error if there is a returnAddr
// 				// (i.e. if the req is forwarded which can only happen for membership request)
// 				ndToSndErrRes = reqCacheEntry.returnAddr
// 			}

// 			// Delete the entry
// 			reqCache_.data.Delete(key)
// 		}
// 	}

// 	return ndToPing, ndToSndErrRes
// }

// /**
// * Puts a message in the request cache.
// * @param id The message id.
// * @param msgType The type of message (i.e. the InternalID)
// * @param msg The serialized message to cache for retries.
// * @param addr The address request is sent to.
// * @param returnAddr The address the response should be forwarded to (nil if it shouldn't be forwarded)
// * @param isFirstHop True if the request originated from a client and is being forwarded internally for the first time, false otherwise
//  */
// func putReqCacheEntry(id string, msgType uint8, msg []byte, destMemberKey int, addr *net.Addr, returnAddr *net.Addr, isFirstHop bool) {
// 	if isFirstHop && msgType != FORWARDED_CLIENT_REQ {
// 		panic("isFirstHop can only be true for client requests")
// 	}

// 	reqCache_.lock.Lock()
// 	res := resCache_.data.Get(id)

// 	// Increment retries if it already exists
// 	if res != nil {
// 		resCacheEntry := res.(ReqCacheEntry)
// 		resCacheEntry.retries += 1
// 		resCache_.data.Put(id, resCacheEntry)

// 		// Otherwise add a new entry
// 	} else {
// 		resCache_.data.Put(id, ReqCacheEntry{msgType, msg, time.Now(), 0, destMemberKey, addr, returnAddr, isFirstHop})
// 	}
// 	resCache_.lock.Unlock()
// }

// /*** END Req Cache Code ***/

// /*** START Res Cache Code ***/
// const RES_CACHE_TIMEOUT = 6 // Messages only required to live in cache for 5 seconds

// /* Note: the cache has a small cap for now due to the nature of only having a small number
// of clients sending messages. The cache will need to expanded in the future once we have a
// better idea of the likely number of clients and their retry rate. */
// const MAX_RES_CACHE_ENTRIES = 50

// // Maps msg ID to serialized response
// var resCache_ *Cache

// type ResCacheEntry struct {
// 	msg  []byte
// 	time time.Time
// }

// /**
// * Removes expired entries in the cache every RES_CACHE_TIMEOUT seconds.
//  */
// func sweepResCache() {
// 	resCache_.lock.Lock()
// 	entries := resCache_.data.Entries()
// 	for i := 0; i < len(entries); i++ {
// 		entry := entries[i]
// 		elapsedTime := time.Now().Sub(entry.Value.(ResCacheEntry).time)
// 		if elapsedTime.Seconds() > RES_CACHE_TIMEOUT {
// 			resCache_.data.Delete(entry.Key)
// 		}
// 	}
// 	resCache_.lock.Unlock()
// 	PrintMemStats()

// 	debug.FreeOSMemory() // Force GO to free unused memory
// }

// /**
// * Puts a message in the cache.
// * @param id The message id.
// * @param msg The serialized message to cache.
//  */
// func putResCacheEntry(id string, msg []byte) {
// 	resCache_.lock.Lock()
// 	for resCache_.data.Len() >= MAX_RES_CACHE_ENTRIES {
// 		resCache_.data.Pull()
// 	}
// 	resCache_.data.Put(id, ResCacheEntry{msg: msg, time: time.Now()})
// 	resCache_.lock.Unlock()
// }

// /*** END Res Cache Code ***/

// /**
// * Initializes the request/reply layer. Must be called before using
// * request/reply layer to get expected functionality.
//  */
// func requestReplyLayerInit() {
// 	/* Set up response cache */
// 	resCache_ = NewCache()

// 	// Sweep cache every RES_CACHE_TIMEOUT seconds
// 	var ticker = time.NewTicker(time.Second * RES_CACHE_TIMEOUT)

// 	go func() {
// 		for {
// 			<-ticker.C
// 			sweepResCache()
// 		}
// 	}()

// 	/* Set up request cache */
// 	reqCache_ = NewCache()

// 	// Sweep cache every RES_CACHE_TIMEOUT seconds
// 	var ticker2 = time.NewTicker(time.Millisecond * REQ_RETRY_TIMEOUT_MS)

// 	go func() {
// 		for {
// 			<-ticker2.C
// 			sweepReqCache()
// 		}
// 	}()
// }

// /**
// * Generates a unique 16 byte ID.
// * @param clientIP The client's IP address.
// * @param port Server port number.
// * @return The unique ID as a 16 byte long byte array.
//  */
// func getmsgID(clientIP string, port uint16) []byte {
// 	ipArr := strings.Split(clientIP, ".")
// 	ipBytes := make([]byte, 5)
// 	for i, s := range ipArr {
// 		val, _ := strconv.Atoi(s)
// 		binary.LittleEndian.PutUint16(ipBytes[i:], uint16(val))
// 	}
// 	ipBytes = ipBytes[0:4]

// 	portBytes := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(portBytes, port)
// 	randBytes := make([]byte, 2)
// 	rand.Read(randBytes)
// 	timeBytes := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(timeBytes, uint64(time.Now().UnixNano()))

// 	id := append(append(append(ipBytes, portBytes...), randBytes...), timeBytes...)
// 	return id
// }

// /**
// * Computes the IEEE CRC checksum based on the message ID and message payload.
// * @param msgID The message ID.
// * @param msgPayload The message payload.
// * @return The checksum.
//  */
// func computeChecksum(msgID []byte, msgPayload []byte) uint32 {
// 	return crc32.ChecksumIEEE(append(msgID, msgPayload...))
// }

// /**
// * Computes the IEEE CRC checksum based on the message ID and message payload.
// * @param msg The received message.
// * @return True if message ID matches the expected ID and checksum is valid, false otherwise.
//  */
// func verifyChecksum(msg *pb.InternalMsg) bool {
// 	// Verify MessageID is as expected
// 	if uint64(computeChecksum((*msg).MessageID, (*msg).Payload)) != (*msg).CheckSum {
// 		return false
// 	}
// 	return true
// }

// /**
// * Writes a message to the connection.
// * @param conn The connection object to send the message over.
// * @param addr The IP address to send to.
// * @param msg The message to send.
//  */
// func writeMsg(addr net.Addr, msg []byte) {
// 	// Send msg
// 	_, err := (*conn).WriteTo(msg, addr)
// 	if err != nil {
// 		log.Println(err)
// 	} else {
// 		// log.Println("INFO:", n, "bytes written.")
// 	}
// }

// /**
// * Sends a UDP message responding to a request.
// * @param conn The connection object to send messages over.
// * @param addr The IP address to send to.
// * @param msgID The message id.
// * @param payload The message payload.
//  */
// func sendUDPResponse(addr net.Addr, msgID []byte, payload []byte, isInternal bool) {
// 	checksum := computeChecksum(msgID, payload)

// 	resMsg := &pb.InternalMsg{
// 		MessageID: msgID,
// 		Payload:   payload,
// 		CheckSum:  uint64(checksum),
// 	}

// 	// Specify it is a response if the destination is another server
// 	if isInternal {
// 		resMsg.IsResponse = true
// 	}

// 	serMsg, err := proto.Marshal(resMsg)
// 	if err != nil {
// 		log.Println(err)
// 	}

// 	// Cache message
// 	putResCacheEntry(string(msgID), serMsg)

// 	writeMsg(addr, serMsg)
// }

// /*
// * Forwards and caches a response.
// * @param conn The connection object to send messages over.
// * @param addr The address to send the message to.
// * @param resMsg The message to send.
// * @param isInternal True if the response is being sent to another server node, false if being sent to a client.
//  */
// func forwardUDPResponse(addr net.Addr, resMsg *pb.InternalMsg, isInternal bool) {
// 	// Remove internal fields if forwarding to a client
// 	if isInternal == false {
// 		resMsg = &pb.InternalMsg{
// 			MessageID: resMsg.MessageID,
// 			Payload:   resMsg.Payload,
// 			CheckSum:  resMsg.CheckSum,
// 		}
// 	}

// 	serMsg, err := proto.Marshal(resMsg)
// 	if err != nil {
// 		log.Println(err)
// 	}

// 	// Cache message
// 	putResCacheEntry(string(resMsg.MessageID), serMsg)

// 	writeMsg(addr, serMsg)
// }

// /*
// * Creates, sends and caches a request.
// * @param conn The connection object to send messages over.
// * @param addr The address to send the message to.
// * @param reqMsg The message to send.
//  */
// // NOTE: this will be used for sending internal messages
// func sendUDPRequest(addr *net.Addr, port int, destMemberKey int, payload []byte, internalID uint8) {
// 	localAddr := strings.Split((*conn).LocalAddr().String(), ":")[0]
// 	fmt.Println(localAddr)
// 	msgID := getmsgID(localAddr, uint16(port))

// 	checksum := computeChecksum(msgID, payload)

// 	reqMsg := &pb.InternalMsg{
// 		MessageID:  msgID,
// 		Payload:    payload,
// 		CheckSum:   uint64(checksum),
// 		InternalID: uint32(internalID),
// 		IsResponse: false,
// 	}

// 	serMsg, err := proto.Marshal(reqMsg)
// 	if err != nil {
// 		log.Println(err)
// 	}

// 	// Add to request cache
// 	putReqCacheEntry(string(msgID), internalID, serMsg, destMemberKey, addr, nil, false)

// 	writeMsg(*addr, serMsg)
// }

// /*
// * Forwards and caches the request
// * @param conn The connection object to send messages over.
// * @param addr The address to send the message to.
// * @param returnAddr The address to forward the response to, nil if the response shouldn't be forwarded.
// * @param reqMsg The message to send.
//  */
// func forwardUDPRequest(addr *net.Addr, destMemberKey int, returnAddr *net.Addr, reqMsg *pb.InternalMsg) {
// 	isFirstHop := false

// 	// Update ID if we are forwarding an external request
// 	if reqMsg.InternalID == EXTERNAL_REQUEST {
// 		reqMsg.InternalID = FORWARDED_CLIENT_REQ
// 		isFirstHop = true
// 	}

// 	serMsg, err := proto.Marshal(reqMsg)
// 	if err != nil {
// 		log.Println(err)
// 	}

// 	// Add to request cache
// 	if reqMsg.InternalID == FORWARDED_CLIENT_REQ {
// 		// No need to cache serialized message if is a client request since client responsible for retries
// 		putReqCacheEntry(string(reqMsg.MessageID), uint8(reqMsg.InternalID), nil, destMemberKey, addr, returnAddr, isFirstHop)
// 	} else {
// 		putReqCacheEntry(string(reqMsg.MessageID), uint8(reqMsg.InternalID), serMsg, destMemberKey, addr, returnAddr, isFirstHop)
// 	}

// 	writeMsg(*addr, serMsg)
// }

// /**
// * Processes a request message and either forwards it or handles it at this node.
// * @param conn The connection object to send messages over.
// * @param returnAddr The IP address to send response to.
// * @param reqMsg The incoming message.
// * @param externalReqHandler The message handler callback for external messages (msgs passed to app layer).
//  */
// func processRequest(returnAddr net.Addr, reqMsg *pb.InternalMsg, externalReqHandler func([]byte) ([]byte, error)) {
// 	// Check if response is already cached
// 	resCache_.lock.Lock()
// 	res := resCache_.data.Get(string(reqMsg.MessageID))
// 	if res != nil {
// 		resCacheEntry := res.(ResCacheEntry)
// 		writeMsg(returnAddr, resCacheEntry.msg)
// 		// Reset timeout
// 		resCacheEntry.time = time.Now()
// 		resCache_.data.Put(string(reqMsg.MessageID), resCacheEntry) // TODO: does this overwrite?
// 		resCache_.lock.Unlock()
// 		return
// 	}
// 	resCache_.lock.Unlock()

// 	// Determine if an internal or external message
// 	if reqMsg.InternalID != EXTERNAL_REQUEST && reqMsg.InternalID != FORWARDED_CLIENT_REQ {
// 		// TODO: pass handler as arg?

// 		// Membership service is reponsible for sending response or forwarding the request
// 		// TODO: internalReqHandler(conn, returnAddr, reqMsg)
// 	} else {
// 		// TODO: determine if the key corresponds to this node
// 		// var addr *net.Addr = nil

// 		// TODO: If key corresponds to this node:
// 		// Pass message to handler
// 		payload, err := externalReqHandler(reqMsg.Payload)
// 		if err != nil {
// 			log.Println("WARN could not handle message. Sender = " + returnAddr.String())
// 			return
// 		}
// 		// Send response
// 		sendUDPResponse(returnAddr, reqMsg.MessageID, payload, reqMsg.InternalID == FORWARDED_CLIENT_REQ)

// 		// TODO: If key doesn't correspond to this node:
// 		// forwardUDPRequest(conn, addr, returnAddr, reqMsg)
// 	}

// }

// /**
// * Processes a response message.
// * @param conn The connection object to send messages over.
// * @param addr The IP address to send response to.
// * @param serialMsg The incoming message.
// * @param handler The message handler callback.
//  */
// func processResponse(resMsg *pb.InternalMsg) {
// 	// Get cached request (ignore if it's not cached)
// 	reqCache_.lock.Lock()
// 	req := reqCache_.data.Get(string(resMsg.MessageID))
// 	if req != nil {
// 		reqCacheEntry := req.(ReqCacheEntry)

// 		// If cached request has return address, forward the request
// 		if reqCacheEntry.returnAddr != nil {
// 			forwardUDPResponse(*reqCacheEntry.returnAddr, resMsg, !reqCacheEntry.isFirstHop)
// 		}

// 		// Otherwise simply remove the message from the queue
// 		// Note: We don't need any response handlers for now
// 		// TODO: what about for TRANSFER_FINISHED and MEMBERSHIP_REQUEST?
// 		reqCache_.data.Delete(resMsg.MessageID)

// 	} else {
// 		log.Println("WARN: Received response for unknown request")
// 	}
// 	reqCache_.lock.Unlock()
// }

// /**
// * Listens for incoming messages, processes them, and then passes them to the handler callback.
// * @param conn The network connection object.
// * @param externalReqHandler The external request handler callback.
// * @return An error message if failed to read from the connection.
//  */
// func msgListener(externalReqHandler func([]byte) ([]byte, error) /*, resHandler func([]byte) ([]byte, error)*/) error {
// 	buffer := make([]byte, MAX_BUFFER_SIZE)

// 	// Listen for packets
// 	for {
// 		//log.Println("Awaiting packet")
// 		n, returnAddr, err := (*conn).ReadFrom(buffer)
// 		if err != nil {
// 			return err
// 		}

// 		// Deserialize message
// 		msg := &pb.InternalMsg{}
// 		err = proto.Unmarshal(buffer[0:n], msg)
// 		if err != nil {
// 			// Disregard messages with invalid format
// 			log.Println("WARN msg with invalid format. Sender = " + returnAddr.String())
// 		}

// 		// Verify checksum
// 		if !verifyChecksum(msg) {
// 			// Disregard messages with invalid checksums
// 			log.Println("WARN checksum mismatch. Sender = " + returnAddr.String())
// 			continue
// 		}

// 		if msg.IsResponse {
// 			go processResponse(msg)
// 		} else {
// 			go processRequest(returnAddr, msg, externalReqHandler)
// 		}
// 	}
// }

// /***** GOSSIP PROTOCOL *****/

// const STATUS_NORMAL = 0x1
// const STATUS_BOOTSTRAPPING = 0x2
// const STATUS_UNAVAILABLE = 0x3

// const HEARTBEAT_INTERVAL = 1000 // ms

// // Maps msg ID to serialized response
// var memberStore_ *MemberStore
// var key_ int

// type Member struct {
// 	ip        string
// 	port      int
// 	key       int // Hash keys of all members (position in ring)
// 	heartbeat int
// 	status    int
// }

// type MemberStore struct {
// 	lock     sync.Mutex
// 	members  []Member
// 	position int
// }

// /**
// * Creates and returns a pointer to a new MemberStore
// * @return The member store.
//  */
// func NewMemberStore() *MemberStore {
// 	memberStore := new(MemberStore)
// 	return memberStore
// }

// /**
// * Sorts and updates the
// * @return The member store.
//  */
// func sortAndUpdateIdx() int {
// 	// TODO: lock
// 	sort.SliceStable(memberStore_.members, func(i, j int) bool {
// 		return memberStore_.members[i].key < memberStore_.members[j].key
// 	})

// 	// find and update index of the current key

// 	for i, _ := range memberStore_.members {
// 		if memberStore_.members[i].key == key_ {
// 			memberStore_.position = i
// 			// TODO: unlock
// 			return i
// 		}
// 	}

// 	// TODO: unlock

// 	// Should never get here!
// 	log.Println("Error: could not find own key in member array")
// 	return -1
// }

// /**
// * Updates the heartbeat by one.
//  */
// func tickHeartbeat() {
// 	memberStore_.lock.Lock()
// 	memberStore_.members[memberStore_.position].heartbeat += 1
// 	memberStore_.lock.Unlock()
// }

// // TOM
// // TASK3 (part 1): Membership protocol (bootstrapping process)
// func makeMembershipReq() {
// 	// Send request to random node (from list of nodes)

// 	// Repeat this request periodically until receive TRANSFER_FINISHED message
// 	// to protect against nodes failing (this will probably be more important for later milestones)
// }

// // Rozhan
// // TASK4 (part 1): Gossip heartbeat (send the entire member array in the MemberStore).
// func gossipHeartbeat() {
// 	// Package MemberStore.members array

// }

// // Shay
// // TASK3 (part 3): Membership protocol - transfers the necessary data to a joined node
// // The actual transfer from the succesor to the predecessor
// // Send a TRANSFER_FINISHED when it's done
// func transferToPredecessor( /* predecessor key */ ) {
// 	// Send all key-value pairs that is the responsibility of the predecessor
// 	// Use PUT requests (like an external client)
// }

// // TOM
// // TASK3 (part 2): Membership protocol
// func membershipReqHandler(addr net.Addr, msg *pb.InternalMsg) {
// 	// Find successor node and forward the
// 	// membership request there to start the transfer

// 	// If this node is the successor, start transferring keys

// 	// If this node is the successor and already in the process of
// 	// receiving or sending a transfer, respond with "Busy" to the request.
// 	// The node sending the request will then re-send it after waiting a bit.
// }

// // Rozhan
// // TASK4 (part 2): Compare incoming member array with current member array and
// // update entries to the one with the larger heartbeat (i.e. newer gossip)
// func heartbeatHandler(addr net.Addr, msg *pb.InternalMsg) {
// 	// Compare Members list and update as necessary
// 	// Need to ignore any statuses of "Unavailable" (or just don't send them)
// 	// since failure detection is local.

// 	// (Not a big priority for M1) If we receive a heartbeat update from a predecessor
// 	// that had status "Unavailable" at this node, then we can transfer any keys we were storing for it
// 	// - need to check version number before writing

// 	// Sort the member store if we just leaned of a new node
// 	sortAndUpdateIdx()
// }

// /* Ignore this */
// // func transferStartedHandler(addr net.Addr, msg *pb.InternalMsg) {
// // 	// Register that the transfer is started
// // 	// Start timeout, save IP of sender, and reset timer everytime receive a write from
// // 	// the IP address
// // 	// If timeout hit, set status to "Normal"
// // }

// // TOM + Shay
// // TASK3 (part 4): Membership protocol - transfer to this node is finished
// func transferFinishedHandler(addr net.Addr, msg *pb.InternalMsg) {
// 	// End timer and set status to "Normal"
// 	// Nodes will now start sending requests directly to us rather than to our successor.
// }

// func internalMsgHandler(conn *net.PacketConn, addr net.Addr, msg *pb.InternalMsg) {
// 	switch msg.InternalID {
// 	case MEMBERSHIP_REQUEST:
// 		membershipReqHandler(addr, msg)

// 	case HEARTBEAT:
// 		heartbeatHandler(addr, msg)

// 	case TRANSFER_FINISHED:
// 		transferFinishedHandler(addr, msg)

// 	default:
// 		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
// 	}
// }

// func bootstrap(conn *net.PacketConn, otherMembers []struct {
// 	string
// 	int
// }, port int) {
// 	memberStore_ = NewMemberStore()

// 	// TODO: problem with not being able to know your own IP?

// 	// TODO: Get Key here
// 	key_ = rand.Intn(100)

// 	var status int
// 	if len(otherMembers) == 0 {
// 		status = STATUS_NORMAL
// 	} else {
// 		status = STATUS_BOOTSTRAPPING
// 	}

// 	// Add this node to Member array
// 	memberStore_.members = append(memberStore_.members, Member{ip: "", port: port, key: key_, heartbeat: 0, status: status})
// 	memberStore_.position = 0

// 	// Update heartbeat every HEARTBEAT_INTERVAL seconds
// 	var ticker = time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL)
// 	go func() {
// 		for {
// 			<-ticker.C
// 			tickHeartbeat()
// 		}
// 	}()

// 	requestReplyLayerInit()

// 	// Send initial membership request message - this tells receiving node they should try to contact the successor first (as well as
// 	// respond with this node's IP address if needed)

// 	// If no other nodes are known, then assume this is the first node
// 	// and this node simply waits to be contacted
// 	if len(otherMembers) != 0 {
// 		makeMembershipReq()
// 	}

// }

// /************* APPLICATION CODE *************/
// const MAX_KEY_LEN = 32
// const MAX_VAL_LEN = 10000

// // REQUEST COMMANDS
// const PUT = 0x01
// const GET = 0x02
// const REMOVE = 0x03
// const SHUTDOWN = 0x04
// const WIPEOUT = 0x05
// const IS_ALIVE = 0x06
// const GET_PID = 0x07
// const GET_MEMBERSHIP_COUNT = 0x08

// // ERROR CODES
// const OK = 0x00
// const NOT_FOUND = 0x01
// const NO_SPACE = 0x02
// const OVERLOAD = 0x03
// const UKN_FAILURE = 0x04
// const UKN_CMD = 0x05
// const INVALID_KEY = 0x06
// const INVALID_VAL = 0x07

// const OVERLOAD_WAIT_TIME = 5000 // ms

// /* Reserve 38 MB of space for program, caching, and serving requests */
// const MAX_KV_STORE_SIZE = 90 * 1024 * 1024

// /* KEY VALUE STORE */
// var kvStore_ *KVStore
// var kvStoreSize_ uint32

// type KVStore struct {
// 	lock sync.Mutex
// 	data *list.List
// }

// type KVEntry struct {
// 	key string
// 	val []byte
// 	ver int32
// }

// /**
// * Creates and returns a pointer to a new key-value store.
// * @return The key-value store.
//  */
// func NewKVStore() *KVStore {
// 	store := new(KVStore)
// 	store.data = list.New()
// 	return store
// }

// /**
// * Fetches an element from the key-value store list.
// * @param key The key to search for.
// * @return The KVEntry if it exists, nil otherwise.
//  */
// func findListElem(key string) *KVEntry {
// 	for e := kvStore_.data.Front(); e != nil; e = e.Next() {
// 		if strings.Compare(e.Value.(KVEntry).key, key) == 0 {
// 			entry := e.Value.(KVEntry)
// 			return &entry
// 		}
// 	}
// 	return nil
// }

// /**
// * Removes an element from the key-value store list.
// * @param key The key of the entry to remove.
// * @return The KVEntry if it exists, nil otherwise.
//  */
// func removeListElem(key string) (bool, int) {
// 	for e := kvStore_.data.Front(); e != nil; e = e.Next() {
// 		if strings.Compare(e.Value.(KVEntry).key, key) == 0 {
// 			len := len(e.Value.(KVEntry).key) + len(e.Value.(KVEntry).val) + 4
// 			kvStore_.data.Remove(e)
// 			return true, len
// 		}
// 	}
// 	return false, 0
// }

// /**
// * Puts an entry in the key-value store.
// * @param key The key.
// * @param val The value.
// * @param version The key-value pair version number.
// * @return OK if there is space, NO_SPACE if the store is full.
//  */
// func putKVEntry(key string, val []byte, version int32) uint32 {
// 	kvStore_.lock.Lock()

// 	// Remove needed to decrement kvStoreSize_ if key already exists
// 	removeKVEntry(key) // *neeeds to be in critical section

// 	// Check if the store is full
// 	if kvStoreSize_ > MAX_KV_STORE_SIZE {
// 		kvStore_.lock.Unlock()
// 		return NO_SPACE
// 	}

// 	kvStore_.data.PushBack(KVEntry{key: key, val: val, ver: version})
// 	kvStoreSize_ += uint32(len(key) + len(val) + 4) // Increase kv store size
// 	kvStore_.lock.Unlock()                          // TODO: this caused fatal error???

// 	log.Println(kvStoreSize_)

// 	return OK
// }

// /**
// * Gets an entry from the key-value store.
// * @param key The key of the entry to retrieve.
// * @return The KVEntry if it exists.
// * @return OK if the key exists, NOT_FOUND otherwise.
//  */
// func getKVEntry(key string) (KVEntry, uint32) {
// 	var found bool
// 	var entry KVEntry
// 	kvStore_.lock.Lock()
// 	res := findListElem(key)
// 	if res != nil {
// 		found = true
// 		entry = *res
// 	} else {
// 		found = false
// 	}
// 	kvStore_.lock.Unlock()

// 	if found {
// 		return entry, OK
// 	} else {
// 		return KVEntry{}, NOT_FOUND
// 	}
// }

// /**
// * Removes an entry from the key-value store.
// * @param key The key of the entry to remove.
// * @return OK if the key exists, NOT_FOUND otherwise.
//  */
// func removeKVEntry(key string) uint32 {
// 	success, n := removeListElem(key)
// 	if success == true {
// 		kvStoreSize_ -= uint32(n) // Decrease kv store size
// 	}

// 	log.Println(kvStoreSize_)

// 	if success == true {
// 		return OK
// 	} else {
// 		return NOT_FOUND
// 	}
// }

// /**
// * Returns the process' memory usage in bytes.
// * @return the memory usage.
//  */
// func memUsage() uint64 {
// 	var stats runtime.MemStats
// 	runtime.ReadMemStats(&stats)
// 	return (stats.Alloc + stats.StackSys)
// }

/**
* Prints the process' memory statistics.
* Source: https://golangcode.com/print-the-current-memory-usage/
 */
func PrintMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	log.Printf("\t Stack = %v\n", bToMb(m.StackSys))
	log.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	log.Printf("\tSys = %v MiB", bToMb(m.Sys))
	log.Printf("\tNum GC cycles = %v\n", m.NumGC)
}

/**
* Converts bytes to megabytes.
* @param b The byte amount.
* @return The corresponding MB amount.
* Source: https://golangcode.com/print-the-current-memory-usage/
 */
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// /**
// * Handles incoming requests.
// * @param serializedReq The serialized KVRequest.
// * @return A serialized KVResponse, nil if there was an error.
// * @return Error object if there was an error, nil otherwise.
//  */
// func handleOverload() *pb.KVResponse {
// 	log.Println("Overloaded: " + strconv.Itoa(int(memUsage())))
// 	PrintMemStats()

// 	debug.FreeOSMemory() // Force GO to free unused memory

// 	kvRes := &pb.KVResponse{}
// 	wait := int32(OVERLOAD_WAIT_TIME)
// 	kvRes.OverloadWaitTime = &wait

// 	return kvRes
// }

// /**
// * Handles incoming requests.
// * @param serializedReq The serialized KVRequest.
// * @return A serialized KVResponse, nil if there was an error.
// * @return Error object if there was an error, nil otherwise.
//  */
// func requestHandler(serializedReq []byte) ([]byte, error) {
// 	var errCode uint32
// 	kvRes := &pb.KVResponse{}

// 	/* NOTE: When there is an OVERLOAD and we are reaching the memory limit,
// 	we only restrict PUT and GET requests. REMOVE and WIPEOUT may increase
// 	the memory momentarily, but the benifit of the freed up space outweighs
// 	the momentary costs. */

// 	// Unmarshal KVRequest
// 	kvRequest := &pb.KVRequest{}
// 	err := proto.Unmarshal(serializedReq, kvRequest)
// 	if err != nil {
// 		return nil, err
// 	}

// 	cmd := kvRequest.Command
// 	key := string(kvRequest.Key)
// 	value := kvRequest.Value
// 	var version int32
// 	if kvRequest.Version != nil {
// 		version = *kvRequest.Version
// 	} else {
// 		version = 0
// 	}

// 	// Determine action based on the command
// 	switch cmd {
// 	case PUT:
// 		if len(key) > MAX_KEY_LEN {
// 			errCode = INVALID_KEY
// 		} else if len(value) > MAX_VAL_LEN {
// 			errCode = INVALID_VAL
// 		} else if memUsage() > MAX_MEM_USAGE {
// 			kvRes = handleOverload()
// 			errCode = OVERLOAD
// 		} else {
// 			errCode = putKVEntry(key, value, version)
// 		}

// 	case GET:
// 		if len(key) > MAX_KEY_LEN {
// 			errCode = INVALID_KEY
// 		} else if memUsage() > MAX_MEM_USAGE {
// 			kvRes = handleOverload()
// 			errCode = OVERLOAD
// 		} else {
// 			entry, code := getKVEntry(key)
// 			if code == OK {
// 				kvRes.Value = entry.val
// 				kvRes.Version = &entry.ver
// 			}

// 			errCode = code
// 		}

// 	case REMOVE:
// 		if len(key) > MAX_KEY_LEN {
// 			errCode = INVALID_KEY
// 		} else {
// 			kvStore_.lock.Lock()
// 			errCode = removeKVEntry(key)
// 			kvStore_.lock.Unlock()
// 		}

// 	case SHUTDOWN:
// 		os.Exit(1)

// 	case WIPEOUT:
// 		kvStore_.data.Init() // Clears the list
// 		debug.FreeOSMemory() // Force GO to free unused memory
// 		errCode = OK
// 		kvStoreSize_ = 0

// 	case IS_ALIVE:
// 		errCode = OK

// 	case GET_PID:
// 		pid := int32(os.Getpid())
// 		kvRes.Pid = &pid
// 		errCode = OK

// 	case GET_MEMBERSHIP_COUNT:
// 		tmpMemCount := int32(1) // Note: this will need to be updated in later PA
// 		kvRes.MembershipCount = &tmpMemCount
// 		errCode = OK

// 	default:
// 		errCode = UKN_CMD

// 	}

// 	kvRes.ErrCode = errCode

// 	// Marshal KV response and return it
// 	resPayload, err := proto.Marshal(kvRes)
// 	if err != nil {
// 		log.Println("Marshaling payload error. ", err.Error())
// 		return nil, err
// 	}

// 	return resPayload, nil
// }

// /**
// * Runs the server. Should never return unless an error is encountered.
// * @param port The port to listen for UDP packets on.
// * @return an error
//  */
// func runServer(otherMembers []struct {
// 	string
// 	int
// }, port int) error {
// 	// Listen on all available IP addresses
// 	connection, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
// 	if err != nil {
// 		return err
// 	}
// 	defer connection.Close()

// 	conn = &connection

// 	// Bootstrap node
// 	bootstrap(conn, otherMembers, port)

// 	kvStore_ = NewKVStore()

// 	err = msgListener(requestHandler /*, internalMsgHandler */)

// 	// Should never get here if everything is working
// 	return err
// }

// func main() {

// 	// TASK 2: Dockerize project and parse port and member list file arguments.
// 	// I added the hardcoded "nodes" array that could be used for testing.
// 	// - Rozhan

// 	// Parse cmd line args
// 	arguments := os.Args
// 	if len(arguments) != 2 {
// 		fmt.Printf("ERROR: Expecting 1 argument (received %d): Port #", len(arguments)-1)
// 		return
// 	}

// 	port, err := strconv.Atoi(arguments[1])
// 	if err != nil {
// 		fmt.Println("ERROR: Port is not a valid number")
// 		return
// 	}

// 	if port < 1 || port > 65535 {
// 		fmt.Println("ERROR: Invalid port number (must be between 1 and 65535)")
// 		return
// 	}

// 	// TODO: parse file with IP addresses and ports of other nodes
// 	nodes := []struct {
// 		string
// 		int
// 	}{{"127.0.0.1", 12345}, {"127.0.0.1", 12346}}

// 	err = runServer(nodes, port)
// 	if err != nil {
// 		fmt.Println("Server encountered an error. " + err.Error())
// 	}

// 	fmt.Println("Server closed")
// }
