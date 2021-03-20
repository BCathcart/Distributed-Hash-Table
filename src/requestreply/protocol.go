package requestreply

import (
	"encoding/binary"
	"hash/crc32"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"google.golang.org/protobuf/proto"
)

var conn *net.PacketConn

/* Internal Msg IDs */
const EXTERNAL_REQ = 0x0
const MEMBERSHIP_REQ = 0x1
const HEARTBEAT_MSG = 0x2
const TRANSFER_FINISHED_MSG = 0x3
const FORWARDED_CLIENT_REQ = 0x4
const PING_MSG = 0x5
const TRANSFER_REQ = 0x6
const DATA_TRANSFER_MSG = 0x7
const FORWARDED_CHAIN_UPDATE_REQ = 0x8
const TRANSFER_RES = 0x9
const GENERIC_RES = 0xA

// Only receive transfer request during bootstrapping

const INTERNAL_REQ_RETRIES = 1

const MAX_BUFFER_SIZE = 11000

/**
* Initializes the request/reply layer. Must be called before using
* request/reply layer to get expected functionality.
 */
func RequestReplyLayerInit(connection *net.PacketConn,
	externalReqHandler func(*pb.InternalMsg) (*net.Addr, bool, []byte, error),
	internalReqHandler func(net.Addr, *pb.InternalMsg) (*net.Addr, bool, []byte, int, error),
	nodeUnavailableHandler func(addr *net.Addr),
	internalResHandler func(addr net.Addr, msg *pb.InternalMsg)) {

	/* Store handlers */
	setExternalReqHandler(externalReqHandler)
	setInternalReqHandler(internalReqHandler)
	setNodeUnavailableHandler(nodeUnavailableHandler)
	setInternalResHandler(internalResHandler)

	/* Set up response cache */
	resCache_ = NewCache()

	// Sweep cache every RES_CACHE_TIMEOUT seconds
	var ticker = time.NewTicker(time.Second * RES_CACHE_TIMEOUT)

	go func() {
		for {
			<-ticker.C
			sweepResCache()
		}
	}()

	/* Set up request cache */
	reqCache_ = NewCache()

	// Sweep cache every REQ_CACHE_TIMEOUT seconds
	var ticker2 = time.NewTicker(time.Millisecond * REQ_RETRY_TIMEOUT_MS)

	go func() {
		for {
			<-ticker2.C
			sweepReqCache()
		}
	}()

	conn = connection
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
func sendUDPResponse(addr net.Addr, msgID []byte, payload []byte, internal_id int, isInternal bool) {
	checksum := computeChecksum(msgID, payload)

	resMsg := &pb.InternalMsg{
		MessageID: msgID,
		Payload:   payload,
		CheckSum:  uint64(checksum),
	}

	// Specify it is a response if the destination is another server
	if isInternal {
		resMsg.InternalID = uint32(internal_id)
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
		/************DEBUGGING****************
		kvResponse := &pb.KVResponse{}
		proto.Unmarshal(resMsg.GetPayload(), kvResponse)
		log.Println("Sending Response to client VALUE:", kvstore.BytetoInt(kvResponse.GetValue()))
		**************************************/
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
func forwardUDPRequest(addr *net.Addr, returnAddr *net.Addr, reqMsg *pb.InternalMsg, isForwardedChainUpdate bool) {
	isFirstHop := false

	// Update ID if we are forwarding an external request
	if reqMsg.InternalID == EXTERNAL_REQ {
		reqMsg.InternalID = FORWARDED_CLIENT_REQ
		isFirstHop = true
	}

	if isForwardedChainUpdate {
		reqMsg.InternalID = FORWARDED_CHAIN_UPDATE_REQ
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
func processRequest(returnAddr net.Addr, reqMsg *pb.InternalMsg) {
	log.Println("Received request of type", reqMsg.GetInternalID())

	var responseType = GENERIC_RES

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
	// TODO: handle DATA_TRANSFER_MSG case
	if reqMsg.InternalID != EXTERNAL_REQ && reqMsg.InternalID != FORWARDED_CLIENT_REQ {
		// Membership service is responsible for sending response or forwarding the request
		fwdAddr, respond, payload, responseType, err := getInternalReqHandler()(returnAddr, reqMsg)
		if err != nil {
			log.Println("WARN could not handle message. Sender = " + returnAddr.String())
			return
		}
		if fwdAddr != nil {
			forwardUDPRequest(fwdAddr, &returnAddr, reqMsg, false)
		} else if respond {
			sendUDPResponse(returnAddr, reqMsg.MessageID, payload, responseType, true)
		}
		return
	}

	fwdAddr, isForwardedChainUpdate, payload, err := getExternalReqHandler()(reqMsg)
	if err != nil {
		log.Println("WARN could not handle message. Sender = " + returnAddr.String())
		return
	}

	if fwdAddr == nil {
		// Send response
		sendUDPResponse(returnAddr, reqMsg.MessageID, payload, responseType, reqMsg.InternalID != EXTERNAL_REQ)
	} else {
		// Forward request if key doesn't correspond to this node:
		forwardUDPRequest(fwdAddr, &returnAddr, reqMsg, isForwardedChainUpdate)
	}

}

/**
* Processes a response message.
* @param conn The connection object to send messages over.
* @param addr The IP address to send response to.
* @param serialMsg The incoming message.
* @param handler The message handler callback.
 */
func processResponse(senderAddr net.Addr, resMsg *pb.InternalMsg) {
	log.Println("Received response of type", resMsg.GetInternalID())

	//util.PrintInternalMsg(resMsg)
	// Get cached request (ignore if it's not cached)
	log.Println("LOCKING REQ CACHE")
	reqCache_.lock.Lock()
	log.Println("REQ CACHE LOCKED")
	req := reqCache_.data.Get(string(resMsg.MessageID))
	if req != nil {
		reqCacheEntry := req.(ReqCacheEntry)
		/************DEBUGGING****************
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		**************************************/

		// If cached request has return address, forward the request
		if reqCacheEntry.returnAddr != nil {
			// log.Println("Forwarding response for request of type", reqCacheEntry.msgType, "for value", kvstore.BytetoInt(res.GetValue()), "to", (*reqCacheEntry.returnAddr).String())

			forwardUDPResponse(*reqCacheEntry.returnAddr, resMsg, !reqCacheEntry.isFirstHop)
		} else {
			// log.Println("Received response for request of type", reqCacheEntry.msgType, "for value", kvstore.BytetoInt(res.GetValue()))
		}

		log.Println("Calling getInternalResHandler()")

		getInternalResHandler()(senderAddr, resMsg)

		log.Println("Returning from getInternalResHandler()")

		// TODO: message handler for internal client requests w/o a return address (PUT requests during transfer)
		// (not needed, just use FORWARDED_EXTERNAL_REQ)

		// Otherwise simply remove the message from the queue
		// Note: We don't need any response handlers for now
		// TODO: Possible special handling for TRANSFER_FINISHED_MSG and MEMBERSHIP_REQ
		reqCache_.data.Delete(string(resMsg.MessageID))

	} else {
		log.Println("WARN: Received response for unknown request of type", resMsg.InternalID)
		/************DEBUGGING****************
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("WARN: Received response for unknown request of type", resMsg.InternalID, "for value", kvstore.BytetoInt(res.GetValue()))
		***************************************/
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
func sendUDPRequest(addr *net.Addr, payload []byte, internalID uint8) {
	log.Println("SEND UDP REQUEST with ID ", internalID)

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
	// don't cache membership requests because we don't expect a response
	if internalID != MEMBERSHIP_REQ {
		putReqCacheEntry(string(msgID), internalID, serMsg, addr, nil, false)
	}

	writeMsg(*addr, serMsg)
}

/**
* Listens for incoming messages, processes them, and then passes them to the handler callback.
* @param conn The network connection object.
* @param externalReqHandler The external request handler callback.
* @return An error message if failed to read from the connection.
 */
func MsgListener() error {
	buffer := make([]byte, MAX_BUFFER_SIZE)

	// Listen for packets
	for {
		n, senderAddr, err := (*conn).ReadFrom(buffer)
		if err != nil {
			return err
		}

		// Deserialize message
		msg := &pb.InternalMsg{}
		err = proto.Unmarshal(buffer[0:n], msg)
		if err != nil {
			// Disregard messages with invalid format
			log.Println("WARN msg with invalid format. Sender = " + senderAddr.String())
		}

		// Verify checksum
		if !verifyChecksum(msg) {
			// Disregard messages with invalid checksums
			log.Println("WARN checksum mismatch. Sender = " + senderAddr.String())
			continue
		}

		if msg.IsResponse {
			go processResponse(senderAddr, msg)
		} else {
			go processRequest(senderAddr, msg)
		}
	}
}
