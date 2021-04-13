package requestreply

import (
	"encoding/binary"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
)

// global variable - the UDP connection
var conn *net.PacketConn

/* Internal Msg IDs */
const EXTERNAL_MSG = 0x0
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
const FORWARD_ACK_RES = 0xB //for acknowledging FORWARDED_CLIENT_REQ

// Only receive transfer request during bootstrapping

const INTERNAL_REQ_RETRIES = 1

const MAX_BUFFER_SIZE = 11000

/**
* Initializes the request/reply layer. Must be called before using
* request/reply layer to get expected functionality.
* @param connection a UDP connection object for this server
* @param *Handler a specification of which handler function to be chosen for each handler
 */
func RequestReplyLayerInit(connection *net.PacketConn,
	externalReqHandler externalReqHandlerFunc,
	internalReqHandler internalReqHandlerFunc,
	nodeUnavailableHandler func(addr *net.Addr),
	internalResHandler func(addr net.Addr, msg *pb.InternalMsg)) {

	/* set req/resp handlers */
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

	// set up the UDP connection for this server
	conn = connection
}

/**
* Generates a unique 16 byte ID based on the client's IP, port number, and current time
* @param clientIP The client's IP address.
* @param port Server port number.
* @return The unique ID as a 16 byte long byte array.
 */
func getmsgID(clientIP string, port uint16) []byte {

	// process client's IP
	ipArr := strings.Split(clientIP, ".")
	ipBytes := make([]byte, 5)
	for i, s := range ipArr {
		val, _ := strconv.Atoi(s)
		binary.LittleEndian.PutUint16(ipBytes[i:], uint16(val))
	}
	ipBytes = ipBytes[0:4]

	// process client's port number
	portBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(portBytes, port)

	// introduce uniqueness
	randBytes := make([]byte, 2)
	rand.Read(randBytes)

	// process time of day
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(time.Now().UnixNano()))

	id := append(append(append(ipBytes, portBytes...), randBytes...), timeBytes...)
	return id
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msg The received message.
* @return True if message ID matches the expected ID and checksum is valid, false otherwise.
 */
func verifyChecksum(msg *pb.InternalMsg) bool {
	// Verify MessageID is as expected
	return uint64(util.ComputeChecksum((*msg).MessageID, (*msg).Payload)) == (*msg).CheckSum
}

/**
* Writes a message to the connection.
* @param addr The IP address to send to.
* @param msg The message to send.
 */
func writeMsg(addr net.Addr, msg []byte) {
	/*****************DEBUGGING**************************
	// Deserialize message
	message := &pb.InternalMsg{}
	err1 := proto.Unmarshal(msg, message)
	if err1 != nil {
		// Disregard messages with invalid format
		log.Println("WARN msg with invalid format to" + addr.String())
	}
	log.Println("Sending message isResponse = ", message.IsResponse, message.MessageID, "of type", message.InternalID, "to", addr.String())
	***************************************************/

	_, err := (*conn).WriteTo(msg, addr)
	if err != nil {
		log.Println(err)
	}
}

/**
* Sends a UDP message responding to a request.
* @param addr The IP address to send to.
* @param msgID The message id.
* @param payload The message payload.
* @param internal_id The internal message id.
* @param isInternal true if responding to an internal message.
 */
func sendUDPResponse(addr net.Addr, msgID []byte, payload []byte, internal_id uint32, isInternal bool) {

	// serialize and add to resCache
	serMsg, err := cacheUDPResponse(msgID, payload, internal_id, isInternal, true)
	if err != nil {
		log.Println(err)
		return
	}

	writeMsg(addr, serMsg)

	/**************DEBUGGING*******************
	if isInternal {
		log.Println("Sending UDP response for msg with msgID", msgID, "internal id ", internal_id, "to", addr.String())
	}
	******************************************/
}

/**
* Send an Acknowledgement for a UDP request (with nil payload)
* @param addr The IP address to send to.
* @param msgID The message id.
* @param internal_id The internal message id.
 */
func sendUDPAck(addr net.Addr, msgID []byte, internal_id uint32) {

	checksum := util.ComputeChecksum(msgID, nil)

	// construct message
	resMsg := &pb.InternalMsg{
		MessageID:  msgID,
		Payload:    nil,
		CheckSum:   uint64(checksum),
		InternalID: internal_id,
		IsResponse: true,
	}

	// serialize
	serMsg, err := proto.Marshal(resMsg)
	if err != nil {
		log.Println(err)
	}

	writeMsg(addr, serMsg)
}

/**
* Cache a UDP request message and return the serialized message.
* @param msgID The message id.
* @param payload The message payload.
* @param internal_id The internal message id.
* @param isInternal true if responding to an internal message.
* @param isReady true if the response is ready to be sent to the client
* @return the serialized message
* @return an error in case of failure
 */
func cacheUDPResponse(msgID []byte, payload []byte, internal_id uint32, isInternal bool, isReady bool) ([]byte, error) {

	checksum := util.ComputeChecksum(msgID, payload)

	// construct message
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

	// serialize
	serMsg, err := proto.Marshal(resMsg)
	if err != nil {
		return nil, err
	}

	// Cache message
	putResCacheEntry(string(msgID), serMsg, isReady)

	return serMsg, err
}

/*
* Forwards and caches a response.
* @param addr The address to send the message to.
* @param resMsg The message to send.
* @param isInternal True if the response is being sent to another server node, false if being sent to a client.
 */
func forwardUDPResponse(addr net.Addr, resMsg *pb.InternalMsg, isInternal bool) {
	// Remove internal fields if forwarding to a client (external message)
	if !isInternal {

		// construct message
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

	// serialize
	serMsg, err := proto.Marshal(resMsg)
	if err != nil {
		log.Println(err)
	}

	// Cache message
	putResCacheEntry(string(resMsg.MessageID), serMsg, false) //the entry is not ready to be sent by default

	writeMsg(addr, serMsg)
}

/*
* Forwards and caches the request
* @param addr The address to send the message to.
* @param returnAddr The address to forward the response to, nil if the response shouldn't be forwarded.
* @param reqMsg The message to send.
* @param isForwardedChainUpdate flag to monitor if request is an update within a chain
 */
func forwardUDPRequest(addr *net.Addr, returnAddr *net.Addr, reqMsg *pb.InternalMsg, isForwardedChainUpdate bool) {

	// flag to monitor if message has been routed before
	isFirstHop := false

	// Update ID if we are forwarding an external request
	if reqMsg.InternalID == EXTERNAL_MSG {
		reqMsg.InternalID = FORWARDED_CLIENT_REQ
		isFirstHop = true
	}

	// Update ID if we are forwarding an a chain update
	if isForwardedChainUpdate {
		reqMsg.InternalID = FORWARDED_CHAIN_UPDATE_REQ
	}

	// serialize
	serMsg, err := proto.Marshal(reqMsg)
	if err != nil {
		log.Println(err)
	}

	// Add to request cache
	if reqMsg.InternalID == FORWARDED_CLIENT_REQ {
		// No need to cache serialized message if is a client request since client responsible for retries
		putReqCacheEntry(string(reqMsg.MessageID), uint8(reqMsg.InternalID), nil, addr, returnAddr, isFirstHop)
	} else {
		// cache normally for all other requests
		putReqCacheEntry(string(reqMsg.MessageID), uint8(reqMsg.InternalID), serMsg, addr, returnAddr, isFirstHop)
	}

	writeMsg(*addr, serMsg)
}

/**
* Processes a request message and either forwards it or handles it at this node.
* @param returnAddr The IP address to send response to.
* @param reqMsg The incoming message.
* @param externalReqHandler The message handler callback for external messages (msgs passed to app layer).
 */
func processRequest(returnAddr net.Addr, reqMsg *pb.InternalMsg) {
	//DEBUGGING
	// log.Println("Received request of type", reqMsg.GetInternalID(), "from", returnAddr.String())

	//  get response corresponding to message ID from the resCache
	resCache_.lock.Lock()
	res := resCache_.data.Get(string(reqMsg.MessageID))
	resCache_.lock.Unlock()

	// Check if response is already cached
	if res != nil {
		resCacheEntry := res.(ResCacheEntry)
		log.Println("Found response for", reqMsg.MessageID, "in resCache")

		if resCacheEntry.isReady {
			if reqMsg.GetAddr() != nil {
				log.Println("Sending cached reply to", reqMsg.GetAddr().String())
				writeMsg(reqMsg.GetAddr(), resCacheEntry.msg)
			} else {
				log.Println("Sending cached reply to", returnAddr.String())
				writeMsg(returnAddr, resCacheEntry.msg)
			}
		} //ignore request if it's not propagated through the chain yet and the response is not ready
		return
	}

	// Determine if an internal or external message
	// TODO: handle DATA_TRANSFER_MSG case
	if reqMsg.InternalID != EXTERNAL_MSG && reqMsg.InternalID != FORWARDED_CLIENT_REQ {
		// Membership service is responsible for sending response or forwarding the request
		fwdAddr, respond, payload, _, err := getInternalReqHandler()(returnAddr, reqMsg)
		if err != nil {
			log.Println("WARN could not handle message. Sender = " + returnAddr.String())
			return
		}
		if fwdAddr != nil {
			forwardUDPRequest(fwdAddr, &returnAddr, reqMsg, false)
		} else if respond {
			id := reqMsg.InternalID
			// Only cache the response if necessary
			if id == PING_MSG || id == HEARTBEAT_MSG || id == DATA_TRANSFER_MSG {
				sendUDPAck(returnAddr, reqMsg.MessageID, id)
			} else {
				sendUDPResponse(returnAddr, reqMsg.MessageID, payload, id, true)
			}
		}
		return
	}

	if reqMsg.GetAddr() == nil {
		reqMsg.Addr = util.CreateAddressStringFromAddr(&returnAddr)
	}

	ProcessExternalRequest(reqMsg, returnAddr)

}

/**
* Processes an external request message and routes to the correct node as needed.
* @param reqMsg The incoming message.
* @param messageSender The node to which an acknowledgement is sent.
 */
func ProcessExternalRequest(reqMsg *pb.InternalMsg, messageSender net.Addr) {
	returnAddr := reqMsg.GetAddr()

	//ExternalReqHandler returns a forward address if the request is to be forwarded
	//to another node and a response payload for any non-kvrequest
	//key-value requests are passed to the chain replication layer and queued so
	//no payload
	fwdAddr, respondToClient, payload, err := getExternalReqHandler()(messageSender, reqMsg)
	if err != nil {
		log.Println("WARN: could not handle message. Sender = " + returnAddr.String())
		return
	}
	// check if request is a forwarded client request (external)
	if reqMsg.InternalID == FORWARDED_CLIENT_REQ {
		// acknowledge forwarded client request
		sendUDPAck(messageSender, reqMsg.MessageID, FORWARDED_CLIENT_REQ)
	}

	// forward or respond to the request
	if respondToClient {
		// Send response to client
		sendUDPResponse(returnAddr, reqMsg.MessageID, payload, reqMsg.InternalID, false)
	} else if fwdAddr != nil {
		// Forward request if key doesn't correspond to this node:
		forwardUDPRequest(fwdAddr, nil, reqMsg, false)
	}
}

/**
* Generate response to a chain request.
* @param fwdAddr the node to forward the request to.
* @param respondAddr node's address to respond to (if this is an intermediate node or the tail).
* @param isTail indicator if this node is the tail of the chain corresponding the request key.
* @param reqMSg the request message used for constructing the response.
* @param payload the payload of the response message.
 */
func RespondToChainRequest(fwdAddr *net.Addr, respondAddr *net.Addr, isTail bool, reqMsg *pb.InternalMsg, payload []byte) {
	/************DEBUGGING****************/
	res := &pb.KVResponse{}
	proto.Unmarshal(payload, res)
	/**************************************/

	isForwardedChainUpdate := reqMsg.InternalID == FORWARDED_CHAIN_UPDATE_REQ

	if isTail {
		// DEBUGGING
		// log.Println("Sending response to client", reqMsg.GetAddr().String(), "for value", kvstore.BytetoInt(res.GetValue()))

		// The client's address is stored in the address field of the message
		sendUDPResponse(reqMsg.GetAddr(), reqMsg.MessageID, payload, reqMsg.InternalID, false)

		if isForwardedChainUpdate {
			//respond to forwarded chain update
			sendUDPResponse(*respondAddr, reqMsg.MessageID, payload, reqMsg.InternalID, true)
		}
	} else if isForwardedChainUpdate {
		if fwdAddr != nil {
			//responses are automatically forwarded back to the HEAD since we set the respondAddr
			forwardUDPRequest(fwdAddr, respondAddr, reqMsg, true)
		}
	} else {
		//HEAD of chain -- cache the response for at-most-once policy
		cacheUDPResponse(reqMsg.MessageID, payload, 0, false, false)
		if fwdAddr != nil {
			forwardUDPRequest(fwdAddr, nil, reqMsg, true)
		}
	}
}

/**
* Processes a response message.
* @param addr The IP address to send response to.
* @param serialMsg The incoming message.
* @param handler The message handler callback.
 */
func processResponse(senderAddr net.Addr, resMsg *pb.InternalMsg) {

	//util.PrintInternalMsg(resMsg)

	// Get cached request (ignore if it's not cached)
	reqCache_.lock.Lock()
	key := string(resMsg.MessageID) + strconv.Itoa(int(resMsg.InternalID))
	req := reqCache_.data.Get(key)
	reqCache_.lock.Unlock()
	if req != nil {
		reqCacheEntry := req.(ReqCacheEntry)

		/************DEBUGGING****************
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("Received response", resMsg.MessageID, "of type", resMsg.InternalID, "for value", kvstore.BytetoInt(res.GetValue()))

		**************************************/

		// If cached request has return address, forward the request
		if reqCacheEntry.returnAddr != nil {
			forwardUDPResponse(*reqCacheEntry.returnAddr, resMsg, !reqCacheEntry.isFirstHop)
		}

		// update resCache if chain update request
		if resMsg.InternalID == FORWARDED_CHAIN_UPDATE_REQ {
			//update the cache to make request available
			//assumes no failure!!!
			updated := setReadyResCacheEntry(resMsg.MessageID)
			if !updated {
				log.Println("WARN: the cached response for updated chain request from", senderAddr.String(), "was not found")
			}
		}

		// handle response
		getInternalResHandler()(senderAddr, resMsg)

		// TODO: message handler for internal client requests w/o a return address (PUT requests during transfer)
		// (not needed, just use FORWARDED_EXTERNAL_REQ)

		// Otherwise simply remove the message from the queue
		// Note: We don't need any response handlers for now
		// TODO: Possible special handling for TRANSFER_FINISHED_MSG and MEMBERSHIP_REQ
		reqCache_.lock.Lock()
		reqCache_.data.Delete(key)
		reqCache_.lock.Unlock()
	} else {
		log.Println("WARN: Received response for unknown request of type", resMsg.InternalID)
		/************DEBUGGING***************
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("WARN: Received response", resMsg.IsResponse, "of type", resMsg.InternalID, "for unknown request with msgID", resMsg.MessageID, "from", senderAddr.String(), "for value", kvstore.BytetoInt(res.GetValue()))
		***************************************/
	}
}

/*
* Creates, sends and caches a request.
* @param addr The address to send the message to.
* @param payload The request payload.
* @param internalID The internal message type.
 */
// NOTE: this will be used for sending internal messages
func sendUDPRequest(addr *net.Addr, payload []byte, internalID uint8) {
	// extract IP and port number
	ip := (*conn).LocalAddr().(*net.UDPAddr).IP.String()
	port := (*conn).LocalAddr().(*net.UDPAddr).Port

	// generate unique message ID and checksum
	msgID := getmsgID(ip, uint16(port))
	checksum := util.ComputeChecksum(msgID, payload)

	// construct request message
	reqMsg := &pb.InternalMsg{
		MessageID:  msgID,
		Payload:    payload,
		CheckSum:   uint64(checksum),
		InternalID: uint32(internalID),
		IsResponse: false,
	}

	// serialize
	serMsg, err := proto.Marshal(reqMsg)
	if err != nil {
		log.Println(err)
	}

	// Add to request cache
	// don't cache membership requests and transfer requests because we don't expect a response
	if internalID != MEMBERSHIP_REQ && internalID != TRANSFER_REQ && internalID != DATA_TRANSFER_MSG {
		putReqCacheEntry(string(msgID), internalID, serMsg, addr, nil, false)
	}

	writeMsg(*addr, serMsg)
}

/**
* Listens for incoming messages, processes them, and then passes them to the handler callback.
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

		// process based on message type
		if msg.IsResponse {
			go processResponse(senderAddr, msg)
		} else {
			go processRequest(senderAddr, msg)
		}
	}
}
