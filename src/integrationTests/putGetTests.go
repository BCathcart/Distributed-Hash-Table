package main

import (
	//b64 "encoding/base64"
	"encoding/binary"
	"fmt"
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

const TIMEOUT_SEC = 3
const MAX_BUFFER_SIZE = 65527
const NUM_RETRIES = 5

const PUT = 0x01
const GET = 0x02
const REMOVE = 0x03
const SHUTDOWN = 0x04
const WIPEOUT = 0x05
const IS_ALIVE = 0x06
const GET_PID = 0x07
const GET_MEMBERSHIP_COUNT = 0x08

/*** REQUEST/REPLY PROTOCOL CODE ***/
/**
* Generates a unique 16 byte ID.
* @param clientIp The client's IP address.
* @param port Server port number.
* @return The unique ID as a 16 byte long byte array.
 */

var portKeyMap = make(map[int]uint32)
var prevRequests []*pb.KVRequest

func getMsgId(clientIp string, port uint16) []byte {
	ipArr := strings.Split(clientIp, ".")
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
func computeChecksum(msgId []byte, msgPayload []byte) uint32 {
	return crc32.ChecksumIEEE(append(msgId, msgPayload...))
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msg The received message.
* @param expectedMsgId The expected message ID to compare against.
* @return True if message ID matches the expected ID and checksum is valid, false otherwise.
 */
func verifyRcvdMsg(msg *pb.Msg, expectedMsgId []byte) bool {
	// Verify MessageID is as expected
	msgId := (*msg).MessageID
	for i := 0; i < len(msgId); i++ {
		if msgId[i] != expectedMsgId[i] {
			fmt.Println("WARN: Unexpected Msg ID")
			return false
		}
	}
	if uint64(computeChecksum((*msg).MessageID, (*msg).Payload)) != (*msg).CheckSum {
		fmt.Println("WARN: Checksum failed")
		return false
	}
	return true
}

/**
* Opens socket for reading/writing to server over UDP.
* @param ip The server's IP address.
* @param port The server's port to send to.
* @return A UDP connection, or nil if an error occurred.
* @return The error or nil if no error occurred.
 */
//func connectToServer(ip string, port int) (*net.UDPConn, error) {
//	raddr, err := net.ResolveUDPAddr("udp", ip+":"+strconv.Itoa(port))
//	if err != nil {
//		return nil, err
//	}
//
//	ClientConn, err := net.DialUDP("udp", nil, raddr)
//	if err != nil {
//		return nil, err
//	}
//
//	return ClientConn, nil
//}

/**
* Sends a UDP message.
* @param ClientConn The UDP connection object to send the message over.
* @param msg The msg to send.
* @return The error object if an error occured, or nil otherwise.
 */
func sendMsgUDP(conn *net.UDPConn, msg *pb.Msg) error {
	serMsg, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Send msg
	_, err = conn.Write(serMsg)
	if err != nil {
		return err
	} else {
		// fmt.Println("INFO:", n, "bytes written.")
	}

	return nil
}

/**
* Receives a UDP message.
* @param ClientConn The UDP connection to listen to for the message.
* @param timeoutMS Retry timeout in milliseconds.
* @return The received message as a byte array, or nil if an error occurred.
* @return The error object if an error occured, nil otherwise.
 */
func rcvMsgUDP(conn *net.UDPConn, timeoutMS int) ([]byte, error) {
	var err error
	var numBytes int

	// Set up timeout
	timeout := time.Now().Add(time.Millisecond * time.Duration(timeoutMS))
	err = conn.SetReadDeadline(timeout)
	if err != nil {
		return nil, err
	}

	// Read from UDP connection
	buffer := make([]byte, MAX_BUFFER_SIZE)
	numBytes, _, err = conn.ReadFromUDP(buffer)
	if err != nil {
		return nil, err
	}

	return buffer[0:numBytes], nil
}

/**
* Makes a request to the server with the given payload. Requests are retried on failure with the
* timeout doubling each time until 3 retries are reached or the timeout exceeds 5000ms.
* @param ClientConn The UDP connection to make the request over.
* @param payload The payload for the request.
* @param port Server port number.
* @param timeoutMS Retry timeout in milliseconds;
*	must be less than 5000 and the recommended default value is 100.
* @return The received message's payload as a byte array, or nil if an error occurred.
* @return The error object if an error occured, nil otherwise.
 */
//func makeRequest(addr payload []byte, port int, timeoutMS int) ([]byte, error) {
//	localAddr := util.
//	msgId := getMsgId(localAddr, uint16(port))
//	checksum := computeChecksum(msgId, payload)
//
//	// Serialize message
//	msg := &pb.Msg{
//		MessageID: msgId,
//		Payload:   payload,
//		CheckSum:  uint64(checksum),
//	}
//
//	var serResPayload []byte
//	var resMsg *pb.Msg
//	for retry := 0; retry <= NUM_RETRIES; retry++ {
//
//		err := sendMsgUDP(ClientConn, msg)
//		if err != nil {
//			fmt.Println("ERROR: Failed to send UDP message")
//			return nil, err
//		}
//
//		// fmt.Println("INFO: Request sent")
//
//		serResPayload, err = rcvMsgUDP(ClientConn, timeoutMS)
//		if err != nil {
//			if retry < NUM_RETRIES && timeoutMS*2 < 5000 {
//				timeoutMS = 2 * timeoutMS // Double the timeout
//				fmt.Println("ERROR: No UDP message received. " + err.Error())
//				fmt.Println("INFO: Retrying with", timeoutMS, "ms timeout")
//			} else {
//				fmt.Println("ERROR: Failed to receive a UDP message.")
//				return nil, err
//			}
//		} else {
//			// Deserialize message
//			resMsg = &pb.Msg{}
//			proto.Unmarshal(serResPayload, resMsg)
//			if !verifyRcvdMsg(resMsg, msgId) {
//				timeoutMS = 2 * timeoutMS // Double the timeout
//				fmt.Println("INFO: Retrying with", timeoutMS, "ms timeout")
//				continue
//			} else {
//				break
//			}
//		}
//	}
//
//	// fmt.Println("INFO: Valid response received")
//
//	return resMsg.Payload, nil
//}

/*** APPLICATION CODE ***/

func putRequest(key []byte, value []byte, version int32) *pb.KVRequest {
	return &pb.KVRequest{
		Command: PUT,
		Key:     key,
		Value:   value,
		Version: &version,
	}
}

func getRequest(key []byte) *pb.KVRequest {
	return &pb.KVRequest{
		Command: GET,
		Key:     key,
	}
}

func removeRequest(key []byte) *pb.KVRequest {
	return &pb.KVRequest{
		Command: REMOVE,
		Key:     key,
	}
}

func otherRequest(cmd uint32) *pb.KVRequest {
	return &pb.KVRequest{
		Command: cmd,
	}
}

func keyValueRequest(addr *net.Addr, payload *pb.KVRequest) {
	// Serialize message payload

	serReqPayload, err := proto.Marshal(payload)
	if err != nil {
		fmt.Println("WARN: Marshaling payload error. ", err.Error())
		return
	}
	sendUDPRequest(addr, serReqPayload)
}

func fillUp(addr *net.Addr, callNum int) {
	//bytesWritten := 0
	//
	//var payload *pb.KVRequest
	//
	//for i := uint32(0); i < 5; i++ {
	//
	//	key := []byte(strconv.Itoa(int(i)))
	//	// val := make([]byte, 1000)
	//	// binary.LittleEndian.PutUint32(val, i)
	//	val := make([]byte, 1000)
	//	rand.Read(val)
	//
	//	payload = putRequest(key, val, 1)
	//	keyValueRequest(addr, payload)
	//
	//	waitForResponse()
	//
	//	payload = getRequest(key)
	//status, kvRes = keyValueRequest(addr, payload)
	//
	//if kvRes.ErrCode != 0 {
	//	fmt.Println("ERROR RESPONSE")
	//	fmt.Println(kvRes.ErrCode)
	//	return
	//}
	//
	//if status == false {
	//	fmt.Println("ERROR")
	//	return
	//}
	//
	//if string(val) != string(kvRes.Value) {
	//	fmt.Println("Call num: ", callNum)
	//	panic("PUT != GET")
	//}
	//
	//bytesWritten += 16 + 10000

	// fmt.Println("Bytes written: " + strconv.Itoa(bytesWritten))
	//}
}

func waitForResponse() {

}

func fillUp2(addr *net.Addr) {
	//var payload *pb.KVRequest

	//for i := uint32(0); i < 2; i++ {
	//	key := getMsgId("1.1.1.1", uint16(44221))
	//	val := make([]byte, 4)
	//	binary.LittleEndian.PutUint32(val, uint32(69+i))
	//
	//	payload = putRequest(key, val, int32(1))
	//	status, kvRes := keyValueRequest( addr, payload)
	//
	//	if kvRes.ErrCode != 0 {
	//		fmt.Println("ERROR RESPONSE")
	//		fmt.Println(kvRes.ErrCode)
	//		return
	//	}
	//
	//	if status == false {
	//		fmt.Println("ERROR")
	//		return
	//	}
	//
	//	/* GET */
	//	payload = getRequest(key)
	//	status, kvRes = keyValueRequest(addr, payload)
	//
	//	fmt.Println()
	//	fmt.Println(kvRes.ErrCode)
	//	fmt.Println(kvRes.Value)
	//	fmt.Println(val)
	//	fmt.Println(*kvRes.Version)
	//	fmt.Println()
	//
	//}
}

func checkGet(conn *net.UDPConn, port int, key uint32) {
	keyArr := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyArr, key)

	//payload := getRequest(keyArr)
	//_, kvRes := keyValueRequest(ClientConn, port, payload)

	fmt.Println()
	//fmt.Println(b64.StdEncoding.EncodeToString(kvRes.Value))
	//fmt.Println()
}

func putGetTests() {
	for i := 1; i < 1000; i++ {
		//fillUp(port, 1)
		//fillUp(port, 2)
	}
	log.Fatal("Shutting down so other tests don't run.")
}
