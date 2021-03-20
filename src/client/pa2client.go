package main

import (
	b64 "encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"hash/crc32"
	"log"
	"math/rand"
	"net"
	"sort"
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
func connectToServer(ip string, port int) (*net.UDPConn, error) {
	raddr, err := net.ResolveUDPAddr("udp", ip+":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

/**
* Sends a UDP message.
* @param conn The UDP connection object to send the message over.
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
* @param conn The UDP connection to listen to for the message.
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
* @param conn The UDP connection to make the request over.
* @param payload The payload for the request.
* @param port Server port number.
* @param timeoutMS Retry timeout in milliseconds;
*	must be less than 5000 and the recommended default value is 100.
* @return The received message's payload as a byte array, or nil if an error occurred.
* @return The error object if an error occured, nil otherwise.
 */
func makeRequest(conn *net.UDPConn, payload []byte, port int, timeoutMS int) ([]byte, error) {
	localAddr := conn.LocalAddr().(*net.UDPAddr).IP.String()
	msgId := getMsgId(localAddr, uint16(port))
	// msgId = []byte("69") // TODO: remove!

	checksum := computeChecksum(msgId, payload)
	// fmt.Println(checksum)

	// Serialize message
	msg := &pb.Msg{
		MessageID: msgId,
		Payload:   payload,
		CheckSum:  uint64(checksum),
	}

	var serResPayload []byte
	var resMsg *pb.Msg
	for retry := 0; retry <= NUM_RETRIES; retry++ {

		err := sendMsgUDP(conn, msg)
		if err != nil {
			fmt.Println("ERROR: Failed to send UDP message")
			return nil, err
		}

		// fmt.Println("INFO: Request sent")

		serResPayload, err = rcvMsgUDP(conn, timeoutMS)
		if err != nil {
			if retry < NUM_RETRIES && timeoutMS*2 < 5000 {
				timeoutMS = 2 * timeoutMS // Double the timeout
				fmt.Println("ERROR: No UDP message received. " + err.Error())
				fmt.Println("INFO: Retrying with", timeoutMS, "ms timeout")
			} else {
				fmt.Println("ERROR: Failed to receive a UDP message.")
				return nil, err
			}
		} else {
			// Deserialize message
			resMsg = &pb.Msg{}
			proto.Unmarshal(serResPayload, resMsg)
			if !verifyRcvdMsg(resMsg, msgId) {
				timeoutMS = 2 * timeoutMS // Double the timeout
				fmt.Println("INFO: Retrying with", timeoutMS, "ms timeout")
				continue
			} else {
				break
			}
		}
	}

	// fmt.Println("INFO: Valid response received")

	return resMsg.Payload, nil
}

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

func keyValueRequest(conn *net.UDPConn, port int, payload *pb.KVRequest) (bool, *pb.KVResponse) {
	// Serialize message payload

	serReqPayload, err := proto.Marshal(payload)
	if err != nil {
		fmt.Println("Marshaling payload error. ", err.Error())
		return false, nil
	}

	serResPayload, err := makeRequest(conn, serReqPayload, port, 100)
	if err != nil {
		fmt.Println("Request failed. ", err.Error())
		return false, nil
	}

	// Deserialize response
	kvResponse := &pb.KVResponse{}
	proto.Unmarshal(serResPayload, kvResponse)

	return true, kvResponse
}

func fillUp(conn *net.UDPConn, port int, callNum int) {
	bytesWritten := 0

	var payload *pb.KVRequest

	for i := uint32(0); i < 5; i++ {

		key := []byte(strconv.Itoa(int(i)))
		// val := make([]byte, 1000)
		// binary.LittleEndian.PutUint32(val, i)
		val := make([]byte, 1000)
		rand.Read(val)

		payload = putRequest(key, val, 1)
		status, kvRes := keyValueRequest(conn, port, payload)

		if kvRes.ErrCode != 0 {
			fmt.Println("ERROR RESPONSE")
			fmt.Println(kvRes.ErrCode)
			return
		}

		if status == false {
			fmt.Println("ERROR")
			return
		}

		payload = getRequest(key)
		status, kvRes = keyValueRequest(conn, port, payload)

		if kvRes.ErrCode != 0 {
			fmt.Println("ERROR RESPONSE")
			fmt.Println(kvRes.ErrCode)
			return
		}

		if status == false {
			fmt.Println("ERROR")
			return
		}

		if string(val) != string(kvRes.Value) {
			fmt.Println("Call num: ", callNum)
			panic("PUT != GET")
		}

		bytesWritten += 16 + 10000

		// fmt.Println("Bytes written: " + strconv.Itoa(bytesWritten))
	}
}

func handleKVRequest(status bool, kVRes *pb.KVResponse) {
	if kVRes.ErrCode != 0 {
		fmt.Println("ERROR RESPONSE")
		fmt.Println(kVRes.ErrCode)
		return
	}

	if status == false {
		fmt.Println("ERROR")
		return
	}
}

func fillUp2(conn *net.UDPConn, port int) {
	var payload *pb.KVRequest

	for i := uint32(0); i < 2; i++ {
		key := getMsgId("1.1.1.1", uint16(44221))
		val := make([]byte, 4)
		binary.LittleEndian.PutUint32(val, uint32(69+i))

		payload = putRequest(key, val, int32(1))
		status, kvRes := keyValueRequest(conn, port, payload)

		if kvRes.ErrCode != 0 {
			fmt.Println("ERROR RESPONSE")
			fmt.Println(kvRes.ErrCode)
			return
		}

		if status == false {
			fmt.Println("ERROR")
			return
		}

		/* GET */
		payload = getRequest(key)
		status, kvRes = keyValueRequest(conn, port, payload)

		fmt.Println()
		fmt.Println(kvRes.ErrCode)
		fmt.Println(kvRes.Value)
		fmt.Println(val)
		fmt.Println(*kvRes.Version)
		fmt.Println()

	}
}

func checkGet(conn *net.UDPConn, port int, key uint32) {
	keyArr := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyArr, key)

	payload := getRequest(keyArr)
	_, kvRes := keyValueRequest(conn, port, payload)

	fmt.Println()
	fmt.Println(b64.StdEncoding.EncodeToString(kvRes.Value))
	fmt.Println()
}

/**
Sends key that will end up at a certain port. Basically just tries / retries
*/
func getPortKV(low uint32, high uint32) *pb.KVRequest {
	//fmt.Printf("SENDING %v keys TO PORT %v", keysCount, targetPort)
	targetKey := findCorrectKey(low, high)
	val := make([]byte, 1000)
	rand.Read(val)
	payload := putRequest(targetKey, val, 1)
	return payload
}

func sendToPort(conn *net.UDPConn, port int, targetPort int, numKeys int) {
	low, high := getKeyRange(targetPort)
	//fmt.Printf("Looking for keys between %v and %v. Map = %v\n", low, high, portKeyMap)

	for i := 0; i < numKeys; i++ {
		payload := getPortKV(low, high)
		prevRequests = append(prevRequests, payload)
		status, kvRes := keyValueRequest(conn, port, payload)
		handleKVRequest(status, kvRes)
	}
}

func findCorrectKey(low uint32, high uint32) []byte {
	maxRetries := 100_000 // arbitrarily large value, should never take more than ~100 retries
	for i := 0; i < maxRetries; i++ {
		randKey := []byte(strconv.Itoa(rand.Intn(100_000)))
		hashKey := util.Hash(randKey)
		if util.BetweenKeys(hashKey, low, high) {
			//log.Printf("FOUND: %v BETWEEN [%v %v]. ", hashKey, low, high)
			//log.Printf("Proof: (target -low ), (high -target) = %v , %v", hashKey-low, high - hashKey)
			log.Printf("FOUND %v\n", randKey)
			return randKey
		}
	}
	log.Fatalf("Couldn't find a key in range even after %v retries. Something is wrong", maxRetries)
	return nil
}

func fetchPrevKeys(conn *net.UDPConn, port int) {
	for i := 0; i < len(prevRequests); i++ {
		targetKey := prevRequests[i].Key
		payload := getRequest(targetKey)
		log.Println(payload.Key)
		status, kvRes := keyValueRequest(conn, port, payload)
		handleKVRequest(status, kvRes)
	}
}

func getKeyRange(portNum int) (uint32, uint32) {
	keyList := make([]int, 0, len(portKeyMap))
	for _, value := range portKeyMap {
		keyList = append(keyList, int(value))
	}
	sort.Ints(keyList)
	keyIdx := -1
	log.Printf("PORT = %v KEYLIST = %v", portNum, keyList)
	for i := 0; i < len(keyList); i++ {
		if keyList[i] == int(portKeyMap[portNum]) {
			keyIdx = i
			break
		}
	}
	succ1, succ2 := (keyIdx+1)%len(keyList), (keyIdx+2)%len(keyList)
	nextKey1, nextKey2 := keyList[succ1], keyList[succ2]
	nextPort1, nextPort2 := -1, -1
	for key, value := range portKeyMap {
		if value == uint32(nextKey1) {
			nextPort1 = key
		} else if value == uint32(nextKey2) {
			nextPort2 = key
		}
	}
	log.Printf("TARGETTING PORT %v, replicated at next 2 ports: %v , %v\n", portNum, nextPort1, nextPort2)
	if keyIdx == 0 { // wrap around
		return uint32(keyList[len(keyList)-1]), uint32(keyList[0])
	} else {
		return uint32(keyList[keyIdx-1]), uint32(keyList[keyIdx])
	}
}

func main() {
	//============================ CUSTOM PARAMETERS ========================================
	serverIPaddress := "192.168.1.74"
	basePort := 8080                                   // Base port client is connecting to.
	targetPort := 8083                                 // Port where keys will target.
	listOfPorts := []int{8080, 8081, 8082, targetPort} // Other ports in system
	numKeysToSend := 100
	// ======================================================================//

	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(serverIPaddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
	// Open socket to the server
	conn, err := connectToServer(serverIPaddress, basePort)
	if err != nil {
		fmt.Println("Failed to connect to server. ", err.Error())
		return
	}
	defer conn.Close()
	sendToPort(conn, basePort, targetPort, numKeysToSend)
	log.Printf("SENT %v  requests", len(prevRequests))
	log.Printf("SLEEPING 30S, MANUALLY KILL SERVER WITH PORT %v", targetPort)
	time.Sleep(30 * time.Second)
	log.Println("WAKING UP, TRYING TO FETCH KEYS NOW")
	fetchPrevKeys(conn, basePort)

	//for i := 1; i < 5; i++ {
	//	fillUp(conn, basePort, 2)
	//}

	// fillUp2(conn, basePort)

	// 	var payload *pb.KVRequest

	// key := make([]byte, 4)
	// binary.LittleEndian.PutUint32(key, 69)

	// val := make([]byte, 4)
	// binary.LittleEndian.PutUint32(val, 420)

	// TODO: Test every possible error code

	/* PUT */
	// payload = putRequest(key, val, 1)
	// status, kvRes := keyValueRequest(conn, basePort, payload)

	// fmt.Println(kvRes.ErrCode)

	/* GET */
	// payload = getRequest(key)
	// status, kvRes = keyValueRequest(conn, basePort, payload)

	// 	fmt.Println(kvRes.ErrCode)
	// 	fmt.Println(kvRes.Value)
	// 	fmt.Println(val)
	// 	fmt.Println(kvRes.Version)

	// 	// /* DELETE */
	// 	payload = removeRequest(key)
	// 	status, kvRes = keyValueRequest(conn, basePort, payload)

	// 	fmt.Println(kvRes.ErrCode)
	// 	fmt.Println("DELETE FINISHED")

	// 	// /* Other */
	// 	payload = otherRequest(WIPEOUT)
	// 	status, kvRes = keyValueRequest(conn, basePort, payload)
	// 	fmt.Println(kvRes.ErrCode)
	// 	fmt.Println("WIPEOUT FINISHED")

	// 	payload = otherRequest(IS_ALIVE)
	// 	status, kvRes = keyValueRequest(conn, basePort, payload)
	// 	fmt.Println(kvRes.ErrCode)
	// 	fmt.Println("IS_ALIVE")

	// 	payload = otherRequest(GET_PID)
	// 	status, kvRes = keyValueRequest(conn, basePort, payload)
	// 	fmt.Println(kvRes.ErrCode)
	// 	fmt.Println(kvRes.Pid)

	// 	payload = otherRequest(GET_MEMBERSHIP_COUNT)
	// 	status, kvRes = keyValueRequest(conn, basePort, payload)
	// 	fmt.Println(kvRes.ErrCode)
	// 	fmt.Println(kvRes.MembershipCount)

	// 	// payload = otherRequest(SHUTDOWN)
	// 	// status, kvRes = keyValueRequest(conn, basePort, payload)
	// 	// fmt.Println("SHUTDOWN")

	// fmt.Println(status)
	// fmt.Println(kvRes)
}
