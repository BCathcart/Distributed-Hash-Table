package main

import (
	"encoding/binary"
	"fmt"
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

func GetAddr(ip string, port int) (*net.Addr, error) {
	addr := ip + ":" + strconv.Itoa(port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	var netAddr net.Addr = udpAddr
	return &netAddr, nil
}

func sendUDPRequest(addr *net.Addr, payload []byte) {
	ip := (*ClientConn).LocalAddr().(*net.UDPAddr).IP.String()
	port := (*ClientConn).LocalAddr().(*net.UDPAddr).Port

	msgID := getmsgID(ip, uint16(port))
	checksum := util.ComputeChecksum(msgID, payload)
	reqMsg := &pb.Msg{
		MessageID: msgID,
		Payload:   payload,
		CheckSum:  uint64(checksum),
	}

	serMsg, err := proto.Marshal(reqMsg)
	if err != nil {
		log.Println(err)
	}
	kvReq := &pb.KVRequest{}
	err = proto.Unmarshal(payload, kvReq)

	putTestReqCacheEntry(string(reqMsg.MessageID), serMsg, kvReq)

	writeMsg(*addr, serMsg)
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

func writeMsg(addr net.Addr, msg []byte) {
	_, err := (*ClientConn).WriteTo(msg, addr)
	if err != nil {
		log.Println(err)
	}
}

/**
    Processes a response message, making sure that it is in the request cache.
	Very similar approach to the processResponse function in the requestreply layer.
	@param senderAddr address of node sending to test client
	@param resMsg the response message to be processed.
*/
func processResponse(senderAddr net.Addr, resMsg *pb.Msg) {
	// Get cached request (ignore if it's not cached)
	testReqCache_.lock.Lock()
	key := string(resMsg.MessageID)
	req := testReqCache_.data.Get(key)
	if req != nil {
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("Received response", resMsg.MessageID, "for value", kvstore.BytetoInt(res.GetValue()))
		trcKVReq := req.(testReqCacheEntry).kvReq
		putGetCheck(trcKVReq, res)
		testReqCache_.data.Delete(key)

	} else {
		res := &pb.KVResponse{}
		proto.Unmarshal(resMsg.Payload, res)
		log.Println("WARN: Received response for unknown request with msgID", resMsg.MessageID, "from", senderAddr.String(), "for value", kvstore.BytetoInt(res.GetValue()))
	}
	testReqCache_.lock.Unlock()
}

func intInSlice(a int, list []int) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

/**
Creates a put request that has a random value, and a key between low, high parameters
*/
func getPortKV(low uint32, high uint32) *pb.KVRequest {
	targetKey := findCorrectKey(low, high)
	val := make([]byte, 1000)
	rand.Read(val)
	payload := putRequest(targetKey, val, 1)
	return payload
}

/*
	Using internal knowledge of the hashing ring, sends keys that will have to be
	processed by the node with port targetPort.
	@param numKeys the number of keys to send.
*/
func sendKeysToPort(addr *net.Addr, targetPort int, numKeys int) {
	low, high := getKeyRange(targetPort)
	for i := 0; i < numKeys; i++ {
		payload := getPortKV(low, high)
		prevRequests = append(prevRequests, payload)
		keyValueRequest(addr, payload)
	}
}

/*
	Retries random hash values until one lands between range low and high
*/
func findCorrectKey(low uint32, high uint32) []byte {
	maxRetries := 100_000 // arbitrarily large value, should never take more than ~100 retries
	for i := 0; i < maxRetries; i++ {
		randKey := []byte(strconv.Itoa(rand.Intn(10_000_000)))
		hashKey := util.Hash(randKey)
		if util.BetweenKeys(hashKey, low, high) {
			//log.Printf("FOUND: %v BETWEEN [%v %v]. ", hashKey, low, high)
			//log.Printf("Proof: (target -low ), (high -target) = %v , %v", hashKey-low, high - hashKey)
			//log.Printf("Attempting  %v\n", randKey)
			return randKey
		}
	}
	log.Fatalf("Couldn't find a key in range even after %v retries. Something is wrong", maxRetries)
	return nil
}

func fetchPrevKeys(addr *net.Addr) {
	for i := 0; i < len(prevRequests); i++ {
		targetKey := prevRequests[i].Key
		payload := getRequest(targetKey)
		log.Println(payload.Key)
		keyValueRequest(addr, payload)
	}
}

/**
Based on a port, returns the key range it is responsible for.
This function should have probably be refactored or shortened.
*/
func getKeyRange(portNum int) (uint32, uint32) {
	keyList := make([]int, 0, len(portKeyMap))
	for _, value := range portKeyMap {
		keyList = append(keyList, int(value))
	}
	sort.Ints(keyList)
	keyIdx := -1
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
	log.Printf("Targetting %v, replicated at next 2 ports: %v , %v\n", portNum, nextPort1, nextPort2)
	if keyIdx == 0 { // wrap around
		return uint32(keyList[len(keyList)-1]), uint32(keyList[0])
	} else {
		return uint32(keyList[keyIdx-1]), uint32(keyList[keyIdx])
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
