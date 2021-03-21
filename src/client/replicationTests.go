package main

import (
	"fmt"
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"time"
)

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
		//time.Sleep(500 * time.Millisecond)
	}
}

func findCorrectKey(low uint32, high uint32) []byte {
	maxRetries := 100_000 // arbitrarily large value, should never take more than ~100 retries
	for i := 0; i < maxRetries; i++ {
		randKey := []byte(strconv.Itoa(rand.Intn(10_000_000)))
		hashKey := util.Hash(randKey)
		if util.BetweenKeys(hashKey, low, high) {
			//log.Printf("FOUND: %v BETWEEN [%v %v]. ", hashKey, low, high)
			//log.Printf("Proof: (target -low ), (high -target) = %v , %v", hashKey-low, high - hashKey)
			log.Printf("Attempting  %v\n", randKey)
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

func replicationTest() {
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
	//sendToPort(conn, basePort, 8081, numKeysToSend)
	// 100 should land on each
	log.Printf("SENT %v  requests", len(prevRequests))
	log.Printf("SLEEPING 30S, MANUALLY KILL SERVER WITH PORT %v", targetPort)
	time.Sleep(50 * time.Second)
	log.Println("WAKING UP, TRYING TO FETCH KEYS NOW")
	fetchPrevKeys(conn, basePort)
}
