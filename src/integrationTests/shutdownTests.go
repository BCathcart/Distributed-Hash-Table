package main

import (
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"time"
)

func shutdownTest() {
	//============================ CUSTOM PARAMETERS ========================================
	serverIPaddress := "192.168.1.74"
	basePort := 8080                                   // Base port client is connecting to.
	targetPort := 8083                                 // Port where keys will target.
	listOfPorts := []int{8080, 8081, 8082, targetPort} // Other ports in system
	numKeysToSend := 100
	// Open sockets to the server
	// ======================================================================//

	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(serverIPaddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
	baseAddr, _ := GetAddr(serverIPaddress, basePort)

	sendToPort(baseAddr, targetPort, numKeysToSend)
	log.Printf("SENT %v  requests", len(prevRequests))
	killAddr, _ := GetAddr(serverIPaddress, targetPort)
	log.Printf("Killing node with address %v", killAddr)
	killPayload := otherRequest(SHUTDOWN)
	keyValueRequest(killAddr, killPayload)
	log.Println("TRYING TO FETCH KEYS NOW")
	fetchPrevKeys(baseAddr)
	log.Printf("REQUEST CACHE SIZE %v", testReqCache_.data.Len())
	log.Println("Sleeping while waiting for messages to come back (for debugging, remove later)")
	time.Sleep(20 * time.Second)
	//intentionallyCrash()
}
