package main

import (
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"time"
)

func shutdownTest() {
	//============================ CUSTOM PARAMETERS ========================================
	serverIPaddress := "192.168.1.74"
	basePort := 8080                      // Base port client is connecting to.
	killPorts := []int{8083}              // Ports to kill
	alivePorts := []int{8080, 8081, 8082} // Ports to keep alive
	listOfPorts := append(alivePorts, killPorts...)
	numKeysToSend := 100
	// Open sockets to the server
	// ======================================================================//

	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(serverIPaddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
	baseAddr, _ := GetAddr(serverIPaddress, basePort)
	for _, killPort := range killPorts {
		sendKeysToPort(baseAddr, killPort, numKeysToSend)
	}

	for _, killPort := range killPorts {
		killNode(serverIPaddress, killPort)
	}

	log.Println("TRYING TO FETCH KEYS NOW")
	aliveAddr, _ := GetAddr(serverIPaddress, alivePorts[0])
	fetchPrevKeys(aliveAddr)
	log.Printf("REQUEST CACHE SIZE %v", testReqCache_.data.Len())
	log.Println("Sleeping while waiting for messages to come back (for debugging, remove later)")
	time.Sleep(30 * time.Second)
	//intentionallyCrash()
}

func killNode(serverIp string, serverPort int) {
	killAddr, _ := GetAddr(serverIp, serverPort)
	log.Printf("Killing node with address %v", killAddr)
	killPayload := otherRequest(SHUTDOWN)
	keyValueRequest(killAddr, killPayload)
}
