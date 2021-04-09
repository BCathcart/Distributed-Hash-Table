package main

import (
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"time"
)

func shutdownTest() {
	//============================ CUSTOM PARAMETERS ========================================
	//localIPAddress := "192.168.1.74"

	numKeysToSend := 100
	// Open sockets to the server
	// ======================================================================//
	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(localIPAddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
	baseAddr, _ := GetAddr(localIPAddress, listOfPorts[0])
	for _, killPort := range targetPorts {
		sendKeysToPort(baseAddr, killPort, numKeysToSend)
	}
	log.Println("SLEEPING Before killing")
	time.Sleep(15 * time.Second)
	for _, killPort := range killPorts {
		killNode(localIPAddress, killPort)
		time.Sleep(2 * time.Second)
	}
	log.Println("Done Killing, TRYING TO FETCH KEYS NOW")
	aliveAddr, _ := GetAddr(localIPAddress, alivePorts[0])
	fetchPrevKeys(aliveAddr)
	log.Println("Done sending fetch requests, sleeping for 20s while waiting for messages to come back")
	time.Sleep(20 * time.Second)
	log.Println("DONE SLEEPING")
	log.Printf("Num Puts: %v\n", putGetCache_.numPuts)
	log.Printf("Get Successes: %v\n", putGetCache_.successfulGets)
	log.Printf("Get Failures: %v\n", putGetCache_.failedGets)
	percentFailed := putGetCache_.failedGets / (putGetCache_.successfulGets + putGetCache_.failedGets) * 100
	log.Printf("Percentage of failures: %v\n", percentFailed)
}

func killNode(serverIp string, serverPort int) {
	killAddr, _ := GetAddr(serverIp, serverPort)
	log.Printf("Killing node with address %v", (*killAddr).String())
	killPayload := otherRequest(SHUTDOWN)
	keyValueRequest(killAddr, killPayload)
}
