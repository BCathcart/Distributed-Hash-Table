package main

import (
	"log"
	"time"
)

const numKeysToSend = 100
const SLEEP_BETWEEN_KILLS = 2 * time.Second
const SLEEP_AFTER_SEND = 15 * time.Second
const SLEEP_AFTER_RETRIEVE = 15 * time.Second

func shutdownTest() {
	baseAddr, _ := GetAddr(localIPAddress, listOfPorts[0])
	for _, targetPort := range targetPorts {
		sendKeysToPort(baseAddr, targetPort, numKeysToSend)
	}
	log.Printf("Sleeping for %v Before killing\n", SLEEP_AFTER_SEND)
	time.Sleep(SLEEP_AFTER_SEND)
	for _, killPort := range killPorts {
		killNode(localIPAddress, killPort)
		time.Sleep(SLEEP_BETWEEN_KILLS)
	}
	log.Printf("Done killing %v nodes, now will attempt to fetch keys\n", len(killPorts))
	aliveAddr, _ := GetAddr(localIPAddress, alivePorts[0])
	getPuts(aliveAddr)
	log.Printf("Done sending fetch requests, sleeping for %v while waiting for messages to come back\n", SLEEP_AFTER_RETRIEVE)
	time.Sleep(SLEEP_AFTER_RETRIEVE)
	log.Println("Done sleeping")
	printPutGetTestResults()
}

func killNode(serverIp string, serverPort int) {
	killAddr, _ := GetAddr(serverIp, serverPort)
	log.Printf("Killing node with address %v", (*killAddr).String())
	killPayload := otherRequest(SHUTDOWN)
	keyValueRequest(killAddr, killPayload)
}
