package main

import (
	"time"
)

const MAX_BUFFER_SIZE = 65527
const KEYS_PER_NODE = 1500
const PUT_GET_SLEEP = 15 * time.Second

func putGetTest() {
	baseAddr, _ := GetAddr(localIPAddress, listOfPorts[0])
	for _, targetPort := range listOfPorts {
		sendKeysToPort(baseAddr, targetPort, KEYS_PER_NODE)
	}
	time.Sleep(PUT_GET_SLEEP)
	getPuts(baseAddr)
	time.Sleep(PUT_GET_SLEEP)
	printSuccessfulPuts(KEYS_PER_NODE * len(listOfPorts))
	printPutGetTestResults()
}
