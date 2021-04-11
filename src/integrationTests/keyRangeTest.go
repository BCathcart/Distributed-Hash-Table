package main

import (
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"sort"
)

func getKeysFromPort() {
	keyList := getSortedKeyList()
	log.Println("PORT ORDERS")
	for i := 0; i < len(keyList); i++ {
		printPortFromKey(uint32(keyList[i]))
	}

}

func getSortedKeyList() []int {
	keyList := make([]int, 0, len(portKeyMap))
	for _, value := range portKeyMap {
		keyList = append(keyList, int(value))
	}
	sort.Ints(keyList)
	return keyList
}

func printPortFromKey(portKey uint32) {
	log.Print(getPortFromKey(portKey), ", ")
}

func getPortFromKey(portKey uint32) int {
	for key, value := range portKeyMap {
		if value == portKey {
			return key
		}
	}
	return 999999999
}

/*
	Gets port associated with a certain key (assumes no changes to keylist since
	removal
*/
func getPortForSentKey(key int) int {
	keyList := getSortedKeyList()
	for _, value := range keyList {
		keyListPort := getPortFromKey(uint32(value))
		if key < keyListPort {
			return keyListPort
		}
	}
	return getPortFromKey(uint32(keyList[0])) // returns first node in chain if wrap around
}

/*
	This test is useful for debugging purposes. It will print out the order of all nodes in the system.
*/
func keyRangeTest() {
	// =========================== CUSTOM PARAMETERS ===========================
	// ======================================================
	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(localIPAddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
	getKeysFromPort()
	intentionallyCrash()
}
