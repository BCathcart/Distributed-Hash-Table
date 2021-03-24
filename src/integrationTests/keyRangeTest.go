package main

import (
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"sort"
)

func getKeysFromPort() {
	keyList := make([]int, 0, len(portKeyMap))
	for _, value := range portKeyMap {
		keyList = append(keyList, int(value))
	}
	sort.Ints(keyList)
	log.Println("PORT ORDERS")
	for i := 0; i < len(keyList); i++ {
		printPortFromKey(uint32(keyList[i]))
	}

}

func printPortFromKey(portKey uint32) {
	for key, value := range portKeyMap {
		if value == portKey {
			log.Print(key, ", ")
		}
	}
}

/*
	This test is useful for debugging purposes. It will print out the order of all nodes in the system.
*/
func keyRangeTest() {
	// =========================== CUSTOM PARAMETERS ===========================
	serverIPaddress := "192.168.1.74"
	listOfPorts := []int{8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087} // Other ports in system
	// ======================================================
	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(serverIPaddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
	getKeysFromPort()
	intentionallyCrash()
}
