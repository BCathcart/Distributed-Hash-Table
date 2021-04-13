package main

import (
	"log"
	"sort"
)

func getSortedKeyList() []int {
	keyList := make([]int, 0, len(portKeyMap))
	for _, value := range portKeyMap {
		keyList = append(keyList, int(value))
	}
	sort.Ints(keyList)
	return keyList
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
	removal)
*/
func getPortForSentKey(key int) int {
	keyList := getSortedKeyList()
	for _, value := range keyList {
		keyListPort := getPortFromKey(uint32(value))
		if key < value {
			return keyListPort
		}
	}
	return getPortFromKey(uint32(keyList[0])) // returns first node in chain if wrap around
}

/*
	This test is useful for debugging purposes. It will print out the order of all nodes in the system,
	and their associated keys.
*/
func printNodeOrder() {
	keyList := getSortedKeyList()
	log.Println("PORT ORDERS")
	for i := 0; i < len(keyList); i++ {
		portKey := uint32(keyList[i])
		log.Printf("Port: %v, Associated key %v", getPortFromKey(portKey), portKey)
	}
}
