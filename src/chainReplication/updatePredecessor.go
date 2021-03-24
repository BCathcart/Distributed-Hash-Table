package chainReplication

import (
	"log"
	"net"

	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

type transferFunc func(destAddr *net.Addr, coordAddr *net.Addr, keys util.KeyRange)
type sweeperFunc func(keys util.KeyRange)

var sweepCache sweeperFunc = kvstore.Sweep
var transferKeys transferFunc = sendDataTransferReq

/** dummyTransfer and dummySweeper are used for development purposes. They share the
same parameters as our actual sweeper / transfer functions, so they can be passed
into our updatePredecessor function to run it without actually modifying the kvStore.
*/
func dummyTransfer(destAddr *net.Addr, coordAddr *net.Addr, keys util.KeyRange) {
	log.Printf("Called Transfer function with range [%v, %v], addr, %v \n", keys.Low, keys.High, (*coordAddr).String())
}

func dummySweeper(keys util.KeyRange) {
	log.Println("Called Sweeper function with range", keys)
}

func UpdatePredecessors(addr []*net.Addr, keys []uint32) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	var newPredecessors [3]*predecessorNode
	for i := 0; i < 3; i++ {
		if addr[i] != nil {
			newPredecessors[i] = &predecessorNode{}
			newPredecessors[i].addr = addr[i]
			newPredecessors[i].keys.High = keys[i]
			if i < 2 && addr[i+1] != nil {
				newPredecessors[i].keys.Low = keys[i+1] + 1
			} else {
				newPredecessors[i].keys.Low = MyKeys.High + 1
			}
		} else {
			newPredecessors[i] = nil
			break
		}
	}
	// checkAddresses(addr, keys)
	checkPredecessors(newPredecessors, transferKeys, sweepCache)
	copyPredecessors(newPredecessors)
	if newPredecessors[0] != nil {
		MyKeys.Low = newPredecessors[0].keys.High + 1
	} else {
		MyKeys.Low = MyKeys.High + 1
	}
}

/**
Helper function to copy all new nodes to old nodes by value.
*/
func copyPredecessors(newPredecessors [3]*predecessorNode) {
	for i := 0; i < len(newPredecessors); i++ {
		predecessors[i] = shallowCopy(newPredecessors[i])
	}
}

/**
Helper function used in development. Compares incoming
addresses / keys to existing ones, then prints the result.

@param addr the new addresses from the membership layer
@param keys the new keys from the membership layer
*/
func checkAddresses(addr []*net.Addr, keys []uint32) {
	var addrString = ""
	for i := 0; i < len(addr); i++ {
		if addr[i] == nil {
			addrString = "NIL"
		} else {
			addrString = (*addr[i]).String()
		}
		if predecessors[i] == nil {
			log.Printf("OLD: NIL, NEW: %v\n", addrString)
		} else {
			log.Printf("OLD: %v, NEW: %v\n", (*getPredAddr(i)).String(), addrString)
		}
	}
	log.Printf("Keys %v\n", keys)
}

func comparePredecessors(newPred *predecessorNode, oldPred *predecessorNode) bool {
	if newPred == nil || oldPred == nil {
		return newPred == oldPred
	}
	// Only check the "high" range of the keys. A change of the "low" indicates the node
	// Has a new predecessor, but not necessarily that the node itself has changed.
	return newPred.keys.High == oldPred.keys.High
}

/*
	Compares a nodes current chain to its previous one. If there are any changes, will need to send/receive/drop
	keys.

	This was done through a series of if/else statements, each handling a different scenario. It was the easiest method
	for our system with a replication factor of 3 as there not too many scenarios for possible changes to a nodes chain.
	A different approach would be necessary for a higher replication factor as there would be too many scenarios to
	cleanly code.

	Scenarios are listed here: https://app.diagrams.net/#G1MaVQbmbZ6cjkAzkG8zbFdaj9r03HWV5A
	TODO: Currently this code does not handle multiple simultaneous changes within a single heartbeat well,
		such as a new node joining and another node in the chain failing. The assumption
		was that this would be a rare enough event.
	@param newPredecessors are the three nodes previous to the current node in the chain, or
	nil if there are not enough nodes in the chain
	@param transferKeys and sweepCache are used to make this function testable - otherwise it would
	be challenging to keep. transferKeys initiates a

	Precondition: if newPredAddrX is nil, all nodes newPredAddr(>X) must also be nil. e.g. if newPredecessors[0] is nil,
	newPredAddr2 and newPredAddr3 must also be nil.
*/

func checkPredecessors(newPredecessors [3]*predecessorNode, transferKeys transferFunc, sweepCache sweeperFunc) {
	// Converting addresses to equivalent keys.
	oldPred1, oldPred2, oldPred3 := predecessors[0], predecessors[1], predecessors[2]
	newPred1, newPred2, newPred3 := newPredecessors[0], newPredecessors[1], newPredecessors[2]
	pred1Equal := comparePredecessors(newPred1, oldPred1)
	pred2Equal := comparePredecessors(newPred2, oldPred2)
	pred3Equal := comparePredecessors(newPred3, oldPred3)

	// If none of the previous three have changed, no need to update.
	if pred1Equal && pred2Equal && pred3Equal {
		return
	}
	PrintKeyChange(newPredecessors)

	log.Println("INFO: NEW PREDECESSOR", pred1Equal, pred2Equal, pred3Equal)

	// If we newly joined, expect to receive keys
	if newPred1 != nil && oldPred1 == nil && newPred2 != nil && oldPred2 == nil {
		log.Println("INFO: EXPECTING TO RECEIVE KEYS FROM PREDECESSORS AFTER BOOTSTRAP")
		addExpectedTransfer(newPred1.addr)
		addExpectedTransfer(newPred2.addr)
		return
	}

	/*
		First and second predecessors stay the same (third is different).
		This could mean either the third predecessor has failed, or a new node has joined
		between the second and third predecessor.
	*/
	if pred1Equal && pred2Equal {
		log.Println("INFO: THIRD PREDECESSOR HAS CHANGED")

		// New node has joined
		if newPred3 != nil && newPred2 != nil && (oldPred3 == nil || util.BetweenKeys(newPred2.keys.Low, oldPred2.keys.Low, oldPred2.keys.High)) {
			sweepCache(newPred3.keys)
		} else { // P3 failed. Will be receiving P3 keys from P1
			if newPred1 != nil {
				addExpectedTransfer(newPred2.addr)
			}
		}
	} else if pred1Equal {
		log.Println("INFO: SECOND PREDECESSOR HAS CHANGED")

		/*
			First predecessor is the same, second is different. This could mean
			either the second node has failed, or there is a new node between the first and second.
		*/

		// If there's no 2nd predecessor, there can only be 2 nodes in the system - not enough
		// for a full chain so nothing needs to be done in terms of replication.

		// TODO: what if a third node joins?
		if oldPred2 == nil || newPred2 == nil {
			return
		} else if comparePredecessors(newPred3, oldPred2) { // New node joined
			log.Println("INFO: NEW SECOND PREDECESSOR JOINED")
			sweepCache(oldPred2.keys)
		} else if comparePredecessors(newPred2, oldPred3) { // P2 Failed. Will be receiving keys from p1 for new p2
			log.Println("INFO: SECOND PREDECESSOR FAILED")
			addExpectedTransfer(newPred2.addr)
			if oldPred2 != nil && oldPred1 != nil {
				log.Printf("TRANSFERRING KEYS TO SUCC %v", (*successor.addr).String())
				go transferKeys(successor.addr, oldPred1.addr, util.KeyRange{Low: oldPred2.keys.Low, High: oldPred2.keys.High}) // Transfer the new keys P2 got to the successor
			}
		} else {
			UnhandledScenarioError(newPredecessors)
		}

	} else { // First predecessor node has changed.
		log.Println("INFO: FIRST PREDECESSOR HAS CHANGED")

		// If there's no first predecessor, there can only be one node in the system - not enough
		// for a full chain, so nothing needs to be done in terms of replication.
		if oldPred1 == nil || newPred1 == nil {
			return
		}
		if util.BetweenKeys(newPred1.keys.High, oldPred1.keys.High, MyKeys.High) { // New node has joined
			log.Println("INFO: NEW FIRST PREDECESSOR HAS JOINED")

			if oldPred2 != nil {
				sweepCache(oldPred2.keys)
			}
		} else if comparePredecessors(oldPred2, newPred1) { // Node 1 has failed, node 2 is still running
			log.Print("INFO: FIRST PREDECESSOR FAILED")

			// GOT EXCEPTION HERE
			if newPred2 != nil {
				addExpectedTransfer(newPred2.addr)
			}
			if oldPred2 != nil {
				go transferKeys(successor.addr, oldPred2.addr, util.KeyRange{Low: oldPred2.keys.Low, High: oldPred2.keys.High})
			}
		} else if comparePredecessors(oldPred3, newPred1) { // Both Node 1 and Node 2 have failed.
			if oldPred2 != nil {
				go transferKeys(successor.addr, newPredecessors[0].addr, util.KeyRange{Low: oldPred2.keys.Low, High: oldPred2.keys.High})
			}
			addExpectedTransfer(newPred1.addr)
			// TODO: Should also transfer keys between (newPredKey3, oldPredKey2). With
			// 	our current architecture this is not possible since we do not yet have those keys.
			//  This should be very rare so may not need to be handled, as the churn is expected to be low.
			log.Println("WARN: TWO NODES FAILED SIMULTANEOUSLY.")
		} else {
			UnhandledScenarioError(newPredecessors)
		}

	}
}

// Helper function for testing / debugging.
func PrintKeyChange(newPredecessors [3]*predecessorNode) {
	log.Printf("OLD KEYS: %v, %v, %v\n", getPredKey(predecessors[0]), getPredKey(predecessors[1]), getPredKey(predecessors[2]))
	log.Printf("NEW KEYS: %v, %v, %v\n", getPredKey(newPredecessors[0]), getPredKey(newPredecessors[1]), getPredKey(newPredecessors[2]))
}

/*
	Used for debugging purposes. Prints out the changes in the keyrange between new and old
	predecessors.

	Ideally, this function will never get called - if it does, it means our UpdatePredecessor implementation
	is not robust enough to handle the different scenarios. While in development, this function would crash
	the program with log.Fatal to allow us to more easily identify issues.
*/
func UnhandledScenarioError(newPredecessors [3]*predecessorNode) {
	log.Println("ERROR: UNHANDLED SCENARIO: OLD KEYS")
	for i := 0; i < len(predecessors); i++ {
		if predecessors[i] == nil {
			log.Println(" NIL ")
		} else {
			log.Println(predecessors[i].keys)
		}
	}
	log.Println("NEW KEYS")
	for i := 0; i < len(newPredecessors); i++ {
		if newPredecessors[i] == nil {
			log.Println(" NIL ")
		} else {
			log.Println(newPredecessors[i].keys)
		}
	}
}

/*
	Helper function to copy a predecessor node by value.
*/
func shallowCopy(orig *predecessorNode) *predecessorNode {
	if orig == nil {
		return nil
	}
	return &predecessorNode{
		keys: orig.keys,
		addr: orig.addr,
	}
}
