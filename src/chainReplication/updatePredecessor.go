package chainReplication

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

type transferFunc func(destAddr *net.Addr, keys util.KeyRange)
type sweeperFunc func(keys util.KeyRange)

// var sweepCache sweeperFunc = kvstore.Sweep
// var transferKeys transferFunc = sendDataTransferReq

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

	if newPredecessors[1] != nil {
		log.Println(1)
		updateRange(util.KeyRange{(*newPredecessors[1]).keys.Low, MyKeys.High}, (*newPredecessors[0]).addr)
	} else if newPredecessors[0] != nil {
		log.Println(2)
		updateRange(util.KeyRange{(*newPredecessors[0]).keys.Low, MyKeys.High}, (*newPredecessors[0]).addr)
	} else {
		log.Println(3)
		updateRange(util.KeyRange{MyKeys.High + 1, MyKeys.High}, nil)
	}

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

func updateRange(newRange util.KeyRange, predAddr *net.Addr) {
	printKeyState(&newRange)

	if responsibleRange.Low == newRange.Low && responsibleRange.High == newRange.High {
		return // no updates
	} else if util.BetweenKeys(newRange.Low, responsibleRange.Low, responsibleRange.High) {
		// Key space has shrunk - delete everything outside the new range
		log.Println("UPDATING RANGE - SWEEP")
		putReplicationEvent(SWEEP, util.KeyRange{newRange.High, newRange.Low}, nil)
	} else if util.BetweenKeys(newRange.Low, responsibleRange.High, responsibleRange.Low-1) {
		// key space has grown
		log.Println("UPDATING RANGE - TRANSFER for ", util.KeyRange{newRange.Low, responsibleRange.Low - 1})
		putReplicationEvent(TRANSFER, util.KeyRange{newRange.Low, responsibleRange.Low - 1}, predAddr)
	}

	responsibleRange.Low = newRange.Low
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
