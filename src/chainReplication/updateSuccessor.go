package chainReplication

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

func UpdateSuccessor(succAddr *net.Addr, minKey uint32, maxKey uint32) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	if succAddr == nil {
		// Clear pending transfers to the old successor
		if successor != nil {
			removePendingTransfersToAMember(successor.addr)
		}

		successor = nil
		log.Println("Warn: Setting address to nil")
		return
	}

	/******************DEBUGGING********************/
	printState()
	/************************************************/

	// No transfer is needed when the node bootstraps (successor will already have a copy of the keys)
	// ASSUMPTION: first node won't receive keys before the second node is launched
	if successor == nil {
		log.Print("\n\n\nUPDATE SUCCESSOR FIRST TIME\n\n\n")
		successor = &successorNode{succAddr, util.KeyRange{minKey, maxKey}}
		return
	}

	// Ignore if information is the same
	if util.CreateAddressStringFromAddr(successor.addr) != util.CreateAddressStringFromAddr(succAddr) {
		log.Print("\n\n\nUPDATE SUCCESSOR\n\n\n")

		// Clear pending transfers to the old successor
		removePendingTransfersToAMember(successor.addr)

		// Determine if new successor is between you and the old successor (i.e. a new node)
		isNewMember := util.BetweenKeys(maxKey, MyKeys.High, successor.keys.High)
		successor = &successorNode{succAddr, util.KeyRange{Low: minKey, High: maxKey}}

		if isNewMember {
			// If the new successor joined, need to transfer your keys and first predecessor's keys

			log.Println("IS_NEW_MEMBER")

			// Transfer this server's keys to the new successor
			sendDataTransferReq(succAddr, MyAddr, MyKeys)

			// Transfer predecessor's keys to the new successor
			if predecessors[0].addr != nil {
				sendDataTransferReq(succAddr, predecessors[0].addr, predecessors[0].keys)
			}
		} else {
			// If the new successor already existed (previous successor failed),
			// only need to transfer the first predecessor's keys
			if predecessors[0].addr != nil {
				sendDataTransferReq(succAddr, predecessors[0].addr, predecessors[0].keys)
			}
		}
	}
}
