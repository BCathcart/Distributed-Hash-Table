package chainReplication

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/*
 * Updates the successor if it has changed and updates the pending and expected transfers accordingly.
 *
 * @param succAddr The current successor member's address
 * @param minKey The start of the successor's key range
 * @param maxKey The end of the successor's key range
 */
func UpdateSuccessor(succAddr *net.Addr, minKey uint32, maxKey uint32) {

	if succAddr == nil {
		return
	} else if successor == nil && succAddr != nil {
		log.Println("INFO: UPDATING SUCCESSOR FOR FIRST TIME")
		successor = &successorNode{succAddr, util.KeyRange{minKey, maxKey}}
	} else if util.CreateAddressStringFromAddr(successor.addr) != util.CreateAddressStringFromAddr(succAddr) {
		log.Println("INFO: Updating the successor to ", (*succAddr).String())
		clearSendingTransfers()
		successor = &successorNode{succAddr, util.KeyRange{Low: minKey, High: maxKey}}
	}

	// if util.CreateAddressStringFromAddr(successor.addr) != util.CreateAddressStringFromAddr(succAddr) {

	// if succAddr == nil {
	// 	// Clear pending transfers to the old successor
	// 	if successor != nil {
	// 		removePendingTransfersToAMember(successor.addr)
	// 	}

	// 	successor = nil
	// 	log.Println("WARN: Setting address to nil")
	// 	return
	// }

	// /******************DEBUGGING********************/
	// printState()
	// /************************************************/

	// // No transfer is needed when the node bootstraps (successor will already have a copy of the keys)
	// // ASSUMPTION: first node won't receive keys before the second node is launched
	// if successor == nil {
	// 	log.Println("INFO: UPDATING SUCCESSOR FOR FIRST TIME")
	// 	successor = &successorNode{succAddr, util.KeyRange{minKey, maxKey}}
	// 	return
	// }

	// // Ignore if information is the same
	// if util.CreateAddressStringFromAddr(successor.addr) != util.CreateAddressStringFromAddr(succAddr) {
	// 	log.Print("INFO: UPDATING SUCCESSOR")

	// 	// Clear pending transfers to the old successor
	// 	removePendingTransfersToAMember(successor.addr)

	// 	// Determine if new successor is between you and the old successor (i.e. a new node)
	// 	isNewMember := util.BetweenKeys(maxKey, MyKeys.High, successor.keys.High)
	// 	successor = &successorNode{succAddr, util.KeyRange{Low: minKey, High: maxKey}}

	// 	if isNewMember {
	// 		// If the new successor joined, need to transfer your keys and first predecessor's keys

	// 		log.Println("INFO: The successor is a new member")

	// 		// Transfer this server's keys to the new successor
	// 		sendDataTransferReq(succAddr, MyAddr, MyKeys)

	// 		// Transfer predecessor's keys to the new successor
	// 		if predecessors[0].addr != nil {
	// 			sendDataTransferReq(succAddr, predecessors[0].addr, predecessors[0].keys)
	// 		}
	// 	} else {
	// 		// If the new successor already existed (previous successor failed),
	// 		// only need to transfer the first predecessor's keys
	// 		if predecessors[0].addr != nil {
	// 			sendDataTransferReq(succAddr, predecessors[0].addr, predecessors[0].keys)
	// 		}
	// 	}
	// }
}
