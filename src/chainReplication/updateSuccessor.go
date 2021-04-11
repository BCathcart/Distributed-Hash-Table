package chainReplication

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/*
 * Updates the successor if it has changed.
 *
 * @param succAddr The current successor member's address
 * @param minKey The start of the successor's key range
 * @param maxKey The end of the successor's key range
 */
func UpdateSuccessor(succAddr *net.Addr, minKey uint32, maxKey uint32) {
	coarseLock.RLock()
	defer coarseLock.RUnlock()

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
}
