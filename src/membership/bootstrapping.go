package membership

import (
	"log"
	"net"
	"time"

	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
)

/*
* Transfers all keys to the bootstrapping node
* @param addr The address of the receiving server
* @param minKey Lower bound of the key range
* @param maxKey Upper bound of the key range
* @return false if a transfer is already in progress, true otherwise
 */
func transferToBootstrappingPred(addr *net.Addr, minKey uint32, maxKey uint32) bool {
	memberStore_.lock.Lock()
	defer memberStore_.lock.Unlock()

	if memberStore_.transferNodeAddr != nil {
		log.Println("WARN: A transfer is already in progress")
		return false
	} else {
		memberStore_.transferNodeAddr = addr
	}

	go transferService.TransferKVStoreData(addr, minKey, maxKey, func() {
		requestreply.SendTransferFinished(nil, addr)
		memberStore_.lock.Lock()
		// Set the node to STATUS_NORMAL here so that if transfer finished is not received
		// it still learns it is done bootstrapping through gossip
		// Need to wait util we have added the bootstrapping node to our memberstore through gossip
		var pos int = -1
		for true {
			pos = GetPosFromAddr(addr)
			if pos != -1 {
				log.Println("INFO: FINISHED BOOTSTRAPPING - Node is in the member store at position ", pos)
				break
			} else {
				log.Println("WARN: FINISHED BOOTSTRAPPING - Node is NOT in the member store yet")
				memberStore_.lock.Unlock()
				time.Sleep(1 * time.Second)
				memberStore_.lock.Lock()
			}
		}
		memberStore_.members[GetPosFromAddr(addr)].Status = STATUS_NORMAL
		memberStore_.transferNodeAddr = nil
		memberStore_.lock.Unlock()
	})

	return true
}

/**
Membership protocol: after receiving the transfer finished from the successor node,
sets status to normal
*/
func BootstrapTransferFinishedHandler() {
	log.Println("INFO: RECEIVED BOOTSTRAPPING TRANSFER FINISHED MSG")
	SetStatusToNormal()
}
