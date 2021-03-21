package membership

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
)

/*
* Transfers all keys to the bootstrapping node
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
	log.Println("RECEIVED TRANSFER FINISHED MSG")

	memberStore_.lock.Lock()
	memberStore_.members[memberStore_.position].Status = STATUS_NORMAL
	memberStore_.lock.Unlock()
}
