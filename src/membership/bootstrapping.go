package membership

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
)

/*
* Transfers all keys to the bootstrapping node
 */
func transferToBootstrappingPred(memberStore *MemberStore, addr *net.Addr, minKey uint32, maxKey uint32) bool {
	memberStore.lock.Lock()
	defer memberStore.lock.Unlock()

	if memberStore.transferNodeAddr != nil {
		log.Println("WARN: A transfer is already in progress")
		return false
	} else {
		memberStore.transferNodeAddr = addr
	}

	go transferService.TransferKVStoreData(addr, minKey, maxKey, func() {
		memberStore.lock.Lock()
		memberStore.transferNodeAddr = nil
		memberStore.lock.Unlock()
	})

	return true
}

/**
Membership protocol: after receiving the transfer finished from the successor node,
sets status to normal
*/
func bootstrapTransferFinishedHandler(memberStore *MemberStore) {
	log.Println("RECEIVED TRANSFER FINISHED MSG")

	memberStore.lock.RLock()
	memberStore.members[memberStore.position].Status = STATUS_NORMAL
	// memberStore_.sortAndUpdateIdx()
	memberStore.lock.RUnlock()
}
