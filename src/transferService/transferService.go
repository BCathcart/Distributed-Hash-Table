package transferService

import (
	"log"
	"strconv"

	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/* Membership protocol - transfers the necessary data to a joined node
@param ipStr/PortStr address to transfer keys to
@param predecessorKey key to transfer to
Sends a TRANSFER_FINISHED when it's done
*/

// Transfer all data with keys between minKey and maxKey
// If minkey is nil, transfer everything before maxKey
// If maxkey is nil, transfer everything after minKey
// Can't both be nil

func TransferKVStoreData(ipStr string, portStr string, minKey *uint32, maxKey *uint32, transferFinishedCallback func()) {

	log.Println("TRANSFERRING KEYS TO PREDECESSOR WITH ADDRESS: ", ipStr, ":", portStr)
	portInt, _ := strconv.Atoi(portStr)
	localKeyList := kvstore.GetKeyList()

	// TODO a map to hold the keys that need to be transferred and whether they've been successfully sent to predecessor
	// var keysToTransfer map[int]bool will be added in next milestone

	// iterate thru key list, if key should be transferred, add it to transfer map, send it to predecessor
	for _, key := range localKeyList {
		hashVal := util.Hash([]byte(key))

		var shouldTransfer = false
		// TODO(Brennan): update how we determine shouldTransfer
		if predecessorKey > curKey { // wrap around
			shouldTransfer = hashVal >= predecessorKey || curKey >= hashVal
		} else {
			shouldTransfer = hashVal <= predecessorKey
		}

		if shouldTransfer {
			// get the kv from local kvStore, serialized and ready to send
			serPayload, err := kvstore.GetPutRequest(key)
			if err != nil {
				log.Println("WARN: Could not get kv from local kvStore")
				continue
			}

			// send kv to predecessor using requestreply.SendTransferRequest (not sure this is what it's meant for)
			// uses sendUDPRequest under the hood
			err = requestreply.SendTransferRequest(serPayload, ipStr, portInt)

			/* TODO: a receipt confirmation mechanism + retry policy: for now, assumes first transfer request is received successfully,
			doesn't wait for response to delete from local kvStore
			*/
			wasRemoved := kvstore.RemoveKey(key)
			if wasRemoved == kvstore.NOT_FOUND {
				log.Println("key", key, "was not found in local kvStore")
			}

		}
	}

	log.Println("SENDING TRANSFER FINISHED TO PREDECESSOR WITH ADDRESS: ", ipStr, ":", portStr)

	_ = requestreply.SendTransferFinished([]byte(""), ipStr, portInt)

	transferFinishedCallback()
}
