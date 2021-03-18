package transferService

import (
	"log"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
)

/* Membership protocol - transfers the necessary data to a joined node
@param ipStr/PortStr address to transfer keys to
@param predecessorKey key to transfer to
Sends a TRANSFER_FINISHED when it's done
*/

func TransferKVStoreData(addr *net.Addr, minKey uint32, maxKey uint32, transferFinishedCallback func()) {

	log.Println("TRANSFERRING KEYS TO PREDECESSOR WITH ADDRESS: ", (*addr).String())
	localKeyList := kvstore.GetKeyList()

	// TODO a map to hold the keys that need to be transferred and whether they've been successfully sent to predecessor
	// var keysToTransfer map[int]bool will be added in next milestone

	// iterate thru key list, if key should be transferred, add it to transfer map, send it to predecessor
	for _, key := range localKeyList {
		hashVal := util.Hash([]byte(key))

		var shouldTransfer = false
		// TODO: verify if this wrap around works
		if minKey > maxKey { // wrap around
			shouldTransfer = hashVal >= minKey || maxKey >= hashVal
		} else {
			shouldTransfer = hashVal <= minKey
		}

		if shouldTransfer {
			// get the kv from local kvStore, serialized and ready to send
			serPayload, err := kvstore.GetPutRequest(key)
			if err != nil {
				log.Println("WARN: Could not get kv from local kvStore")
				continue
			}

			err = requestreply.SendDataTransferMessage(serPayload, addr)

			/* TODO: a receipt confirmation mechanism + retry policy: for now, assumes first transfer request is received successfully,
			doesn't wait for response to delete from local kvStore
			*/
			wasRemoved := kvstore.RemoveKey(key)
			if wasRemoved == kvstore.NOT_FOUND {
				log.Println("key", key, "was not found in local kvStore")
			}

		}
	}

	log.Println("SENDING TRANSFER FINISHED TO PREDECESSOR WITH ADDRESS: ", (*addr).String())

	_ = requestreply.SendTransferFinished([]byte(""), addr)

	transferFinishedCallback()
}

// DATA_TRANSFER internal msg type
func HandleDataMsg(addr net.Addr, msg *pb.InternalMsg) error {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return err
	}

	err = kvstore.InternalDataUpdate(kvRequest)
	return err
}
