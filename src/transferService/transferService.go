package transferService

import (
	"log"
	"net"
	"time"

	pb "github.com/BCathcart/Distributed-Hash-Table/pb/protobuf"
	kvstore "github.com/BCathcart/Distributed-Hash-Table/src/kvStore"
	"github.com/BCathcart/Distributed-Hash-Table/src/requestreply"
	"github.com/BCathcart/Distributed-Hash-Table/src/util"
	"google.golang.org/protobuf/proto"
)

/* Membership protocol - transfers the necessary data to a joined node
@param ipStr/PortStr address to transfer keys to
@param predecessorKey key to transfer to
Sends a TRANSFER_FINISHED_MSG when it's done
*/

func TransferKVStoreData(addr *net.Addr, minKey uint32, maxKey uint32, transferFinishedCallback func()) {
	log.Println("TRANSFERRING KEYS TO MEMBER WITH ADDRESS: ", (*addr).String())
	localKeyList := kvstore.GetKeyList()

	// TODO a map to hold the keys that need to be transferred and whether they've been successfully sent to predecessor
	// var keysToTransfer map[int]bool will be added in next milestone

	// iterate thru key list, if key should be transferred, add it to transfer map, send it to predecessor
	for _, key := range localKeyList {
		hashVal := util.Hash([]byte(key))

		var shouldTransfer = util.BetweenKeys(hashVal, minKey, maxKey)

		if shouldTransfer {
			// get the kv from local kvStore, serialized and ready to send
			serPayload, err := kvstore.GetPutRequest(key)
			if err != nil {
				log.Println("WARN: Could not get kv from local kvStore")
				continue
			}

			log.Println("Sending transfer message to ", (*addr).String(), " with key ", hashVal)

			time.Sleep(1 * time.Millisecond)

			requestreply.SendDataTransferMessage(serPayload, addr)

			/* TODO: a receipt confirmation mechanism + retry policy: for now, assumes first transfer request is received successfully,
			doesn't wait for response to delete from local kvStore
			*/

		}
	}

	log.Println("TRANSFER FINISHED TO MEMBER WITH ADDRESS: ", (*addr).String())

	if transferFinishedCallback != nil {
		transferFinishedCallback()
	}
}

// DATA_TRANSFER_MSG internal msg type
func HandleDataMsg(addr net.Addr, msg *pb.InternalMsg) error {
	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(msg.GetPayload(), kvRequest)
	if err != nil {
		return err
	}

	log.Println("Got data transfer message for key ", util.Hash(kvRequest.Key))

	// TODO: send ack

	err = kvstore.InternalDataUpdate(kvRequest)
	return err
}

func HandleDataAck() {
	// remove from "waiting ack" store
}
