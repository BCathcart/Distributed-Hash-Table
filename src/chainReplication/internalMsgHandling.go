package chainReplication

import (
	"log"
	"net"
	"time"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/transferService"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

// TRANSFER_REQ internal msg type
/*
 * Sends a request to initiate a data transfer with its successor (the receiver of the transfer).
 *
 * @param succAddr The address of the successor.
 * @param coorAddr The coordinator for the key range being transferred.
 * @param keys The key range to transfer.
 */
func sendDataTransferReq(succAddr *net.Addr, keys util.KeyRange, retry bool, force bool) {
	if succAddr == nil {
		log.Println("ERROR: Successor address should not be nil")
		return
	}

	if !retry {
		addExpectedTransfer(keys)
	}

	payload := util.SerializeKeyRangeTranReq(keys, force)
	log.Println("INFO: SENDING TRANSFER REQUEST FOR ", keys)
	requestreply.SendTransferReq(payload, succAddr)
}

/*
 * Responds to the transfer intiator if the transfer is expected. Drops the message otherwise.
 *
 * @param msg The transfer request message.
 * @return The payload for the response message.
 * @return True if a response should be sent, false otherwise
 */
func HandleTransferReq(senderAddr *net.Addr, msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	if msg.Payload == nil {
		log.Println("ERROR: HandleTransferReq - Payload can't be null")
		return
	}

	if successor == nil {
		log.Println("WARN: Cannot handle transfer request b/c successor is null")
		return
	}

	// Make sure the sender is our successor to keep the system consistent
	// TODO(Brennan): verify if this is needed
	if util.CreateAddressStringFromAddr(successor.addr) != util.CreateAddressStringFromAddr(senderAddr) {
		log.Println("WARN: Cannot handle transfer request b/c successor doesn't match the sender")
		return
	}

	keys, forceTransfer := util.DeserializeKeyRangeTranReq(msg.Payload)
	log.Println("RECEIVING TRANSFER REQUEST FOR ", keys)

	// Check if keys are in sendingTransfers
	for _, transferKeys := range sendingTransfers {
		if transferKeys.Low == keys.Low && transferKeys.High == keys.High {
			log.Println("INFO: Transfer is already in progress")
			return
		}
	}

	// Send transfer DONE if requests is between successor.High and myKeys.High
	// which means these keys were lost
	// TODO(Brennan): this gets triggerred when second joins
	log.Println(keys)
	log.Println(currentRange)
	log.Println(successor.keys)
	if (util.BetweenKeys(keys.High, responsibleRange.High, successor.keys.High) ||
		util.BetweenKeys(keys.Low, responsibleRange.High, successor.keys.High)) &&
		predecessors[2] != nil &&
		util.CreateAddressStringFromAddr(predecessors[2].addr) != util.CreateAddressStringFromAddr(successor.addr) {
		log.Println("WARN: Lost keys: ", keys)
		addSendingTransfer(keys)
		requestreply.SendTransferFinished(util.SerializeKeyRange(keys), successor.addr)
		return
	}

	if !forceTransfer {
		// Check if we have the keys
		if !util.BetweenKeys(keys.Low, currentRange.Low, currentRange.High) ||
			!util.BetweenKeys(keys.High, currentRange.Low, currentRange.High) {
			log.Println("WARN: Request for keys ", keys, " are not in the current range ", currentRange)
			return
		}
	} else {
		log.Println("WARN: Transfer being forced for ", keys, " with current range ", currentRange)
	}

	addSendingTransfer(keys)

	// Start the transfer
	succAddr := successor.addr
	go transferService.TransferKVStoreData(succAddr, keys.Low, keys.High, func() {
		log.Println("SENDING TRANSFER FINISHED to ", succAddr, " FOR ", keys)

		requestreply.SendTransferFinished(util.SerializeKeyRange(keys), succAddr)

		// Time out after 10 seconds (successor can then request again).
		// Assuming everything is working properly, this likely means the successor failed and that
		// the transfer will be removed anyways - still good to do theough just in case.
		delayedRemoveSendingTransfer(keys, 10)
	})
}

/*
 * Removes the finished transfer from the expected transfer array.
 *
 * @param msg The received TRANSFER_FINISHED message.
 */
func HandleTransferFinishedMsg(msg *pb.InternalMsg) []byte {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	keys := util.DeserializeKeyRange(msg.Payload)
	log.Println("RECEIVING TRANSFER FINISHED MSG FOR ", keys)

	removed := removeExpectedTransfer(keys)

	if !removed {
		// TODO: make log.Println()
		log.Fatal("ERROR: Unexpected HandleTransferFinishedMsg ", keys,
			". Expected transfers ", expectedTransfers)
	}

	updateCurrentRange(keys.Low)

	// Artificial delay to make sure any in-flight transfer requests
	// are handled before ACK is received
	time.Sleep(200 * time.Millisecond)

	return msg.Payload
}

/*
 * Starts the transfer to the accepting receiver.
 *
 * @param sender The address of the sender who acknowledged the transfer request.
 * @param msg The transfer request ACK message.
 */
func HandleDataTransferFinishedAck(sender *net.Addr, msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	if msg.Payload == nil {
		log.Println("INFO: Received ACK for bootstraping transfer finished msg")
		return
	}

	// Remove sending transfer
	// This ACK prevents the sender to respond to a re-sent transfer request before the
	// receiver knows that this transfer is finished (whole transfer would run again)

	keys := util.DeserializeKeyRange(msg.Payload)
	log.Println("RECEIVING TRANSFER FINISHED ACK FOR ", keys)

	log.Println(sendingTransfers)
	removed := removeSendingTransfer(keys)

	if !removed {
		log.Fatal("ERROR: Unexpected HandleDataTransferFinishedAck ", util.CreateAddressStringFromAddr(sender), keys)
	}
}
