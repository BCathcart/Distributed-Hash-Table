package chainReplication

import (
	"log"
	"net"
	"time"

	pb "github.com/BCathcart/Distributed-Hash-Table/pb/protobuf"
	"github.com/BCathcart/Distributed-Hash-Table/src/requestreply"
	"github.com/BCathcart/Distributed-Hash-Table/src/transferService"
	"github.com/BCathcart/Distributed-Hash-Table/src/util"
)

/*
 * Sends a request to initiate a data transfer with its successor (the receiver of the transfer).
 *
 * @param predAddr The address of the first predecessor.
 * @param keys The key range to request a transfer for.
 * @param retry True if this is a retry, false if this is the first request for the key range.
 * @param force True if the predecessor should be forced to do the transfer even if it doesn't have the full key range.
 */
func sendDataTransferReq(predAddr *net.Addr, keys util.KeyRange, retry bool, force bool) {
	if predAddr == nil {
		log.Println("ERROR: Can't request transfer from nil predecessor")
		return
	}

	if !retry {
		addExpectedTransfer(keys)
	}

	payload := util.SerializeKeyRangeTranReq(keys, force)
	log.Println("INFO: SENDING TRANSFER REQUEST FOR ", keys)
	requestreply.SendTransferReq(payload, predAddr)
}

/*
 * Responds to the transfer intiator if the transfer can be done. Drops the message otherwise.
 *
 * @param senderAddr The sender's address.
 * @param msg The transfer request message.
 */
func HandleTransferReq(senderAddr *net.Addr, msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	// For now, let's only allow one transfer at a time for easier debugging
	if len(sendingTransfers) > 0 {
		log.Println("WARN: Dropping transfer req b/c only allowing one transfer at a time ", sendingTransfers[0])
		return
	}

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
	log.Println("INFO: Received transfer request for ", keys)

	if !forceTransfer {
		// Check if we don't have the keys
		if !util.BetweenKeys(keys.Low, currentRange.Low, currentRange.High) ||
			!util.BetweenKeys(keys.High, currentRange.Low, currentRange.High) {
			log.Println("WARN: Request for keys ", keys, " are not in the current range ", currentRange)
			if predecessors[2] != nil {
				// Get overlapping range
				overlapKeys := util.OverlappingKeyRange(currentRange, keys)
				if overlapKeys == nil {
					return
				}
				log.Println("INFO: Sending part of the range of keys ", overlapKeys)

				// NOTE: Any keys requested above the range the predecessor currently has are lost
				keys = util.KeyRange{overlapKeys.Low, keys.High}
			} else {
				// Let the force transfers play out if their are less than 3 nodes
				return
			}
		}
	} else {
		log.Println("WARN: Transfer being forced for ", keys, " with current range ", currentRange)
	}

	// Check if keys are in sendingTransfers
	for _, transferKeys := range sendingTransfers {
		if transferKeys.Low == keys.Low && transferKeys.High == keys.High {
			log.Println("INFO: Transfer is already in progress")
			return
		}
	}

	addSendingTransfer(keys)

	// Start the transfer
	succAddr := successor.addr
	go transferService.TransferKVStoreData(succAddr, keys.Low, keys.High, func() {
		log.Println("INFO: SENDING TRANSFER FINISHED to ", (*succAddr).String(), " FOR ", keys)

		requestreply.SendTransferFinished(util.SerializeKeyRange(keys), succAddr)

		time.Sleep(250 * time.Millisecond)

		// Time out after 10 seconds (successor can then request again).
		// Assuming everything is working properly, this likely means the successor failed and that
		// the transfer will be removed anyways - still good to do just in case.
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

	if msg.Payload == nil {
		log.Println("ERROR: HandleTransferFinishedMsg - Payload can't be null")
		return nil
	}

	keys := util.DeserializeKeyRange(msg.Payload)
	log.Println("RECEIVING TRANSFER FINISHED MSG FOR ", keys)

	if len(expectedTransfers) == 0 {
		log.Println("ERROR: No transfers expected - received transfer for ", keys)
		return nil
	}

	// NOTE: we only have one expected transfer at a time right now
	if expectedTransfers[0].keys.High != keys.High {
		log.Println("ERROR: Unexpected HandleTransferFinishedMsg ", keys,
			". Expected transfers ", expectedTransfers)
	} else if expectedTransfers[0].keys.Low == keys.Low {
		removed := removeExpectedTransfer(keys)
		if !removed {
			log.Println("WARN: Unexpected HandleTransferFinishedMsg ", keys,
				". Expected transfers: ")
			for _, transfer := range expectedTransfers {
				log.Println(*transfer)
			}
		}
		updateCurrentRange(keys.Low, true)
	} else {
		// We received a partial transfer, update the expected transfer
		log.Println("INFO: We received a partial transfer ", keys)
		expectedTransfers[0].keys.High = keys.Low - 1
		updateCurrentRange(keys.Low, false)
	}

	// Artificial delay to make sure any in-flight transfer requests
	// are handled before ACK is received
	time.Sleep(10 * time.Millisecond)

	return msg.Payload
}

/*
 * Removes the transfer from sendingTransfers array. This ACK prevents the sender to respond to
 * a re-sent transfer request before the receiver knows that this transfer is finished
 * (whole transfer would run again).
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

	keys := util.DeserializeKeyRange(msg.Payload)
	log.Println("RECEIVING TRANSFER FINISHED ACK FOR ", keys)

	removed := removeSendingTransfer(keys)
	if !removed {
		log.Println("ERROR: Unexpected HandleDataTransferFinishedAck ", util.CreateAddressStringFromAddr(sender), keys)
	}
}
