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
func sendDataTransferReq(succAddr *net.Addr, coorAddr *net.Addr, keys util.KeyRange) {
	if succAddr == nil {
		log.Println("ERROR: Successor address should not be nil")
		return
	}

	addPendingTransfer(succAddr, coorAddr, keys)

	payload := util.SerializeAddr(coorAddr)
	log.Println("\nSENDING TRANSFER REQUEST FOR", (*coorAddr).String())
	requestreply.SendTransferReq(payload, succAddr)
}

/*
 * Responds to the transfer intiator if the transfer is expected. Drops the message otherwise.
 *
 * @param msg The transfer request message.
 * @return The payload for the response message.
 * @return True if a response should be sent, false otherwise
 */
func HandleTransferReq(msg *pb.InternalMsg) ([]byte, bool) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("RECEIVING TRANSFER REQUEST FOR ", (*addr).String())

	// ACK if the address in in expectedTransfers
	// i.e. we are expecting the transfer
	// This ensures the transfer only happens when both parties are ready

	if msg.Payload == nil {
		log.Println("ERROR: HandleTransferReq - Coordinator address can't be null")
		return nil, false
	}

	// Check if the transfer is expected
	for _, transfer := range expectedTransfers {
		if util.CreateAddressStringFromAddr(transfer.coordinator) == string(msg.Payload) {
			log.Println("INFO: TRANSFER IS EXPECTED!")
			transfer.timer.Reset(15 * time.Second) // Reset timer
			return msg.Payload, true
		}
	}

	addr, err := util.DeserializeAddr(msg.Payload)
	if err != nil {
		log.Println("ERROR: HandleTransferReq - ", err)
	} else {
		log.Println("ERROR: Not expecting a transfer for keys coordinated by ", util.CreateAddressStringFromAddr(addr))
		log.Println("Expecting ", expectedTransfers)
		log.Println("Predecessors: ", predecessors)
	}

	return nil, false
}

/*
 * Starts the transfer to the accepting receiver.
 *
 * @param sender The address of the sender who acknowledged the transfer request.
 * @param msg The transfer request ACK message.
 */
func HandleDataTransferRes(sender *net.Addr, msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("RECEIVING TRANSFER ACK FOR ", (*addr).String())

	if msg.Payload == nil {
		log.Println("ERROR: HandleDataTransferRes - Coordinator address can't be null")
		return
	}

	// Check if the transfer is expected
	for i, transfer := range pendingTransfers {
		log.Println("Checking for pending transfers")
		if string(util.SerializeAddr(transfer.coordinator)) == string(msg.Payload) &&
			string(util.SerializeAddr(transfer.receiver)) == string(util.SerializeAddr(sender)) {

			pendingTransfers = removePendingTransferInfoFromArr(pendingTransfers, i)

			// Start the transfer
			go transferService.TransferKVStoreData(transfer.receiver, transfer.keys.Low, transfer.keys.High, func() {
				log.Println("SENDING TRANSFER FINISHED FOR ", (*transfer.coordinator).String(), (*transfer.receiver).String())
				requestreply.SendTransferFinished(util.SerializeAddr(transfer.coordinator), transfer.receiver)
			})
			return
		}
	}

	addr, err := util.DeserializeAddr(msg.Payload)
	if err != nil {
		log.Println("ERROR: HandleDataTransferRes - ", err)
	} else {
		log.Println("ERROR: Not expecting a transfer for keys coordinated by ", util.CreateAddressStringFromAddr(addr))
	}
}

/*
 * Removes the finished transfer from the expected transfer array.
 *
 * @param msg The received TRANSFER_FINISHED message.
 */
func HandleTransferFinishedMsg(msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	coorAddr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("RECEIVING TRANSFER FINISHED MSG FOR ", (*coorAddr).String())

	removed := removeExpectedTransfer(coorAddr)

	if !removed {
		addr, _ := util.DeserializeAddr(msg.Payload)
		log.Println("ERROR: Unexpected HandleTransferFinishedMsg", util.CreateAddressStringFromAddr(addr))
	}
}
