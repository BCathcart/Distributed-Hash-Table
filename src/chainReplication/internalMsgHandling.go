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

// TRANSFER_REQ internal msg type
func HandleTransferReq(msg *pb.InternalMsg) ([]byte, bool) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("\nRECEIVING TRANSFER REQUEST FOR ", (*addr).String())

	// ACK if the address in in expectedTransfers
	// i.e. we are expecting the transfer
	// This ensures the transfer only happens when both parties
	// are ready

	if msg.Payload == nil {
		log.Println("ERROR: HandleTransferReq - Coordinator address can't be null")
		return nil, false
	}

	// Check if the transfer is expected
	for _, transfer := range expectedTransfers {
		if util.CreateAddressStringFromAddr(transfer.coordinator) == string(msg.Payload) {
			log.Print("\nTRANSFER IS EXPECTED!\n")
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

// TRANSFER_RES internal msg
func HandleDataTransferRes(sender *net.Addr, msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	addr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("\nRECEIVING TRANSFER ACK FOR ", (*addr).String())

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
				log.Println("\n SENDING TRANSFER FINISHED FOR ", (*transfer.coordinator).String(), (*transfer.receiver).String())
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

// TRANSFER_FINISHED_MSG internal msg type
func HandleTransferFinishedMsg(msg *pb.InternalMsg) {
	coarseLock.Lock()
	defer coarseLock.Unlock()

	coorAddr, _ := util.DeserializeAddr(msg.Payload)
	log.Println("\nRECEIVING TRANSFER FINISHED MSG FOR ", (*coorAddr).String())

	removed := removeExpectedTransfer(coorAddr)

	if !removed {
		addr, _ := util.DeserializeAddr(msg.Payload)
		log.Println("ERROR: Unexpected HandleTransferFinishedMsg", util.CreateAddressStringFromAddr(addr))
	}
}
