package main

import (
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"google.golang.org/protobuf/proto"
	"log"
)

/*
	This file was copied over from protocol.go in the requestreply layer.
*/
func MsgListener() error {
	buffer := make([]byte, MAX_BUFFER_SIZE)

	// Listen for packets
	for {
		n, senderAddr, err := (*ClientConn).ReadFrom(buffer)
		if err != nil {
			return err
		}

		// Deserialize message
		msg := &pb.Msg{}
		err = proto.Unmarshal(buffer[0:n], msg)
		if err != nil {
			// Disregard messages with invalid format
			log.Println("WARN msg with invalid format. Sender = " + senderAddr.String())
		}

		// Verify checksum
		if !verifyChecksum(msg) {
			// Disregard messages with invalid checksums
			log.Println("WARN checksum mismatch. Sender = " + senderAddr.String())
			continue
		}

		go processResponse(senderAddr, msg)
	}
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msg The received message.
* @return True if message ID matches the expected ID and checksum is valid, false otherwise.
 */
func verifyChecksum(msg *pb.Msg) bool {
	// Verify MessageID is as expected
	if uint64(util.ComputeChecksum((*msg).MessageID, (*msg).Payload)) != (*msg).CheckSum {
		return false
	}
	return true
}
