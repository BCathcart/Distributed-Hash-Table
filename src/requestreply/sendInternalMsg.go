package requestreply

import (
	"log"
	"net"

	"github.com/BCathcart/Distributed-Hash-Table/src/util"
)

/*
 * API to the request-reply layer
 * Sender interface functions for every type of internal message
 * Each function:
 * 	- constructs the recipient address
 * 	- calls sendUDPRequest() with the appropriate payload
 * All take @param ip and @param port
 * @param payload is a marshalled differently for each message type
 */

//SendMembershipRequest - sent by a bootstrapping node to an existing member node
func SendMembershipRequest(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, MEMBERSHIP_REQ)
	return nil
}

//SendPingRequest - a simple ping
func SendPingRequest(addr *net.Addr) error {
	sendUDPRequest(addr, nil, PING_MSG)
	return nil
}

//SendHeartbeatMessage - gossip heartbeat
func SendHeartbeatMessage(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	log.Println("SendHeartbeatMessage to", (*addr).String())

	sendUDPRequest(addr, payload, HEARTBEAT_MSG)
	return nil
}

//SendTransferFinished - successor sends to predecessor to indicate all KV pairs
// have been sent - can complete boostrap and assume responsibility for the keys
func SendTransferFinished(payload []byte, addr *net.Addr) {
	sendUDPRequest(addr, payload, TRANSFER_FINISHED_MSG)
}

//SendDataTransferMessage - sends a key-value pair (i.e. an internal PUT request)
func SendDataTransferMessage(payload []byte, addr *net.Addr) {
	sendUDPRequest(addr, payload, DATA_TRANSFER_MSG)
}

// TRANSFER_REQ - establishes mutual agreement that a transfer is needed for replication purposes
func SendTransferReq(payload []byte, addr *net.Addr) {
	sendUDPRequest(addr, payload, TRANSFER_REQ)
}
