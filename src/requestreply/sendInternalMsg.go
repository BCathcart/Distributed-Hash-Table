package requestreply

import (
	"log"
	"net"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
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
	sendUDPRequest(addr, payload, MEMBERSHIP_REQUEST)
	return nil
}

//SendPingRequest - a simple ping
func SendPingRequest(ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, nil, PING)
	return nil
}

//SendHeartbeatMessage - gossip heartbeat
func SendHeartbeatMessage(payload []byte, ip string, port int) error {
	log.Println("SendHeartbeatMessage")
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, HEARTBEAT)
	return nil
}

//SendTransferFinished - successor sends to predecessor to indicate all KV pairs
// have been sent - can complete boostrap and assume responsibility for the keys
func SendTransferFinished(payload []byte, addr *net.Addr) error {
	sendUDPRequest(addr, payload, TRANSFER_FINISHED)
	return nil
}

//SendDataTransferMessage - sends a key-value pair (i.e. an internal PUT request)
func SendDataTransferMessage(payload []byte, addr *net.Addr) error {
	sendUDPRequest(addr, payload, DATA_TRANSFER)
	return nil
}
