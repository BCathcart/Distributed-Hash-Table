package requestreply

import (
	"log"

	"github.com/abcpen431/miniproject/src/util"
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

//SendTransferRequest - a successor sends a KV pair to a joining predecessor
func SendTransferRequest(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, FORWARDED_CLIENT_REQ)
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
func SendTransferFinished(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, TRANSFER_FINISHED)
	return nil
}
