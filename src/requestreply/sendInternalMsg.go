package requestreply

import (
	"log"
	"net"

	"github.com/abcpen431/miniproject/src/util"
)

func SendMembershipRequest(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, MEMBERSHIP_REQUEST)
	return nil
}

func SendTransferRequest(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, TRANSFER_REQ)
	return nil
}

func SendPingRequest(ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, nil, PING)
	return nil
}

func SendHeartbeatMessage(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, HEARTBEAT)
	return nil
}

func SendTransferFinished(payload []byte, ip string, port int) error {
	addr, err := util.GetAddr(ip, port)
	if err != nil {
		log.Println("WARN Could not resolve member UDP addr")
		return err
	}
	sendUDPRequest(addr, payload, TRANSFER_FINISHED)
	return nil
}

func SendHeatbeatRespose(addr net.Addr, messageID []byte) {
	sendUDPResponse(addr, messageID, nil, true)
	//TODO: nil payload will not cause issues?
}
