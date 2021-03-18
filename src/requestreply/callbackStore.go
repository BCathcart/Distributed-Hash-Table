package requestreply

import (
	"log"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
)

/*
 * Getters and setters for internal (server-server) and external (client-server) request-response handlers
 */

var externalReqHandler func(*pb.InternalMsg) (*net.Addr, bool, []byte, error) = nil
var internalReqHandler func(net.Addr, *pb.InternalMsg) (*net.Addr, bool, []byte, error) = nil
var nodeUnavailableHandler func(*net.Addr) = nil
var internalResHandler func(net.Addr, *pb.InternalMsg) = nil

func setExternalReqHandler(handler func(*pb.InternalMsg) (*net.Addr, bool, []byte, error)) {
	externalReqHandler = handler
}

func setInternalReqHandler(handler func(net.Addr, *pb.InternalMsg) (*net.Addr, bool, []byte, error)) {
	internalReqHandler = handler
}

func setNodeUnavailableHandler(handler func(*net.Addr)) {
	nodeUnavailableHandler = handler
}

func setInternalResHandler(handler func(net.Addr, *pb.InternalMsg)) {
	internalResHandler = handler
}

func getExternalReqHandler() func(*pb.InternalMsg) (*net.Addr, bool, []byte, error) {
	if externalReqHandler == nil {
		log.Println("Error: External request handler has not been set")
	}
	return externalReqHandler
}

func getInternalReqHandler() func(net.Addr, *pb.InternalMsg) (*net.Addr, bool, []byte, error) {
	if internalReqHandler == nil {
		log.Println("Error: Internal request handler has not been set")
	}
	return internalReqHandler
}

func getNodeUnavailableHandler() func(*net.Addr) {
	if nodeUnavailableHandler == nil {
		log.Println("Error: Node unavailable handler has not been set")
	}
	return nodeUnavailableHandler
}

func getInternalResHandler() func(net.Addr, *pb.InternalMsg) {
	if internalResHandler == nil {
		log.Println("Error: Internal response handler has not been set")
	}
	return internalResHandler
}
