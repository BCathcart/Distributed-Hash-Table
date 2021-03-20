package requestreply

import (
	"log"
	"net"

	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
)

/*
 * Getters and setters for internal (server-server) and external (client-server) request-response handlers
 */

type reqHandlerFunc func(net.Addr, *pb.InternalMsg) (*net.Addr, bool, []byte, error)

var externalReqHandler reqHandlerFunc = nil
var internalReqHandler reqHandlerFunc = nil
var nodeUnavailableHandler func(*net.Addr) = nil
var internalResHandler func(net.Addr, *pb.InternalMsg) = nil

func setExternalReqHandler(handler reqHandlerFunc) {
	externalReqHandler = handler
}

func setInternalReqHandler(handler reqHandlerFunc) {
	internalReqHandler = handler
}

func setNodeUnavailableHandler(handler func(*net.Addr)) {
	nodeUnavailableHandler = handler
}

func setInternalResHandler(handler func(net.Addr, *pb.InternalMsg)) {
	internalResHandler = handler
}

func getExternalReqHandler() reqHandlerFunc {
	if externalReqHandler == nil {
		log.Println("Error: External request handler has not been set")
	}
	return externalReqHandler
}

func getInternalReqHandler() reqHandlerFunc {
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
