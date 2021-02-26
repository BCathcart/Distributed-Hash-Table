package requestreply

import (
	"log"
	"net"

	pb "github.com/abcpen431/miniproject/pb/protobuf"
)

var externalReqHandler func([]byte) ([]byte, error) = nil
var internalReqHandler func(net.Addr, *pb.InternalMsg) = nil
var nodeUnavailableHandler func(addr *net.Addr) = nil

func setExternalReqHandler(handler func([]byte) ([]byte, error)) {
	externalReqHandler = handler
}

func setInternalReqHandler(handler func(net.Addr, *pb.InternalMsg)) {
	internalReqHandler = handler
}

func setNodeUnavailableHandler(handler func(addr *net.Addr)) {
	nodeUnavailableHandler = handler
}

func getExternalReqHandler() func([]byte) ([]byte, error) {
	if externalReqHandler == nil {
		log.Println("Error: External request handler has not been set")
	}
	return externalReqHandler
}

func getInternalReqHandler() func(net.Addr, *pb.InternalMsg) {
	if internalReqHandler == nil {
		log.Println("Error: Internal request handler has not been set")
	}
	return internalReqHandler
}

func getNodeUnavailableHandler() func(addr *net.Addr) {
	if nodeUnavailableHandler == nil {
		log.Println("Error: Node unavailable handler has not been set")
	}
	return nodeUnavailableHandler
}
