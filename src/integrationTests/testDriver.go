package main

import (
	pb "github.com/BCathcart/Distributed-Hash-Table/pb/protobuf"
	"github.com/BCathcart/Distributed-Hash-Table/src/util"
	"net"
	"strconv"
)

// Global variables
var ClientConn *net.PacketConn
var testReqCache_ *testCache
var putGetCache_ *putGetCache
var portKeyMap = make(map[int]uint32)
var prevRequests []*pb.KVRequest

// Global constants
const SHUTDOWN_TEST = 1
const PRINT_NODEORDER = 2
const PUTGET_TEST = 3

// ================ Global parameters - edit these ================
var localIPAddress = "192.168.1.74" // Make sure to use your actual IP, not 127.0.0.1 or localhost
var clientPort = 8090
var targetPorts = []int{8083} // Ports you want keys to land on. Will get replicated at next two ports

//	Kill Ports & alive ports are only used in the shutdown test.
var killPorts = targetPorts              // Ports to kill - does not have to be the same as targetPorts but is often convenient
var alivePorts = []int{8080, 8081, 8082} // Ports to keep alive
var listOfPorts = append(alivePorts, killPorts...)

const testToRun = SHUTDOWN_TEST

// ========================================================
func main() {
	validateParams()
	testReqCache_ = newTestCache()
	putGetCache_ = newPutGetCache()
	connection, err := net.ListenPacket("udp", ":"+strconv.Itoa(clientPort))
	if err != nil {
		panic(err)
	}
	setupPortMap()
	if testToRun != PRINT_NODEORDER {
		go MsgListener()
	}
	ClientConn = &connection
	switch testToRun {
	case SHUTDOWN_TEST:
		shutdownTest()
	case PUTGET_TEST:
		putGetTest()
	case PRINT_NODEORDER:
		printNodeOrder()
	}
}

func setupPortMap() {
	for i := 0; i < len(listOfPorts); i++ {
		addr, _ := util.GetAddr(localIPAddress, listOfPorts[i])
		portKeyMap[listOfPorts[i]] = util.GetAddrKey(addr)
	}
}

func paramError(errString string) {
	panic("Parameter error: " + errString)
}
func validateParams() {
	if len(alivePorts) == 0 {
		paramError("Need to leave one port alive")
	}

	for _, alivePort := range alivePorts {
		for _, killPort := range killPorts {
			if alivePort == killPort {
				paramError("Can't have port be both alive and to be killed")
			} else if alivePort == clientPort || killPort == clientPort {
				paramError("Can't have client port be on list of server ports")
			}
		}
	}
	for _, targetPort := range targetPorts {
		if !intInSlice(targetPort, listOfPorts) {
			paramError("Target port " + strconv.Itoa(targetPort) + " not in list of ports")
		}
	}
}
