package main

import (
	"net"
	"strconv"
)

/*
	These tests were used primarily for debugging, however are no longer functional as we have changed the
	request / reply functionality so that the node a client sends to may not be the node that replies.

	This means we need to switch to a connectionless setup, and cache requests / responses similar to the internal
	messages. Currently a work in progress.
*/

var ClientConn *net.PacketConn
var testReqCache_ *testCache
var putGetCache_ *putGetCache

// ================ Edit these parameters ================
var localIPAddress = "192.168.1.74"
var clientPort = 8090
var targetPorts = []int{8081, 8083, 8084} // Ports you want keys to land on. Will get replicated at next two ports
// Kill Ports & alive ports are only used in the shutdown test
//var killPorts = []int{8083, 8081, 8082} // Ports to kill
var killPorts = targetPorts
var alivePorts = []int{8080, 8082, 8085, 8086, 8087, 8088, 8089} // Ports to keep alive
var listOfPorts = append(alivePorts, killPorts...)

// ========================================================
func main() {
	//log.Println(ClientConn.LocalAddr().String())
	validateParams()
	testReqCache_ = newTestCache()
	putGetCache_ = newPutGetCache()
	connection, _ := net.ListenPacket("udp", ":"+strconv.Itoa(clientPort))
	ClientConn = &connection
	//defer ClientConn.Close()
	//replicationTest() //Work in progress
	//putGetTests() Work in progress
	//keyRangeTest() // Run this test first to get your key orders
	go MsgListener()
	shutdownTest()
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
