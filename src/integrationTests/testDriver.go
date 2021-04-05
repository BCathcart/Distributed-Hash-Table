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
var localIPAddr = "192.168.1.74"
var clientPort = 8089

func main() {
	//log.Println(ClientConn.LocalAddr().String())
	testReqCache_ = newTestCache()
	connection, _ := net.ListenPacket("udp", ":"+strconv.Itoa(clientPort))
	ClientConn = &connection
	//defer ClientConn.Close()
	//replicationTest() //Uncomment to run these tests
	//putGetTests() Work in progress
	go MsgListener()
	shutdownTest()
}
