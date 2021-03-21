package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/CPEN-431-2021/dht-abcpen431/src/membership"
	requesthandler "github.com/CPEN-431-2021/dht-abcpen431/src/requestHandler"
	requestreply "github.com/CPEN-431-2021/dht-abcpen431/src/requestreply"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/**
* Runs the server. Should never return unless an error is encountered.
* @param port The port to listen for UDP packets on.
* @return an error
 */
func runServer(otherMembers []*net.UDPAddr, port int) error {
	// Listen on all available IP addresses
	connection, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer connection.Close()

	// find local IP address
	ip := util.GetOutboundIP()
	fmt.Println("MyIP", ip)

	// Bootstrap node
	// init the request/reply protocol layer
	requestreply.RequestReplyLayerInit(&connection, requesthandler.ExternalReqHandler, requesthandler.InternalReqHandler, membership.MemberUnavailableHandler, requesthandler.InternalResHandler)

	// init the membership service layer
	membership.Init(&connection, otherMembers, ip.String(), int32(port))

	// run until interrupted
	err = requestreply.MsgListener()

	// Should never get here if everything is working
	return err
}

func main() {

	// TASK 2: Dockerize project and parse port and member list file arguments.
	// I added the hardcoded "nodes" array that could be used for testing.
	// - Rozhan

	// Parse cmd line args
	args := os.Args
	if len(args) != 3 {
		fmt.Printf("ERROR: Expecting 2 arguments (received %d): $PORT $PEERS_FILE\n", len(args)-1)
		return
	}

	port, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Println("ERROR: Port is not a valid number")
		return
	}

	if port < 1 || port > 65535 {
		fmt.Println("ERROR: Invalid port number (must be between 1 and 65535)")
		return
	}

	data, err := util.ReadLines(args[2])
	if err != nil {
		log.Println("Could not read", args[2])
	}

	// get addresses to existing nodes
	var nodes []*net.UDPAddr
	for _, line := range data {
		addr, err := net.ResolveUDPAddr("udp", line)
		if err != nil {
			fmt.Println("WARN invalid UDP address provided")
		} else {
			nodes = append(nodes, addr)
		}
	}
	fmt.Println(nodes)

	// start the server
	err = runServer(nodes, port)
	if err != nil {
		fmt.Println("Server encountered an error. " + err.Error())
	}

	fmt.Println("Server closed")
}
