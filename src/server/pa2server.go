package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	kvstore "github.com/abcpen431/miniproject/src/kvStore"
	"github.com/abcpen431/miniproject/src/membership"
	requestreply "github.com/abcpen431/miniproject/src/requestreply"
	"github.com/abcpen431/miniproject/src/util"
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

	ip := GetOutboundIP()
	fmt.Println("MyIP", ip)

	// Bootstrap node
	requestreply.RequestReplyLayerInit(&connection)
	membership.MembershipLayerInit(&connection, otherMembers, ip.String(), int32(port))

	err = requestreply.MsgListener(kvstore.RequestHandler, membership.InternalMsgHandler)

	// Should never get here if everything is working
	return err
}

// Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
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

	// TODO: parse file with IP addresses and ports of other nodes
	var nodes []*net.UDPAddr
	data, err := util.ReadLines(args[2])
	if err != nil {
		log.Println("Could not read", args[2])
	}
	for _, line := range data {
		addr, err := net.ResolveUDPAddr("udp", line)
		if err != nil {
			fmt.Println("WARN invalid UDP address provided")
		} else {
			nodes = append(nodes, addr)
		}
	}
	if len(nodes) == 0 {
		fmt.Println("Error: No valid peer address provided")
		return
	}
	fmt.Println(nodes)

	err = runServer(nodes, port)
	if err != nil {
		fmt.Println("Server encountered an error. " + err.Error())
	}

	fmt.Println("Server closed")
}
