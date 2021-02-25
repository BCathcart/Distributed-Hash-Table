package main

import (
	"container/list"
	"fmt"
	pb "github.com/abcpen431/miniproject/pb/protobuf"
	"github.com/abcpen431/miniproject/src/server/util"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

var MAX_MEM_USAGE uint64 = 120 * 1024 * 1024 // Max is actually 128 MB (8MB of buffer)

/***** GOSSIP PROTOCOL *****/

const STATUS_NORMAL = 0x1
const STATUS_BOOTSTRAPPING = 0x2
const STATUS_UNAVAILABLE = 0x3

const HEARTBEAT_INTERVAL = 1000 // ms

// Maps msg ID to serialized response
var memberStore_ *MemberStore
var key_ int

type Member struct {
	ip        string
	port      int
	key       int // Hash keys of all members (position in ring)
	heartbeat int
	status    int
}

type MemberStore struct {
	lock     sync.Mutex
	members  []Member
	position int
}

/**
* Creates and returns a pointer to a new MemberStore
* @return The member store.
 */
func NewMemberStore() *MemberStore {
	memberStore := new(MemberStore)
	return memberStore
}

/**
* Sorts and updates the
* @return The member store.
 */
func sortAndUpdateIdx() int {
	// TODO: lock
	sort.SliceStable(memberStore_.members, func(i, j int) bool {
		return memberStore_.members[i].key < memberStore_.members[j].key
	})

	// find and update index of the current key

	for i, _ := range memberStore_.members {
		if memberStore_.members[i].key == key_ {
			memberStore_.position = i
			// TODO: unlock
			return i
		}
	}

	// TODO: unlock

	// Should never get here!
	log.Println("Error: could not find own key in member array")
	return -1
}

/**
* Updates the heartbeat by one.
 */
func tickHeartbeat() {
	memberStore_.lock.Lock()
	memberStore_.members[memberStore_.position].heartbeat += 1
	memberStore_.lock.Unlock()
}

// Source: Sathish's campuswire post #310, slightly modified
func GetOutboundAddress() net.Addr {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr()

	return localAddr
}

// TOM
// TASK3 (part 1): Membership protocol (bootstrapping process)
func makeMembershipReq() {
	// Send request to random node (from list of nodes)
	randMember := getRandMember()
	randMemberAddr := util.CreateAddressString(randMember.ip, randMember.port)
	localAddr := GetOutboundAddress()
	localAddrStr := localAddr.String()
	reqPayload := []byte(localAddrStr)

	udpAddr, _ := net.ResolveUDPAddr("udp", randMemberAddr)
	var addr net.Addr = udpAddr
	sendUDPRequest(&addr, 1, randMember.key, reqPayload, MEMBERSHIP_REQUEST)

	// TODO: This comment below
	// Repeat this request periodically until receive TRANSFER_FINISHED message
	// to protect against nodes failing (this will probably be more important for later milestones)
}

// Rozhan
// TASK4 (part 1): Gossip heartbeat (send the entire member array in the MemberStore).
func gossipHeartbeat() {
	// Package MemberStore.members array

}

// Shay
// TASK3 (part 3): Membership protocol - transfers the necessary data to a joined node
// The actual transfer from the succesor to the predecessor
// Send a TRANSFER_FINISHED when it's done
func transferToPredecessor(addr net.Addr, targetKey int /* predecessor key */) { // Did this so there wouldn't be an error
	// Send all key-value pairs that is the responsibility of the predecessor
	// Use PUT requests (like an external client)
}

// TOM
// TASK3 (part 2): Membership protocol
/**
@param addr the address that sent the message
@param InternalMsg the internal message being sent
*/
func membershipReqHandler(addr net.Addr, msg *pb.InternalMsg) {
	// Find successor node and forward the
	// membership request there to start the transfer

	ipStr, portStr := util.GetIPPort(string(msg.Payload))
	targetKey := util.GetNodeKey(ipStr, portStr)
	nodeIsSuccessor, err := isSuccessor(targetKey)
	if err != nil {
		log.Println("Error finding successor") // TODO actually handle error
	}
	if nodeIsSuccessor {
		go transferToPredecessor(addr, targetKey) // TODO Not sure about how to call this.
	} else {
		targetNodePosition := searchForSuccessor(targetKey)
		targetMember := memberStore_.members[targetNodePosition]
		targetMemberAddr := util.CreateAddressString(targetMember.ip, targetMember.port)
		udpAddr, _ := net.ResolveUDPAddr("udp", targetMemberAddr)
		var addr net.Addr = udpAddr
		forwardUDPRequest(&addr, targetMember.key, nil, msg) // TODO Don't know about return addr param
	}

	// If this node is the successor, start transferring keys

	// If this node is the successor and already in the process of
	// receiving or sending a transfer, respond with "Busy" to the request.
	// The node sending the request will then re-send it after waiting a bit.
}

// Rozhan
// TASK4 (part 2): Compare incoming member array with current member array and
// update entries to the one with the larger heartbeat (i.e. newer gossip)
func heartbeatHandler(addr net.Addr, msg *pb.InternalMsg) {
	// Compare Members list and update as necessary
	// Need to ignore any statuses of "Unavailable" (or just don't send them)
	// since failure detection is local.

	// (Not a big priority for M1) If we receive a heartbeat update from a predecessor
	// that had status "Unavailable" at this node, then we can transfer any keys we were storing for it
	// - need to check version number before writing

	// Sort the member store if we just leaned of a new node
	sortAndUpdateIdx()
}

/* Ignore this */
// func transferStartedHandler(addr net.Addr, msg *pb.InternalMsg) {
// 	// Register that the transfer is started
// 	// Start timeout, save IP of sender, and reset timer everytime receive a write from
// 	// the IP address
// 	// If timeout hit, set status to "Normal"
// }

// TOM + Shay
// TASK3 (part 4): Membership protocol - transfer to this node is finished
func transferFinishedHandler(addr net.Addr, msg *pb.InternalMsg) {
	memberStore_.members[memberStore_.position].status = STATUS_NORMAL
	// End timer and set status to "Normal"
	// Nodes will now start sending requests directly to us rather than to our successor.
}

func internalMsgHandler(conn *net.PacketConn, addr net.Addr, msg *pb.InternalMsg) {
	switch msg.InternalID {
	case MEMBERSHIP_REQUEST:
		membershipReqHandler(addr, msg)

	case HEARTBEAT:
		heartbeatHandler(addr, msg)

	case TRANSFER_FINISHED:
		transferFinishedHandler(addr, msg)

	default:
		log.Println("WARN: Invalid InternalID: " + strconv.Itoa(int(msg.InternalID)))
	}
}

func bootstrap(conn *net.PacketConn, otherMembers []struct {
	string
	int
}, port int) {
	memberStore_ = NewMemberStore()

	curIP := GetOutboundAddress().(*net.UDPAddr).IP
	curPort := GetOutboundAddress().(*net.UDPAddr).Port

	key_ = util.GetNodeKey(string(curIP), strconv.Itoa(curPort))

	var status int
	if len(otherMembers) == 0 {
		status = STATUS_NORMAL
	} else {
		status = STATUS_BOOTSTRAPPING
	}

	// Add this node to Member array
	memberStore_.members = append(memberStore_.members, Member{ip: "", port: port, key: key_, heartbeat: 0, status: status})
	memberStore_.position = 0

	// Update heartbeat every HEARTBEAT_INTERVAL seconds
	var ticker = time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL)
	go func() {
		for {
			<-ticker.C
			tickHeartbeat()
		}
	}()

	requestReplyLayerInit()

	// Send initial membership request message - this tells receiving node they should try to contact the successor first (as well as
	// respond with this node's IP address if needed)

	// If no other nodes are known, then assume this is the first node
	// and this node simply waits to be contacted
	if len(otherMembers) != 0 {
		makeMembershipReq()
	}

}

/************* APPLICATION CODE *************/
const MAX_KEY_LEN = 32
const MAX_VAL_LEN = 10000

// REQUEST COMMANDS
const PUT = 0x01
const GET = 0x02
const REMOVE = 0x03
const SHUTDOWN = 0x04
const WIPEOUT = 0x05
const IS_ALIVE = 0x06
const GET_PID = 0x07
const GET_MEMBERSHIP_COUNT = 0x08

// ERROR CODES
const OK = 0x00
const NOT_FOUND = 0x01
const NO_SPACE = 0x02
const OVERLOAD = 0x03
const UKN_FAILURE = 0x04
const UKN_CMD = 0x05
const INVALID_KEY = 0x06
const INVALID_VAL = 0x07

const OVERLOAD_WAIT_TIME = 5000 // ms

/* Reserve 38 MB of space for program, caching, and serving requests */
const MAX_KV_STORE_SIZE = 90 * 1024 * 1024

/* KEY VALUE STORE */
var kvStore_ *KVStore
var kvStoreSize_ uint32

type KVStore struct {
	lock sync.Mutex
	data *list.List
}

type KVEntry struct {
	key string
	val []byte
	ver int32
}

/**
* Creates and returns a pointer to a new key-value store.
* @return The key-value store.
 */
func NewKVStore() *KVStore {
	store := new(KVStore)
	store.data = list.New()
	return store
}

/**
* Fetches an element from the key-value store list.
* @param key The key to search for.
* @return The KVEntry if it exists, nil otherwise.
 */
func findListElem(key string) *KVEntry {
	for e := kvStore_.data.Front(); e != nil; e = e.Next() {
		if strings.Compare(e.Value.(KVEntry).key, key) == 0 {
			entry := e.Value.(KVEntry)
			return &entry
		}
	}
	return nil
}

/**
* Removes an element from the key-value store list.
* @param key The key of the entry to remove.
* @return The KVEntry if it exists, nil otherwise.
 */
func removeListElem(key string) (bool, int) {
	for e := kvStore_.data.Front(); e != nil; e = e.Next() {
		if strings.Compare(e.Value.(KVEntry).key, key) == 0 {
			len := len(e.Value.(KVEntry).key) + len(e.Value.(KVEntry).val) + 4
			kvStore_.data.Remove(e)
			return true, len
		}
	}
	return false, 0
}

/**
* Puts an entry in the key-value store.
* @param key The key.
* @param val The value.
* @param version The key-value pair version number.
* @return OK if there is space, NO_SPACE if the store is full.
 */
func putKVEntry(key string, val []byte, version int32) uint32 {
	kvStore_.lock.Lock()

	// Remove needed to decrement kvStoreSize_ if key already exists
	removeKVEntry(key) // *neeeds to be in critical section

	// Check if the store is full
	if kvStoreSize_ > MAX_KV_STORE_SIZE {
		kvStore_.lock.Unlock()
		return NO_SPACE
	}

	kvStore_.data.PushBack(KVEntry{key: key, val: val, ver: version})
	kvStoreSize_ += uint32(len(key) + len(val) + 4) // Increase kv store size
	kvStore_.lock.Unlock()                          // TODO: this caused fatal error???

	log.Println(kvStoreSize_)

	return OK
}

/**
* Gets an entry from the key-value store.
* @param key The key of the entry to retrieve.
* @return The KVEntry if it exists.
* @return OK if the key exists, NOT_FOUND otherwise.
 */
func getKVEntry(key string) (KVEntry, uint32) {
	var found bool
	var entry KVEntry
	kvStore_.lock.Lock()
	res := findListElem(key)
	if res != nil {
		found = true
		entry = *res
	} else {
		found = false
	}
	kvStore_.lock.Unlock()

	if found {
		return entry, OK
	} else {
		return KVEntry{}, NOT_FOUND
	}
}

/**
* Removes an entry from the key-value store.
* @param key The key of the entry to remove.
* @return OK if the key exists, NOT_FOUND otherwise.
 */
func removeKVEntry(key string) uint32 {
	success, n := removeListElem(key)
	if success == true {
		kvStoreSize_ -= uint32(n) // Decrease kv store size
	}

	log.Println(kvStoreSize_)

	if success == true {
		return OK
	} else {
		return NOT_FOUND
	}
}

/**
* Returns the process' memory usage in bytes.
* @return the memory usage.
 */
func memUsage() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return (stats.Alloc + stats.StackSys)
}

/**
* Prints the process' memory statistics.
* Source: https://golangcode.com/print-the-current-memory-usage/
 */
func PrintMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	log.Printf("\t Stack = %v\n", bToMb(m.StackSys))
	log.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	log.Printf("\tSys = %v MiB", bToMb(m.Sys))
	log.Printf("\tNum GC cycles = %v\n", m.NumGC)
}

/**
* Converts bytes to megabytes.
* @param b The byte amount.
* @return The corresponding MB amount.
* Source: https://golangcode.com/print-the-current-memory-usage/
 */
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

/**
* Handles incoming requests.
* @param serializedReq The serialized KVRequest.
* @return A serialized KVResponse, nil if there was an error.
* @return Error object if there was an error, nil otherwise.
 */
func handleOverload() *pb.KVResponse {
	log.Println("Overloaded: " + strconv.Itoa(int(memUsage())))
	PrintMemStats()

	debug.FreeOSMemory() // Force GO to free unused memory

	kvRes := &pb.KVResponse{}
	wait := int32(OVERLOAD_WAIT_TIME)
	kvRes.OverloadWaitTime = &wait

	return kvRes
}

/**
* Handles incoming requests.
* @param serializedReq The serialized KVRequest.
* @return A serialized KVResponse, nil if there was an error.
* @return Error object if there was an error, nil otherwise.
 */
func requestHandler(serializedReq []byte) ([]byte, error) {
	var errCode uint32
	kvRes := &pb.KVResponse{}

	/* NOTE: When there is an OVERLOAD and we are reaching the memory limit,
	we only restrict PUT and GET requests. REMOVE and WIPEOUT may increase
	the memory momentarily, but the benifit of the freed up space outweighs
	the momentary costs. */

	// Unmarshal KVRequest
	kvRequest := &pb.KVRequest{}
	err := proto.Unmarshal(serializedReq, kvRequest)
	if err != nil {
		return nil, err
	}

	cmd := kvRequest.Command
	key := string(kvRequest.Key)
	value := kvRequest.Value
	var version int32
	if kvRequest.Version != nil {
		version = *kvRequest.Version
	} else {
		version = 0
	}

	// Determine action based on the command
	switch cmd {
	case PUT:
		if len(key) > MAX_KEY_LEN {
			errCode = INVALID_KEY
		} else if len(value) > MAX_VAL_LEN {
			errCode = INVALID_VAL
		} else if memUsage() > MAX_MEM_USAGE {
			kvRes = handleOverload()
			errCode = OVERLOAD
		} else {
			errCode = putKVEntry(key, value, version)
		}

	case GET:
		if len(key) > MAX_KEY_LEN {
			errCode = INVALID_KEY
		} else if memUsage() > MAX_MEM_USAGE {
			kvRes = handleOverload()
			errCode = OVERLOAD
		} else {
			entry, code := getKVEntry(key)
			if code == OK {
				kvRes.Value = entry.val
				kvRes.Version = &entry.ver
			}

			errCode = code
		}

	case REMOVE:
		if len(key) > MAX_KEY_LEN {
			errCode = INVALID_KEY
		} else {
			kvStore_.lock.Lock()
			errCode = removeKVEntry(key)
			kvStore_.lock.Unlock()
		}

	case SHUTDOWN:
		os.Exit(1)

	case WIPEOUT:
		kvStore_.data.Init() // Clears the list
		debug.FreeOSMemory() // Force GO to free unused memory
		errCode = OK
		kvStoreSize_ = 0

	case IS_ALIVE:
		errCode = OK

	case GET_PID:
		pid := int32(os.Getpid())
		kvRes.Pid = &pid
		errCode = OK

	case GET_MEMBERSHIP_COUNT:
		tmpMemCount := int32(1) // Note: this will need to be updated in later PA
		kvRes.MembershipCount = &tmpMemCount
		errCode = OK

	default:
		errCode = UKN_CMD

	}

	kvRes.ErrCode = errCode

	// Marshal KV response and return it
	resPayload, err := proto.Marshal(kvRes)
	if err != nil {
		log.Println("Marshaling payload error. ", err.Error())
		return nil, err
	}

	return resPayload, nil
}

/**
* Runs the server. Should never return unless an error is encountered.
* @param port The port to listen for UDP packets on.
* @return an error
 */
func runServer(otherMembers []struct {
	string
	int
}, port int) error {
	// Listen on all available IP addresses
	connection, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer connection.Close()

	conn = &connection

	// Bootstrap node
	bootstrap(conn, otherMembers, port)

	kvStore_ = NewKVStore()

	err = msgListener(requestHandler /*, internalMsgHandler */)

	// Should never get here if everything is working
	return err
}

func main() {

	// TASK 2: Dockerize project and parse port and member list file arguments.
	// I added the hardcoded "nodes" array that could be used for testing.
	// - Rozhan

	// Parse cmd line args
	arguments := os.Args
	if len(arguments) != 2 {
		fmt.Printf("ERROR: Expecting 1 argument (received %d): Port #", len(arguments)-1)
		return
	}

	port, err := strconv.Atoi(arguments[1])
	if err != nil {
		fmt.Println("ERROR: Port is not a valid number")
		return
	}

	if port < 1 || port > 65535 {
		fmt.Println("ERROR: Invalid port number (must be between 1 and 65535)")
		return
	}

	// TODO: parse file with IP addresses and ports of other nodes
	nodes := []struct {
		string
		int
	}{{"127.0.0.1", 12345}, {"127.0.0.1", 12346}}

	err = runServer(nodes, port)
	if err != nil {
		fmt.Println("Server encountered an error. " + err.Error())
	}

	fmt.Println("Server closed")
}
