package main

import (
	"container/list"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	maps "github.com/ross-oreto/go-list-map"

	pb "github.com/abcpen431/miniproject/pb/protobuf"

	"google.golang.org/protobuf/proto"
)

var MAX_MEM_USAGE uint64 = 120 * 1024 * 1024 // Max is actually 128 MB (8MB of buffer)

/************* REQUEST/REPLY PROTOCOL CODE *************/

const MAX_BUFFER_SIZE = 11000
const CACHE_TIMEOUT = 6 // Messages only required to live in cache for 5 seconds

/* Note: the cash has a small cap for now due to the nature of only having a small number
of clients sending messages. The cache will need to expanded in the future once we have a
better idea of the likely number of clients and their retry rate */
const MAX_CACHE_ENTRIES = 10

/* CACHE */
// Maps msg ID to serialized response
var resCache_ *Cache
var cacheSize_ uint32

type Cache struct {
	lock sync.Mutex
	data *maps.Map
}

type CacheEntry struct {
	msg  []byte
	time time.Time
}

/**
* Creates and returns a pointer to a new cache
* @return The cache.
 */
func NewCache() *Cache {
	cache := new(Cache)
	cache.data = maps.New()
	return cache
}

/**
* Initializes the request/reply layer. Must be called before using
* request/reply layer to get expected functionality.
 */
func requestReplyLayerInit() {
	resCache_ = NewCache()
	cacheSize_ = 0

	// Sweep cache every CACHE_TIMEOUT seconds
	var ticker = time.NewTicker(time.Second * CACHE_TIMEOUT)

	go func() {
		for {
			<-ticker.C
			sweepCache()
		}
	}()

	fmt.Println(getOutboundIP())

}

func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

/**
* Removes expired entries in the cache every CACHE_TIMEOUT seconds.
 */
func sweepCache() {
	resCache_.lock.Lock()
	entries := resCache_.data.Entries()
	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		elapsedTime := time.Now().Sub(entry.Value.(CacheEntry).time)
		if elapsedTime.Seconds() > CACHE_TIMEOUT {
			resCache_.data.Delete(entry.Key)
		}
	}
	resCache_.lock.Unlock()
	PrintMemStats()

	debug.FreeOSMemory() // Force GO to free unused memory
}

/**
* Puts a message in the cache.
* @param id The message id.
* @param msg The serialized message to cache.
 */
func putCacheEntry(id string, msg []byte) {
	resCache_.lock.Lock()
	for resCache_.data.Len() >= MAX_CACHE_ENTRIES {
		resCache_.data.Pull()
	}
	resCache_.data.Put(string(id), CacheEntry{msg: msg, time: time.Now()})
	resCache_.lock.Unlock()
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msgID The message ID.
* @param msgPayload The message payload.
* @return The checksum.
 */
func computeChecksum(msgId []byte, msgPayload []byte) uint32 {
	return crc32.ChecksumIEEE(append(msgId, msgPayload...))
}

/**
* Computes the IEEE CRC checksum based on the message ID and message payload.
* @param msg The received message.
* @return True if message ID matches the expected ID and checksum is valid, false otherwise.
 */
func verifyChecksum(msg *pb.Msg) bool {
	// Verify MessageID is as expected
	if uint64(computeChecksum((*msg).MessageID, (*msg).Payload)) != (*msg).CheckSum {
		return false
	}
	return true
}

/**
* Writes a message to the connection.
* @param conn The connection object to send the message over.
* @param addr The IP address to send to.
* @param msg The message to send.
 */
func writeMsg(conn *net.PacketConn, addr net.Addr, msg []byte) {
	// Send msg
	_, err := (*conn).WriteTo(msg, addr)
	if err != nil {
		log.Println(err)
	} else {
		// log.Println("INFO:", n, "bytes written.")
	}
}

/**
* Sends a UDP message.
* @param conn The connection object to send messages over.
* @param addr The IP address to send to.
* @param msgId The message id.
* @param payload The message payload.
 */
func sendUDPMsg(conn *net.PacketConn, addr net.Addr, msgId []byte, payload []byte) {
	checksum := computeChecksum(msgId, payload)

	resMsg := &pb.Msg{
		MessageID: msgId,
		Payload:   payload,
		CheckSum:  uint64(checksum),
	}

	serMsg, err := proto.Marshal(resMsg)
	if err != nil {
		log.Println(err)
	}

	// Cache message
	putCacheEntry(string(msgId), serMsg)

	writeMsg(conn, addr, serMsg)
}

/**
* Processes a received message and passes it to the handler callback.
* @param conn The connection object to send messages over.
* @param addr The IP address to send to.
* @param serialMsg The incoming message.
* @param handler The message handler callback.
 */
func processMsg(conn *net.PacketConn, addr net.Addr, serialMsg []byte, handler func([]byte) ([]byte, error)) {
	// Deserialize message
	reqMsg := &pb.Msg{}
	err := proto.Unmarshal(serialMsg, reqMsg)
	if err != nil {
		// Disregard messages with invalid format
		log.Println("WARN msg with invalid format. Sender = " + addr.String())
		return
	}

	// Verify checksum
	if !verifyChecksum(reqMsg) {
		// Disregard messages with invalid checksums
		log.Println("WARN checksum mismatch. Sender = " + addr.String())
		return
	}

	// Check if response is already cached
	resCache_.lock.Lock()
	res := resCache_.data.Get(string(reqMsg.MessageID))
	if res != nil {
		cacheEntry := res.(CacheEntry)
		writeMsg(conn, addr, cacheEntry.msg)
		// Reset timeout
		cacheEntry.time = time.Now()
		resCache_.data.Put(string(reqMsg.MessageID), cacheEntry) // TODO: does this overwrite?
		resCache_.lock.Unlock()
		return
	}
	resCache_.lock.Unlock()

	// Pass message to handler
	payload, err := handler(reqMsg.Payload)
	if err != nil {
		log.Println("WARN could not handle message. Sender = " + addr.String())
		return
	}

	// Send response
	sendUDPMsg(conn, addr, reqMsg.MessageID, payload)
}

/**
* Listens for incoming messages, processes them, and then passes them to the handler callback.
* @param conn The network connection object.
* @param handler The message handler callback.
* @return An error message if failed to read from the connection.
 */
func requestListener(conn *net.PacketConn, handler func([]byte) ([]byte, error)) error {
	buffer := make([]byte, MAX_BUFFER_SIZE)

	// Listen for packets
	for {
		//log.Println("Awaiting packet")
		n, addr, err := (*conn).ReadFrom(buffer)
		if err != nil {
			return err
		}

		go processMsg(conn, addr, buffer[0:n], handler)
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
func runServer(port int) error {
	// Listen on all available IP addresses
	conn, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer conn.Close()

	kvStore_ = NewKVStore()

	err = requestListener(&conn, requestHandler)

	// Should never get here if everything is working
	return err
}

func main() {
	// Parse cmd line args
	arguments := os.Args
	if len(arguments) != 2 {
		fmt.Printf("ERROR: Expecting 1 argument (received %d): Port #\n", len(arguments)-1)
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

	requestReplyLayerInit()

	err = runServer(port)
	if err != nil {
		fmt.Println("Server encountered an error. " + err.Error())
	}

	fmt.Println("Server closed")
}
