package kvstore

import (
	"encoding/binary"
	"errors"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	pb "github.com/BCathcart/Distributed-Hash-Table/pb/protobuf"
	"github.com/BCathcart/Distributed-Hash-Table/src/util"

	"google.golang.org/protobuf/proto"
)

var MAX_MEM_USAGE uint64 = 120 * 1024 * 1024 // Max is actually 128 MB (8MB of buffer)

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

var kvStore_ *KVStore = NewKVStore()

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
* Handles incoming requests.
* @param serializedReq The serialized KVRequest.
* @return A serialized KVResponse, nil if there was an error.
* @return Error object if there was an error, nil otherwise.
 */
func handleOverload() *pb.KVResponse {
	log.Println("Overloaded: " + strconv.Itoa(int(memUsage())))
	util.PrintMemStats()

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
* @return the errorcode
 */
func RequestHandler(kvRequest *pb.KVRequest, membershipCount int, requestOwner util.KeyRange) ([]byte, error, uint32) {
	var errCode uint32
	kvRes := &pb.KVResponse{}

	/* NOTE: When there is an OVERLOAD and we are reaching the memory limit,
	we only restrict PUT and GET requests. REMOVE and WIPEOUT may increase
	the memory momentarily, but the benifit of the freed up space outweighs
	the momentary costs. */
	cmd := kvRequest.GetCommand()
	key := string(kvRequest.GetKey())
	value := kvRequest.GetValue()
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
			log.Println("OVERLOADED FOR PUT---", "KEY", util.Hash(kvRequest.GetKey()), "VALUE:", BytetoInt(value))
			errCode = OVERLOAD
		} else {
			errCode = kvStore_.Put(key, value, version)
			//DEBUGGING
			kvRes.Value = value
		}

		//DEBUGGING
		// log.Println("PUT---", "KEY", util.Hash(kvRequest.GetKey()), "VALUE:", BytetoInt(value))

	case GET:
		if len(key) > MAX_KEY_LEN {
			errCode = INVALID_KEY
		} else if memUsage() > MAX_MEM_USAGE {
			kvRes = handleOverload()
			errCode = OVERLOAD
		} else {
			value, version, code := kvStore_.Get(key)
			if code == OK {
				kvRes.Value = value
				kvRes.Version = &version
			}

			errCode = code
		}

		if errCode == NOT_FOUND {
			log.Println("Key not found!")
			log.Println(util.Hash([]byte(key)))
		}

		//DEBUGGING
		// log.Println("GOT---", "KEY", util.Hash(kvRequest.GetKey()), "VALUE:", BytetoInt(kvRes.Value))

	case REMOVE:
		if len(key) > MAX_KEY_LEN {
			errCode = INVALID_KEY
		} else {
			errCode = kvStore_.Remove(key)
		}

	case SHUTDOWN:
		os.Exit(1)

	case WIPEOUT:
		kvStore_.WipeoutKeys(requestOwner)
		log.Println("WIPING OUT KEYS", requestOwner)
		PrintKVStoreSize()
		debug.FreeOSMemory() // Force GO to free unused memory
		errCode = OK

	case IS_ALIVE:
		errCode = OK

	case GET_PID:
		pid := int32(os.Getpid())
		kvRes.Pid = &pid
		errCode = OK

	case GET_MEMBERSHIP_COUNT:
		count := int32(membershipCount)
		kvRes.MembershipCount = &count
		errCode = OK

	default:
		errCode = UKN_CMD

	}

	kvRes.ErrCode = errCode

	// Marshal KV response and return it
	resPayload, err := proto.Marshal(kvRes)
	if err != nil {
		log.Println("Marshaling payload error. ", err.Error())
		return nil, err, errCode
	}

	return resPayload, nil, errCode
}

func InternalDataUpdate(kvRequest *pb.KVRequest) error {
	cmd := kvRequest.Command
	key := string(kvRequest.Key)

	var errCode uint32
	switch cmd {
	case PUT:
		var version int32
		if kvRequest.Version != nil {
			version = *kvRequest.Version
		} else {
			version = 0
		}

		if memUsage() > MAX_MEM_USAGE {
			handleOverload()
			return errors.New("Overload")
		} else {
			errCode = kvStore_.Put(key, kvRequest.Value, version)
		}

	case REMOVE:
		errCode = kvStore_.Remove(key)

	default:
		return errors.New("Command is not an update")
	}

	if errCode != OK {
		return errors.New("Data update failed with error code: " + strconv.Itoa(int(errCode)))
	}

	return nil
}

/*
* Deletes all keys in the kvStore within the key range
 */
func Sweep(keys util.KeyRange, callback func()) {
	log.Println("INFO: Sweeping keys ", keys)
	kvStore_.WipeoutKeys(keys)
	callback()
}

/*
* PrintKVStoreSize prints out number of elements of the kvstore
 */
func PrintKVStoreSize() {
	log.Print("\n\n\n =======SIZE:===============", len(kvStore_.data), "=========\n\n\n")
}

/**
* IsGetRequest returns true if the KVRequest is a GET request
 */
func IsGetRequest(kvrequest *pb.KVRequest) bool {
	return kvrequest.GetCommand() == GET
}

/**
* IsUpdateRequest returns true if the KVRequest is an update, i.e. PUT or REMOVE request
 */
func IsUpdateRequest(kvrequest *pb.KVRequest) bool {
	return kvrequest.GetCommand() == PUT ||
		kvrequest.GetCommand() == REMOVE || kvrequest.GetCommand() == WIPEOUT
}

/**
* IsUpdateRequest returns true if the KVRequest is a key-value request (i.e. PUT, GET, REMOVE, or WIPEOUT)
 */
func IsKVRequest(kvrequest *pb.KVRequest) bool {
	return kvrequest.GetCommand() == PUT || kvrequest.GetCommand() == GET ||
		kvrequest.GetCommand() == REMOVE || kvrequest.GetCommand() == WIPEOUT
}

//For DEBUGGING
func BytetoInt(val []byte) uint32 {
	var tmp_value []byte
	if len(val) >= 4 {
		tmp_value = val[:4]
		return binary.LittleEndian.Uint32(tmp_value)
	}
	return 0
}
