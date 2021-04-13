package main

import (
	"bytes"
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	kvstore "github.com/CPEN-431-2021/dht-abcpen431/src/kvStore"
	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
	"log"
	"net"
	"strconv"
)

type putGetCache struct {
	numPuts        uint32
	successfulGets uint32
	failedGets     uint32
	data           map[string][]byte // Map of keys to values for each successful put request
}

/**
* Creates and returns a pointer to a new putGetCache
* @return The putGetCache.
 */
func newPutGetCache() *putGetCache {
	cache := new(putGetCache)
	cache.data = make(map[string][]byte)
	return cache
}

/**
When a put request is received, cache it in the putGetCache.
When a get request is received, compare the value with the previously placed put
*/
func putGetCheck(req *pb.KVRequest, res *pb.KVResponse) {
	if req.Command == PUT {
		putGetCache_.data[string(req.Key)] = req.Value
		putGetCache_.numPuts++
	} else if req.Command == GET {
		if res.ErrCode == kvstore.NOT_FOUND {
			keyInt := util.Hash(req.Key)
			log.Println("ERR: Couldn't find value for key: " + string(req.Key) + ", associated Port: " + strconv.Itoa(getPortForSentKey(int(keyInt))))
			putGetCache_.failedGets++
		} else if bytes.Compare(putGetCache_.data[string(req.Key)], res.Value) != 0 {
			log.Printf("ERR: PUT ( %v ) != GET ( %v )", putGetCache_.data[string(req.Key)], res.Value)
			putGetCache_.failedGets++
		} else {
			putGetCache_.successfulGets++
		}
	}

}

/**
For each putRequest in the cache, sends a get request.
*/
func getPuts(addr *net.Addr) {
	// Making a copy may not be necessary, but this function will take a while to run
	// as it is making many getRequests so did not want to be iterating through the cache
	putGetMapCopy := make(map[string][]byte)
	for k, v := range putGetCache_.data {
		putGetMapCopy[k] = v
	}
	kvRequests := 0
	for key, _ := range putGetMapCopy {
		payload := getRequest([]byte(key))
		keyValueRequest(addr, payload)
		kvRequests++
	}
	log.Printf("Sent %v get requests\n", kvRequests)
}

func printPutGetTestResults() {
	log.Println("\n\n============= PUT GET RESULTS ============")
	totalPuts := putGetCache_.numPuts
	totalGets := putGetCache_.successfulGets + putGetCache_.failedGets
	log.Printf("Put Sucesses: %v\n", totalPuts)
	log.Printf("Get Successes: %v\n", putGetCache_.successfulGets)
	log.Printf("Get Failures: %v\n", putGetCache_.failedGets)
	percentFailed := 100.0 * putGetCache_.failedGets / totalGets
	log.Printf("Percentage of failures: %v%%\n", percentFailed)
}

func printSuccessfulPuts(putsSent int) {
	log.Printf("Number of puts sent %v \n", putsSent)
	log.Printf("Number of puts received %v \n", putGetCache_.numPuts)
	passedPuts := 0
	if putsSent != 0 || putGetCache_.numPuts != 0 {
		passedPuts = int(100 * putGetCache_.numPuts / uint32(putsSent))
	}
	log.Printf("Without retrying, %v%% of puts were successfully received", passedPuts)
}
