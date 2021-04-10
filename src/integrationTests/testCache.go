package main

import (
	pb "github.com/CPEN-431-2021/dht-abcpen431/pb/protobuf"
	maps "github.com/ross-oreto/go-list-map"
	"sync"
	"time"
)

// Cache with lock for concurrent access
type testCache struct {
	lock sync.Mutex
	data *maps.Map
}

type testReqCacheEntry struct {
	//msgType uint8  // i.e. Internal ID
	msg     []byte // serialized message to re-send
	time    time.Time
	retries uint8
	kvReq   *pb.KVRequest
}

/*

 */
type putGetCache struct {
	numPuts        uint32
	successfulGets uint32
	failedGets     uint32
	data           map[string][]byte // Map of keys to values
}

/**
* Creates and returns a pointer to a new testCache
* @return The testCache.
 */
func newTestCache() *testCache {
	cache := new(testCache)
	cache.data = maps.New()
	return cache
} /**
* Creates and returns a pointer to a new putGetCache
* @return The putGetCache.
 */
func newPutGetCache() *putGetCache {
	cache := new(putGetCache)
	cache.data = make(map[string][]byte)
	return cache
}

func putTestReqCacheEntry(id string, msg []byte, kvReq *pb.KVRequest) {

	// generate the key and construct entry
	testReqCache_.lock.Lock()
	key := id // + strconv.Itoa(int(msgType))
	req := testReqCache_.data.Get(key)

	// Increment retries if it already exists
	if req != nil {
		reqCacheEntry := req.(testReqCacheEntry)
		reqCacheEntry.retries++
		testReqCache_.data.Put(key, reqCacheEntry)
		// Otherwise add a new entry
	} else {
		testReqCache_.data.Put(key, testReqCacheEntry{msg, time.Now(), 0, kvReq})
	}
	testReqCache_.lock.Unlock()
}
