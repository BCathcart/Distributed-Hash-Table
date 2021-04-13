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
	msg     []byte // serialized message to re-send
	time    time.Time
	retries uint8
	kvReq   *pb.KVRequest
}

/**
* Creates and returns a pointer to a new testCache
* @return The testCache.
 */
func newTestCache() *testCache {
	cache := new(testCache)
	cache.data = maps.New()
	return cache
}

func putTestReqCacheEntry(id string, msg []byte, kvReq *pb.KVRequest) {

	testReqCache_.lock.Lock()
	key := id
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
