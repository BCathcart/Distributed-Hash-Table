package main

import (
	"sync"
	"time"

	maps "github.com/ross-oreto/go-list-map"
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

func putTestReqCacheEntry(id string, msg []byte) {

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
		testReqCache_.data.Put(key, testReqCacheEntry{msg, time.Now(), 0})
	}
	testReqCache_.lock.Unlock()
}
