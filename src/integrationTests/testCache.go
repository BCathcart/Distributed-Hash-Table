package main

import (
	"net"
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
	msgType    uint8  // i.e. Internal ID
	msg        []byte // serialized message to re-send
	time       time.Time
	retries    uint8
	addr       *net.Addr
	returnAddr *net.Addr
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
