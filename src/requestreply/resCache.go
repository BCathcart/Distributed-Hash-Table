package requestreply

import (
	"runtime/debug"
	"time"

	"github.com/abcpen431/miniproject/src/util"
)

// Maps msg ID to serialized response
var resCache_ *Cache

type ResCacheEntry struct {
	msg  []byte
	time time.Time
}

/**
* Removes expired entries in the cache every RES_CACHE_TIMEOUT seconds.
 */
func sweepResCache() {
	resCache_.lock.Lock()
	entries := resCache_.data.Entries()
	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		elapsedTime := time.Now().Sub(entry.Value.(ResCacheEntry).time)
		if elapsedTime.Seconds() > RES_CACHE_TIMEOUT {
			resCache_.data.Delete(entry.Key)
		}
	}
	resCache_.lock.Unlock()
	util.PrintMemStats()

	debug.FreeOSMemory() // Force GO to free unused memory
}

/**
* Puts a message in the cache.
* @param id The message id.
* @param msg The serialized message to cache.
 */
func putResCacheEntry(id string, msg []byte) {
	resCache_.lock.Lock()
	for resCache_.data.Len() >= MAX_RES_CACHE_ENTRIES {
		resCache_.data.Pull()
	}
	resCache_.data.Put(id, ResCacheEntry{msg: msg, time: time.Now()})
	resCache_.lock.Unlock()
}
