package requestreply

import (
	"runtime/debug"
	"time"

	"github.com/abcpen431/miniproject/src/util"
)

//RES_CACHE_TIMEOUT - for sweeing cache
const RES_CACHE_TIMEOUT = 6 // Messages only required to live in cache for 5 seconds

/*MAX_RES_CACHE_ENTRIES - cache size
Note: the cache has a small cap for now due to the nature of only having a small number
of clients sending messages. The cache will need to expanded in the future once we have a
better idea of the likely number of clients and their retry rate. */
const MAX_RES_CACHE_ENTRIES = 50

// Maps msg ID to serialized response
var resCache_ *Cache

//ResCacheEntry - response cache entry type
type ResCacheEntry struct {
	msg  []byte
	time time.Time
}

/**
* Removes expired entries in the cache every RES_CACHE_TIMEOUT seconds.
* Lock-protected operation
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

	// log and free memory
	util.PrintMemStats()
	debug.FreeOSMemory() // Force GO to free unused memory
}

/**
* Puts a message in the cache.
* @param id The message id.
* @param msg The serialized message to cache.
* Lock-protected operation
 */
func putResCacheEntry(id string, msg []byte) {
	resCache_.lock.Lock()

	for resCache_.data.Len() >= MAX_RES_CACHE_ENTRIES {
		resCache_.data.Pull()
	}
	resCache_.data.Put(id, ResCacheEntry{msg: msg, time: time.Now()})

	resCache_.lock.Unlock()
}
