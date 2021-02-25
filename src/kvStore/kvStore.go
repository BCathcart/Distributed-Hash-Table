package kvstore

import (
	"container/list"
	"log"
	"strings"
	"sync"
)

/* KEY VALUE STORE */

var kvStoreSize_ uint32

type KVStore struct {
	lock sync.RWMutex
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
* Puts an entry in the key-value store.
* @param key The key.
* @param val The value.
* @param version The key-value pair version number.
* @return OK if there is space, NO_SPACE if the store is full.
 */
func (kvStore_ *KVStore) Put(key string, val []byte, version int32) uint32 {
	kvStore_.lock.Lock()

	// Remove needed to decrement kvStoreSize_ if key already exists
	kvStore_.Remove(key) // *neeeds to be in critical section

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
func (kvStore_ *KVStore) Get(key string) (KVEntry, uint32) {
	var found bool
	var entry KVEntry
	kvStore_.lock.RLock()
	res := kvStore_.findListElem(key)
	if res != nil {
		found = true
		entry = *res
	} else {
		found = false
	}
	kvStore_.lock.RUnlock()

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
func (kvStore_ *KVStore) Remove(key string) uint32 {
	success, n := kvStore_.removeListElem(key)
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
* Fetches an element from the key-value store list.
* @param key The key to search for.
* @return The KVEntry if it exists, nil otherwise.
 */
func (kvStore_ *KVStore) findListElem(key string) *KVEntry {
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
func (kvStore_ *KVStore) removeListElem(key string) (bool, int) {
	for e := kvStore_.data.Front(); e != nil; e = e.Next() {
		if strings.Compare(e.Value.(KVEntry).key, key) == 0 {
			len := len(e.Value.(KVEntry).key) + len(e.Value.(KVEntry).val) + 4
			kvStore_.data.Remove(e)
			return true, len
		}
	}
	return false, 0
}
