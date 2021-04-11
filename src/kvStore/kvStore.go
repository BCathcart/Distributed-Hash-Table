package kvstore

import (
	"log"
	"sync"

	"github.com/CPEN-431-2021/dht-abcpen431/src/util"
)

/* KEY VALUE STORE */
type KVStore struct {
	lock sync.RWMutex
	data map[string]kventry
	size uint32
}

type kventry struct {
	val []byte
	ver int32
}

/**
* Creates and returns a pointer to a new key-value store.
* @return The key-value store.
 */
func NewKVStore() *KVStore {
	store := new(KVStore)
	store.data = make(map[string]kventry)
	return store
}

/**
* Puts an entry in the key-value store.
* @param key The key.
* @param val The value.
* @param version The key-value pair version number.
* @return OK if there is space, NO_SPACE if the store is full.
 */
func (kvs *KVStore) Put(key string, val []byte, version int32) uint32 {
	// Remove needed to decrement kvStoreSize_ if key already exists
	kvs.lock.Lock()
	// Check if the store is full
	if kvs.size > MAX_KV_STORE_SIZE {
		kvs.lock.Unlock()
		return NO_SPACE
	}

	kvs.data[key] = kventry{val: val, ver: version}
	kvs.size += uint32(len(key) + len(val) + 4) // Increase kv store size
	kvs.lock.Unlock()
	return OK
}

/**
* Gets an entry from the key-value store.
* @param key The key of the entry to retrieve.
* @return The kventry value if it exists.
* @return The kventry version if it exists.
* @return OK if the key exists, NOT_FOUND otherwise.
 */
func (kvs *KVStore) Get(key string) ([]byte, int32, uint32) {
	kvs.lock.RLock()
	entry, found := kvs.data[key]
	kvs.lock.RUnlock()

	if found {
		return entry.val, entry.ver, OK
	}

	return nil, 0, NOT_FOUND
}

/**
* Removes an entry from the key-value store.
* @param key The key of the entry to remove.
* @return OK if the key exists, NOT_FOUND otherwise.
 */
func (kvs *KVStore) Remove(key string) uint32 {
	kvs.lock.Lock()
	entry, ok := kvs.data[key]
	if ok == true {
		delete(kvs.data, key)
		kvs.size -= uint32(len(key) + len(entry.val) + 4)
	}
	kvs.lock.Unlock()

	if ok == true {
		return OK
	}

	return NOT_FOUND
}

/**
* * @return the size of the KVStore
 */
func (kvs *KVStore) GetSize() uint32 {
	return kvs.size
}

// shay
/**
* Getter for a list of all keys stored in kvStore
* @return keyList A []int with all the keys
 */
func (kvs *KVStore) getAllKeys() []string {
	kvs.lock.RLock()

	if kvs.GetSize() == 0 {
		kvs.lock.RUnlock()
		return nil
	}

	i := 0
	keys := make([]string, len(kvs.data))
	for k, _ := range kvs.data {
		keys[i] = k
		i++
	}

	kvs.lock.RUnlock()
	return keys
}

func (kvs *KVStore) WipeoutKeys(keys util.KeyRange) {
	kvs.lock.Lock()
	for key, entry := range kvs.data {
		if keys.IncludesKey(util.Hash([]byte(key))) {
			delete(kvs.data, key)
			kvs.size -= uint32(len(key) + len(entry.val) + 4)
		}
	}
	kvs.lock.Unlock()
	log.Println("LEN KV STORE = ", len(kvs.data))
}
