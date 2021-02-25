package kvstore

import (
	"container/list"
	"log"
	"sync"
)

/* KEY VALUE STORE */
type KVStore struct {
	lock sync.RWMutex
	data *list.List
	size uint32
}

type kventry struct {
	key uint32
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
func (kvs *KVStore) Put(key uint32, val []byte, version int32) uint32 {
	kvs.lock.Lock()

	// Remove needed to decrement kvStoreSize_ if key already exists
	kvs.Remove(key) // *neeeds to be in critical section

	// Check if the store is full
	if kvs.size > MAX_KV_STORE_SIZE {
		kvs.lock.Unlock()
		return NO_SPACE
	}

	kvs.data.PushBack(kventry{key: key, val: val, ver: version})
	kvs.size += uint32(4 + len(val) + 4) // Increase kv store size

	log.Println(kvs.size)
	kvs.lock.Unlock() // TODO: this caused fatal error???

	return OK
}

/**
* Gets an entry from the key-value store.
* @param key The key of the entry to retrieve.
* @return The kventry value if it exists.
* @return The kventry version if it exists.
* @return OK if the key exists, NOT_FOUND otherwise.
 */
func (kvs *KVStore) Get(key uint32) ([]byte, int32, uint32) {
	var found bool
	var entry kventry
	kvs.lock.RLock()
	res := kvs.findListElem(key)
	if res != nil {
		found = true
		entry = *res
	} else {
		found = false
	}
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
func (kvs *KVStore) Remove(key uint32) uint32 {
	kvs.lock.Lock()
	success, n := kvs.removeListElem(key)
	if success == true {
		kvs.size -= uint32(n) // Decrease kv store size
	}

	log.Println(kvs.size)
	kvs.lock.RUnlock()

	if success == true {
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

/**
* * Wipes out the entire KVStore
 */
func (kvs *KVStore) Wipeout() {
	kvs.lock.Lock()
	kvs.data.Init() // Clears the list
	kvs.size = 0
	kvs.lock.Unlock()

}

/**
* Fetches an element from the key-value store list.
* @param key The key to search for.
* @return The KVEntry if it exists, nil otherwise.
 */
func (kvs *KVStore) findListElem(key uint32) *kventry {
	for e := kvs.data.Front(); e != nil; e = e.Next() {
		if e.Value.(kventry).key == key {
			entry := e.Value.(kventry)
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
func (kvs *KVStore) removeListElem(key uint32) (bool, int) {
	for e := kvs.data.Front(); e != nil; e = e.Next() {
		if e.Value.(kventry).key == key {
			len := 4 + len(e.Value.(kventry).val) + 4
			kvs.data.Remove(e)
			return true, len
		}
	}
	return false, 0
}
