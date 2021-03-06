package kvstore

import (
	"container/list"
	"strings"
	"sync"
)

/* KEY VALUE STORE */
type KVStore struct {
	lock sync.RWMutex
	data *list.List
	size uint32
}

type kventry struct {
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
	store.size = 0
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

	kvs.Remove(key)

	kvs.data.PushBack(kventry{key: key, val: val, ver: version})
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
func (kvs *KVStore) Remove(key string) uint32 {
	success, n := kvs.removeListElem(key)
	if success == true {
		kvs.size -= uint32(n) // Decrease kv store size
	}

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
* @return The kventry if it exists, nil otherwise.
 */
func (kvs *KVStore) findListElem(key string) *kventry {
	for e := kvs.data.Front(); e != nil; e = e.Next() {
		if strings.Compare(e.Value.(kventry).key, key) == 0 {
			entry := e.Value.(kventry)
			return &entry
		}
	}
	return nil
}

/**
* Removes an element from the key-value store list.
* @param key The key of the entry to remove.
* @return The kventry if it exists, nil otherwise.
 */
func (kvStore_ *KVStore) removeListElem(key string) (bool, int) {
	for e := kvStore_.data.Front(); e != nil; e = e.Next() {
		if strings.Compare(e.Value.(kventry).key, key) == 0 {
			len := len(e.Value.(kventry).key) + len(e.Value.(kventry).val) + 4
			kvStore_.data.Remove(e)
			return true, len
		}
	}
	return false, 0
}

// shay
/**
* Getter for a list of all keys stored in kvStore
* @return keyList A []int with all the keys
 */
func (kvs *KVStore) getAllKeys() []string {
	var keyList []string

	kvs.lock.RLock()

	if kvs.GetSize() == 0 {
		kvs.lock.RUnlock()
		return nil
	}

	for e := kvs.data.Front(); e != nil; e = e.Next() {
		keyList = append(keyList, e.Value.(kventry).key)
	}

	kvs.lock.RUnlock()
	return keyList
}
