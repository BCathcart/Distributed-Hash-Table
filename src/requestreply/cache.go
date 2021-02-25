package requestreply

import (
	"sync"

	maps "github.com/ross-oreto/go-list-map"
)

// Cache with lock for concurrent access
type Cache struct {
	lock sync.Mutex
	data *maps.Map
}

/**
* Creates and returns a pointer to a new cache
* @return The cache.
 */
func NewCache() *Cache {
	cache := new(Cache)
	cache.data = maps.New()
	return cache
}
