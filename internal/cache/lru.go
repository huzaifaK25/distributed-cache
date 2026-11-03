package cache

import (
	"container/list"
	"time"
)

// lruList tracks access order: the front is most recently used and the back is
// least recently used. All methods must be called while InMemoryCache.mu is held.
type lruList struct {
	order *list.List
	index map[string]*list.Element
}

// newLRUList creates an empty recency tracker.
func newLRUList() *lruList {
	return &lruList{
		// Initialize a new doubly-linked list from container/list
		order: list.New(),
		// Map for O(1) access to list elements
		index: make(map[string]*list.Element),
	}
}

// add places a new key at the front of the recency list.
func (l *lruList) add(key string) {
	l.index[key] = l.order.PushFront(key)
}

// touch moves an existing key to the front.
func (l *lruList) touch(key string) {
	if elem, ok := l.index[key]; ok {
		l.order.MoveToFront(elem)
	}
}

// remove detaches a key from the recency list.
func (l *lruList) remove(key string) {
	if elem, ok := l.index[key]; ok {
		l.order.Remove(elem)
		delete(l.index, key)
	}
}

// oldest returns the least recently used key.
func (l *lruList) oldest() (string, bool) {
	elem := l.order.Back()
	if elem == nil {
		return "", false
	}
	return elem.Value.(string), true
}

// touchLocked updates access metadata and moves the key to the LRU front.
func (c *InMemoryCache) touchLocked(key string, item *CacheItem, now time.Time) {
	item.LastAccessed = now
	c.lru.touch(key)
}

// removeLocked removes a key from both the map and the LRU list.
func (c *InMemoryCache) removeLocked(key string) {
	c.lru.remove(key)
	delete(c.data, key)
}

// evictLocked removes the least recently used item and records the eviction.
func (c *InMemoryCache) evictLocked() bool {
	key, ok := c.lru.oldest()
	if !ok {
		return false
	}

	c.removeLocked(key)
	c.stats.evictions.Add(1)
	return true
}

// makeRoomLocked evicts entries until one more insert respects MaxItems.
func (c *InMemoryCache) makeRoomLocked() {
	if c.config.MaxItems <= 0 {
		return
	}

	for len(c.data) >= c.config.MaxItems {
		if !c.evictLocked() {
			return
		}
	}
}
