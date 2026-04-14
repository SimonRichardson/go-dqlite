// Copyright 2026 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"strings"
	"unicode"

	"github.com/tidwall/btree"
)

// Cache is an interface for caching prepared statements.
type PreparedStatementCache interface {
	// Put adds a new statement to the cache.
	Put(query string, stmt *Stmt)

	// TryGet retrieves a statement from the cache based on the query. If the
	// statement is found, it is returned along with the number of characters
	// matched from the query. If the statement is not found, nil is returned.
	TryGet(query string) (*Stmt, int)

	// Close releases all resources associated with the cache.
	Close()
}

type noopCache struct{}

// Put does nothing, as this is a no-op cache.
func (c *noopCache) Put(query string, stmt *Stmt) {}

// TryGet always returns nil, as this is a no-op cache.
func (c *noopCache) TryGet(query string) (*Stmt, int) {
	return nil, 0
}

// Close does nothing, as this is a no-op cache.
func (c *noopCache) Close() {}

// cacheEntry represents a single entry in the cache.
type cacheEntry struct {
	// query is the original query string that was executed.
	query string
	// stmt is the prepared statement associated with the query.
	stmt *Stmt
	// prev and next are pointers to the previous and next entries in the doubly
	// linked list.
	prev, next *cacheEntry
}

// stmtCache is a simple LRU cache for prepared statements.
type stmtCache struct {
	// capacity is the maximum number of entries the cache can hold.
	capacity int
	// btree is a ordered tree for efficient prefix matching of queries.
	btree *btree.BTreeG[*cacheEntry]
	// head, tail are pointers to the head and tail of the doubly linked list.
	head, tail *cacheEntry
}

// newStmtCache creates a new stmtCache with the given capacity.
func newStmtCache(capacity int) *stmtCache {
	return &stmtCache{
		capacity: capacity,
		btree: btree.NewBTreeGOptions(cacheEntryLess, btree.Options{
			NoLocks: true,
		}),
	}
}

// Put adds a new entry to the cache. If the cache exceeds its capacity, the
// least recently used entry is evicted.
func (c *stmtCache) Put(query string, stmt *Stmt) {
	// Normalize the query for consistent caching.
	normalizedQuery := normalizeQuery(query)

	// Check if the entry already exists in the cache.
	existing, exists := c.btree.Get(&cacheEntry{query: normalizedQuery})
	if exists {
		// Move the existing entry to the front of the list (most recently
		// used).
		c.moveToFront(existing)
		return
	}

	// Increment reference count for the new statement.
	stmt.refCount++

	// Create a new cache entry.
	entry := &cacheEntry{
		query: normalizedQuery,
		stmt:  stmt,
	}

	c.btree.Set(entry)
	c.addToFront(entry)

	// If the cache exceeds its capacity, evict the least recently used entry.
	if c.btree.Len() > c.capacity {
		c.evict()
	}
}

// TryGet retrieves an entry from the cache based on the query. If the entry is
// found, it is moved to the front of the list (most recently used) and returned.
// If the entry is not found, nil is returned.
func (c *stmtCache) TryGet(query string) (*Stmt, int) {
	// Normalize the query for consistent caching.
	normalizedQuery := normalizeQuery(query)

	var match *cacheEntry
	var matchLen int

	// Use btree Descend to iterate in sorted order starting from the normalized query.
	// This allows us to efficiently find entries that match the query prefix.
	c.btree.Descend(&cacheEntry{query: normalizedQuery}, func(entry *cacheEntry) bool {
		if len(normalizedQuery) < len(entry.query) {
			return true
		}

		if normalizedQuery[0:len(entry.query)] != entry.query {
			return false
		}

		if len := isMatch(normalizedQuery, entry.query); len > 0 {
			match = entry
			matchLen = len
			return false
		}

		return true
	})

	if match != nil {
		// Move the matched entry to the front of the list (most recently used).
		c.moveToFront(match)
		match.stmt.refCount++
		return match.stmt, matchLen + len(query) - len(normalizedQuery)
	}

	return nil, 0
}

// Close releases all resources associated with the cache. It closes all cached
// statements and clears the cache.
func (c *stmtCache) Close() {
	c.btree.Scan(func(item *cacheEntry) bool {
		_ = item.stmt.Close()
		return true
	})
	c.btree.Clear()
}

func (c *stmtCache) moveToFront(entry *cacheEntry) {
	if c.head == entry {
		// Already at the front.
		return
	}

	c.removeFromList(entry)
	c.addToFront(entry)
}

// removeFromList removes an entry from the doubly linked list.
func (c *stmtCache) removeFromList(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
}

// addToFront adds an entry to the front of the doubly linked list.
func (c *stmtCache) addToFront(entry *cacheEntry) {
	entry.next = c.head
	entry.prev = nil

	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

// evict removes the least recently used entry from the cache and decrements its
// reference count. If the reference count reaches zero, the statement is
// closed.
func (c *stmtCache) evict() {
	if c.tail == nil {
		return
	}

	// Remove the least recently used entry (the tail).
	evicted := c.tail
	c.removeFromList(evicted)
	c.btree.Delete(evicted)
	_ = evicted.stmt.Close()
}

func normalizeQuery(query string) string {
	// For simplicity, we just trim whitespace.
	return strings.TrimLeftFunc(query, unicode.IsSpace)
}

func cacheEntryLess(a, b *cacheEntry) bool {
	return a.query < b.query
}

func isMatch(normalizedQuery, entryQuery string) int {
	// Position after the cached query in the original query.
	pos := len(entryQuery)

	// Skip trailing whitespace in the cached query.
	for pos < len(normalizedQuery) && unicode.IsSpace(rune(normalizedQuery[pos])) {
		pos++
	}

	// Check if we're at end of string or at at semicolon.
	if pos >= len(normalizedQuery) {
		// End of string - valid match
		return pos
	}

	if normalizedQuery[pos] == ';' {
		// Semicolon - valid match
		return pos + 1
	}

	// Not a valid match.
	return 0
}
