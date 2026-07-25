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

// stmtCache is a bounded, per-connection LRU of exact query strings. The map
// provides direct lookup while the entries themselves form an intrusive list,
// avoiding the allocation and interface value required by container/list.
//
// Entries with a nil statement form a bounded negative cache for SQL that the
// server reports as compound or otherwise unsuitable for this cache. They keep
// repeated compound queries on the existing direct execution path.
//
// A Conn is not used concurrently by database/sql, so the cache deliberately
// has no lock. Its statements share the Conn's request and response buffers and
// therefore have the same serialization requirement as the Conn.
type stmtCache struct {
	capacity int
	entries  map[string]*stmtCacheEntry
	head     *stmtCacheEntry
	tail     *stmtCacheEntry
	finalize func(*Stmt)
}

type stmtCacheEntry struct {
	query string
	stmt  *Stmt
	prev  *stmtCacheEntry
	next  *stmtCacheEntry
}

func newStmtCache(capacity int) *stmtCache {
	cache := &stmtCache{finalize: finalizeCachedStmt}
	if capacity <= 0 {
		return cache
	}
	cache.capacity = capacity
	// Do not reserve capacity eagerly: a database may open many connections
	// that never execute enough distinct SQL to fill their caches.
	cache.entries = make(map[string]*stmtCacheEntry)
	return cache
}

func finalizeCachedStmt(stmt *Stmt) {
	_ = stmt.Close()
}

// lookup returns the statement and whether the query is known to the cache.
// A known query with a nil statement is a negative-cache hit. A disabled cache
// reports every query as known so callers remain on the direct execution path.
func (c *stmtCache) lookup(query string) (*Stmt, bool) {
	if c == nil || c.capacity == 0 {
		return nil, true
	}

	entry, ok := c.entries[query]
	if !ok {
		return nil, false
	}
	c.moveToFront(entry)
	return entry.stmt, true
}

func (c *stmtCache) store(query string, stmt *Stmt) {
	c.put(query, stmt)
}

func (c *stmtCache) reject(query string) {
	c.put(query, nil)
}

func (c *stmtCache) put(query string, stmt *Stmt) {
	if c == nil || c.capacity == 0 {
		if stmt != nil {
			finalizeCachedStmt(stmt)
		}
		return
	}

	if entry, ok := c.entries[query]; ok {
		if entry.stmt != nil && entry.stmt != stmt {
			c.finalize(entry.stmt)
		}
		entry.stmt = stmt
		c.moveToFront(entry)
		return
	}

	entry := &stmtCacheEntry{query: query, stmt: stmt}
	c.entries[query] = entry
	c.addToFront(entry)
	if len(c.entries) > c.capacity {
		c.evict(c.tail)
	}
}

func (c *stmtCache) moveToFront(entry *stmtCacheEntry) {
	if entry == c.head {
		return
	}
	c.remove(entry)
	c.addToFront(entry)
}

func (c *stmtCache) addToFront(entry *stmtCacheEntry) {
	entry.prev = nil
	entry.next = c.head
	if c.head != nil {
		c.head.prev = entry
	} else {
		c.tail = entry
	}
	c.head = entry
}

func (c *stmtCache) remove(entry *stmtCacheEntry) {
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
	entry.prev = nil
	entry.next = nil
}

func (c *stmtCache) evict(entry *stmtCacheEntry) {
	if entry == nil {
		return
	}
	delete(c.entries, entry.query)
	c.remove(entry)
	if entry.stmt != nil {
		// Finalization errors are deliberately ignored during eviction. The
		// operation that made the connection unusable will surface its error;
		// cache maintenance must not replace it with a secondary error.
		c.finalize(entry.stmt)
	}
}

// discard releases the client-side index without finalizing statements. It is
// used only while closing the underlying connection, which releases all of its
// server-side statements at once and avoids one finalize round trip per entry.
func (c *stmtCache) discard() {
	if c == nil {
		return
	}
	c.entries = nil
	c.head = nil
	c.tail = nil
}
