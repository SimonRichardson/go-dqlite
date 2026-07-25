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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStmtCacheHit(t *testing.T) {
	cache := newTestStmtCache(2)
	stmt := &Stmt{}
	cache.store("SELECT ?", stmt)

	got, found := cache.lookup("SELECT ?")
	assert.True(t, found)
	assert.Same(t, stmt, got)
}

func TestStmtCacheMiss(t *testing.T) {
	cache := newTestStmtCache(2)

	stmt, found := cache.lookup("SELECT ?")
	assert.False(t, found)
	assert.Nil(t, stmt)
}

func TestStmtCacheLRU(t *testing.T) {
	cache := newTestStmtCache(2)
	stmt1 := &Stmt{}
	stmt2 := &Stmt{}
	stmt3 := &Stmt{}
	cache.store("SELECT 1", stmt1)
	cache.store("SELECT 2", stmt2)

	// Refresh SELECT 1, making SELECT 2 the eviction candidate.
	got, found := cache.lookup("SELECT 1")
	require.True(t, found)
	assert.Same(t, stmt1, got)

	cache.store("SELECT 3", stmt3)
	_, found = cache.lookup("SELECT 2")
	assert.False(t, found)
	assert.Len(t, cache.entries, 2)
	assert.Same(t, stmt1, cache.entries["SELECT 1"].stmt)
	assert.Same(t, stmt3, cache.entries["SELECT 3"].stmt)
}

func TestStmtCacheReject(t *testing.T) {
	cache := newTestStmtCache(1)
	query := "SELECT 1; SELECT 2"
	cache.reject(query)

	stmt, found := cache.lookup(query)
	assert.True(t, found)
	assert.Nil(t, stmt)
}

func TestStmtCacheIsStrictlyBounded(t *testing.T) {
	const capacity = 1000
	cache := newTestStmtCache(capacity)
	for i := 0; i < capacity*10; i++ {
		cache.store(fmt.Sprintf("SELECT %d", i), &Stmt{})
		assert.LessOrEqual(t, len(cache.entries), capacity)
	}
	assert.Len(t, cache.entries, capacity)
}

func TestStmtCacheDiscardDoesNotFinalize(t *testing.T) {
	cache := newStmtCache(3)
	closed := make(map[*Stmt]int)
	cache.finalize = func(stmt *Stmt) { closed[stmt]++ }
	stmts := []*Stmt{{}, {}, {}}
	for i, stmt := range stmts {
		cache.store(fmt.Sprintf("SELECT %d", i), stmt)
	}

	cache.discard()
	assert.Empty(t, cache.entries)
	assert.Nil(t, cache.head)
	assert.Nil(t, cache.tail)
	for _, stmt := range stmts {
		assert.Zero(t, closed[stmt])
	}
}

func TestStmtCacheDisabled(t *testing.T) {
	for _, capacity := range []int{-1, 0} {
		cache := newStmtCache(capacity)
		stmt, found := cache.lookup("SELECT 1")
		assert.True(t, found)
		assert.Nil(t, stmt)
		assert.Nil(t, cache.entries)
	}
}

func BenchmarkStmtCacheLookupHit(b *testing.B) {
	for _, capacity := range []int{100, 1000, 10000} {
		for _, queryLen := range []int{64, 256, 1024} {
			b.Run(fmt.Sprintf("capacity=%d/bytes=%d", capacity, queryLen), func(b *testing.B) {
				cache, queries := populatedBenchmarkCache(capacity, queryLen)
				b.SetBytes(int64(queryLen))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					stmt, found := cache.lookup(queries[i%len(queries)])
					if !found || stmt == nil {
						b.Fatal("cache hit reported as miss")
					}
				}
			})
		}
	}
}

func BenchmarkStmtCacheLookupMiss(b *testing.B) {
	for _, capacity := range []int{100, 1000, 10000} {
		for _, queryLen := range []int{64, 256, 1024} {
			b.Run(fmt.Sprintf("capacity=%d/bytes=%d", capacity, queryLen), func(b *testing.B) {
				cache, _ := populatedBenchmarkCache(capacity, queryLen)
				query := strings.Repeat("x", queryLen)
				b.SetBytes(int64(queryLen))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, found := cache.lookup(query); found {
						b.Fatal("cache miss reported as hit")
					}
				}
			})
		}
	}
}

func BenchmarkStmtCacheEviction(b *testing.B) {
	for _, capacity := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("capacity=%d", capacity), func(b *testing.B) {
			cache, _ := populatedBenchmarkCache(capacity, 128)
			queries := make([]string, capacity+1)
			for i := range queries {
				queries[i] = fmt.Sprintf("INSERT INTO benchmark_%d VALUES (?)", i+capacity)
			}
			stmt := &Stmt{}
			b.SetBytes(128)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.store(queries[i%len(queries)], stmt)
			}
		})
	}
}

func newTestStmtCache(capacity int) *stmtCache {
	cache := newStmtCache(capacity)
	cache.finalize = func(*Stmt) {}
	return cache
}

func populatedBenchmarkCache(capacity, queryLen int) (*stmtCache, []string) {
	cache := newTestStmtCache(capacity)
	queries := make([]string, capacity)
	for i := range queries {
		prefix := fmt.Sprintf("SELECT value FROM benchmark_%d WHERE key = ? -- ", i)
		if len(prefix) < queryLen {
			prefix += strings.Repeat("x", queryLen-len(prefix))
		}
		queries[i] = prefix
		cache.store(prefix, &Stmt{})
	}
	return cache, queries
}
