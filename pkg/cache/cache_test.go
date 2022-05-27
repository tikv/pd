// Copyright 2016 TiKV Project Authors.
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

package cache

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpireRegionCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := NewIDTTL(ctx, time.Second, 2*time.Second)
	// Test Pop
	cache.PutWithTTL(9, "9", 5*time.Second)
	cache.PutWithTTL(10, "10", 5*time.Second)
	require.Equal(t, 2, cache.Len())
	k, v, success := cache.pop()
	require.True(t, success)
	require.Equal(t, 1, cache.Len())
	k2, v2, success := cache.pop()
	require.True(t, success)
	// we can't ensure the order which the key/value pop from cache, so we save into a map
	kvMap := map[uint64]string{
		9:  "9",
		10: "10",
	}
	expV, ok := kvMap[k.(uint64)]
	require.True(t, ok)
	require.Equal(t, expV, v.(string))
	expV, ok = kvMap[k2.(uint64)]
	require.True(t, ok)
	require.Equal(t, expV, v2.(string))

	cache.PutWithTTL(11, "11", 1*time.Second)
	time.Sleep(5 * time.Second)
	k, v, success = cache.pop()
	require.False(t, success)
	require.Nil(t, k)
	require.Nil(t, v)

	// Test Get
	cache.PutWithTTL(1, 1, 1*time.Second)
	cache.PutWithTTL(2, "v2", 5*time.Second)
	cache.PutWithTTL(3, 3.0, 5*time.Second)

	value, ok := cache.Get(1)
	require.True(t, ok)
	require.Equal(t, 1, value)

	value, ok = cache.Get(2)
	require.True(t, ok)
	require.Equal(t, "v2", value)

	value, ok = cache.Get(3)
	require.True(t, ok)
	require.Equal(t, 3.0, value)

	require.Equal(t, 3, cache.Len())

	require.True(t, reflect.DeepEqual(sortIDs(cache.GetAllID()), []uint64{1, 2, 3}))

	time.Sleep(2 * time.Second)

	value, ok = cache.Get(1)
	require.False(t, ok)
	require.Nil(t, value)

	value, ok = cache.Get(2)
	require.True(t, ok)
	require.Equal(t, "v2", value)

	value, ok = cache.Get(3)
	require.True(t, ok)
	require.Equal(t, 3.0, value)

	require.Equal(t, 2, cache.Len())
	require.True(t, reflect.DeepEqual(sortIDs(cache.GetAllID()), []uint64{2, 3}))

	cache.Remove(2)

	value, ok = cache.Get(2)
	require.False(t, ok)
	require.Nil(t, value)

	value, ok = cache.Get(3)
	require.True(t, ok)
	require.Equal(t, 3.0, value)

	require.Equal(t, 1, cache.Len())
	require.True(t, reflect.DeepEqual(sortIDs(cache.GetAllID()), []uint64{3}))
}

func sortIDs(ids []uint64) []uint64 {
	ids = append(ids[:0:0], ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func TestLRUCache(t *testing.T) {
	cache := newLRU(3)

	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")

	val, ok := cache.Get(3)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "3"))

	val, ok = cache.Get(2)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "2"))

	val, ok = cache.Get(1)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "1"))

	require.Equal(t, 3, cache.Len())

	cache.Put(4, "4")

	require.Equal(t, 3, cache.Len())

	val, ok = cache.Get(3)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(1)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "1"))

	val, ok = cache.Get(2)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "2"))

	val, ok = cache.Get(4)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "4"))

	require.Equal(t, 3, cache.Len())

	val, ok = cache.Peek(1)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "1"))

	elems := cache.Elems()
	require.Len(t, elems, 3)
	require.True(t, reflect.DeepEqual(elems[0].Value, "4"))
	require.True(t, reflect.DeepEqual(elems[1].Value, "2"))
	require.True(t, reflect.DeepEqual(elems[2].Value, "1"))

	cache.Remove(1)
	cache.Remove(2)
	cache.Remove(4)

	require.Equal(t, 0, cache.Len())

	val, ok = cache.Get(1)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(2)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(3)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(4)
	require.False(t, ok)
	require.Nil(t, val)
}

func TestFifoCache(t *testing.T) {
	cache := NewFIFO(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")
	require.Equal(t, 3, cache.Len())

	cache.Put(4, "4")
	require.Equal(t, 3, cache.Len())

	elems := cache.Elems()
	require.Len(t, elems, 3)
	require.True(t, reflect.DeepEqual(elems[0].Value, "2"))
	require.True(t, reflect.DeepEqual(elems[1].Value, "3"))
	require.True(t, reflect.DeepEqual(elems[2].Value, "4"))

	elems = cache.FromElems(3)
	require.Len(t, elems, 1)
	require.True(t, reflect.DeepEqual(elems[0].Value, "4"))

	cache.Remove()
	cache.Remove()
	cache.Remove()
	require.Equal(t, 0, cache.Len())
}

func TestTwoQueueCache(t *testing.T) {
	cache := newTwoQueue(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")

	val, ok := cache.Get(3)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "3"))

	val, ok = cache.Get(2)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "2"))

	val, ok = cache.Get(1)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "1"))

	require.Equal(t, 3, cache.Len())

	cache.Put(4, "4")

	require.Equal(t, 3, cache.Len())

	val, ok = cache.Get(3)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(1)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "1"))

	val, ok = cache.Get(2)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "2"))

	val, ok = cache.Get(4)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "4"))

	require.Equal(t, 3, cache.Len())

	val, ok = cache.Peek(1)
	require.True(t, ok)
	require.True(t, reflect.DeepEqual(val, "1"))

	elems := cache.Elems()
	require.Len(t, elems, 3)
	require.True(t, reflect.DeepEqual(elems[0].Value, "4"))
	require.True(t, reflect.DeepEqual(elems[1].Value, "2"))
	require.True(t, reflect.DeepEqual(elems[2].Value, "1"))

	cache.Remove(1)
	cache.Remove(2)
	cache.Remove(4)

	require.Equal(t, 0, cache.Len())

	val, ok = cache.Get(1)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(2)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(3)
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = cache.Get(4)
	require.False(t, ok)
	require.Nil(t, val)
}

var _ PriorityQueueItem = PriorityQueueItemTest(0)

type PriorityQueueItemTest uint64

func (pq PriorityQueueItemTest) ID() uint64 {
	return uint64(pq)
}

func TestPriorityQueue(t *testing.T) {
	testData := []PriorityQueueItemTest{0, 1, 2, 3, 4, 5}
	pq := NewPriorityQueue(0)
	require.False(t, pq.Put(1, testData[1]))

	// it will have priority-value pair as 1-1 2-2 3-3
	pq = NewPriorityQueue(3)
	require.True(t, pq.Put(1, testData[1]))
	require.True(t, pq.Put(2, testData[2]))
	require.True(t, pq.Put(3, testData[4]))
	require.True(t, pq.Put(5, testData[4]))
	require.False(t, pq.Put(5, testData[5]))
	require.True(t, pq.Put(3, testData[3]))
	require.True(t, pq.Put(3, testData[3]))
	require.Nil(t, pq.Get(4))
	require.Equal(t, 3, pq.Len())

	// case1 test getAll, the highest element should be the first
	entries := pq.Elems()
	require.Len(t, entries, 3)
	require.Equal(t, 1, entries[0].Priority)
	require.Equal(t, testData[1], entries[0].Value)
	require.Equal(t, 2, entries[1].Priority)
	require.Equal(t, testData[2], entries[1].Value)
	require.Equal(t, 3, entries[2].Priority)
	require.Equal(t, testData[3], entries[2].Value)

	// case2 test remove the high element, and the second element should be the first
	pq.Remove(uint64(1))
	require.Nil(t, pq.Get(1))
	require.Equal(t, 2, pq.Len())
	entry := pq.Peek()
	require.Equal(t, 2, entry.Priority)
	require.Equal(t, testData[2], entry.Value)

	// case3 update 3's priority to highest
	pq.Put(-1, testData[3])
	entry = pq.Peek()
	require.Equal(t, -1, entry.Priority)
	require.Equal(t, testData[3], entry.Value)
	pq.Remove(entry.Value.ID())
	require.Equal(t, testData[2], pq.Peek().Value)
	require.Equal(t, 1, pq.Len())

	// case4 remove all element
	pq.Remove(uint64(2))
	require.Equal(t, 0, pq.Len())
	require.Len(t, pq.items, 0)
	require.Nil(t, pq.Peek())
	require.Nil(t, pq.Tail())
}
