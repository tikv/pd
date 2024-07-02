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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestExpireRegionCache(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := NewIDTTL(ctx, 10*time.Millisecond, 20*time.Millisecond)
	// Test Pop
	cache.PutWithTTL(9, "9", 50*time.Millisecond)
	cache.PutWithTTL(10, "10", 50*time.Millisecond)
	re.Equal(2, cache.Len())
	k, v, success := cache.pop()
	re.True(success)
	re.Equal(1, cache.Len())
	k2, v2, success := cache.pop()
	re.True(success)
	// we can't ensure the order which the key/value pop from cache, so we save into a map
	kvMap := map[uint64]string{
		9:  "9",
		10: "10",
	}
	expV, ok := kvMap[k.(uint64)]
	re.True(ok)
	re.Equal(expV, v.(string))
	expV, ok = kvMap[k2.(uint64)]
	re.True(ok)
	re.Equal(expV, v2.(string))

	cache.PutWithTTL(11, "11", 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	k, v, success = cache.pop()
	re.False(success)
	re.Nil(k)
	re.Nil(v)

	// Test Get
	cache.PutWithTTL(1, 1, 10*time.Millisecond)
	cache.PutWithTTL(2, "v2", 50*time.Millisecond)
	cache.PutWithTTL(3, 3.0, 50*time.Millisecond)

	value, ok := cache.Get(1)
	re.True(ok)
	re.Equal(1, value)

	value, ok = cache.Get(2)
	re.True(ok)
	re.Equal("v2", value)

	value, ok = cache.Get(3)
	re.True(ok)
	re.Equal(3.0, value)

	re.Equal(3, cache.Len())

	re.Equal([]uint64{1, 2, 3}, sortIDs(cache.GetAllID()))

	// after 20ms, the key 1 will be expired
	time.Sleep(20 * time.Millisecond)

	value, ok = cache.Get(1)
	re.False(ok)
	re.Nil(value)

	value, ok = cache.Get(2)
	re.True(ok)
	re.Equal("v2", value)

	value, ok = cache.Get(3)
	re.True(ok)
	re.Equal(3.0, value)

	testutil.Eventually(re, func() bool {
		// we can't ensure whether gc is executed, so we check the length of cache in a loop.
		return cache.Len() == 2
	}, testutil.WithWaitFor(50*time.Millisecond), testutil.WithTickInterval(time.Millisecond))
	re.Equal([]uint64{2, 3}, sortIDs(cache.GetAllID()))

	cache.Remove(2)

	value, ok = cache.Get(2)
	re.False(ok)
	re.Nil(value)

	value, ok = cache.Get(3)
	re.True(ok)
	re.Equal(3.0, value)

	re.Equal(1, cache.Len())
	re.Equal([]uint64{3}, sortIDs(cache.GetAllID()))
}

func sortIDs(ids []uint64) []uint64 {
	ids = append(ids[:0:0], ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func TestLRUCache(t *testing.T) {
	re := require.New(t)
	cache := newLRU(3)

	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")

	val, ok := cache.Get(3)
	re.True(ok)
	re.Equal("3", val)

	val, ok = cache.Get(2)
	re.True(ok)
	re.Equal("2", val)

	val, ok = cache.Get(1)
	re.True(ok)
	re.Equal("1", val)

	re.Equal(3, cache.Len())

	cache.Put(4, "4")

	re.Equal(3, cache.Len())

	val, ok = cache.Get(3)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(1)
	re.True(ok)
	re.Equal("1", val)

	val, ok = cache.Get(2)
	re.True(ok)
	re.Equal("2", val)

	val, ok = cache.Get(4)
	re.True(ok)
	re.Equal("4", val)

	re.Equal(3, cache.Len())

	val, ok = cache.Peek(1)
	re.True(ok)
	re.Equal("1", val)

	elems := cache.Elems()
	re.Len(elems, 3)
	re.Equal("4", elems[0].Value)
	re.Equal("2", elems[1].Value)
	re.Equal("1", elems[2].Value)

	cache.Remove(1)
	cache.Remove(2)
	cache.Remove(4)

	re.Equal(0, cache.Len())

	val, ok = cache.Get(1)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(2)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(3)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(4)
	re.False(ok)
	re.Nil(val)
}

func TestFifoCache(t *testing.T) {
	re := require.New(t)
	cache := NewFIFO(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")
	re.Equal(3, cache.Len())

	cache.Put(4, "4")
	re.Equal(3, cache.Len())

	elems := cache.Elems()
	re.Len(elems, 3)
	re.Equal("2", elems[0].Value)
	re.Equal("3", elems[1].Value)
	re.Equal("4", elems[2].Value)

	elems = cache.FromElems(3)
	re.Len(elems, 1)
	re.Equal("4", elems[0].Value)

	cache.Remove()
	cache.Remove()
	cache.Remove()
	re.Equal(0, cache.Len())
}

func TestFifoFromLastSameElems(t *testing.T) {
	re := require.New(t)
	type testStruct struct {
		value string
	}
	cache := NewFIFO(4)
	cache.Put(1, &testStruct{value: "1"})
	cache.Put(1, &testStruct{value: "2"})
	cache.Put(1, &testStruct{value: "3"})
	fun := func() []*Item {
		return cache.FromLastSameElems(
			func(i any) (bool, string) {
				result, ok := i.(*testStruct)
				if result == nil {
					return ok, ""
				}
				return ok, result.value
			})
	}
	items := fun()
	re.Len(items, 1)
	cache.Put(1, &testStruct{value: "3"})
	cache.Put(2, &testStruct{value: "3"})
	items = fun()
	re.Len(items, 3)
	re.Equal("3", items[0].Value.(*testStruct).value)
	cache.Put(1, &testStruct{value: "2"})
	items = fun()
	re.Len(items, 1)
	re.Equal("2", items[0].Value.(*testStruct).value)
}

func TestTwoQueueCache(t *testing.T) {
	re := require.New(t)
	cache := newTwoQueue(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")

	val, ok := cache.Get(3)
	re.True(ok)
	re.Equal("3", val)

	val, ok = cache.Get(2)
	re.True(ok)
	re.Equal("2", val)

	val, ok = cache.Get(1)
	re.True(ok)
	re.Equal("1", val)

	re.Equal(3, cache.Len())

	cache.Put(4, "4")

	re.Equal(3, cache.Len())

	val, ok = cache.Get(3)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(1)
	re.True(ok)
	re.Equal("1", val)

	val, ok = cache.Get(2)
	re.True(ok)
	re.Equal("2", val)

	val, ok = cache.Get(4)
	re.True(ok)
	re.Equal("4", val)

	re.Equal(3, cache.Len())

	val, ok = cache.Peek(1)
	re.True(ok)
	re.Equal("1", val)

	elems := cache.Elems()
	re.Len(elems, 3)
	re.Equal("4", elems[0].Value)
	re.Equal("2", elems[1].Value)
	re.Equal("1", elems[2].Value)

	cache.Remove(1)
	cache.Remove(2)
	cache.Remove(4)

	re.Equal(0, cache.Len())

	val, ok = cache.Get(1)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(2)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(3)
	re.False(ok)
	re.Nil(val)

	val, ok = cache.Get(4)
	re.False(ok)
	re.Nil(val)
}

var _ PriorityQueueItem = PriorityQueueItemTest(0)

type PriorityQueueItemTest uint64

func (pq PriorityQueueItemTest) ID() uint64 {
	return uint64(pq)
}

func TestPriorityQueue(t *testing.T) {
	re := require.New(t)
	testData := []PriorityQueueItemTest{0, 1, 2, 3, 4, 5}
	pq := NewPriorityQueue(0)
	re.False(pq.Put(1, testData[1]))

	// it will have priority-value pair as 1-1 2-2 3-3
	pq = NewPriorityQueue(3)
	re.True(pq.Put(1, testData[1]))
	re.True(pq.Put(2, testData[2]))
	re.True(pq.Put(3, testData[4]))
	re.True(pq.Put(5, testData[4]))
	re.False(pq.Put(5, testData[5]))
	re.True(pq.Put(3, testData[3]))
	re.True(pq.Put(3, testData[3]))
	re.Nil(pq.Get(4))
	re.Equal(3, pq.Len())

	// case1 test getAll, the highest element should be the first
	entries := pq.Elems()
	re.Len(entries, 3)
	re.Equal(1, entries[0].Priority)
	re.Equal(testData[1], entries[0].Value)
	re.Equal(2, entries[1].Priority)
	re.Equal(testData[2], entries[1].Value)
	re.Equal(3, entries[2].Priority)
	re.Equal(testData[3], entries[2].Value)

	// case2 test remove the high element, and the second element should be the first
	pq.Remove(uint64(1))
	re.Nil(pq.Get(1))
	re.Equal(2, pq.Len())
	entry := pq.peek()
	re.Equal(2, entry.Priority)
	re.Equal(testData[2], entry.Value)

	// case3 update 3's priority to highest
	pq.Put(-1, testData[3])
	entry = pq.peek()
	re.Equal(-1, entry.Priority)
	re.Equal(testData[3], entry.Value)
	pq.Remove(entry.Value.ID())
	re.Equal(testData[2], pq.peek().Value)
	re.Equal(1, pq.Len())

	// case4 remove all element
	pq.Remove(uint64(2))
	re.Equal(0, pq.Len())
	re.Empty(pq.items)
	re.Nil(pq.peek())
	re.Nil(pq.tail())
}
