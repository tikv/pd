// Copyright 2022 TiKV Project Authors.
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

package id

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestBatchAllocation(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	alloc := NewAllocator(&AllocatorParams{Client: client, Member: memberVal, Label: DefaultLabel})
	var last uint64
	for _, count := range []uint32{0, 1, 998, 2, 5000, math.MaxUint32} {
		t.Run(strconv.FormatUint(uint64(count), 10), func(t *testing.T) {
			before, err := client.Get(t.Context(), keypath.AllocIDPath())
			require.NoError(t, err)
			id, actual, err := alloc.Alloc(count)
			require.NoError(t, err)
			require.Equal(t, count, actual)
			require.Equal(t, last+uint64(count), id)
			last = id
			after, err := client.Get(t.Context(), keypath.AllocIDPath())
			require.NoError(t, err)
			require.LessOrEqual(t, after.Header.Revision-before.Header.Revision, int64(1), "a batch must reserve its window in at most one transaction")
			if count != 0 {
				require.Len(t, after.Kvs, 1)
				end, err := typeutil.BytesToUint64(after.Kvs[0].Value)
				require.NoError(t, err)
				require.GreaterOrEqual(t, end, id)
			}
		})
	}
}

func TestBatchAllocationAcrossAllocators(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	params := &AllocatorParams{Client: client, Member: memberVal, Label: DefaultLabel}
	first, second := NewAllocator(params), NewAllocator(params)
	allocated := make(map[uint64]struct{})
	for _, request := range []struct {
		alloc Allocator
		count uint32
	}{{first, 3}, {second, 7}, {first, 1000}, {second, 1500}} {
		last, count, err := request.alloc.Alloc(request.count)
		require.NoError(t, err)
		require.Equal(t, request.count, count)
		for id := last - uint64(count) + 1; id <= last; id++ {
			_, exists := allocated[id]
			require.False(t, exists, "batch contains an ID returned by another allocator: %d", id)
			allocated[id] = struct{}{}
		}
	}
	persisted, err := client.Get(t.Context(), keypath.AllocIDPath())
	require.NoError(t, err)
	end, err := typeutil.BytesToUint64(persisted.Kvs[0].Value)
	require.NoError(t, err)
	restarted := NewAllocator(params)
	require.NoError(t, restarted.Rebase())
	last, count, err := restarted.Alloc(5)
	require.NoError(t, err)
	require.Equal(t, uint32(5), count)
	require.Greater(t, last-uint64(count)+1, end)
}

func TestSetBasePreservesAllocatedIDs(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	for _, label := range []label{DefaultLabel, KeyspaceLabel} {
		t.Run(string(label), func(t *testing.T) {
			alloc := NewAllocator(&AllocatorParams{Client: client, Member: memberVal, Label: label})
			key := keypath.AllocIDPath()
			if label == KeyspaceLabel {
				key = keypath.KeyspaceAllocIDPath()
			}
			require.NoError(t, alloc.SetBase(5000))
			last, _, err := alloc.Alloc(3)
			require.NoError(t, err)
			for _, base := range []uint64{0, 5000, 5000, last} {
				before, err := client.Get(t.Context(), key)
				require.NoError(t, err)
				require.NoError(t, alloc.SetBase(base))
				after, err := client.Get(t.Context(), key)
				require.NoError(t, err)
				next, _, err := alloc.Alloc(1)
				require.NoError(t, err)
				require.Equal(t, last+1, next, "a smaller recovery request must not rewind allocated IDs")
				require.Equal(t, before.Header.Revision, after.Header.Revision, "a recovery retry must not rewrite the ID window")
				last = next
			}
			// Advance within the current window without lowering its persisted end.
			before, err := client.Get(t.Context(), key)
			require.NoError(t, err)
			require.NoError(t, alloc.SetBase(5500))
			after, err := client.Get(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, before.Header.Revision, after.Header.Revision)
			next, _, err := alloc.Alloc(1)
			require.NoError(t, err)
			require.Equal(t, uint64(5501), next)

			before, err = client.Get(t.Context(), key)
			require.NoError(t, err)
			require.NoError(t, alloc.SetBase(10000))
			after, err = client.Get(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, before.Header.Revision+1, after.Header.Revision)
			next, _, err = alloc.Alloc(1)
			require.NoError(t, err)
			require.Equal(t, uint64(10001), next)
		})
	}
}

func TestSetBasePreservesOtherAllocatorWindows(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	params := &AllocatorParams{Client: client, Member: memberVal, Label: DefaultLabel}
	first, second := NewAllocator(params), NewAllocator(params)
	_, _, err = first.Alloc(1)
	require.NoError(t, err)
	secondLast, _, err := second.Alloc(1)
	require.NoError(t, err)
	before, err := client.Get(t.Context(), keypath.AllocIDPath())
	require.NoError(t, err)
	end, err := typeutil.BytesToUint64(before.Kvs[0].Value)
	require.NoError(t, err)

	// The requested base is in the second allocator's reserved range.
	require.NoError(t, first.SetBase(secondLast))
	after, err := client.Get(t.Context(), keypath.AllocIDPath())
	require.NoError(t, err)
	firstLast, _, err := first.Alloc(1)
	require.NoError(t, err)
	require.Greater(t, firstLast, end)
	nextSecond, _, err := second.Alloc(1)
	require.NoError(t, err)
	require.Equal(t, secondLast+1, nextSecond)
	require.Less(t, nextSecond, firstLast)
	require.Equal(t, before.Header.Revision, after.Header.Revision)

	// A fresh allocator must also respect a range already persisted by others.
	before, err = client.Get(t.Context(), keypath.AllocIDPath())
	require.NoError(t, err)
	end, err = typeutil.BytesToUint64(before.Kvs[0].Value)
	require.NoError(t, err)
	restarted := NewAllocator(params)
	require.NoError(t, restarted.SetBase(1))
	next, _, err := restarted.Alloc(1)
	require.NoError(t, err)
	require.Greater(t, next, end)

	// Concurrent recovery requests must not reserve overlapping windows.
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, alloc := range []Allocator{first, second} {
		wg.Go(func() { results <- alloc.SetBase(10000) })
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	firstLast, _, err = first.Alloc(1)
	require.NoError(t, err)
	secondLast, _, err = second.Alloc(1)
	require.NoError(t, err)
	require.Greater(t, firstLast, uint64(10000))
	require.Greater(t, secondLast, uint64(10000))
	require.NotEqual(t, firstLast, secondLast)
}

func TestBatchAllocationExhaustion(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	alloc := NewAllocator(&AllocatorParams{Client: client, Member: memberVal, Label: DefaultLabel})
	require.NoError(t, alloc.SetBase(math.MaxUint64-5))
	_, _, err = alloc.Alloc(6)
	require.ErrorIs(t, err, errs.ErrIDExhausted)
	last, count, err := alloc.Alloc(3)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64-2), last)
	require.Equal(t, uint32(3), count)
	last, count, err = alloc.Alloc(2)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), last)
	require.Equal(t, uint32(2), count)
	_, _, err = alloc.Alloc(1)
	require.ErrorIs(t, err, errs.ErrIDExhausted)
	require.ErrorIs(t, alloc.Rebase(), errs.ErrIDExhausted)
	require.NoError(t, alloc.SetBase(math.MaxUint64-10))
	_, _, err = alloc.Alloc(1)
	require.ErrorIs(t, err, errs.ErrIDExhausted, "recovery must not reopen an exhausted range")
}

func TestBatchAllocationLeaderChange(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	alloc := NewAllocator(&AllocatorParams{Client: client, Member: memberVal, Label: DefaultLabel})
	last, _, err := alloc.Alloc(3)
	require.NoError(t, err)
	_, err = client.Put(t.Context(), leaderPath, "new-member")
	require.NoError(t, err)
	require.ErrorIs(t, alloc.SetBase(1), errs.ErrEtcdTxnConflict)
	require.ErrorIs(t, alloc.SetBase(5000), errs.ErrEtcdTxnConflict)
	_, _, err = alloc.Alloc(5000)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	_, err = client.Put(t.Context(), leaderPath, memberVal)
	require.NoError(t, err)
	next, count, err := alloc.Alloc(5000)
	require.NoError(t, err)
	require.Equal(t, uint32(5000), count)
	require.Equal(t, last+5000, next, "failed reservation must leave the cached window unchanged")
}

const (
	leaderPath = "/pd/0/leader"
	memberVal  = "member"
	step       = uint64(500)
)

// TestMultipleAllocator tests situation where multiple allocators that
// share rootPath and member val update their ids concurrently.
func TestMultipleAllocator(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	// Put memberValue to leaderPath to simulate an election success.
	_, err := client.Put(context.Background(), leaderPath, memberVal)
	re.NoError(err)

	var i uint64
	wg := sync.WaitGroup{}
	fn := func(label label) {
		wg.Add(1)
		// Different allocators have different labels and steps.
		allocator := NewAllocator(&AllocatorParams{
			Client: client,
			Label:  label,
			Member: memberVal,
			Step:   step * i, // allocator 0, 1 should have step size 1000 (default), 500 respectively.
		})
		go func(re *require.Assertions, allocator Allocator) {
			defer wg.Done()
			testAllocator(re, allocator)
		}(re, allocator)
		i++
	}
	fn(DefaultLabel)
	fn(KeyspaceLabel)
	wg.Wait()
}

// testAllocator sequentially updates given allocator and check if values are expected.
func testAllocator(re *require.Assertions, allocator Allocator) {
	startID, _, err := allocator.Alloc(1)
	re.NoError(err)
	for i := startID + 1; i < startID+step*20; i++ {
		id, _, err := allocator.Alloc(1)
		re.NoError(err)
		re.Equal(i, id)
	}
}

// TestIDAllocationEndValue tests if keyspace allocator hits ErrIDExhausted when trying to allocate into reserved range.
func TestIDAllocationEndValue(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	_, err := client.Put(context.Background(), leaderPath, memberVal)
	re.NoError(err)
	if kerneltype.IsNextGen() {
		checkIDAllocationEndValue(t, client, constant.ReservedKeyspaceIDStart-1)
	} else {
		checkIDAllocationEndValue(t, client, uint64(constant.MaxValidKeyspaceID))
	}
}

func checkIDAllocationEndValue(t *testing.T, client *clientv3.Client, endID uint64) {
	t.Run("KeyspaceLabel should hit ErrIDExhausted when trying to allocate into unavailable range", func(t *testing.T) {
		re := require.New(t)
		for _, step := range []uint64{1, 10, 1024, 1025} {
			// Each step size starts with a fresh range; SetBase cannot rewind an exhausted one.
			_, err := client.Delete(t.Context(), keypath.KeyspaceAllocIDPath())
			re.NoError(err)
			keyspaceAllocator := NewAllocator(&AllocatorParams{
				Client: client,
				Label:  KeyspaceLabel,
				Member: memberVal,
				Step:   step,
			})
			initialBaseValue := endID - step*3

			err = keyspaceAllocator.SetBase(initialBaseValue)
			re.NoError(err)
			var lastAllocatedID uint64
			for {
				var id uint64
				id, _, err = keyspaceAllocator.Alloc(1)
				if err != nil {
					break
				}
				re.GreaterOrEqual(id, lastAllocatedID)
				lastAllocatedID = id
			}
			re.Error(err)
			re.True(errs.ErrIDExhausted.Equal(err))
			re.Equal(endID, lastAllocatedID)
		}
	})

	t.Run("SetBase should fail if newBase enters unavailable range", func(t *testing.T) {
		re := require.New(t)
		keyspaceAllocator := NewAllocator(&AllocatorParams{
			Client: client,
			Label:  KeyspaceLabel,
			Member: memberVal,
			Step:   step,
		})

		err := keyspaceAllocator.SetBase(endID - 1)
		re.NoError(err)

		err = keyspaceAllocator.SetBase(endID)
		re.Error(err)
		re.True(errs.ErrIDExhausted.Equal(err))

		err = keyspaceAllocator.SetBase(endID + 10)
		re.Error(err)
		re.True(errs.ErrIDExhausted.Equal(err))
	})
}
