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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

const (
	leaderPath = "/pd/0/leader"
	memberVal  = "member"
	step       = uint64(500)
)

// TestMultipleAllocator tests situation where multiple allocators that
// share rootPath and member val update their ids concurrently.
func TestMultipleAllocator(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
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
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	_, err := client.Put(context.Background(), leaderPath, memberVal)
	re.NoError(err)
	checkIDAllocationEndValue(t, client, nonNextGenKeyspaceIDLimit)
	failpoint.Enable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag", `return(true)`)
	defer func() {
		failpoint.Disable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag")
	}()
	checkIDAllocationEndValue(t, client, reservedKeyspaceIDStart-1)
}

func checkIDAllocationEndValue(t *testing.T, client *clientv3.Client, endID uint64) {
	t.Run("KeyspaceLabel should hit ErrIDExhausted when trying to allocate into unavailable range", func(t *testing.T) {
		re := require.New(t)
		for _, step := range []uint64{1, 10, 1024, 1025} {
			keyspaceAllocator := NewAllocator(&AllocatorParams{
				Client: client,
				Label:  KeyspaceLabel,
				Member: memberVal,
				Step:   step,
			})
			initialBaseValue := endID - step*3

			err := keyspaceAllocator.SetBase(initialBaseValue)
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
