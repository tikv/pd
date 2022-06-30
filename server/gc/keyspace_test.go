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

package gc

import (
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
	"math"
	"sync"
	"testing"
	"time"
)

func newKeyspaceGCStorage() endpoint.KeyspaceGCSafePointStorage {
	return endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
}

func mustSequentialUpdateGCSafePoint(manager *KeyspaceSafePointManager, re *require.Assertions, spaceID uint32) {
	curSafePoint := uint64(0)
	// update gc safePoint with asc value.
	for id := 10; id < 20; id++ {
		safePoint, err := manager.LoadGCSafePoint(spaceID)
		re.NoError(err)
		re.Equal(curSafePoint, safePoint)
		previousSafePoint := curSafePoint
		curSafePoint = uint64(id)
		oldSafePoint, err := manager.UpdateGCSafePoint(spaceID, curSafePoint)
		re.NoError(err)
		re.Equal(previousSafePoint, oldSafePoint)
	}

	safePoint, err := manager.LoadGCSafePoint(spaceID)
	re.NoError(err)
	re.Equal(curSafePoint, safePoint)
	// update with smaller value should be failed.
	oldSafePoint, err := manager.UpdateGCSafePoint(spaceID, safePoint-5)
	re.NoError(err)
	re.Equal(safePoint, oldSafePoint)
	curSafePoint, err = manager.LoadGCSafePoint(spaceID)
	re.NoError(err)
	// current safePoint should not change since the update value was smaller
	re.Equal(safePoint, curSafePoint)
}

func mustConcurrentUpdateGCSafePoint(manager *KeyspaceSafePointManager, re *require.Assertions, spaceID uint32) {
	maxSafePoint := uint64(1000)
	wg := sync.WaitGroup{}

	// update gc safePoint concurrently
	for id := 0; id < 20; id++ {
		wg.Add(1)
		go func(step uint64) {
			for safePoint := step; safePoint <= maxSafePoint; safePoint += step {
				_, err := manager.UpdateGCSafePoint(spaceID, safePoint)
				re.NoError(err)
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	safePoint, err := manager.LoadGCSafePoint(spaceID)
	re.NoError(err)
	re.Equal(maxSafePoint, safePoint)
}

func mustUpdateServiceSafePoint(manager *KeyspaceSafePointManager, re *require.Assertions, spaceID uint32) {

	gcWorkerServiceID := "gc_worker"
	cdcServiceID := "cdc"
	brServiceID := "br"
	gcWorkerSafePoint := uint64(8)
	cdcServiceSafePoint := uint64(10)
	brSafePoint := uint64(15)

	wg := sync.WaitGroup{}
	wg.Add(5)
	// update the safepoint for cdc to 10 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.UpdateServiceSafePoint(spaceID, cdcServiceID, cdcServiceSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service should init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// update the safepoint for br to 15 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.UpdateServiceSafePoint(spaceID, brServiceID, brSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// gc_worker's safe point should remain 0
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// update safepoint to 8 for gc_worker should be success
	go func() {
		defer wg.Done()
		// update with valid ttl for gc_worker should be success.
		min, updated, _ := manager.UpdateServiceSafePoint(spaceID, gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
		re.True(updated)
		// the current min safepoint should be 8 for gc_worker(cdc 10)
		re.Equal(gcWorkerSafePoint, min.SafePoint)
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	go func() {
		defer wg.Done()
		// update safepoint of gc_worker's service with ttl not infinity should be failed.
		_, updated, err := manager.UpdateServiceSafePoint(spaceID, gcWorkerServiceID, 10000, 10, time.Now())
		re.Error(err)
		re.False(updated)
	}()

	// update safepoint with negative ttl should be failed.
	go func() {
		defer wg.Done()
		brTTL := int64(-100)
		_, updated, err := manager.UpdateServiceSafePoint(spaceID, brServiceID, 10000, brTTL, time.Now())
		re.NoError(err)
		re.False(updated)
	}()

	wg.Wait()
	// update safepoint to 15(>10 for cdc) for gc_worker
	gcWorkerSafePoint = uint64(15)
	min, updated, err := manager.UpdateServiceSafePoint(spaceID, gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
	re.NoError(err)
	re.True(updated)
	re.Equal(cdcServiceID, min.ServiceID)
	re.Equal(cdcServiceSafePoint, min.SafePoint)

	// the value shouldn't be updated with current safepoint smaller than the min safepoint.
	brTTL := int64(100)
	brSafePoint = min.SafePoint - 5
	min, updated, err = manager.UpdateServiceSafePoint(spaceID, brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.False(updated)

	brSafePoint = min.SafePoint + 10
	_, updated, err = manager.UpdateServiceSafePoint(spaceID, brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.True(updated)
}

func TestSingleKeyspaceSequentialUpdate(t *testing.T) {
	manager := NewKeyspaceSafePointManager(newKeyspaceGCStorage())
	re := require.New(t)
	mustSequentialUpdateGCSafePoint(manager, re, 100)
}

func TestMultipleKeyspaceSequentialUpdate(t *testing.T) {
	manager := NewKeyspaceSafePointManager(newKeyspaceGCStorage())
	re := require.New(t)
	wg := sync.WaitGroup{}

	for spaceID := uint32(100); spaceID < 110; spaceID++ {
		wg.Add(1)
		go func(spaceID uint32) {
			mustSequentialUpdateGCSafePoint(manager, re, spaceID)
			wg.Done()
		}(spaceID)
	}
	wg.Wait()
}

func TestMultipleKeyspaceConcurrentUpdate(t *testing.T) {
	manager := NewKeyspaceSafePointManager(newKeyspaceGCStorage())
	re := require.New(t)
	wg := sync.WaitGroup{}

	for spaceID := uint32(100); spaceID < 110; spaceID++ {
		wg.Add(1)
		go func(spaceID uint32) {
			mustConcurrentUpdateGCSafePoint(manager, re, spaceID)
			wg.Done()
		}(spaceID)
	}
	wg.Wait()
}

func TestUpdateServiceSafePoint(t *testing.T) {
	manager := NewKeyspaceSafePointManager(newKeyspaceGCStorage())
	re := require.New(t)
	wg := sync.WaitGroup{}

	for spaceID := uint32(100); spaceID < 110; spaceID++ {
		wg.Add(1)
		go func(spaceID uint32) {
			mustUpdateServiceSafePoint(manager, re, spaceID)
			wg.Done()
		}(spaceID)
	}
	wg.Wait()
}
