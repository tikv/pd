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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/server/config"
)

func newGCStorage() endpoint.GCSafePointStorage {
	return endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
}

func TestGCSafePointUpdateSequentially(t *testing.T) {
	gcSafePointManager := NewSafePointManager(newGCStorage(), config.PDServerConfig{})
	re := require.New(t)
	curSafePoint := uint64(0)
	// update gc safePoint with asc value.
	for id := 10; id < 20; id++ {
		safePoint, err := gcSafePointManager.LoadGCSafePoint()
		re.NoError(err)
		re.Equal(curSafePoint, safePoint)
		previousSafePoint := curSafePoint
		curSafePoint = uint64(id)
		oldSafePoint, err := gcSafePointManager.UpdateGCSafePoint(curSafePoint)
		re.NoError(err)
		re.Equal(previousSafePoint, oldSafePoint)
	}

	safePoint, err := gcSafePointManager.LoadGCSafePoint()
	re.NoError(err)
	re.Equal(curSafePoint, safePoint)
	// update with smaller value should be failed.
	oldSafePoint, err := gcSafePointManager.UpdateGCSafePoint(safePoint - 5)
	re.NoError(err)
	re.Equal(safePoint, oldSafePoint)
	curSafePoint, err = gcSafePointManager.LoadGCSafePoint()
	re.NoError(err)
	// current safePoint should not change since the update value was smaller
	re.Equal(safePoint, curSafePoint)
}

func TestGCSafePointUpdateCurrently(t *testing.T) {
	gcSafePointManager := NewSafePointManager(newGCStorage(), config.PDServerConfig{})
	maxSafePoint := uint64(1000)
	wg := sync.WaitGroup{}
	re := require.New(t)

	// update gc safePoint concurrently
	for id := range 20 {
		wg.Add(1)
		go func(step uint64) {
			for safePoint := step; safePoint <= maxSafePoint; safePoint += step {
				_, err := gcSafePointManager.UpdateGCSafePoint(safePoint)
				re.NoError(err)
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	safePoint, err := gcSafePointManager.LoadGCSafePoint()
	re.NoError(err)
	re.Equal(maxSafePoint, safePoint)
}

func TestServiceGCSafePointUpdate(t *testing.T) {
	re := require.New(t)
	manager := NewSafePointManager(newGCStorage(), config.PDServerConfig{})
	gcWorkerServiceID := "gc_worker"
	cdcServiceID := "cdc"
	brServiceID := "br"
	cdcServiceSafePoint := uint64(10)
	gcWorkerSafePoint := uint64(8)
	brSafePoint := uint64(15)

	wg := sync.WaitGroup{}
	wg.Add(5)
	// update the safepoint for cdc to 10 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.UpdateServiceGCSafePoint(cdcServiceID, cdcServiceSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// update the safepoint for br to 15 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.UpdateServiceGCSafePoint(brServiceID, brSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// update safepoint to 8 for gc_worker should be success
	go func() {
		defer wg.Done()
		// update with valid ttl for gc_worker should be success.
		min, updated, _ := manager.UpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
		re.True(updated)
		// the current min safepoint should be 8 for gc_worker(cdc 10)
		re.Equal(gcWorkerSafePoint, min.SafePoint)
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	go func() {
		defer wg.Done()
		// update safepoint of gc_worker's service with ttl not infinity should be failed.
		_, updated, err := manager.UpdateServiceGCSafePoint(gcWorkerServiceID, 10000, 10, time.Now())
		re.Error(err)
		re.False(updated)
	}()

	// update safepoint with negative ttl should be failed.
	go func() {
		defer wg.Done()
		brTTL := int64(-100)
		_, updated, err := manager.UpdateServiceGCSafePoint(brServiceID, uint64(10000), brTTL, time.Now())
		re.NoError(err)
		re.False(updated)
	}()

	wg.Wait()
	// update safepoint to 15(>10 for cdc) for gc_worker
	gcWorkerSafePoint = uint64(15)
	min, updated, err := manager.UpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
	re.NoError(err)
	re.True(updated)
	re.Equal(cdcServiceID, min.ServiceID)
	re.Equal(cdcServiceSafePoint, min.SafePoint)

	// the value shouldn't be updated with current safepoint smaller than the min safepoint.
	brTTL := int64(100)
	brSafePoint = min.SafePoint - 5
	min, updated, err = manager.UpdateServiceGCSafePoint(brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.False(updated)

	brSafePoint = min.SafePoint + 10
	_, updated, err = manager.UpdateServiceGCSafePoint(brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.True(updated)
}

func TestBlockUpdateSafePointV1(t *testing.T) {
	re := require.New(t)
	manager := NewSafePointManager(newGCStorage(), config.PDServerConfig{BlockSafePointV1: true})
	gcworkerServiceID := "gc_worker"
	gcWorkerSafePoint := uint64(8)

	min, updated, err := manager.UpdateServiceGCSafePoint(gcworkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
	re.Error(err, blockServiceSafepointErrmsg)
	re.Equal(err.Error(), blockServiceSafepointErrmsg)
	re.False(updated)
	re.Nil(min)

	oldSafePoint, err := manager.UpdateGCSafePoint(gcWorkerSafePoint)
	re.Error(err)
	re.Equal(err.Error(), blockGCSafePointErrmsg)

	re.Equal(uint64(0), oldSafePoint)
}
