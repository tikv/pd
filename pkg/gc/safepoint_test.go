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
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server/config"
)

func newGCStorage(t *testing.T) (storage endpoint.GCStateStorage, clean func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	kvBase := kv.NewEtcdKVBase(client, rootPath)

	s := endpoint.NewStorageEndpoint(kvBase, nil)

	return endpoint.NewGCStateStorage(s), clean
}

func testGCSafePointUpdateSequentiallyImpl(t *testing.T,
	loadFunc func(m *GCStateManager) (uint64, error),
	advanceFunc func(m *GCStateManager, target uint64) (uint64, uint64, error)) {

	storage, clean := newGCStorage(t)
	defer clean()
	gcStateManager := NewGCStateManager(storage, config.PDServerConfig{})
	re := require.New(t)
	curGCSafePoint := uint64(0)
	// Update GC safe point with asc value.
	for id := 10; id < 20; id++ {
		safePoint, err := loadFunc(gcStateManager)
		re.NoError(err)
		re.Equal(curGCSafePoint, safePoint)
		previousGCSafePoint := curGCSafePoint
		curGCSafePoint = uint64(id)
		oldGCSafePoint, newGCSafePoint, err := advanceFunc(gcStateManager, curGCSafePoint)
		re.NoError(err)
		re.Equal(previousGCSafePoint, oldGCSafePoint)
		re.Equal(curGCSafePoint, newGCSafePoint)
	}

	gcSafePoint, err := gcStateManager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	re.Equal(curGCSafePoint, gcSafePoint)
	// Update with smaller value should be failed.
	oldGCSafePoint, newGCSafePoint, err := advanceFunc(gcStateManager, gcSafePoint-5)
	re.NoError(err)
	re.Equal(gcSafePoint, oldGCSafePoint)
	re.Equal(gcSafePoint, newGCSafePoint)
	curGCSafePoint, err = gcStateManager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	// Current GC safe point should not change since the update value was smaller
	re.Equal(gcSafePoint, curGCSafePoint)
}

func TestGCSafePointUpdateSequentially(t *testing.T) {
	legacyLoad := func(m *GCStateManager) (uint64, error) {
		return m.CompatibleLoadGCSafePoint()
	}
	legacyAdvance := func(m *GCStateManager, target uint64) (uint64, uint64, error) {
		return m.CompatibleUpdateGCSafePoint(target)
	}
	newLoad := func(m *GCStateManager) (uint64, error) {
		state, err := m.GetGCState(constant.NullKeyspaceID)
		if err != nil {
			return 0, err
		}
		return state.GCSafePoint, nil
	}
	newAdvance := func(m *GCStateManager, target uint64) (uint64, uint64, error) {
		return m.AdvanceGCSafePoint(constant.NullKeyspaceID, target)
	}
	testGCSafePointUpdateSequentiallyImpl(t, legacyLoad, legacyAdvance)
	testGCSafePointUpdateSequentiallyImpl(t, legacyLoad, newAdvance)
	testGCSafePointUpdateSequentiallyImpl(t, newLoad, legacyAdvance)
	testGCSafePointUpdateSequentiallyImpl(t, newLoad, newAdvance)
}

func TestGCSafePointUpdateConcurrently(t *testing.T) {
	storage, clean := newGCStorage(t)
	defer clean()
	gcSafePointManager := NewGCStateManager(storage, config.PDServerConfig{})
	maxSafePoint := uint64(1000)
	wg := sync.WaitGroup{}
	re := require.New(t)

	// Update GC safe point concurrently
	for id := range 20 {
		wg.Add(1)
		go func(step uint64) {
			for gcSafePoint := step; gcSafePoint <= maxSafePoint; gcSafePoint += step {
				// Mix using new and legacy API
				var err error
				if (gcSafePoint/step)%2 == 0 {
					_, _, err = gcSafePointManager.AdvanceGCSafePoint(constant.NullKeyspaceID, gcSafePoint)
				} else {
					_, _, err = gcSafePointManager.CompatibleUpdateGCSafePoint(gcSafePoint)
				}
				re.NoError(err)
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	gcSafePoint, err := gcSafePointManager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	re.Equal(maxSafePoint, gcSafePoint)
}

func TestServiceGCSafePointUpdate(t *testing.T) {
	storage, clean := newGCStorage(t)
	defer clean()
	manager := NewGCStateManager(storage, config.PDServerConfig{})

	re := require.New(t)
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
		min, updated, err := manager.CompatibleUpdateServiceGCSafePoint(cdcServiceID, cdcServiceSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// update the safepoint for br to 15 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// update safepoint to 8 for gc_worker should be success
	go func() {
		defer wg.Done()
		// update with valid ttl for gc_worker should be success.
		min, updated, _ := manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
		re.True(updated)
		// the current min safepoint should be 8 for gc_worker(cdc 10)
		re.Equal(gcWorkerSafePoint, min.SafePoint)
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	go func() {
		defer wg.Done()
		// update safepoint of gc_worker's service with ttl not infinity should be failed.
		_, updated, err := manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, 10000, 10, time.Now())
		re.Error(err)
		re.False(updated)
	}()

	// update safepoint with negative ttl should be failed.
	go func() {
		defer wg.Done()
		brTTL := int64(-100)
		_, updated, err := manager.CompatibleUpdateServiceGCSafePoint(brServiceID, uint64(10000), brTTL, time.Now())
		re.NoError(err)
		re.False(updated)
	}()

	wg.Wait()
	// update safepoint to 15(>10 for cdc) for gc_worker
	gcWorkerSafePoint = uint64(15)
	min, updated, err := manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
	re.NoError(err)
	re.True(updated)
	re.Equal(cdcServiceID, min.ServiceID)
	re.Equal(cdcServiceSafePoint, min.SafePoint)

	// the value shouldn't be updated with current safepoint smaller than the min safepoint.
	brTTL := int64(100)
	brSafePoint = min.SafePoint - 5
	min, updated, err = manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.False(updated)

	brSafePoint = min.SafePoint + 10
	_, updated, err = manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.True(updated)
}
