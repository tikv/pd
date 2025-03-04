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
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

func newGCStateManager(t *testing.T) (provider endpoint.GCStateProvider, gccStateManager *GCStateManager, clean func()) {
	cfg := config.NewConfig()
	re := require.New(t)

	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	kvBase := kv.NewEtcdKVBase(client)

	// Simulate a member which id.Allocator may need to check.
	err := kvBase.Save(keypath.LeaderPath(nil), "member1")
	re.NoError(err)

	s := endpoint.NewStorageEndpoint(kvBase, nil)
	allocator := id.NewAllocator(&id.AllocatorParams{
		Client: client,
		Label:  id.KeyspaceLabel,
		Member: "member1",
		Step:   keyspace.AllocStep,
	})
	kgm := keyspace.NewKeyspaceGroupManager(context.Background(), s, client)
	keyspaceManager := keyspace.NewKeyspaceManager(context.Background(), s, mockcluster.NewCluster(context.Background(), config.NewPersistOptions(cfg)), allocator, &config.KeyspaceConfig{}, kgm)
	gcStateManager := NewGCStateManager(s.GetGCStateProvider(), cfg.PDServerCfg, keyspaceManager)

	err = kgm.Bootstrap(context.Background())
	re.NoError(err)
	err = keyspaceManager.Bootstrap()
	re.NoError(err)

	ks1, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "ks1",
		Config:     map[string]string{"gc_management_type": "global_gc"},
		CreateTime: time.Now().Unix(),
		IsPreAlloc: false,
	})
	re.NoError(err)
	re.Equal(uint32(1), ks1.Id)

	ks2, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "ks2",
		Config:     map[string]string{"gc_management_type": "keyspace_level_gc"},
		CreateTime: time.Now().Unix(),
		IsPreAlloc: false,
	})
	re.NoError(err)
	re.Equal(uint32(2), ks2.Id)

	ks3, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "ks3",
		Config:     map[string]string{},
		CreateTime: time.Now().Unix(),
		IsPreAlloc: false,
	})
	re.NoError(err)
	re.Equal(uint32(3), ks3.Id)

	return s.GetGCStateProvider(), gcStateManager, clean
}

func TestAdvanceGCSafePointBasic(t *testing.T) {

}

func testGCSafePointUpdateSequentiallyImpl(t *testing.T,
	loadFunc func(m *GCStateManager) (uint64, error),
	advanceFunc func(m *GCStateManager, target uint64) (uint64, uint64, error)) {

	_, gcStateManager, clean := newGCStateManager(t)
	defer clean()
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
	_, manager, clean := newGCStateManager(t)
	defer clean()
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
					_, _, err = manager.AdvanceGCSafePoint(constant.NullKeyspaceID, gcSafePoint)
				} else {
					_, _, err = manager.CompatibleUpdateGCSafePoint(gcSafePoint)
				}
				re.NoError(err)
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	gcSafePoint, err := manager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	re.Equal(maxSafePoint, gcSafePoint)
}

func TestLegacyServiceGCSafePointUpdate(t *testing.T) {
	_, manager, clean := newGCStateManager(t)
	defer clean()

	re := require.New(t)
	gcWorkerServiceID := "gc_worker"
	cdcServiceID := "cdc"
	brServiceID := "br"
	cdcServiceSafePoint := uint64(10)
	gcWorkerSafePoint := uint64(8)
	brSafePoint := uint64(15)

	wg := sync.WaitGroup{}
	wg.Add(5)
	// Updating the service safe point for cdc to 10 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.CompatibleUpdateServiceGCSafePoint(cdcServiceID, cdcServiceSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// The service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// Updating the service safe point for br to 15 should success
	go func() {
		defer wg.Done()
		min, updated, err := manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// Updating the service safe point to 8 for gc_worker should be success
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
		// Updating the service safe point of gc_worker's service with ttl not infinity should be failed.
		_, updated, err := manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, 10000, 10, time.Now())
		re.Error(err)
		re.False(updated)
	}()

	// Updating the service safe point with negative ttl should be failed.
	go func() {
		defer wg.Done()
		brTTL := int64(-100)
		_, updated, err := manager.CompatibleUpdateServiceGCSafePoint(brServiceID, uint64(10000), brTTL, time.Now())
		re.NoError(err)
		re.False(updated)
	}()

	wg.Wait()
	// Updating the service safe point to 15(>10 for cdc) for gc_worker
	gcWorkerSafePoint = uint64(15)
	min, updated, err := manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
	re.NoError(err)
	re.True(updated)
	re.Contains(min.ServiceID, "BarrierID: \""+cdcServiceID+"\"")
	re.Equal(cdcServiceSafePoint, min.SafePoint)

	// The value shouldn't be updated with current service safe point smaller than the min safe point.
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

func TestLegacyServiceGCSafePointRoundingTTL(t *testing.T) {
	_, manager, clean := newGCStateManager(t)
	defer clean()

	re := require.New(t)

	var maxTTL int64 = 9223372036

	_, updated, err := manager.CompatibleUpdateServiceGCSafePoint("svc1", 10, maxTTL, time.Now())
	re.NoError(err)
	re.True(updated)

	state, err := manager.GetGCState(constant.NullKeyspaceID)
	re.NoError(err)
	re.Len(state.GCBarriers, 1)
	re.Equal("svc1", state.GCBarriers[0].BarrierID)
	re.NotNil(state.GCBarriers[0].ExpirationTime)
	// The given `maxTTL` is valid but super large.
	re.True(state.GCBarriers[0].ExpirationTime.After(time.Now().Add(time.Hour*24*365*10)), state.GCBarriers[0].ExpirationTime)

	_, updated, err = manager.CompatibleUpdateServiceGCSafePoint("svc1", 10, maxTTL+1, time.Now())
	re.NoError(err)
	re.True(updated)

	state, err = manager.GetGCState(constant.NullKeyspaceID)
	re.NoError(err)
	re.Len(state.GCBarriers, 1)
	re.Equal("svc1", state.GCBarriers[0].BarrierID)
	re.Nil(state.GCBarriers[0].ExpirationTime)
	// Nil in GCBarrier.ExpirationTime represents never expires.
	re.False(state.GCBarriers[0].IsExpired(time.Now().Add(time.Hour*24*365*10)), state.GCBarriers[0])
}

func TestAdvanceTxnSafePoint(t *testing.T) {
	//_, manager, clean := newGCStateManager(t)
	//defer clean()
	//
	//re := require.New(t)
	//
	//checkTxnSafePoint := func(keyspaceID uint32, expectedTxnSafePoint uint64) {
	//	state, err := manager.GetGCState(keyspaceID)
	//	re.NoError(err)
	//	re.Equal(expectedTxnSafePoint, state.TxnSafePoint)
	//}

}

func TestGCBarriers(t *testing.T) {

}

func TestGCStateConstraints(t *testing.T) {

}

func TestServiceGCSafePointCompatibility(t *testing.T) {
	//_, manager, clean := newGCStateManager(t)
	//defer clean()
	//
	//re := require.New(t)

}

func TestRedirectKeyspace(t *testing.T) {
	_, manager, clean := newGCStateManager(t)
	defer clean()

	re := require.New(t)

	keyspaces := []uint32{constant.NullKeyspaceID, 1, 2, 3, 0x1000000, 0xffffff}
	expectError := []error{nil, nil, nil, nil, nil, errs.ErrKeyspaceNotFound}
	redirectTarget := []uint32{constant.NullKeyspaceID, constant.NullKeyspaceID, 2, constant.NullKeyspaceID, constant.NullKeyspaceID, 0}
	systemAPIAllowed := []bool{true, false, true, false, true, false}

	for i, keyspaceID := range keyspaces {
		target, err := manager.redirectKeyspace(keyspaceID, true)
		if expectError[i] != nil {
			re.Error(err, "index: %d", i)
			re.ErrorIs(err, expectError[i], "index: %d", i)
		} else {
			re.NoError(err, "index: %d", i)
			re.Equal(redirectTarget[i], target, "index: %d", i)
		}

		target, err = manager.redirectKeyspace(keyspaceID, false)
		if expectError[i] != nil {
			re.Error(err, "index: %d", i)
			re.ErrorIs(err, expectError[i], "index: %d", i)
		} else if systemAPIAllowed[i] {
			re.NoError(err, "index: %d", i)
			re.Equal(redirectTarget[i], target, "index: %d", i)
		} else {
			re.Error(err, "index: %d", i)
			re.ErrorIs(err, errs.ErrGCOnInvalidKeyspace, "index: %d", i)
		}
	}

	// Non-null but not existing keyspace id.
	_, err := manager.redirectKeyspace(0xffffff, true)
	re.Error(err)
	re.ErrorIs(err, errs.ErrKeyspaceNotFound)

	// Check all public methods that accepts keyspaceID is correctly redirected.
	testedFunc := []func(keyspaceID uint32) error{
		func(keyspaceID uint32) error {
			_, err1 := manager.GetGCState(keyspaceID)
			return err1
		},
		func(keyspaceID uint32) error {
			_, err1 := manager.AdvanceTxnSafePoint(keyspaceID, 10)
			return err1
		},
		func(keyspaceID uint32) error {
			_, _, err1 := manager.AdvanceGCSafePoint(keyspaceID, 10)
			return err1
		},
		func(keyspaceID uint32) error {
			_, err1 := manager.SetGCBarrier(keyspaceID, "b", 15, time.Hour, time.Now())
			return err1
		},
		func(keyspaceID uint32) error {
			_, err1 := manager.DeleteGCBarrier(keyspaceID, "b")
			return err1
		},
	}
	isUserAPI := []bool{true, false, false, true, true}
	for keyspaceIndex, keyspaceID := range keyspaces {
		for funcIndex, f := range testedFunc {
			err = f(keyspaceID)
			if expectError[keyspaceIndex] != nil {
				// Report error no matter it is user API or not.
				re.Error(err)
				re.ErrorIs(err, expectError[keyspaceIndex])
			} else if isUserAPI[funcIndex] {
				// User API is always redirected.
				re.NoError(err)
			} else if systemAPIAllowed[keyspaceIndex] {
				// It's not a user API, and the current keyspace allows running system API.
				re.NoError(err)
			} else {
				// It's not a user API and the current keyspace doesn't allow running system API.
				re.Error(err)
				re.ErrorIs(err, errs.ErrGCOnInvalidKeyspace)
			}
		}
	}
}
