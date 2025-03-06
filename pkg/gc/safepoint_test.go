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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

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

type gcManagerTestSuite struct {
	suite.Suite

	provider endpoint.GCStateProvider
	manager  *GCStateManager
	clean    func()

	keyspacePresets struct {
		// A set of shortcuts for different kinds of keyspaces. Initialized in SetupTest.
		// Tests are suggested to iterate over all these possibilities.

		// All valid keyspaces.
		all []uint32
		// Subset of `all` that can manage their own GC. Includes NullKeyspace and keyspaces configured keyspace-level GC.
		manageable []uint32
		// all - manageable.
		unmanageable []uint32
		// Subset of `all` that uses unified GC (equals to unmanageable + NullKeyspace).
		unifiedGC []uint32
		// A set of not existing keyspace IDs. GC methods are mostly expected to fail on them.
		notExisting []uint32
		// A set of different keyspaceIDs that are expected to be regarded the same as NullKeyspaceID (0xffffffff).
		// NullKeyspaceID is included.
		nullSynonyms []uint32
	}
}

func TestGCManager(t *testing.T) {
	suite.Run(t, new(gcManagerTestSuite))
}

func newGCStateManager(t *testing.T) (provider endpoint.GCStateProvider, gcStateManager *GCStateManager, clean func()) {
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
	gcStateManager = NewGCStateManager(s.GetGCStateProvider(), cfg.PDServerCfg, keyspaceManager)

	err = kgm.Bootstrap(context.Background())
	re.NoError(err)
	err = keyspaceManager.Bootstrap()
	re.NoError(err)

	// keyspaceID 0 exists automatically after bootstrapping.

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

func (s *gcManagerTestSuite) SetupTest() {
	s.provider, s.manager, s.clean = newGCStateManager(s.T())

	s.keyspacePresets.all = []uint32{constant.NullKeyspaceID, 0, 1, 2, 3}
	s.keyspacePresets.manageable = []uint32{constant.NullKeyspaceID, 2}
	s.keyspacePresets.unmanageable = []uint32{0, 1, 3}
	s.keyspacePresets.unifiedGC = []uint32{constant.NullKeyspaceID, 0, 1, 3}
	s.keyspacePresets.notExisting = []uint32{4, 0xffffff}
	s.keyspacePresets.nullSynonyms = []uint32{constant.NullKeyspaceID, 0x1000000, 0xfffffffe}
}

func (s *gcManagerTestSuite) TearDownTest() {
	s.clean()
}

func (s *gcManagerTestSuite) TestAdvanceTxnSafePointBasic() {
	re := s.Require()
	now := time.Now()

	checkTxnSafePoint := func(keyspaceID uint32, expectedTxnSafePoint uint64) {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		re.Equal(expectedTxnSafePoint, state.TxnSafePoint)
	}

	for _, keyspaceID := range s.keyspacePresets.all {
		checkTxnSafePoint(keyspaceID, 0)
	}

	for _, keyspaceID := range s.keyspacePresets.manageable {
		res, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 10, now)
		re.NoError(err)
		re.Equal(uint64(0), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Equal(uint64(10), res.Target)
		re.Empty(res.BlockerDescription)

		checkTxnSafePoint(keyspaceID, 10)

		// Allows updating with the same value (no effect).
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 10, now)
		re.NoError(err)
		re.Equal(uint64(10), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Equal(uint64(10), res.Target)
		re.Empty(res.BlockerDescription)

		// Does not allow decreasing.
		_, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 9, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrDecreasingTxnSafePoint)

		// Does not test blocking by GC barriers here. It will be separated in another test case.
	}

	for _, keyspaceID := range s.keyspacePresets.unmanageable {
		_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 20, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCOnInvalidKeyspace)
		// Updated in previous loop when updating NullKeyspaceID.
		checkTxnSafePoint(keyspaceID, 10)
	}

	for _, keyspaceID := range s.keyspacePresets.notExisting {
		_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 30, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrKeyspaceNotFound)
	}

	for i, keyspaceID := range s.keyspacePresets.nullSynonyms {
		// Previously updated to 10. Update to 10+i+1 in i-th loop.
		res, err := s.manager.AdvanceTxnSafePoint(keyspaceID, uint64(10+i+1), now)
		re.NoError(err)
		re.Equal(uint64(10+i), res.OldTxnSafePoint)
		re.Equal(uint64(10+i+1), res.NewTxnSafePoint)

		for _, checkingKeyspaceID := range s.keyspacePresets.unifiedGC {
			checkTxnSafePoint(checkingKeyspaceID, uint64(10+i))
		}
		for _, checkingKeyspaceID := range s.keyspacePresets.nullSynonyms {
			checkTxnSafePoint(checkingKeyspaceID, uint64(10+i))
		}
	}
}

func (s *gcManagerTestSuite) TestAdvanceGCSafePointBasic() {
	re := s.Require()

	checkGCSafePoint := func(keyspaceID uint32, expectedGCSafePoint uint64) {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		re.Equal(expectedGCSafePoint, state.GCSafePoint)
	}

	for _, keyspaceID := range s.keyspacePresets.all {
		checkGCSafePoint(keyspaceID, 0)
	}

	for _, keyspaceID := range slices.Concat(s.keyspacePresets.manageable, s.keyspacePresets.nullSynonyms) {
		// Txn safe point is not set yet. It should fail first.
		_, _, err := s.manager.AdvanceGCSafePoint(keyspaceID, 10)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCSafePointExceedsTxnSafePoint)

		// Check there's no effect.
		checkGCSafePoint(keyspaceID, 0)
	}

	for _, keyspaceID := range s.keyspacePresets.unmanageable {
		// Keyspace check is prior to all other errors.
		_, _, err := s.manager.AdvanceGCSafePoint(keyspaceID, 10)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCOnInvalidKeyspace)
	}

	for _, keyspaceID := range s.keyspacePresets.notExisting {
		_, _, err := s.manager.AdvanceGCSafePoint(keyspaceID, 10)
		re.Error(err)
		re.ErrorIs(err, errs.ErrKeyspaceNotFound)
	}

	for _, keyspaceID := range s.keyspacePresets.manageable {
		_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 10, time.Now())
		re.NoError(err)

		oldValue, newValue, err := s.manager.AdvanceGCSafePoint(keyspaceID, 5)
		re.NoError(err)
		re.Equal(uint64(0), oldValue)
		re.Equal(uint64(5), newValue)
		checkGCSafePoint(keyspaceID, 5)

		oldValue, newValue, err = s.manager.AdvanceGCSafePoint(keyspaceID, 10)
		re.NoError(err)
		re.Equal(uint64(5), oldValue)
		re.Equal(uint64(10), newValue)
		checkGCSafePoint(keyspaceID, 10)

		oldValue, newValue, err = s.manager.AdvanceGCSafePoint(keyspaceID, 11)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCSafePointExceedsTxnSafePoint)

		// Allows updating with the same value (no effect).
		oldValue, newValue, err = s.manager.AdvanceGCSafePoint(keyspaceID, 10)
		re.NoError(err)
		re.Equal(uint64(10), oldValue)
		re.Equal(uint64(10), newValue)

		// Does not allow decreasing.
		oldValue, newValue, err = s.manager.AdvanceGCSafePoint(keyspaceID, 9)
		re.Error(err)
		re.ErrorIs(err, errs.ErrDecreasingGCSafePoint)
	}

	_, err := s.manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 30, time.Now())
	re.NoError(err)
	for i, keyspaceID := range s.keyspacePresets.nullSynonyms {
		// The GC safe point in Already updated to 10 in previous check. So in i-th loop here, we update from 10+i to
		// 10+i+1.
		oldValue, newValue, err := s.manager.AdvanceGCSafePoint(keyspaceID, uint64(10+i+1))
		re.NoError(err)
		re.Equal(uint64(10+i), oldValue)
		re.Equal(uint64(10+i+1), newValue)
		for _, checkingKeyspaceID := range slices.Concat(s.keyspacePresets.unifiedGC, s.keyspacePresets.nullSynonyms) {
			checkGCSafePoint(checkingKeyspaceID, uint64(10+i+1))
		}
	}
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

func (s *gcManagerTestSuite) TestGCBarriers() {
	re := s.Require()

	getGCBarrier := func(keyspaceID uint32, barrierID string) *endpoint.GCBarrier {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		idx := slices.IndexFunc(state.GCBarriers, func(b *endpoint.GCBarrier) bool {
			return b.BarrierID == barrierID
		})
		if idx == -1 {
			return nil
		}
		return state.GCBarriers[idx]
	}

	getAllGCBarriers := func(keyspaceID uint32) []*endpoint.GCBarrier {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		return state.GCBarriers
	}

	checkTxnSafePoint := func(keyspaceID uint32, expectedTxnSafePoint uint64) {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		re.Equal(expectedTxnSafePoint, state.TxnSafePoint)
	}

	// Helper for getting pointer of time.
	ptime := func(t time.Time) *time.Time { return &t }

	now := time.Date(2025, 03, 06, 11, 50, 30, 0, time.Local)

	for _, keyspaceID := range s.keyspacePresets.all {
		re.Empty(getAllGCBarriers(keyspaceID))
	}

	// Test basic functionality within a single keyspace.
	for _, keyspaceID := range s.keyspacePresets.manageable {
		b, err := s.manager.SetGCBarrier(keyspaceID, "b1", 10, time.Hour, now)
		re.NoError(err)
		expected := endpoint.NewGCBarrier("b1", 10, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))

		// Empty barrierID is forbidden.
		_, err = s.manager.SetGCBarrier(keyspaceID, "", 10, time.Hour, now)
		re.Error(err)

		// Updating the value of the existing GC barrier
		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 15, time.Hour, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 15, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))

		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 15, time.Hour*2, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 15, ptime(now.Add(time.Hour*2)))
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))

		// Allows shrinking the barrier ts.
		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 10, time.Hour, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 10, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))

		// Never expiring
		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 10, time.Duration(math.MaxInt64), now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 10, nil)
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))

		// GC barriers blocks the txn safe point.
		res, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 5, now)
		re.NoError(err)
		re.Equal(uint64(0), res.OldTxnSafePoint)
		re.Equal(uint64(5), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 10, now)
		re.NoError(err)
		re.Equal(uint64(5), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 15, now)
		re.NoError(err)
		re.Equal(uint64(10), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Equal(uint64(15), res.Target)
		re.Contains(res.BlockerDescription, "BarrierID: \"b1\"")
		checkTxnSafePoint(keyspaceID, 10)

		_, err = s.manager.SetGCBarrier(keyspaceID, "b1", 15, time.Hour, now)
		re.NoError(err)
		// AdvanceTxnSafePoint advances the txn safe point as much as possible.
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 20, now)
		re.NoError(err)
		re.Equal(uint64(10), res.OldTxnSafePoint)
		re.Equal(uint64(15), res.NewTxnSafePoint)
		re.Equal(uint64(20), res.Target)
		re.Contains(res.BlockerDescription, "BarrierID: \"b1\"")

		// Multiple GC barriers
		_, err = s.manager.SetGCBarrier(keyspaceID, "b1", 20, time.Hour, now)
		re.NoError(err)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b2", 20, time.Hour, now)
		re.NoError(err)
		re.Len(getAllGCBarriers(keyspaceID), 2)
		expected = endpoint.NewGCBarrier("b1", 20, ptime(now.Add(time.Hour)))
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))
		expected = endpoint.NewGCBarrier("b2", 20, ptime(now.Add(time.Hour)))
		re.Equal(expected, getGCBarrier(keyspaceID, "b2"))

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 25, now)
		re.NoError(err)
		re.Equal(uint64(15), res.OldTxnSafePoint)
		re.Equal(uint64(20), res.NewTxnSafePoint)
		re.Equal(uint64(25), res.Target)
		re.NotEmpty(res.BlockerDescription)

		// When there are different GC barriers, block with the minimum one.
		_, err = s.manager.SetGCBarrier(keyspaceID, "b1", 25, time.Hour, now)
		re.NoError(err)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b2", 27, time.Hour, now)
		re.NoError(err)
		re.Len(getAllGCBarriers(keyspaceID), 2)
		expected = endpoint.NewGCBarrier("b1", 25, ptime(now.Add(time.Hour)))
		re.Equal(expected, getGCBarrier(keyspaceID, "b1"))
		expected = endpoint.NewGCBarrier("b2", 27, ptime(now.Add(time.Hour)))
		re.Equal(expected, getGCBarrier(keyspaceID, "b2"))

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, now)
		re.NoError(err)
		re.Equal(uint64(20), res.OldTxnSafePoint)
		re.Equal(uint64(25), res.NewTxnSafePoint)
		re.Equal(uint64(30), res.Target)
		re.Contains(res.BlockerDescription, "BarrierID: \"b1\"")

		// Deleting GC barriers
		b, err = s.manager.DeleteGCBarrier(keyspaceID, "b1")
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 25, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, now)
		re.NoError(err)
		re.Equal(uint64(25), res.OldTxnSafePoint)
		re.Equal(uint64(27), res.NewTxnSafePoint)
		re.Equal(uint64(30), res.Target)
		re.Contains(res.BlockerDescription, "BarrierID: \"b2\"")

		b, err = s.manager.DeleteGCBarrier(keyspaceID, "b2")
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b2", 27, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Empty(getAllGCBarriers(keyspaceID))

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, now)
		re.NoError(err)
		re.Equal(uint64(27), res.OldTxnSafePoint)
		re.Equal(uint64(30), res.NewTxnSafePoint)
		re.Equal(uint64(30), res.Target)
		re.Empty(res.BlockerDescription)

		// Deleting non-existing GC barrier.
		b, err = s.manager.DeleteGCBarrier(keyspaceID, "b1")
		re.NoError(err)
		re.Nil(b)

		// Test TTL
		_, err = s.manager.SetGCBarrier(keyspaceID, "b3", 40, time.Minute, now)
		re.NoError(err)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b4", 45, time.Minute*2, now)
		re.NoError(err)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b5", 50, time.Duration(math.MaxInt64), now)
		re.NoError(err)

		// Not expiring
		for _, t := range []time.Time{now, now.Add(time.Second * 59), now.Add(time.Minute)} {
			res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, t)
			re.NoError(err)
			re.Equal(uint64(40), res.NewTxnSafePoint)
			re.Contains(res.BlockerDescription, "BarrierID: \"b3\"")
			checkTxnSafePoint(keyspaceID, 40)
			re.Len(getAllGCBarriers(keyspaceID), 3)
		}

		// b3 expires
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, now.Add(time.Minute*2))
		re.NoError(err)
		re.Equal(uint64(45), res.NewTxnSafePoint)
		re.Contains(res.BlockerDescription, "BarrierID: \"b4\"")
		checkTxnSafePoint(keyspaceID, 45)
		re.Len(getAllGCBarriers(keyspaceID), 2)

		// b4 expires, but b5 never expires.
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, now.Add(time.Hour*24*365*100))
		re.NoError(err)
		re.Equal(uint64(50), res.NewTxnSafePoint)
		re.Contains(res.BlockerDescription, "BarrierID: \"b5\"")
		checkTxnSafePoint(keyspaceID, 50)
		re.Len(getAllGCBarriers(keyspaceID), 1)

		// Manually delete b5
		b, err = s.manager.DeleteGCBarrier(keyspaceID, "b5")
		re.NoError(err)
		re.Equal("b5", b.BarrierID)

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, now.Add(time.Hour*24*365*100))
		re.NoError(err)
		re.Equal(uint64(60), res.NewTxnSafePoint)
		checkTxnSafePoint(keyspaceID, 60)

		re.Empty(getAllGCBarriers(keyspaceID))

		// Disallows setting GC barrier before txn safe point.
		_, err = s.manager.SetGCBarrier(keyspaceID, "b6", 50, time.Hour, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCBarrierTSBehindTxnSafePoint)
		re.Empty(getAllGCBarriers(keyspaceID))
		// BarrierTS exactly equals to txn safe point is allowed.
		b, err = s.manager.SetGCBarrier(keyspaceID, "b6", 60, time.Hour, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b6", 60, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, getGCBarrier(keyspaceID, "b6"))

		// Clear.
		_, err = s.manager.DeleteGCBarrier(keyspaceID, "b6")
		re.NoError(err)
	}

	// As a user API, it's allowed to be called in keyspaces without enabling keyspace-level GC, and it actually takes
	// effect to the NullKeyspace.
	for _, keyspaceID := range slices.Concat(s.keyspacePresets.unifiedGC, s.keyspacePresets.nullSynonyms) {
		b, err := s.manager.SetGCBarrier(keyspaceID, "b1", 100, time.Hour, now)
		re.NoError(err)
		expected := endpoint.NewGCBarrier("b1", 100, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		for _, checkingKeyspaceID := range slices.Concat(s.keyspacePresets.unifiedGC, s.keyspacePresets.nullSynonyms) {
			re.Len(getAllGCBarriers(checkingKeyspaceID), 1)
			re.Equal(expected, getGCBarrier(checkingKeyspaceID, "b1"))
		}

		_, err = s.manager.DeleteGCBarrier(keyspaceID, "b1")
		re.NoError(err)
		re.Len(getAllGCBarriers(keyspaceID), 0)
	}

	// Fail when trying to set not-existing keyspace.
	for _, keyspaceID := range s.keyspacePresets.notExisting {
		_, err := s.manager.SetGCBarrier(keyspaceID, "b1", 100, time.Hour, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrKeyspaceNotFound)
	}

	// Isolated between different keyspaces.
	ks1 := s.keyspacePresets.manageable[0]
	ks2 := s.keyspacePresets.manageable[1]
	_, err := s.manager.SetGCBarrier(ks1, "b1", 200, time.Hour, now)
	re.NoError(err)
	expected := endpoint.NewGCBarrier("b1", 200, ptime(now.Add(time.Hour)))
	re.Equal(expected, getGCBarrier(ks1, "b1"))
	re.Nil(getGCBarrier(ks2, "b1"))
	res, err := s.manager.AdvanceTxnSafePoint(ks2, 300, now)
	re.NoError(err)
	re.Equal(uint64(300), res.NewTxnSafePoint)
	re.Empty(res.BlockerDescription)
}

func TestGCStateConstraints(t *testing.T) {

}

func TestServiceGCSafePointCompatibility(t *testing.T) {
	//_, manager, clean := newGCStateManager(t)
	//defer clean()
	//
	//re := require.New(t)

}

func (s *gcManagerTestSuite) TestRedirectKeyspace() {
	re := s.Require()

	for _, keyspaceID := range s.keyspacePresets.manageable {
		for _, isUserAPI := range []bool{true, false} {
			redirected, err := s.manager.redirectKeyspace(keyspaceID, isUserAPI)
			re.NoError(err, "keyspaceID: %d, isUserAPI: %v", keyspaceID, isUserAPI)
			re.Equal(keyspaceID, redirected, "keyspaceID: %d, isUserAPI: %v", keyspaceID, isUserAPI)
		}
	}

	for _, keyspaceID := range s.keyspacePresets.unmanageable {
		redirected, err := s.manager.redirectKeyspace(keyspaceID, true)
		re.NoError(err, "keyspaceID: %d", keyspaceID)
		re.Equal(constant.NullKeyspaceID, redirected, "keyspaceID: %d", keyspaceID)

		_, err = s.manager.redirectKeyspace(keyspaceID, false)
		re.Error(err, "keyspaceID: %d", keyspaceID)
		re.ErrorIs(err, errs.ErrGCOnInvalidKeyspace, "keyspaceID: %d", keyspaceID)
	}

	for _, keyspaceID := range s.keyspacePresets.notExisting {
		for _, isUserAPI := range []bool{true, false} {
			_, err := s.manager.redirectKeyspace(keyspaceID, isUserAPI)
			re.Error(err, "keyspaceID: %d, isUserAPI: %v", keyspaceID, isUserAPI)
			re.ErrorIs(err, errs.ErrKeyspaceNotFound, "keyspaceID: %d, isUserAPI: %v", keyspaceID, isUserAPI)
		}
	}

	for _, keyspaceID := range s.keyspacePresets.nullSynonyms {
		for _, isUserAPI := range []bool{true, false} {
			redirected, err := s.manager.redirectKeyspace(keyspaceID, isUserAPI)
			re.NoError(err)
			re.Equal(constant.NullKeyspaceID, redirected)
		}
	}

	// Check all public methods that accepts keyspaceID are all correctly redirected.
	testedFunc := []func(keyspaceID uint32) error{
		func(keyspaceID uint32) error {
			_, err1 := s.manager.GetGCState(keyspaceID)
			return errors.AddStack(err1)
		},
		func(keyspaceID uint32) error {
			_, err1 := s.manager.AdvanceTxnSafePoint(keyspaceID, 10, time.Now())
			return errors.AddStack(err1)
		},
		func(keyspaceID uint32) error {
			_, _, err1 := s.manager.AdvanceGCSafePoint(keyspaceID, 10)
			return errors.AddStack(err1)
		},
		func(keyspaceID uint32) error {
			_, err1 := s.manager.SetGCBarrier(keyspaceID, "b", 15, time.Hour, time.Now())
			return errors.AddStack(err1)
		},
		func(keyspaceID uint32) error {
			_, err1 := s.manager.DeleteGCBarrier(keyspaceID, "b")
			return errors.AddStack(err1)
		},
	}
	isUserAPI := []bool{true, false, false, true, true}

	for funcIndex, f := range testedFunc {
		for _, keyspaceID := range s.keyspacePresets.manageable {
			err := f(keyspaceID)
			re.NoError(err)
		}

		for _, keyspaceID := range s.keyspacePresets.unmanageable {
			err := f(keyspaceID)
			if isUserAPI[funcIndex] {
				re.NoError(err)
			} else {
				re.Error(err)
				re.ErrorIs(err, errs.ErrGCOnInvalidKeyspace)
			}
		}

		for _, keyspaceID := range s.keyspacePresets.notExisting {
			err := f(keyspaceID)
			re.Error(err)
			re.ErrorIs(err, errs.ErrKeyspaceNotFound)
		}

		for _, keyspaceID := range s.keyspacePresets.nullSynonyms {
			err := f(keyspaceID)
			re.NoError(err)
		}
	}
}
