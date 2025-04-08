// Copyright 2025 TiKV Project Authors.
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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/errors"

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

type gcStateManagerTestSuite struct {
	suite.Suite

	storage  *endpoint.StorageEndpoint
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

func TestGCStateManager(t *testing.T) {
	suite.Run(t, new(gcStateManagerTestSuite))
}

func newGCStateManagerForTest(t *testing.T) (storage *endpoint.StorageEndpoint, provider endpoint.GCStateProvider, gcStateManager *GCStateManager, clean func()) {
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
		Config:     map[string]string{"gc_management_type": "global"},
		CreateTime: time.Now().Unix(),
		IsPreAlloc: false,
	})
	re.NoError(err)
	re.Equal(uint32(1), ks1.Id)

	ks2, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "ks2",
		Config:     map[string]string{"gc_management_type": "keyspace_level"},
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

	return s, s.GetGCStateProvider(), gcStateManager, clean
}

func (s *gcStateManagerTestSuite) SetupTest() {
	s.storage, s.provider, s.manager, s.clean = newGCStateManagerForTest(s.T())

	s.keyspacePresets.all = []uint32{constant.NullKeyspaceID, 0, 1, 2, 3}
	s.keyspacePresets.manageable = []uint32{constant.NullKeyspaceID, 2}
	s.keyspacePresets.unmanageable = []uint32{0, 1, 3}
	s.keyspacePresets.unifiedGC = []uint32{constant.NullKeyspaceID, 0, 1, 3}
	s.keyspacePresets.notExisting = []uint32{4, 0xffffff}
	s.keyspacePresets.nullSynonyms = []uint32{constant.NullKeyspaceID, 0x1000000, 0xfffffffe}
}

func (s *gcStateManagerTestSuite) TearDownTest() {
	s.clean()
}

func (s *gcStateManagerTestSuite) checkTxnSafePoint(keyspaceID uint32, expectedTxnSafePoint uint64) {
	re := s.Require()
	state, err := s.manager.GetGCState(keyspaceID)
	re.NoError(err)
	re.Equal(expectedTxnSafePoint, state.TxnSafePoint)
}

func (s *gcStateManagerTestSuite) TestAdvanceTxnSafePointBasic() {
	re := s.Require()
	now := time.Now()

	for _, keyspaceID := range s.keyspacePresets.all {
		s.checkTxnSafePoint(keyspaceID, 0)
	}

	for _, keyspaceID := range s.keyspacePresets.manageable {
		res, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 10, now)
		re.NoError(err)
		re.Equal(uint64(0), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Equal(uint64(10), res.Target)
		re.Empty(res.BlockerDescription)

		s.checkTxnSafePoint(keyspaceID, 10)

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
		s.checkTxnSafePoint(keyspaceID, 10)
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
			s.checkTxnSafePoint(checkingKeyspaceID, uint64(10+i+1))
		}
		for _, checkingKeyspaceID := range s.keyspacePresets.nullSynonyms {
			s.checkTxnSafePoint(checkingKeyspaceID, uint64(10+i+1))
		}
	}
}

func (s *gcStateManagerTestSuite) TestAdvanceGCSafePointBasic() {
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

		_, _, err = s.manager.AdvanceGCSafePoint(keyspaceID, 11)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCSafePointExceedsTxnSafePoint)

		// Allows updating with the same value (no effect).
		oldValue, newValue, err = s.manager.AdvanceGCSafePoint(keyspaceID, 10)
		re.NoError(err)
		re.Equal(uint64(10), oldValue)
		re.Equal(uint64(10), newValue)

		// Does not allow decreasing.
		_, _, err = s.manager.AdvanceGCSafePoint(keyspaceID, 9)
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

func (s *gcStateManagerTestSuite) testGCSafePointUpdateSequentiallyImpl(loadFunc func() (uint64, error)) {
	re := s.Require()
	curGCSafePoint := uint64(0)
	// Update GC safe point with asc value.
	for id := 10; id < 20; id++ {
		safePoint, err := loadFunc()
		re.NoError(err)
		re.Equal(curGCSafePoint, safePoint)
		previousGCSafePoint := curGCSafePoint
		curGCSafePoint = uint64(id)
		// Blocked by txn safe point.
		_, _, err = s.manager.CompatibleUpdateGCSafePoint(curGCSafePoint)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCSafePointExceedsTxnSafePoint)
		_, err = s.manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, curGCSafePoint, time.Now())
		re.NoError(err)
		oldGCSafePoint, newGCSafePoint, err := s.manager.CompatibleUpdateGCSafePoint(curGCSafePoint)
		re.NoError(err)
		re.Equal(previousGCSafePoint, oldGCSafePoint)
		re.Equal(curGCSafePoint, newGCSafePoint)
	}

	gcSafePoint, err := s.manager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	re.Equal(curGCSafePoint, gcSafePoint)
	// Update with smaller value should be failed.
	oldGCSafePoint, newGCSafePoint, err := s.manager.CompatibleUpdateGCSafePoint(gcSafePoint - 5)
	re.NoError(err)
	re.Equal(gcSafePoint, oldGCSafePoint)
	re.Equal(gcSafePoint, newGCSafePoint)
	curGCSafePoint, err = s.manager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	// Current GC safe point should not change since the update value was smaller
	re.Equal(gcSafePoint, curGCSafePoint)
}

func (s *gcStateManagerTestSuite) TestCompatibleUpdateGCSafePointSequentiallyWithLegacyLoad() {
	s.testGCSafePointUpdateSequentiallyImpl(func() (uint64, error) {
		return s.manager.CompatibleLoadGCSafePoint()
	})
}

func (s *gcStateManagerTestSuite) TestCompatibleUpdateGCSafePointSequentiallyWithNewLoad() {
	s.testGCSafePointUpdateSequentiallyImpl(func() (uint64, error) {
		state, err := s.manager.GetGCState(constant.NullKeyspaceID)
		if err != nil {
			return 0, err
		}
		return state.GCSafePoint, nil
	})
}

func (s *gcStateManagerTestSuite) TestGCSafePointUpdateConcurrently() {
	maxGCSafePoint := uint64(1000)
	wg := sync.WaitGroup{}
	re := s.Require()

	// Advance txn safe point first, otherwise the GC safe point can't be advanced.
	_, err := s.manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, maxGCSafePoint, time.Now())
	re.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 20)

	// Update GC safe point concurrently
	for id := range 20 {
		wg.Add(1)
		go func(step uint64) {
		loop:
			for gcSafePoint := step; gcSafePoint <= maxGCSafePoint; gcSafePoint += step {
				select {
				case <-ctx.Done():
					break loop
				default:
				}

				// Mix using new and legacy API
				var err error
				if (gcSafePoint/step)%2 == 0 {
					_, _, err = s.manager.AdvanceGCSafePoint(constant.NullKeyspaceID, gcSafePoint)
					if err != nil && errors.ErrorEqual(err, errs.ErrDecreasingGCSafePoint) {
						err = nil
					}
				} else {
					_, _, err = s.manager.CompatibleUpdateGCSafePoint(gcSafePoint)
				}
				if err != nil {
					errCh <- err
					cancel()
					break
				}
			}
			wg.Done()
		}(uint64(id + 1))
	}
	wg.Wait()
	select {
	case err := <-errCh:
		re.NoError(err)
	default:
	}
	gcSafePoint, err := s.manager.CompatibleLoadGCSafePoint()
	re.NoError(err)
	re.Equal(maxGCSafePoint, gcSafePoint)
}

func (s *gcStateManagerTestSuite) TestLegacyServiceGCSafePointUpdate() {
	re := s.Require()
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
		min, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint(cdcServiceID, cdcServiceSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// The service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// Updating the service safe point for br to 15 should success
	go func() {
		defer wg.Done()
		min, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, 10000, time.Now())
		re.NoError(err)
		re.True(updated)
		// the service will init the service safepoint to 0(<10 for cdc) for gc_worker.
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	// Updating the service safe point to 8 for gc_worker should be success
	go func() {
		defer wg.Done()
		// update with valid ttl for gc_worker should be success.
		min, updated, _ := s.manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
		re.True(updated)
		// the current min safepoint should be 8 for gc_worker(cdc 10)
		re.Equal(gcWorkerSafePoint, min.SafePoint)
		re.Equal(gcWorkerServiceID, min.ServiceID)
	}()

	go func() {
		defer wg.Done()
		// Updating the service safe point of gc_worker's service with ttl not infinity should be failed.
		_, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, 10000, 10, time.Now())
		re.Error(err)
		re.False(updated)
	}()

	// Updating the service safe point with negative ttl should be failed.
	go func() {
		defer wg.Done()
		brTTL := int64(-100)
		_, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint(brServiceID, uint64(10000), brTTL, time.Now())
		re.NoError(err)
		re.False(updated)
	}()

	wg.Wait()
	// Updating the service safe point to 15(>10 for cdc) for gc_worker
	gcWorkerSafePoint = uint64(15)
	min, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint(gcWorkerServiceID, gcWorkerSafePoint, math.MaxInt64, time.Now())
	re.NoError(err)
	re.True(updated)
	re.Contains(min.ServiceID, "BarrierID: \""+cdcServiceID+"\"")
	re.Equal(cdcServiceSafePoint, min.SafePoint)

	// The value shouldn't be updated with current service safe point smaller than the min safe point.
	brTTL := int64(100)
	brSafePoint = min.SafePoint - 5
	min, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.False(updated)

	brSafePoint = min.SafePoint + 10
	_, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint(brServiceID, brSafePoint, brTTL, time.Now())
	re.NoError(err)
	re.True(updated)
}

func (s *gcStateManagerTestSuite) TestLegacyServiceGCSafePointRoundingTTL() {
	re := s.Require()

	var maxTTL int64 = 9223372036

	_, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 10, maxTTL, time.Now())
	re.NoError(err)
	re.True(updated)

	state, err := s.manager.GetGCState(constant.NullKeyspaceID)
	re.NoError(err)
	re.Len(state.GCBarriers, 1)
	re.Equal("svc1", state.GCBarriers[0].BarrierID)
	re.NotNil(state.GCBarriers[0].ExpirationTime)
	// The given `maxTTL` is valid but super large.
	re.True(state.GCBarriers[0].ExpirationTime.After(time.Now().Add(time.Hour*24*365*10)), state.GCBarriers[0].ExpirationTime)

	_, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 10, maxTTL+1, time.Now())
	re.NoError(err)
	re.True(updated)

	state, err = s.manager.GetGCState(constant.NullKeyspaceID)
	re.NoError(err)
	re.Len(state.GCBarriers, 1)
	re.Equal("svc1", state.GCBarriers[0].BarrierID)
	re.Nil(state.GCBarriers[0].ExpirationTime)
	// Nil in GCBarrier.ExpirationTime represents never expires.
	re.False(state.GCBarriers[0].IsExpired(time.Now().Add(time.Hour*24*365*10)), state.GCBarriers[0])
}

func (s *gcStateManagerTestSuite) getGCBarrier(keyspaceID uint32, barrierID string) *endpoint.GCBarrier {
	re := s.Require()
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

func (s *gcStateManagerTestSuite) getAllGCBarriers(keyspaceID uint32) []*endpoint.GCBarrier {
	re := s.Require()
	state, err := s.manager.GetGCState(keyspaceID)
	re.NoError(err)
	return state.GCBarriers
}

// ptime is a helper for getting pointer of time.
func ptime(t time.Time) *time.Time {
	return &t
}

func (s *gcStateManagerTestSuite) TestGCBarriers() {
	re := s.Require()

	now := time.Date(2025, 03, 06, 11, 50, 30, 0, time.Local)

	for _, keyspaceID := range s.keyspacePresets.all {
		re.Empty(s.getAllGCBarriers(keyspaceID))
	}

	// Test basic functionality within a single keyspace.
	for _, keyspaceID := range s.keyspacePresets.manageable {
		b, err := s.manager.SetGCBarrier(keyspaceID, "b1", 10, time.Hour, now)
		re.NoError(err)
		expected := endpoint.NewGCBarrier("b1", 10, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))

		// Empty barrierID is forbidden.
		_, err = s.manager.SetGCBarrier(keyspaceID, "", 10, time.Hour, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrInvalidArgument)
		_, err = s.manager.DeleteGCBarrier(keyspaceID, "")
		re.Error(err)
		re.ErrorIs(err, errs.ErrInvalidArgument)

		// Non-positive TTL is forbidden.
		_, err = s.manager.SetGCBarrier(keyspaceID, "b1", 10, 0, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrInvalidArgument)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b2", 10, 0, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrInvalidArgument)
		// b1 is not changed.
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))
		// b2 still doesn't exist.
		re.Nil(s.getGCBarrier(keyspaceID, "b2"))

		// Updating the value of the existing GC barrier
		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 15, time.Hour, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 15, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))

		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 15, time.Hour*2, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 15, ptime(now.Add(time.Hour*2)))
		re.Equal(expected, b)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))

		// Allows shrinking the barrier ts.
		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 10, time.Hour, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 10, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))

		// Never expiring
		b, err = s.manager.SetGCBarrier(keyspaceID, "b1", 10, time.Duration(math.MaxInt64), now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b1", 10, nil)
		re.Equal(expected, b)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))

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
		s.checkTxnSafePoint(keyspaceID, 10)

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
		re.Len(s.getAllGCBarriers(keyspaceID), 2)
		expected = endpoint.NewGCBarrier("b1", 20, ptime(now.Add(time.Hour)))
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))
		expected = endpoint.NewGCBarrier("b2", 20, ptime(now.Add(time.Hour)))
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b2"))

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
		re.Len(s.getAllGCBarriers(keyspaceID), 2)
		expected = endpoint.NewGCBarrier("b1", 25, ptime(now.Add(time.Hour)))
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b1"))
		expected = endpoint.NewGCBarrier("b2", 27, ptime(now.Add(time.Hour)))
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b2"))

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
		re.Len(s.getAllGCBarriers(keyspaceID), 1)

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
		re.Empty(s.getAllGCBarriers(keyspaceID))

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
			s.checkTxnSafePoint(keyspaceID, 40)
			re.Len(s.getAllGCBarriers(keyspaceID), 3)
		}

		// b3 expires
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, now.Add(time.Minute*2))
		re.NoError(err)
		re.Equal(uint64(45), res.NewTxnSafePoint)
		re.Contains(res.BlockerDescription, "BarrierID: \"b4\"")
		s.checkTxnSafePoint(keyspaceID, 45)
		re.Len(s.getAllGCBarriers(keyspaceID), 2)

		// b4 expires, but b5 never expires.
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, now.Add(time.Hour*24*365*100))
		re.NoError(err)
		re.Equal(uint64(50), res.NewTxnSafePoint)
		re.Contains(res.BlockerDescription, "BarrierID: \"b5\"")
		s.checkTxnSafePoint(keyspaceID, 50)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)

		// Manually delete b5
		b, err = s.manager.DeleteGCBarrier(keyspaceID, "b5")
		re.NoError(err)
		re.Equal("b5", b.BarrierID)

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 60, now.Add(time.Hour*24*365*100))
		re.NoError(err)
		re.Equal(uint64(60), res.NewTxnSafePoint)
		s.checkTxnSafePoint(keyspaceID, 60)

		re.Empty(s.getAllGCBarriers(keyspaceID))

		// Disallows setting GC barrier before txn safe point.
		_, err = s.manager.SetGCBarrier(keyspaceID, "b6", 50, time.Hour, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrGCBarrierTSBehindTxnSafePoint)
		re.Empty(s.getAllGCBarriers(keyspaceID))
		// BarrierTS exactly equals to txn safe point is allowed.
		b, err = s.manager.SetGCBarrier(keyspaceID, "b6", 60, time.Hour, now)
		re.NoError(err)
		expected = endpoint.NewGCBarrier("b6", 60, ptime(now.Add(time.Hour)))
		re.Equal(expected, b)
		re.Len(s.getAllGCBarriers(keyspaceID), 1)
		re.Equal(expected, s.getGCBarrier(keyspaceID, "b6"))

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
			re.Len(s.getAllGCBarriers(checkingKeyspaceID), 1)
			re.Equal(expected, s.getGCBarrier(checkingKeyspaceID, "b1"))
		}

		_, err = s.manager.DeleteGCBarrier(keyspaceID, "b1")
		re.NoError(err)
		re.Empty(s.getAllGCBarriers(keyspaceID))
	}

	// Fail when trying to set not-existing keyspace.
	for _, keyspaceID := range s.keyspacePresets.notExisting {
		_, err := s.manager.SetGCBarrier(keyspaceID, "b1", 100, time.Hour, now)
		re.Error(err)
		re.ErrorIs(err, errs.ErrKeyspaceNotFound)
	}

	// Rejects reserved barrier ID: rejects "gc_worker" in NullKeyspace.
	_, err := s.manager.SetGCBarrier(constant.NullKeyspaceID, "gc_worker", 100, time.Hour, now)
	re.Error(err)
	re.ErrorIs(err, errs.ErrReservedGCBarrierID)
	re.Nil(s.getGCBarrier(constant.NullKeyspaceID, "gc_worker"))
	_, err = s.manager.DeleteGCBarrier(constant.NullKeyspaceID, "gc_worker")
	re.Error(err)
	re.ErrorIs(err, errs.ErrReservedGCBarrierID)

	// Isolated between different keyspaces.
	ks1 := s.keyspacePresets.manageable[0]
	ks2 := s.keyspacePresets.manageable[1]
	_, err = s.manager.SetGCBarrier(ks1, "b1", 200, time.Hour, now)
	re.NoError(err)
	expected := endpoint.NewGCBarrier("b1", 200, ptime(now.Add(time.Hour)))
	re.Equal(expected, s.getGCBarrier(ks1, "b1"))
	re.Nil(s.getGCBarrier(ks2, "b1"))
	res, err := s.manager.AdvanceTxnSafePoint(ks2, 300, now)
	re.NoError(err)
	re.Equal(uint64(300), res.NewTxnSafePoint)
	re.Empty(res.BlockerDescription)
}

func (s *gcStateManagerTestSuite) TestTiDBMinStartTS() {
	re := s.Require()

	setTiDBMinStartTS := func(keyspaceID uint32, instance string, ts uint64) {
		keyspaceID, err := s.manager.redirectKeyspace(keyspaceID, true)
		re.NoError(err)
		prefix := keypath.CompatibleTiDBMinStartTSPrefix(keyspaceID)
		key := prefix + instance
		err = s.storage.Save(key, strconv.FormatUint(ts, 10))
		re.NoError(err)
	}

	deleteTiDBMinStartTS := func(keyspaceID uint32, instance string) {
		keyspaceID, err := s.manager.redirectKeyspace(keyspaceID, true)
		re.NoError(err)
		prefix := keypath.CompatibleTiDBMinStartTSPrefix(keyspaceID)
		key := prefix + instance
		err = s.storage.Remove(key)
		re.NoError(err)
	}

	for _, keyspaceID := range s.keyspacePresets.manageable {
		setTiDBMinStartTS(keyspaceID, "instance1", 10)
		res, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 5, time.Now())
		re.NoError(err)
		re.Equal(uint64(0), res.OldTxnSafePoint)
		re.Equal(uint64(5), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 10, time.Now())
		re.NoError(err)
		re.Equal(uint64(5), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 15, time.Now())
		re.NoError(err)
		re.Equal(uint64(10), res.OldTxnSafePoint)
		re.Equal(uint64(10), res.NewTxnSafePoint)
		re.Equal(uint64(15), res.Target)
		re.Regexp("TiDBMinStartTS.*instance1", res.BlockerDescription)

		s.checkTxnSafePoint(keyspaceID, 10)

		// Mixing multiple TiDB min start ts and GC barriers.
		setTiDBMinStartTS(keyspaceID, "instance1", 20)
		setTiDBMinStartTS(keyspaceID, "instance2", 22)
		setTiDBMinStartTS(keyspaceID, "instance3", 26)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b1", 24, time.Hour, time.Now())
		re.NoError(err)
		_, err = s.manager.SetGCBarrier(keyspaceID, "b2", 28, time.Hour, time.Now())
		re.NoError(err)

		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(10), res.OldTxnSafePoint)
		re.Equal(uint64(20), res.NewTxnSafePoint)
		re.Regexp("TiDBMinStartTS.*instance1", res.BlockerDescription)

		deleteTiDBMinStartTS(keyspaceID, "instance1")
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(20), res.OldTxnSafePoint)
		re.Equal(uint64(22), res.NewTxnSafePoint)
		re.Regexp("TiDBMinStartTS.*instance2", res.BlockerDescription)

		deleteTiDBMinStartTS(keyspaceID, "instance2")
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(22), res.OldTxnSafePoint)
		re.Equal(uint64(24), res.NewTxnSafePoint)
		re.Contains(res.BlockerDescription, `BarrierID: "b1"`)

		_, err = s.manager.DeleteGCBarrier(keyspaceID, "b1")
		re.NoError(err)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(24), res.OldTxnSafePoint)
		re.Equal(uint64(26), res.NewTxnSafePoint)
		re.Regexp("TiDBMinStartTS.*instance3", res.BlockerDescription)

		deleteTiDBMinStartTS(keyspaceID, "instance3")
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(26), res.OldTxnSafePoint)
		re.Equal(uint64(28), res.NewTxnSafePoint)
		re.Contains(res.BlockerDescription, `BarrierID: "b2"`)

		_, err = s.manager.DeleteGCBarrier(keyspaceID, "b2")
		re.NoError(err)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(28), res.OldTxnSafePoint)
		re.Equal(uint64(30), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)

		// If there's a TiDB node in old version that writes the TiDBMinStartTS, it's possible that TiDBMinStartTS become
		// lower than txn safe point (as it writes directly to etcd instead of checking constraints in a transaction).
		// In this case, the txn safe point should neither be pushed nor go backward.
		setTiDBMinStartTS(keyspaceID, "instance1", 25)
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 35, time.Now())
		re.NoError(err)
		re.Equal(uint64(30), res.OldTxnSafePoint)
		re.Equal(uint64(30), res.NewTxnSafePoint)
		re.Equal(uint64(35), res.Target)
		re.Regexp("TiDBMinStartTS.*instance1", res.BlockerDescription)

		deleteTiDBMinStartTS(keyspaceID, "instance1")
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 35, time.Now())
		re.NoError(err)
		re.Equal(uint64(30), res.OldTxnSafePoint)
		re.Equal(uint64(35), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)
	}
}

func (s *gcStateManagerTestSuite) TestServiceGCSafePointCompatibility() {
	re := s.Require()

	var nowUnix int64 = 1741584577
	now := time.Unix(nowUnix, 0)

	// Service safe points & GC barriers shares the same data storage and are mutually convertable.
	minSsp, updated, err := s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 10, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(0), minSsp.SafePoint)
	re.Equal("gc_worker", minSsp.ServiceID)

	res, err := s.manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 15, now)
	re.NoError(err)
	re.Equal(uint64(10), res.NewTxnSafePoint)

	expected := endpoint.NewGCBarrier("svc1", 10, nil)
	re.Equal(expected, s.getGCBarrier(constant.NullKeyspaceID, "svc1"))

	// SetGCBarrier can also affect service safe points.
	_, err = s.manager.SetGCBarrier(constant.NullKeyspaceID, "svc1", 15, time.Hour, now)
	re.NoError(err)
	_, allSsp, err := s.provider.CompatibleLoadAllServiceGCSafePoints()
	re.NoError(err)
	re.Len(allSsp, 1)
	re.Equal("svc1", allSsp[0].ServiceID)
	re.Equal(uint64(15), allSsp[0].SafePoint)
	re.Equal(nowUnix+3600, allSsp[0].ExpiredAt)

	// Disallow decreasing behind the txn safe point. But it doesn't return error.
	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 8, math.MaxInt64, now)
	re.NoError(err)
	re.False(updated)
	re.Equal(uint64(10), minSsp.SafePoint)
	expected = endpoint.NewGCBarrier("svc1", 15, ptime(now.Add(time.Hour)))
	re.Equal(expected, s.getGCBarrier(constant.NullKeyspaceID, "svc1"))

	// Disallow inserting new service safe point before the txn safe point.
	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc2", 8, math.MaxInt64, now)
	re.NoError(err)
	re.False(updated)
	re.Equal(uint64(10), minSsp.SafePoint)
	re.Nil(s.getGCBarrier(constant.NullKeyspaceID, "svc2"))

	// But decreasing a service safe point to a value larger than the current txn safe point is allowed. Note that this
	// behavior is not completely the same as old versions.
	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 12, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(10), minSsp.SafePoint)
	expected = endpoint.NewGCBarrier("svc1", 12, nil)
	re.Equal(expected, s.getGCBarrier(constant.NullKeyspaceID, "svc1"))

	// Allows setting different TTL.
	_, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 12, 3600, now)
	re.NoError(err)
	re.True(updated)
	_, allSsp, err = s.provider.CompatibleLoadAllServiceGCSafePoints()
	re.NoError(err)
	re.Len(allSsp, 1)
	re.Equal("svc1", allSsp[0].ServiceID)
	re.Equal(uint64(12), allSsp[0].SafePoint)
	re.Equal(nowUnix+3600, allSsp[0].ExpiredAt)
	_, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 12, 3600, now.Add(time.Hour))
	re.NoError(err)
	re.True(updated)
	_, allSsp, err = s.provider.CompatibleLoadAllServiceGCSafePoints()
	re.NoError(err)
	re.Len(allSsp, 1)
	re.Equal(nowUnix+7200, allSsp[0].ExpiredAt)

	// Internally calls AdvanceTxnSafePoint when simulating updating "gc_worker".
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint("gc_worker", 20, 3600, now)
	// Cannot use finite TTL for "gc_worker".
	re.Error(err)
	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("gc_worker", 20, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(12), minSsp.SafePoint)
	re.NotEqual("gc_worker", minSsp.ServiceID)
	s.checkTxnSafePoint(constant.NullKeyspaceID, 12)

	// Deleting service safe point by passing zero TTL
	_, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 12, 0, now)
	re.NoError(err)
	// Deleting is regarded as not-updating. This is consistent with the old behavior.
	re.False(updated)
	// Then updating "gc_worker" won't be blocked.
	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("gc_worker", 20, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(20), minSsp.SafePoint)
	re.Equal("gc_worker", minSsp.ServiceID)
	s.checkTxnSafePoint(constant.NullKeyspaceID, 20)

	// If there's already old data written by the old version, there will exist a persisted "gc_worker" service safe
	// point, in which case AdvanceTxnSafePoint needs to update it as well for guaranteeing the safety of
	// rolling-upgrading and downgrading the cluster. Everytime AdvanceTxnSafePoint is called, the service safe point
	// of "gc_worker" is updated to the same value as the txn safe point.
	// This behavior only exist in the NullKeyspace.
	re.NoError(s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		return wb.SetGCBarrier(constant.NullKeyspaceID, endpoint.NewGCBarrier("gc_worker", 20, nil))
	}))
	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("svc1", 25, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(20), minSsp.SafePoint)
	re.Equal("gc_worker", minSsp.ServiceID)

	minSsp, updated, err = s.manager.CompatibleUpdateServiceGCSafePoint("gc_worker", 28, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(25), minSsp.SafePoint)
	re.NotEqual("gc_worker", minSsp.ServiceID)
	_, allSsp, err = s.provider.CompatibleLoadAllServiceGCSafePoints()
	re.NoError(err)
	re.Len(allSsp, 2)
	re.Equal("gc_worker", allSsp[0].ServiceID)
	re.Equal(uint64(25), allSsp[0].SafePoint)
	re.Equal("svc1", allSsp[1].ServiceID)
	re.Equal(uint64(25), allSsp[1].SafePoint)

	res, err = s.manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 29, now)
	re.NoError(err)
	re.Equal(uint64(25), res.NewTxnSafePoint)
	re.Contains(res.BlockerDescription, `BarrierID: "svc1"`)
	_, allSsp, err = s.provider.CompatibleLoadAllServiceGCSafePoints()
	re.NoError(err)
	re.Len(allSsp, 2)
	re.Equal("gc_worker", allSsp[0].ServiceID)
	re.Equal(uint64(25), allSsp[0].SafePoint)

	// The service safe point "gc_worker" is ignored by GetGCState.
	allBarriers := s.getAllGCBarriers(constant.NullKeyspaceID)
	re.Len(allBarriers, 1)
	re.Equal("svc1", allBarriers[0].BarrierID)

	// The service safe point can't be controlled by SetGCBarrier and DeleteGCBarrier.
	_, err = s.manager.SetGCBarrier(constant.NullKeyspaceID, "gc_worker", 30, time.Duration(math.MaxInt64), now)
	re.Error(err)
	re.ErrorIs(err, errs.ErrReservedGCBarrierID)
	_, err = s.manager.DeleteGCBarrier(constant.NullKeyspaceID, "gc_worker")
	re.Error(err)
	re.ErrorIs(err, errs.ErrReservedGCBarrierID)

	// The same behavior doesn't exist in other keyspaces with keyspace-level GC enabled.
	_, err = s.manager.SetGCBarrier(2, "gc_worker", 10, time.Hour, now)
	re.NoError(err)
	res, err = s.manager.AdvanceTxnSafePoint(2, 15, now)
	re.NoError(err)
	re.Equal(uint64(10), res.NewTxnSafePoint)
	re.Contains(res.BlockerDescription, `BarrierID: "gc_worker"`)
	_, err = s.manager.DeleteGCBarrier(2, "gc_worker")
	re.NoError(err)
	res, err = s.manager.AdvanceTxnSafePoint(2, 15, now)
	re.NoError(err)
	re.Equal(uint64(15), res.NewTxnSafePoint)
	re.Empty(res.BlockerDescription)
}

func (s *gcStateManagerTestSuite) TestRedirectKeyspace() {
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

func (s *gcStateManagerTestSuite) TestGetGCState() {
	re := s.Require()

	// Check the result of GetAllKeyspaceGCStates and GetGCState are matching.
	checkAllKeyspaceGCStates := func() {
		allStates, err := s.manager.GetAllKeyspacesGCStates()
		re.NoError(err)
		re.Len(allStates, len(s.keyspacePresets.all))
		for keyspaceID, state := range allStates {
			if slices.Contains(s.keyspacePresets.manageable, keyspaceID) {
				re.Equal(keyspaceID, state.KeyspaceID)

				s, err := s.manager.GetGCState(keyspaceID)
				re.NoError(err)
				re.Equal(s, state)
			} else {
				re.Contains(s.keyspacePresets.unmanageable, keyspaceID)
				re.Equal(keyspaceID, state.KeyspaceID)
				re.False(state.IsKeyspaceLevel)
			}
		}
	}

	for _, keyspaceID := range s.keyspacePresets.manageable {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		re.Equal(keyspaceID, state.KeyspaceID)
		if keyspaceID == constant.NullKeyspaceID {
			re.False(state.IsKeyspaceLevel)
		} else {
			re.True(state.IsKeyspaceLevel)
		}
		re.Equal(uint64(0), state.TxnSafePoint)
		re.Equal(uint64(0), state.GCSafePoint)
		re.Empty(state.GCBarriers)
	}

	for _, keyspaceID := range slices.Concat(s.keyspacePresets.unmanageable, s.keyspacePresets.nullSynonyms) {
		state, err := s.manager.GetGCState(keyspaceID)
		re.NoError(err)
		re.Equal(constant.NullKeyspaceID, state.KeyspaceID)
		re.False(state.IsKeyspaceLevel)
		re.Equal(uint64(0), state.TxnSafePoint)
		re.Equal(uint64(0), state.GCSafePoint)
		re.Empty(state.GCBarriers)
	}

	for _, keyspaceID := range s.keyspacePresets.notExisting {
		_, err := s.manager.GetGCState(keyspaceID)
		re.Error(err)
		re.ErrorIs(err, errs.ErrKeyspaceNotFound)
	}

	checkAllKeyspaceGCStates()

	now := time.Now().Truncate(time.Second)

	// Do some operations to change their states.
	_, err := s.manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 20, now)
	re.NoError(err)
	_, _, err = s.manager.AdvanceGCSafePoint(constant.NullKeyspaceID, 15)
	re.NoError(err)
	_, err = s.manager.SetGCBarrier(constant.NullKeyspaceID, "b1", 25, time.Hour, now)
	re.NoError(err)
	_, err = s.manager.SetGCBarrier(constant.NullKeyspaceID, "b2", 25, time.Hour*2, now)
	re.NoError(err)
	_, err = s.manager.AdvanceTxnSafePoint(2, 50, now)
	re.NoError(err)
	_, _, err = s.manager.AdvanceGCSafePoint(2, 45)
	re.NoError(err)
	_, err = s.manager.SetGCBarrier(2, "b1", 55, time.Hour, now)
	re.NoError(err)
	_, err = s.manager.SetGCBarrier(2, "b3", 60, time.Duration(math.MaxInt64), now)
	re.NoError(err)

	state, err := s.manager.GetGCState(constant.NullKeyspaceID)
	re.NoError(err)
	re.Equal(constant.NullKeyspaceID, state.KeyspaceID)
	re.False(state.IsKeyspaceLevel)
	re.Equal(uint64(20), state.TxnSafePoint)
	re.Equal(uint64(15), state.GCSafePoint)
	re.Equal([]*endpoint.GCBarrier{
		endpoint.NewGCBarrier("b1", 25, ptime(now.Add(time.Hour))),
		endpoint.NewGCBarrier("b2", 25, ptime(now.Add(time.Hour*2))),
	}, state.GCBarriers)

	state, err = s.manager.GetGCState(2)
	re.NoError(err)
	re.Equal(uint32(2), state.KeyspaceID)
	re.True(state.IsKeyspaceLevel)
	re.Equal(uint64(50), state.TxnSafePoint)
	re.Equal(uint64(45), state.GCSafePoint)
	re.Equal([]*endpoint.GCBarrier{
		endpoint.NewGCBarrier("b1", 55, ptime(now.Add(time.Hour))),
		endpoint.NewGCBarrier("b3", 60, nil),
	}, state.GCBarriers)

	checkAllKeyspaceGCStates()
}

func (s *gcStateManagerTestSuite) TestWeakenedConstraints() {
	re := s.Require()

	// In some cases the constraints to GC barrier can be violated. Test that the GCStateManager handles these case in
	// proper way.
	for _, keyspaceID := range s.keyspacePresets.manageable {
		_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
		re.NoError(err)
		// Force writing a GC barrier with a barrierTS that is smaller than the current txn safe point.
		err = s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
			return wb.SetGCBarrier(keyspaceID, endpoint.NewGCBarrier("b1", 10, nil))
		})
		re.NoError(err)
		// Further advancement of txn safe point takes no effect, and the txn safe point neither goes forward nor
		// backward.
		res, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(20), res.OldTxnSafePoint)
		re.Equal(uint64(20), res.NewTxnSafePoint)
		re.Equal(uint64(30), res.Target)
		re.Contains(res.BlockerDescription, "BarrierID: \"b1\"")
		s.checkTxnSafePoint(keyspaceID, 20)

		// DeleteGCBarrier can be used to remove the barrier as usual.
		_, err = s.manager.DeleteGCBarrier(keyspaceID, "b1")
		re.NoError(err)
		// The txn safe point can be advanced then.
		res, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
		re.NoError(err)
		re.Equal(uint64(20), res.OldTxnSafePoint)
		re.Equal(uint64(30), res.NewTxnSafePoint)
		re.Empty(res.BlockerDescription)
		s.checkTxnSafePoint(keyspaceID, 30)
	}
}

func TestSaturatingDuration(t *testing.T) {
	re := require.New(t)

	re.Equal(time.Second*2, saturatingDuration(2, time.Second))
	re.Equal(time.Second*2, saturatingDuration(-2, -time.Second))
	re.Equal(time.Second*2, saturatingDuration(2000, time.Millisecond))
	re.Equal(time.Duration(0), saturatingDuration(-2, time.Second))
	re.Equal(time.Duration(0), saturatingDuration(2, -time.Second))

	re.Equal(time.Duration(math.MaxInt64), saturatingDuration(1<<32, 1<<32))
	re.Equal(time.Duration(math.MaxInt64), saturatingDuration(1<<62, 2))
	re.Equal(time.Duration(1<<62), saturatingDuration(1<<61, 2))
	re.Equal(time.Duration(math.MaxInt64), saturatingDuration(1<<62, 1<<62))
	re.Equal(time.Duration(math.MaxInt64), saturatingDuration(math.MaxInt64, time.Second))
	re.Equal(time.Duration(math.MaxInt64), saturatingDuration(math.MaxInt64, math.MaxInt64))
}
