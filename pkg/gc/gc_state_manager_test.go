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
		Name: "ks1",
		// TODO: Change the value into "unified" when the configuration value is renamed
		Config:     map[string]string{"gc_management_type": "global_gc"},
		CreateTime: time.Now().Unix(),
		IsPreAlloc: false,
	})
	re.NoError(err)
	re.Equal(uint32(1), ks1.Id)

	ks2, err := keyspaceManager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name: "ks2",
		// TODO: Change the value into "keyspace_level" when the configuration value is renamed
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
