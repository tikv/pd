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

package schedulers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

type evictSlowStoreTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	es     Scheduler
	bs     Scheduler
	oc     *operator.Controller
}

func TestEvictSlowStoreTestSuite(t *testing.T) {
	suite.Run(t, new(evictSlowStoreTestSuite))
}

func (suite *evictSlowStoreTestSuite) SetupTest() {
	re := suite.Require()
	suite.cancel, _, suite.tc, suite.oc = prepareSchedulersTest()

	// Add stores 1, 2
	suite.tc.AddLeaderStore(1, 0)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	// Add regions 1, 2 with leaders in stores 1, 2
	suite.tc.AddLeaderRegion(1, 1, 2)
	suite.tc.AddLeaderRegion(2, 2, 1)
	suite.tc.UpdateLeaderCount(2, 16)

	storage := storage.NewStorageWithMemoryBackend()
	var err error
	suite.es, err = CreateScheduler(types.EvictSlowStoreScheduler, suite.oc, storage, ConfigSliceDecoder(types.EvictSlowStoreScheduler, []string{}), nil)
	re.NoError(err)
	suite.bs, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage, ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{}), nil)
	re.NoError(err)
}

func (suite *evictSlowStoreTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStore() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	// Add evict leader scheduler to store 1
	ops, _ := suite.es.Schedule(suite.tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	re.Equal(types.EvictSlowStoreScheduler.String(), ops[0].Desc())
	// Cannot balance leaders to store 1
	ops, _ = suite.bs.Schedule(suite.tc, false)
	re.Empty(ops)
	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})
	suite.tc.PutStore(newStoreInfo)
	// Evict leader scheduler of store 1 should be removed, then leader can be balanced to store 1
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	ops, _ = suite.bs.Schedule(suite.tc, false)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpLeader, 2, 1)

	// no slow store need to evict.
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)

	es2, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictStore())

	// check the value from storage.
	var persistValue evictSlowStoreSchedulerConfig
	err := es2.conf.load(&persistValue)
	re.NoError(err)

	re.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	re.Zero(persistValue.evictStore())
	re.True(persistValue.readyForRecovery())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStorePrepare() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictStore())
	// prepare with no evict store.
	err := suite.es.PrepareConfig(suite.tc)
	re.NoError(err)

	err = es2.conf.setStoreAndPersist(1)
	re.NoError(err)
	re.Equal(uint64(1), es2.conf.evictStore())
	re.False(es2.conf.readyForRecovery())
	// prepare with evict store.
	err = suite.es.PrepareConfig(suite.tc)
	re.NoError(err)
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStorePersistFail() {
	re := suite.Require()
	persisFail := "github.com/tikv/pd/pkg/schedule/schedulers/persistFail"
	re.NoError(failpoint.Enable(persisFail, "return(true)"))

	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	// Add evict leader scheduler to store 1
	ops, _ := suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.NoError(failpoint.Disable(persisFail))
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.NotEmpty(ops)
}

func TestEvictSlowStoreBatch(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Add stores
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add regions with leader in store 1
	for i := range 10000 {
		tc.AddLeaderRegion(uint64(i), 1, 2)
	}

	storage := storage.NewStorageWithMemoryBackend()
	es, err := CreateScheduler(types.EvictSlowStoreScheduler, oc, storage, ConfigSliceDecoder(types.EvictSlowStoreScheduler, []string{}), nil)
	re.NoError(err)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	storeInfo := tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	tc.PutStore(newStoreInfo)
	re.True(es.IsScheduleAllowed(tc))
	// Add evict leader scheduler to store 1
	ops, _ := es.Schedule(tc, false)
	re.Len(ops, 3)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	re.Equal(types.EvictSlowStoreScheduler.String(), ops[0].Desc())

	es.(*evictSlowStoreScheduler).conf.Batch = 5
	re.NoError(es.(*evictSlowStoreScheduler).conf.save())
	ops, _ = es.Schedule(tc, false)
	re.Len(ops, 5)

	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})

	tc.PutStore(newStoreInfo)
	// no slow store need to evict.
	ops, _ = es.Schedule(tc, false)
	re.Empty(ops)

	es2, ok := es.(*evictSlowStoreScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictStore())

	// check the value from storage.
	var persistValue evictSlowStoreSchedulerConfig
	err = es2.conf.load(&persistValue)
	re.NoError(err)

	re.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	re.Zero(persistValue.evictStore())
	re.True(persistValue.readyForRecovery())
	re.Equal(5, persistValue.Batch)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func TestRecoveryTime(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Add stores 1, 2, 3 with different leader counts
	tc.AddLeaderStore(1, 10)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)

	// Add regions with leader in store 1
	for i := range 10 {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}

	storage := storage.NewStorageWithMemoryBackend()
	es, err := CreateScheduler(types.EvictSlowStoreScheduler, oc, storage,
		ConfigSliceDecoder(types.EvictSlowStoreScheduler, []string{}), nil)
	re.NoError(err)
	bs, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage,
		ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{}), nil)
	re.NoError(err)

	var recoveryTimeInSec uint64 = 1
	recoveryTime := 1 * time.Second
	es.(*evictSlowStoreScheduler).conf.RecoveryDurationGap = recoveryTimeInSec

	// Mark store 1 as slow
	storeInfo := tc.GetStore(1)
	slowStore := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	tc.PutStore(slowStore)

	// Verify store is marked for eviction
	ops, _ := es.Schedule(tc, false)
	re.NotEmpty(ops)
	re.Equal(types.EvictSlowStoreScheduler.String(), ops[0].Desc())
	re.Equal(uint64(1), es.(*evictSlowStoreScheduler).conf.evictStore())

	// Store recovers from being slow
	time.Sleep(recoveryTime)
	recoveredStore := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})
	tc.PutStore(recoveredStore)

	// Should not recover immediately due to recovery time window
	for range 10 {
		// trigger recovery check
		es.Schedule(tc, false)
		ops, _ = bs.Schedule(tc, false)
		re.Empty(ops)
		re.Equal(uint64(1), es.(*evictSlowStoreScheduler).conf.evictStore())
	}

	// Store is slow again before recovery time is over
	time.Sleep(recoveryTime / 2)
	slowStore = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	tc.PutStore(slowStore)
	time.Sleep(recoveryTime / 2)
	// Should not recover due to recovery time window recalculation
	for range 10 {
		// trigger recovery check
		es.Schedule(tc, false)
		ops, _ = bs.Schedule(tc, false)
		re.Empty(ops)
		re.Equal(uint64(1), es.(*evictSlowStoreScheduler).conf.evictStore())
	}

	// Store recovers from being slow
	time.Sleep(recoveryTime)
	recoveredStore = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})
	tc.PutStore(recoveredStore)

	// Should not recover immediately due to recovery time window
	for range 10 {
		// trigger recovery check
		es.Schedule(tc, false)
		ops, _ = bs.Schedule(tc, false)
		re.Empty(ops)
		re.Equal(uint64(1), es.(*evictSlowStoreScheduler).conf.evictStore())
	}

	// Should now recover
	time.Sleep(recoveryTime)
	// trigger recovery check
	es.Schedule(tc, false)

	ops, _ = bs.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(es.(*evictSlowStoreScheduler).conf.evictStore())

	// Verify persistence
	var persistValue evictSlowStoreSchedulerConfig
	err = es.(*evictSlowStoreScheduler).conf.load(&persistValue)
	re.NoError(err)
	re.Zero(persistValue.evictStore())
	re.True(persistValue.readyForRecovery())
}
