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

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
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

	// Add stores 1, 2, 3, 4
	suite.tc.AddLeaderStore(1, 0)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	suite.tc.AddLeaderStore(4, 0)
	// Add regions 1, 2 with leaders in stores 1, 2
	suite.tc.AddLeaderRegion(1, 1, 2)
	suite.tc.AddLeaderRegion(2, 2, 1)
	suite.tc.UpdateLeaderCount(2, 16)

	storage := storage.NewStorageWithMemoryBackend()
	var err error
	suite.es, err = CreateScheduler(EvictSlowStoreType, suite.oc, storage, ConfigSliceDecoder(EvictSlowStoreType, []string{}), nil)
	re.NoError(err)
	suite.bs, err = CreateScheduler(BalanceLeaderType, suite.oc, storage, ConfigSliceDecoder(BalanceLeaderType, []string{}), nil)
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
	re.Equal(EvictSlowStoreType, ops[0].Desc())
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

func (suite *evictSlowStoreTestSuite) TestNetworkSlowStore() {
	const (
		storeID1 uint64 = 1
		storeID2 uint64 = 2
		storeID3 uint64 = 3
		storeID4 uint64 = 4
	)
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	testCases := []struct {
		networkSlowScoresFunc func(store *core.StoreInfo)
		expectedSlow          bool
	}{
		// Note: The test cases are sequential.
		// There will be a causal relationship between before and after
		{
			// One 100 score does not trigger evict slow store
			networkSlowScoresFunc: func(store *core.StoreInfo) {
				store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
					storeID2: 100,
				}
			},
			expectedSlow: false,
		},
		{
			// Does not meet PausedNetworkSlowStoresecondThreshold
			networkSlowScoresFunc: func(store *core.StoreInfo) {
				store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
					storeID2: 1,
					storeID3: 1,
					storeID4: 100,
				}
			},
			expectedSlow: false,
		},
		{
			// Successfully triggers slow store
			networkSlowScoresFunc: func(store *core.StoreInfo) {
				store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
					storeID2: 10,
					storeID3: 10,
					storeID4: 100,
				}
			},
			expectedSlow: true,
		},
		{
			// Score decreases but still slow store
			networkSlowScoresFunc: func(store *core.StoreInfo) {
				store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
					storeID2: 10,
					storeID3: 10,
					storeID4: 10,
				}
			},
			expectedSlow: true,
		},
		{
			// Successfully recovers from slow store
			networkSlowScoresFunc: func(store *core.StoreInfo) {
				store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
					storeID2: 1,
					storeID3: 1,
					storeID4: 1,
				}
			},
			expectedSlow: false,
		},
		{
			// Test large cluster with many stores
			networkSlowScoresFunc: func(store *core.StoreInfo) {
				store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
					storeID2: 100,
					storeID3: 10,
					storeID4: 10,
					5:        1,
					6:        1,
					7:        1,
					8:        1,
					9:        1,
					10:       1,
				}
			},
			expectedSlow: true,
		},
	}

	es, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	for i, tc := range testCases {
		suite.T().Logf("Test case %d", i+1)
		storeInfo := suite.tc.GetStore(storeID1)
		suite.tc.PutStore(storeInfo.Clone(tc.networkSlowScoresFunc))

		suite.es.Schedule(suite.tc, false)

		checkNetworkSlowStore(re, es.conf, suite.tc.BasicCluster, storeID1, tc.expectedSlow, tc.expectedSlow)
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestNetworkSlowStoreReachLimit() {
	const (
		storeID1 = 1
		storeID2 = 2
		storeID3 = 3
		storeID4 = 4
		storeID5 = 5
	)
	re := suite.Require()
	reachedLimit := false
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/schedule/schedulers/evictSlowStoreTriggerLimit", func() {
		reachedLimit = true
	}))
	testCases := []struct {
		name                   string
		scheduleStore          uint64
		networkSlowScoresFunc  map[uint64]func(store *core.StoreInfo)
		expectedSlow           bool
		expectedPausedTransfer bool
		expectedReachedLimit   bool
	}{
		// Note: The test cases are sequential.
		// There will be a causal relationship between before and after
		{
			name:          "store 1 becomes slow",
			scheduleStore: storeID1,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID1: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID2: 10,
						storeID3: 10,
						storeID4: 100,
					}
				},
			},
			expectedSlow:           true,
			expectedPausedTransfer: true,
		},
		{
			name:          "store 2 is normal",
			scheduleStore: storeID2,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID2: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID1: 100,
						storeID3: 1,
						storeID4: 1,
					}
				},
			},
			expectedSlow:           false,
			expectedPausedTransfer: false,
		},
		{
			// store 2 becomes slow, but it does not meet the number of networkSlowStoreFluctuationThreshold
			// so it is not considered as slow store
			name:          "store 2 becomes slow",
			scheduleStore: storeID2,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID2: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID1: 100, // storeID1 will be filtered out because it is already slow
						storeID3: 100,
						storeID4: 10,
					}
				},
			},
			expectedSlow:           false,
			expectedPausedTransfer: false,
		},
		{
			// Scale out store 5, thus store 2 meet the number of networkSlowStoreFluctuationThreshold
			// and it is considered as slow store. And reached limit.
			name:          "scale out store 5",
			scheduleStore: storeID2,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID2: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID1: 100, // storeID1 will be filtered out because it is already slow
						storeID3: 100,
						storeID4: 10,
						storeID5: 10,
					}
				},
			},
			expectedSlow:           true,
			expectedPausedTransfer: false,
			expectedReachedLimit:   true,
		},
		{
			// Store 1 successfully recovers from slow store
			name:          "store 1 recovers",
			scheduleStore: storeID1,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID1: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID2: 100,
						storeID3: 1,
						storeID4: 1,
						storeID5: 1,
					}
				},
			},
			expectedSlow:           false,
			expectedPausedTransfer: false,
		},
		{
			// Store 2 is still slow, but does not reach the limit
			name:          "store 2 is still slow",
			scheduleStore: storeID2,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID2: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID1: 100,
						storeID3: 100,
						storeID4: 100,
						storeID5: 100,
					}
				},
			},
			expectedSlow:           true,
			expectedPausedTransfer: true,
			expectedReachedLimit:   false,
		},
		{
			// Store 3 becomes slow, and reaches the limit
			name:          "store 3 becomes slow",
			scheduleStore: storeID3,
			networkSlowScoresFunc: map[uint64]func(store *core.StoreInfo){
				storeID3: func(store *core.StoreInfo) {
					store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
						storeID1: 100, // storeID1 will be filtered out because it is already slow
						storeID2: 100,
						storeID4: 100,
						storeID5: 100,
					}
				},
			},
			expectedSlow:           true,
			expectedPausedTransfer: false,
			expectedReachedLimit:   true,
		},
	}

	es, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	for _, tc := range testCases {
		suite.T().Log(tc.name)
		for id, fn := range tc.networkSlowScoresFunc {
			storeInfo := suite.tc.GetStore(id)
			suite.tc.PutStore(storeInfo.Clone(fn))
		}

		suite.es.Schedule(suite.tc, false)
		checkNetworkSlowStore(re, es.conf, suite.tc.BasicCluster, tc.scheduleStore, tc.expectedSlow, tc.expectedPausedTransfer)

		re.Equal(tc.expectedReachedLimit, reachedLimit)
		if reachedLimit {
			reachedLimit = false // reset for next iteration
		}
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestNetworkSlowStoreSwitchEnableToDisable() {
	const (
		storeID1 uint64 = 1
		storeID2 uint64 = 2
		storeID3 uint64 = 3
		storeID4 uint64 = 4
	)
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))

	es, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	storeInfo := suite.tc.GetStore(storeID1)
	suite.tc.PutStore(storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
			storeID2: 10,
			storeID3: 10,
			storeID4: 100,
		}
	}))

	suite.es.Schedule(suite.tc, false)
	checkNetworkSlowStore(re, es.conf, suite.tc.BasicCluster, storeID1, true, true)

	es.conf.EnableNetworkSlowStore = false
	suite.es.Schedule(suite.tc, false)
	re.NotContains(es.conf.PausedNetworkSlowStores, storeID1)
	checkNetworkSlowStore(re, es.conf, suite.tc.BasicCluster, storeID1, false, false)
	re.True(suite.tc.BasicCluster.GetStore(storeID2).AllowLeaderTransfer())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func checkNetworkSlowStore(
	re *require.Assertions,
	conf *evictSlowStoreSchedulerConfig,
	cluster *core.BasicCluster,
	storeID uint64,
	expectedSlow bool,
	expectedPausedTransfer bool,
) {
	_, ok := conf.networkSlowStoreCaptureTSs[storeID]
	re.Equal(expectedSlow, ok)
	if expectedPausedTransfer {
		re.Contains(conf.PausedNetworkSlowStores, storeID)
	} else {
		re.NotContains(conf.PausedNetworkSlowStores, storeID)
	}
	re.Equal(expectedPausedTransfer, !cluster.GetStore(storeID).AllowLeaderTransfer())

	// check the value from storage.
	var persistValue evictSlowStoreSchedulerConfig
	_ = conf.load(&persistValue)
	_, ok = persistValue.networkSlowStoreCaptureTSs[storeID]
	re.False(ok)
	if expectedPausedTransfer {
		re.Contains(persistValue.PausedNetworkSlowStores, storeID)
	} else {
		re.NotContains(persistValue.PausedNetworkSlowStores, storeID)
	}
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStorePrepare() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictStore())
	// prepare with no evict store.
	suite.es.PrepareConfig(suite.tc)

	es2.conf.setStoreAndPersist(1)
	re.Equal(uint64(1), es2.conf.evictStore())
	re.False(es2.conf.readyForRecovery())
	// prepare with evict store.
	suite.es.PrepareConfig(suite.tc)
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
	for i := 0; i < 10000; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2)
	}

	storage := storage.NewStorageWithMemoryBackend()
	es, err := CreateScheduler(EvictSlowStoreType, oc, storage, ConfigSliceDecoder(EvictSlowStoreType, []string{}), nil)
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
	re.Equal(EvictSlowStoreType, ops[0].Desc())

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
	for i := 0; i < 10; i++ {
		tc.AddLeaderRegion(uint64(i), 1, 2, 3)
	}

	storage := storage.NewStorageWithMemoryBackend()
	es, err := CreateScheduler(EvictSlowStoreType, oc, storage,
		ConfigSliceDecoder(EvictSlowStoreType, []string{}), nil)
	re.NoError(err)
	bs, err := CreateScheduler(BalanceLeaderType, oc, storage,
		ConfigSliceDecoder(BalanceLeaderType, []string{}), nil)
	re.NoError(err)

	var recoveryTimeInSec uint64 = 1
	recoveryTime := 1 * time.Second
	es.(*evictSlowStoreScheduler).conf.RecoverySec = recoveryTimeInSec

	// Mark store 1 as slow
	storeInfo := tc.GetStore(1)
	slowStore := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	tc.PutStore(slowStore)

	// Verify store is marked for eviction
	ops, _ := es.Schedule(tc, false)
	re.NotEmpty(ops)
	re.Equal(EvictSlowStoreType, ops[0].Desc())
	re.Equal(uint64(1), es.(*evictSlowStoreScheduler).conf.evictStore())

	// Store recovers from being slow
	time.Sleep(recoveryTime)
	recoveredStore := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})
	tc.PutStore(recoveredStore)

	// Should not recover immediately due to recovery time window
	for i := 0; i < 10; i++ {
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
	for i := 0; i < 10; i++ {
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
	for i := 0; i < 10; i++ {
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

func TestCalculateAvgScore(t *testing.T) {
	re := require.New(t)
	// Empty map
	re.Equal(uint64(0), calculateAvgScore(map[uint64]uint64{}))
	// Single value
	re.Equal(uint64(10), calculateAvgScore(map[uint64]uint64{1: 10}))
	// Multiple values
	re.Equal(uint64(5), calculateAvgScore(map[uint64]uint64{1: 2, 2: 5, 3: 8}))
	// All zeros
	re.Equal(uint64(0), calculateAvgScore(map[uint64]uint64{1: 0, 2: 0}))
}
