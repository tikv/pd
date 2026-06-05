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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

const (
	storeID1 = 1
	storeID2 = 2
	storeID3 = 3
	storeID4 = 4
	storeID5 = 5
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
	suite.es, err = CreateScheduler(types.EvictSlowStoreScheduler, suite.oc, storage, ConfigSliceDecoder(types.EvictSlowStoreScheduler, []string{}), nil)
	re.NoError(err)
	es, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	es.conf.EnableNetworkSlowStore = true
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
	re.Empty(es2.conf.evictedStores())

	// check the value from storage.
	var persistValue evictSlowStoreSchedulerConfig
	err := es2.conf.load(&persistValue)
	re.NoError(err)

	re.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	re.Empty(persistValue.evictedStores())
	re.True(persistValue.readyForRecovery())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestNetworkNotConflictWithOtherScheduler() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))

	originStoreInfo := suite.tc.GetStore(storeID1).Clone()
	es := suite.es.(*evictSlowStoreScheduler)
	// step a
	triggerDiskSlowStore := func() {
		suite.tc.PutStore(suite.tc.GetStore(storeID1).Clone(func(store *core.StoreInfo) {
			store.GetStoreStats().SlowScore = 100
		}))
		es.scheduleDiskSlowStore(suite.tc)
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())
	}
	// step b
	triggerNetworkSlowStore := func() {
		suite.tc.PutStore(suite.tc.GetStore(storeID1).Clone(func(store *core.StoreInfo) {
			store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{
				storeID2: 10,
				storeID3: 10,
				storeID4: 100,
			}
		}))
		checkNetworkSlowStore(re, suite.es.(*evictSlowStoreScheduler), suite.tc, storeID1, true, true, false)
	}
	// step c
	triggerNetworkSlowStoreEvicted := func() {
		for range 10 {
			suite.tc.TriggerNetworkSlowEvict(storeID1)
		}

		es.scheduleNetworkSlowStore(suite.tc)
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())
	}
	// step d
	recoverNetworkSlowStore := func(stillEvicted bool) {
		suite.tc.PutStore(suite.tc.GetStore(storeID1).Clone(func(store *core.StoreInfo) {
			store.GetStoreStats().NetworkSlowScores = map[uint64]uint64{}
		}))
		checkNetworkSlowStore(re, suite.es.(*evictSlowStoreScheduler), suite.tc, storeID1, false, false, true)
		re.Equal(stillEvicted, suite.tc.GetStore(storeID1).EvictedAsSlowStore())
	}
	// step e
	recoverDiskSlowStore := func(stillEvicted bool) {
		suite.tc.PutStore(suite.tc.GetStore(storeID1).Clone(func(store *core.StoreInfo) {
			store.GetStoreStats().SlowScore = 0
		}))
		es.scheduleDiskSlowStore(suite.tc)
		re.Equal(stillEvicted, suite.tc.GetStore(storeID1).EvictedAsSlowStore())
	}

	// test conflict with disk slow store scheduler
	{
		log.Info("a -> b -> c -> d -> e")
		triggerDiskSlowStore()
		triggerNetworkSlowStore()

		// trigger network slow store doesn't affect disk slow store
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())

		triggerNetworkSlowStoreEvicted()
		recoverNetworkSlowStore(true)
		recoverDiskSlowStore(false)

		suite.tc.PutStore(originStoreInfo)
	}
	{
		log.Info("a -> b -> c -> e -> d")
		triggerDiskSlowStore()
		triggerNetworkSlowStore()

		// trigger network slow store doesn't affect disk slow store
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())

		triggerNetworkSlowStoreEvicted()
		recoverDiskSlowStore(true)

		// recover disk slow store doesn't affect network slow store
		re.False(suite.tc.GetStore(storeID1).AllowLeaderTransfer())
		recoverNetworkSlowStore(false)

		suite.tc.PutStore(originStoreInfo)
	}
	{
		log.Info("b -> a -> c -> d -> e")
		triggerNetworkSlowStore()
		triggerDiskSlowStore()

		// trigger network slow store doesn't affect disk slow store
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())

		triggerNetworkSlowStoreEvicted()
		recoverNetworkSlowStore(true)
		recoverDiskSlowStore(false)

		suite.tc.PutStore(originStoreInfo)
	}
	{
		log.Info("b -> a -> c -> e -> d")
		triggerNetworkSlowStore()
		triggerDiskSlowStore()

		// trigger network slow store doesn't affect disk slow store
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())

		triggerNetworkSlowStoreEvicted()
		recoverDiskSlowStore(true)

		// recover disk slow store doesn't affect network slow store
		re.False(suite.tc.GetStore(storeID1).AllowLeaderTransfer())
		recoverNetworkSlowStore(false)

		suite.tc.PutStore(originStoreInfo)
	}
	{
		log.Info("b -> c -> a -> d -> e")
		triggerNetworkSlowStore()
		triggerNetworkSlowStoreEvicted()
		triggerDiskSlowStore()

		// trigger disk slow store doesn't affect network slow store
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())
		recoverNetworkSlowStore(true)
		recoverDiskSlowStore(false)
		suite.tc.PutStore(originStoreInfo)
	}
	{
		log.Info("b -> c -> a -> e -> d")
		triggerNetworkSlowStore()
		triggerNetworkSlowStoreEvicted()
		triggerDiskSlowStore()

		// trigger disk slow store doesn't affect network slow store
		re.True(suite.tc.GetStore(storeID1).EvictedAsSlowStore())

		// previous disk slow store doesn't trigger successful,
		// so even if disk slow store recover, store 1 is still evicted.
		recoverDiskSlowStore(true)
		recoverNetworkSlowStore(false)
		re.False(suite.tc.GetStore(storeID1).EvictedAsSlowStore())

		suite.tc.PutStore(originStoreInfo)
	}

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestNetworkSlowStore() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	testCases := []struct {
		networkSlowScoresFunc func(store *core.StoreInfo)
		expectedSlow          bool
		recovery              bool
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
			recovery:     true,
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

		checkNetworkSlowStore(re, es, suite.tc, storeID1, tc.expectedSlow, tc.expectedSlow, tc.recovery)
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestNetworkSlowStoreReachLimit() {
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
		recovery               bool
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
			recovery:               true,
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

		checkNetworkSlowStore(re, es, suite.tc, tc.scheduleStore, tc.expectedSlow, tc.expectedPausedTransfer, tc.recovery)

		re.Equal(tc.expectedReachedLimit, reachedLimit)
		if reachedLimit {
			reachedLimit = false // reset for next iteration
		}
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestNetworkSlowStoreSwitchEnableToDisable() {
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
	checkNetworkSlowStore(re, es, suite.tc, storeID1, true, true, false)

	es.conf.EnableNetworkSlowStore = false
	suite.es.Schedule(suite.tc, false)
	re.NotContains(es.conf.PausedNetworkSlowStores, storeID1)
	checkNetworkSlowStore(re, es, suite.tc, storeID1, false, false, false)
	re.True(suite.tc.BasicCluster.GetStore(storeID2).AllowLeaderTransfer())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func checkNetworkSlowStore(
	re *require.Assertions,
	es *evictSlowStoreScheduler,
	tc *mockcluster.Cluster,
	storeID uint64,
	expectedSlow bool,
	expectedPausedTransfer bool,
	recovery bool,
) {
	if recovery {
		ts, ok := es.conf.networkSlowStoreRecoverStartAts[storeID]
		re.True(ok)
		re.Nil(ts)
		es.scheduleNetworkSlowStore(tc)
		ts, ok = es.conf.networkSlowStoreRecoverStartAts[storeID]
		re.True(ok)
		re.NotNil(ts)
	}

	es.scheduleNetworkSlowStore(tc)

	_, ok := es.conf.networkSlowStoreRecoverStartAts[storeID]
	re.Equal(expectedSlow, ok)
	if expectedPausedTransfer {
		re.Contains(es.conf.PausedNetworkSlowStores, storeID)
	} else {
		re.NotContains(es.conf.PausedNetworkSlowStores, storeID)
	}
	re.Equal(expectedPausedTransfer, !tc.GetStore(storeID).AllowLeaderTransfer())

	// check the value from storage.
	var persistValue evictSlowStoreSchedulerConfig
	_ = es.conf.load(&persistValue)
	_, ok = persistValue.networkSlowStoreRecoverStartAts[storeID]
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
	re.Empty(es2.conf.evictedStores())
	// prepare with no evict store.
	suite.es.PrepareConfig(suite.tc)

	re.NoError(es2.conf.setEvictedStoresAndPersist([]uint64{1}))
	re.ElementsMatch([]uint64{1}, es2.conf.evictedStores())
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
	re.Empty(es2.conf.evictedStores())

	// check the value from storage.
	var persistValue evictSlowStoreSchedulerConfig
	err = es2.conf.load(&persistValue)
	re.NoError(err)

	re.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	re.Empty(persistValue.evictedStores())
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
	re.Equal(types.EvictSlowStoreScheduler.String(), ops[0].Desc())
	re.ElementsMatch([]uint64{1}, es.(*evictSlowStoreScheduler).conf.evictedStores())

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
		re.ElementsMatch([]uint64{1}, es.(*evictSlowStoreScheduler).conf.evictedStores())
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
		re.ElementsMatch([]uint64{1}, es.(*evictSlowStoreScheduler).conf.evictedStores())
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
		re.ElementsMatch([]uint64{1}, es.(*evictSlowStoreScheduler).conf.evictedStores())
	}

	// Should now recover
	time.Sleep(recoveryTime)
	// trigger recovery check
	es.Schedule(tc, false)

	ops, _ = bs.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(es.(*evictSlowStoreScheduler).conf.evictedStores())

	// Verify persistence
	var persistValue evictSlowStoreSchedulerConfig
	err = es.(*evictSlowStoreScheduler).conf.load(&persistValue)
	re.NoError(err)
	re.Empty(persistValue.evictedStores())
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

func setStoreSlowScore(tc *mockcluster.Cluster, id uint64, score uint64) {
	tc.PutStore(tc.GetStore(id).Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = score
	}))
}

func createTestEvictSlowStoreScheduler(re *require.Assertions, oc *operator.Controller) Scheduler {
	es, err := CreateScheduler(types.EvictSlowStoreScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictSlowStoreScheduler, []string{}), nil)
	re.NoError(err)
	return es
}

// setGroupEvictionLabels configures the failure-domain labels that enable group eviction.
func setGroupEvictionLabels(es Scheduler, labels ...string) {
	es.(*evictSlowStoreScheduler).conf.GroupEvictionLabels = labels
}

// TestEvictSlowStoreSameZoneGroup: two slow stores sharing a zone are both evicted.
func TestEvictSlowStoreSameZoneGroup(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)

	ops, _ := es.Schedule(tc, false)
	re.Len(ops, 2)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())
	re.True(tc.GetStore(1).EvictedAsSlowStore())
	re.True(tc.GetStore(2).EvictedAsSlowStore())
}

// TestEvictSlowStoreCrossZoneNoEvict: slow stores spanning two zones evict nothing.
func TestEvictSlowStoreCrossZoneNoEvict(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 3, 1, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	// One slow store in z1, another in z2.
	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 3, 100)

	ops, _ := es.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(conf.evictedStores())
	re.False(tc.GetStore(1).EvictedAsSlowStore())
	re.False(tc.GetStore(3).EvictedAsSlowStore())
}

// TestEvictSlowStoreDefaultNoGroup: with no group-eviction-labels configured, 2+ slow stores
// fall back to the conservative do-nothing behavior.
func TestEvictSlowStoreDefaultNoGroup(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	// group-eviction-labels left at its default (empty).
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)

	ops, _ := es.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(conf.evictedStores())
}

// TestEvictSlowStoreMultiLevelMatch: group-eviction-labels "zone,rack"; two stores sharing both
// zone and rack are evicted together.
func TestEvictSlowStoreMultiLevelMatch(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "rack": "r2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "rack": "r3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone", "rack")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)

	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())
}

// TestEvictSlowStoreMultiLevelDiffer: group-eviction-labels "zone,rack"; stores in the same zone
// but different racks are NOT a single domain, so no group eviction.
func TestEvictSlowStoreMultiLevelDiffer(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "rack": "r3", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "rack": "r4", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone", "rack")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)

	ops, _ := es.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(conf.evictedStores())
}

// TestEvictSlowStoreMissingLabel: a slow store missing a configured label is never grouped.
func TestEvictSlowStoreMissingLabel(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"host": "h2"}) // no zone label
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)

	ops, _ := es.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(conf.evictedStores())
}

// TestEvictSlowStoreInsufficientHealthyZones: a topology with too few zones to
// absorb a zone drain blocks group eviction.
func TestEvictSlowStoreInsufficientHealthyZones(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// Only z1 (slow) and z2 exist; draining z1 leaves a single healthy zone.
	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLeaderRegion(1, 1, 3)
	tc.AddLeaderRegion(2, 2, 3)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)

	ops, _ := es.Schedule(tc, false)
	re.Empty(ops)
	re.Empty(conf.evictedStores())
}

// TestEvictSlowStoreReconcileAndFreeze: while draining a zone, newly-slow same-zone
// stores are added, but a slow store appearing in another zone freezes expansion.
func TestEvictSlowStoreReconcileAndFreeze(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z1", "host": "h5"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)
	tc.AddLeaderRegion(5, 5, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	// Step 1: only store 1 slow → single-store eviction.
	setStoreSlowScore(tc, 1, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1}, conf.evictedStores())

	// Step 2: store 2 (same zone) becomes slow → reconcile adds it.
	setStoreSlowScore(tc, 2, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())

	// Step 3: a store in another zone goes slow → freeze; a new z1 store is NOT added.
	setStoreSlowScore(tc, 3, 100)
	setStoreSlowScore(tc, 5, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())
	re.False(tc.GetStore(5).EvictedAsSlowStore())
}

// TestEvictSlowStoreGroupRecovery: the group is released only once every evicted
// store has recovered.
func TestEvictSlowStoreGroupRecovery(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())

	// Only store 1 recovers: the group keeps draining (store 1 stays evicted).
	setStoreSlowScore(tc, 1, 0)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())
	re.True(tc.GetStore(1).EvictedAsSlowStore())

	// Store 2 recovers too: the whole group is released.
	setStoreSlowScore(tc, 2, 0)
	es.Schedule(tc, false)
	re.Empty(conf.evictedStores())
	re.False(tc.GetStore(1).EvictedAsSlowStore())
	re.False(tc.GetStore(2).EvictedAsSlowStore())
}

// TestEvictSlowStoreReconcileInsufficientZones: when a group forms via single-store
// eviction followed by reconcile, the domain guard must still block expansion if the
// topology has too few healthy zones to absorb the whole zone being drained.
func TestEvictSlowStoreReconcileInsufficientZones(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	// Only two zones: z1 (slow) and z2. Draining all of z1 leaves a single healthy zone.
	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLeaderRegion(1, 1, 3)
	tc.AddLeaderRegion(2, 2, 3)

	es := createTestEvictSlowStoreScheduler(re, oc)
	conf := es.(*evictSlowStoreScheduler).conf

	// Store 1 slow → single-store path evicts it (no domain guard on single-store path,
	// matching the pre-existing behavior).
	setStoreSlowScore(tc, 1, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1}, conf.evictedStores())

	// Store 2 (same zone) becomes slow → reconcile must NOT add it because the
	// topology only has 1 other zone (z2), below minRemainingHealthyDomains=2.
	setStoreSlowScore(tc, 2, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1}, conf.evictedStores())
	re.False(tc.GetStore(2).EvictedAsSlowStore())
}

// TestEvictSlowStoreRemovedMidDrain: a store that leaves the cluster while being
// evicted is dropped from the group; the remaining stores keep draining.
func TestEvictSlowStoreRemovedMidDrain(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())

	// Store 1 is physically removed from the cluster mid-drain.
	tc.PutStore(tc.GetStore(1).Clone(func(store *core.StoreInfo) {
		store.GetMeta().NodeState = metapb.NodeState_Removed
	}))

	// Next cycle: store 1 is dropped from the eviction set; store 2 keeps draining.
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{2}, conf.evictedStores())
	re.False(tc.GetStore(1).EvictedAsSlowStore())
	re.True(tc.GetStore(2).EvictedAsSlowStore())
}

// TestEvictSlowStoreGroupIdempotent: repeated reconcile cycles never double-mark a
// store, so a single recovery fully clears the slow-evicted state.
func TestEvictSlowStoreGroupIdempotent(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
	}()

	tc.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 0, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3", "host": "h4"})
	tc.AddLeaderRegion(1, 1, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4)

	es := createTestEvictSlowStoreScheduler(re, oc)
	setGroupEvictionLabels(es, "zone")
	conf := es.(*evictSlowStoreScheduler).conf

	setStoreSlowScore(tc, 1, 100)
	setStoreSlowScore(tc, 2, 100)
	es.Schedule(tc, false)
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())

	// Several reconcile cycles with no change must not re-mark the stores.
	for range 5 {
		es.Schedule(tc, false)
	}
	re.ElementsMatch([]uint64{1, 2}, conf.evictedStores())

	// A single recovery must fully clear the evicted mark (counter back to zero).
	setStoreSlowScore(tc, 1, 0)
	setStoreSlowScore(tc, 2, 0)
	es.Schedule(tc, false)
	re.Empty(conf.evictedStores())
	re.False(tc.GetStore(1).EvictedAsSlowStore())
	re.False(tc.GetStore(2).EvictedAsSlowStore())
}
