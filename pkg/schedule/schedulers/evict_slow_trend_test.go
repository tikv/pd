// Copyright 2023 TiKV Project Authors.
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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

type evictSlowTrendTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	es     Scheduler
	bs     Scheduler
	oc     *operator.Controller
}

func TestEvictSlowTrendTestSuite(t *testing.T) {
	suite.Run(t, new(evictSlowTrendTestSuite))
}

func (suite *evictSlowTrendTestSuite) SetupTest() {
	re := suite.Require()
	suite.cancel, _, suite.tc, suite.oc = prepareSchedulersTest()

	suite.tc.AddLeaderStore(1, 10)
	suite.tc.AddLeaderStore(2, 99)
	suite.tc.AddLeaderStore(3, 100)
	suite.tc.AddLeaderRegion(1, 1, 2, 3)
	suite.tc.AddLeaderRegion(2, 2, 1, 3)
	suite.tc.AddLeaderRegion(3, 3, 1, 2)

	now := time.Now()
	for i := 1; i <= 3; i++ {
		storeInfo := suite.tc.GetStore(uint64(i))
		newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
			store.GetStoreStats().SlowTrend = &pdpb.SlowTrend{
				CauseValue:  5.0e6,
				CauseRate:   0.0,
				ResultValue: 5.0e3,
				ResultRate:  0.0,
			}
		}, core.SetLastHeartbeatTS(now))
		suite.tc.PutStore(newStoreInfo)
	}

	storage := storage.NewStorageWithMemoryBackend()
	var err error
	suite.es, err = CreateScheduler(types.EvictSlowTrendScheduler, suite.oc, storage, ConfigSliceDecoder(types.EvictSlowTrendScheduler, []string{}))
	re.NoError(err)
	suite.bs, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage, ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{}))
	re.NoError(err)
}

func (suite *evictSlowTrendTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *evictSlowTrendTestSuite) TestEvictSlowTrendBasicFuncs() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowTrendScheduler)
	re.True(ok)

	re.Zero(es2.conf.evictedStore())
	re.Zero(es2.conf.candidate())

	// Test capture store 1
	store := suite.tc.GetStore(1)
	es2.conf.captureCandidate(store.GetID())
	lastCapturedCandidate := es2.conf.lastCapturedCandidate()
	re.Equal(*lastCapturedCandidate, es2.conf.evictCandidate)
	re.Zero(es2.conf.candidateCapturedSecs())
	re.Zero(es2.conf.lastCandidateCapturedSecs())
	re.False(es2.conf.readyForRecovery())
	recoverTS := lastCapturedCandidate.recoverTS
	re.True(recoverTS.After(lastCapturedCandidate.captureTS))
	// Pop captured store 1 and mark it has recovered.
	time.Sleep(50 * time.Millisecond)
	re.Equal(es2.conf.popCandidate(true), store.GetID())
	re.Equal(slowCandidate{}, es2.conf.evictCandidate)
	es2.conf.markCandidateRecovered()
	lastCapturedCandidate = es2.conf.lastCapturedCandidate()
	re.Positive(lastCapturedCandidate.recoverTS.Compare(recoverTS))
	re.Equal(lastCapturedCandidate.storeID, store.GetID())

	// Test capture another store 2
	store = suite.tc.GetStore(2)
	es2.conf.captureCandidate(store.GetID())
	lastCapturedCandidate = es2.conf.lastCapturedCandidate()
	re.Equal(uint64(1), lastCapturedCandidate.storeID)
	re.Equal(es2.conf.candidate(), store.GetID())
	re.Zero(es2.conf.candidateCapturedSecs())

	re.Equal(es2.conf.popCandidate(false), store.GetID())
	re.Equal(uint64(1), lastCapturedCandidate.storeID)
}

func (suite *evictSlowTrendTestSuite) TestEvictSlowTrend() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowTrendScheduler)
	re.True(ok)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))

	// Set store-1 to slow status, generate evict candidate
	re.Zero(es2.conf.evictedStore())
	re.Zero(es2.conf.candidate())
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowTrend = &pdpb.SlowTrend{
			CauseValue:  5.0e8,
			CauseRate:   1e7,
			ResultValue: 3.0e3,
			ResultRate:  -1e7,
		}
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	ops, _ := suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.Equal(uint64(1), es2.conf.candidate())
	re.Zero(es2.conf.evictedStore())

	// Update other stores' heartbeat-ts, do evicting
	for storeID := uint64(2); storeID <= uint64(3); storeID++ {
		storeInfo := suite.tc.GetStore(storeID)
		newStoreInfo := storeInfo.Clone(
			core.SetLastHeartbeatTS(storeInfo.GetLastHeartbeatTS().Add(time.Second)),
		)
		suite.tc.PutStore(newStoreInfo)
	}
	ops, _ = suite.es.Schedule(suite.tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2, 3})
	re.Equal(types.EvictSlowTrendScheduler.String(), ops[0].Desc())
	re.Zero(es2.conf.candidate())
	re.Equal(uint64(1), es2.conf.evictedStore())
	// Cannot balance leaders to store 1
	ops, _ = suite.bs.Schedule(suite.tc, false)
	re.Empty(ops)

	// Set store-1 to normal status
	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowTrend = &pdpb.SlowTrend{
			CauseValue:  5.0e6,
			CauseRate:   0.0,
			ResultValue: 5.0e3,
			ResultRate:  0.0,
		}
	})
	suite.tc.PutStore(newStoreInfo)
	// Evict leader scheduler of store 1 should be removed, then leaders should be balanced from store-3 to store-1
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.Zero(es2.conf.evictedStore())
	ops, _ = suite.bs.Schedule(suite.tc, false)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpLeader, 3, 1)

	// no slow store need to evict.
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.Zero(es2.conf.evictedStore())

	// check the value from storage.
	var persistValue evictSlowTrendSchedulerConfig
	err := es2.conf.load(&persistValue)
	re.NoError(err)
	re.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	re.Zero(persistValue.evictedStore())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowTrendTestSuite) TestEvictSlowTrendV2() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowTrendScheduler)
	re.True(ok)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/mockRaftKV2", "return(true)"))

	re.Zero(es2.conf.evictedStore())
	re.Zero(es2.conf.candidate())
	// Set store-1 to slow status, generate slow candidate but under faster limit
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowTrend = &pdpb.SlowTrend{
			CauseValue:  5.0e6 + 100,
			CauseRate:   1e7,
			ResultValue: 3.0e3,
			ResultRate:  -1e7,
		}
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	ops, _ := suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.Zero(es2.conf.evictedStore())
	re.Equal(uint64(1), es2.conf.candidate())
	re.Zero(es2.conf.lastCandidateCapturedSecs())
	// Rescheduling to make it filtered by the related faster judgement.
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.Zero(es2.conf.evictedStore())
	re.Zero(es2.conf.candidate())

	// Set store-1 to slow status as network-io delays
	storeInfo = suite.tc.GetStore(1)
	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowTrend = &pdpb.SlowTrend{
			CauseValue:  5.0e6,
			CauseRate:   1e7,
			ResultValue: 0,
			ResultRate:  0,
		}
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.Zero(es2.conf.evictedStore())
	re.Zero(es2.conf.lastCandidateCapturedSecs())

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/mockRaftKV2"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowTrendTestSuite) TestEvictSlowTrendPrepare() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowTrendScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictedStore())
	// prepare with no evict store.
	suite.es.PrepareConfig(suite.tc)

	es2.conf.setStoreAndPersist(1)
	re.Equal(uint64(1), es2.conf.evictedStore())
	// prepare with evict store.
	suite.es.PrepareConfig(suite.tc)
}
