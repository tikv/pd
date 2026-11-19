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

package schedulers

import (
	"context"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

type evictStoppingStoreTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	es     Scheduler
	bs     Scheduler
	oc     *operator.Controller
}

func TestEvictStoppingStoreTestSuite(t *testing.T) {
	suite.Run(t, new(evictStoppingStoreTestSuite))
}

func (suite *evictStoppingStoreTestSuite) SetupTest() {
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
	suite.es, err = CreateScheduler(types.EvictStoppingStoreScheduler, suite.oc, storage, ConfigSliceDecoder(types.EvictStoppingStoreScheduler, []string{}), nil)
	re.NoError(err)
	suite.bs, err = CreateScheduler(types.BalanceLeaderScheduler, suite.oc, storage, ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{}), nil)
	re.NoError(err)
}

func (suite *evictStoppingStoreTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *evictStoppingStoreTestSuite) TestEvictStoppingStore() {
	re := suite.Require()

	// Set store 1 as stopping
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().IsStopping = true
	})
	suite.tc.PutStore(newStoreInfo)

	re.True(suite.es.IsScheduleAllowed(suite.tc))
	// Add evict leader scheduler to store 1
	ops, _ := suite.es.Schedule(suite.tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	re.Equal(types.EvictStoppingStoreScheduler.String(), ops[0].Desc())

	// Cannot balance leaders to store 1
	ops, _ = suite.bs.Schedule(suite.tc, false)
	re.Empty(ops)
}

func (suite *evictStoppingStoreTestSuite) TestEvictStoppingStoreRecovery() {
	re := suite.Require()

	// Set store 1 as stopping
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().IsStopping = true
	})
	suite.tc.PutStore(newStoreInfo)

	// Should generate evict leader operator
	ops, _ := suite.es.Schedule(suite.tc, false)
	re.NotEmpty(ops)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})

	// Set store 1 as recovered (not stopping)
	storeInfo = suite.tc.GetStore(1)
	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().IsStopping = false
	})
	suite.tc.PutStore(newStoreInfo)

	// Should not generate evict leader operator after recovery
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)

	// Balance leader scheduler should work normally
	ops, _ = suite.bs.Schedule(suite.tc, false)
	re.NotEmpty(ops)
}

func (suite *evictStoppingStoreTestSuite) TestRemovedStoppingStore() {
	re := suite.Require()

	// Set store 1 as stopping
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().IsStopping = true
	})
	suite.tc.PutStore(newStoreInfo)

	// Should generate evict leader operator
	ops, _ := suite.es.Schedule(suite.tc, false)
	re.NotEmpty(ops)

	// Mark store 1 as removed
	storeInfo = suite.tc.GetStore(1)
	newStoreInfo = storeInfo.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	suite.tc.PutStore(newStoreInfo)

	// Should not panic and not generate any operators
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
}

func (suite *evictStoppingStoreTestSuite) TestEvictedStoreMetricsCleanup() {
	es := suite.es.(*evictStoppingStoreScheduler)
	gauge := evictedStoppingStoreStatusGauge
	for _, state := range []string{"recovered", "tombstone", "deleted"} {
		suite.Run(state, func() {
			re := suite.Require()
			const storeID = 5
			gauge.Reset()
			suite.T().Cleanup(gauge.Reset)
			suite.tc.AddLeaderStore(storeID, 0)
			re.NoError(es.prepareEvictLeader(suite.tc, storeID))
			metric := gauge.WithLabelValues("5")
			metric.Set(1)
			store := suite.tc.GetStore(storeID)
			switch state {
			case "tombstone":
				suite.tc.PutStore(store.Clone(core.SetStoreState(metapb.StoreState_Tombstone)))
				gauge.DeleteLabelValues("5") // Simulate metric cleanup before the scheduler runs.
			case "deleted":
				suite.tc.DeleteStore(store)
			}

			ops, _ := es.Schedule(suite.tc, false)
			re.Empty(ops)
			re.Zero(es.conf.evictStore())
			if state == "recovered" {
				re.Equal(1, promtestutil.CollectAndCount(gauge))
				re.Zero(promtestutil.ToFloat64(metric))
			} else {
				re.Zero(promtestutil.CollectAndCount(gauge))
			}
		})
	}
}
