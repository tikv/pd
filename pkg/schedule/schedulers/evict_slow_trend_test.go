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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

type evictSlowTrendTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	es     schedule.Scheduler
	bs     schedule.Scheduler
	oc     *operator.Controller
}

func TestEvictSlowTrendTestSuite(t *testing.T) {
	suite.Run(t, new(evictSlowTrendTestSuite))
}

func (suite *evictSlowTrendTestSuite) SetupTest() {
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
	suite.es, err = schedule.CreateScheduler(EvictSlowTrendType, suite.oc, storage, schedule.ConfigSliceDecoder(EvictSlowTrendType, []string{}))
	suite.NoError(err)
	suite.bs, err = schedule.CreateScheduler(BalanceLeaderType, suite.oc, storage, schedule.ConfigSliceDecoder(BalanceLeaderType, []string{}))
	suite.NoError(err)
}

func (suite *evictSlowTrendTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *evictSlowTrendTestSuite) TestEvictSlowTrend() {
	es2, ok := suite.es.(*evictSlowTrendScheduler)
	suite.True(ok)

	// Set store-1 to slow status, generate evict candidate
	suite.Equal(es2.conf.evictedStore(), uint64(0))
	suite.Equal(es2.conf.candidate(), uint64(0))
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
	suite.True(suite.es.IsScheduleAllowed(suite.tc))
	ops, _ := suite.es.Schedule(suite.tc, false)
	suite.Empty(ops)
	suite.Equal(es2.conf.candidate(), uint64(1))
	suite.Equal(es2.conf.evictedStore(), uint64(0))

	// Update other stores' heartbeat-ts, do evicting
	for storeID := uint64(2); storeID <= uint64(3); storeID++ {
		storeInfo := suite.tc.GetStore(storeID)
		newStoreInfo := storeInfo.Clone(
			core.SetLastHeartbeatTS(storeInfo.GetLastHeartbeatTS().Add(time.Second)),
		)
		suite.tc.PutStore(newStoreInfo)
	}
	ops, _ = suite.es.Schedule(suite.tc, false)
	operatorutil.CheckMultiTargetTransferLeader(suite.Require(), ops[0], operator.OpLeader, 1, []uint64{2, 3})
	suite.Equal(EvictSlowTrendType, ops[0].Desc())
	suite.Equal(es2.conf.candidate(), uint64(0))
	suite.Equal(es2.conf.evictedStore(), uint64(1))
	// Cannot balance leaders to store 1
	ops, _ = suite.bs.Schedule(suite.tc, false)
	suite.Empty(ops)

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
	suite.Empty(ops)
	suite.Zero(es2.conf.evictedStore())
	ops, _ = suite.bs.Schedule(suite.tc, false)
	operatorutil.CheckTransferLeader(suite.Require(), ops[0], operator.OpLeader, 3, 1)

	// no slow store need to evict.
	ops, _ = suite.es.Schedule(suite.tc, false)
	suite.Empty(ops)
	suite.Zero(es2.conf.evictedStore())

	// check the value from storage.
	sches, vs, err := es2.conf.storage.LoadAllScheduleConfig()
	suite.NoError(err)
	valueStr := ""
	for id, sche := range sches {
		if strings.EqualFold(sche, EvictSlowTrendName) {
			valueStr = vs[id]
		}
	}

	var persistValue evictSlowTrendSchedulerConfig
	err = json.Unmarshal([]byte(valueStr), &persistValue)
	suite.NoError(err)
	suite.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	suite.Zero(persistValue.evictedStore())
}

func (suite *evictSlowTrendTestSuite) TestEvictSlowTrendPrepare() {
	es2, ok := suite.es.(*evictSlowTrendScheduler)
	suite.True(ok)
	suite.Zero(es2.conf.evictedStore())
	// prepare with no evict store.
	suite.es.Prepare(suite.tc)

	es2.conf.setStoreAndPersist(1)
	suite.Equal(uint64(1), es2.conf.evictedStore())
	// prepare with evict store.
	suite.es.Prepare(suite.tc)
}
