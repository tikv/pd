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
	"encoding/json"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testEvictSlowStoreSuite{})

type testEvictSlowStoreSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	es     schedule.Scheduler
	bs     schedule.Scheduler
	oc     *schedule.OperatorController
}

func (s *testEvictSlowStoreSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	opt := config.NewTestOptions()
	s.tc = mockcluster.NewCluster(s.ctx, opt)

	// Add stores 1, 2
	s.tc.AddLeaderStore(1, 0)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	// Add regions 1, 2 with leaders in stores 1, 2
	s.tc.AddLeaderRegion(1, 1, 2)
	s.tc.AddLeaderRegion(2, 2, 1)
	s.tc.UpdateLeaderCount(2, 16)

	s.oc = schedule.NewOperatorController(s.ctx, nil, nil)
	storage := storage.NewStorageWithMemoryBackend()
	var err error
	s.es, err = schedule.CreateScheduler(EvictSlowStoreType, s.oc, storage, schedule.ConfigSliceDecoder(EvictSlowStoreType, []string{}))
	c.Assert(err, IsNil)
	s.bs, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage, schedule.ConfigSliceDecoder(BalanceLeaderType, []string{}))
	c.Assert(err, IsNil)
}

func (s *testEvictSlowStoreSuite) TestEvictSlowStore(c *C) {
	defer s.cancel()
	storeInfo := s.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	s.tc.PutStore(newStoreInfo)
	c.Assert(s.es.IsScheduleAllowed(s.tc), IsTrue)
	// Add evict leader scheduler to store 1
	op := s.es.Schedule(s.tc)
	testutil.CheckMultiTargetTransferLeader(c, op[0], operator.OpLeader, 1, []uint64{2})
	c.Assert(op[0].Desc(), Equals, EvictSlowStoreType)
	// Cannot balance leaders to store 1
	op = s.bs.Schedule(s.tc)
	c.Assert(op, HasLen, 0)
	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})
	s.tc.PutStore(newStoreInfo)
	// Evict leader scheduler of store 1 should be removed, then leader can be balanced to store 1
	c.Check(s.es.Schedule(s.tc), IsNil)
	op = s.bs.Schedule(s.tc)
	testutil.CheckTransferLeader(c, op[0], operator.OpLeader, 2, 1)

	// no slow store need to evict.
	op = s.es.Schedule(s.tc)
	c.Assert(op, IsNil)

	es2, ok := s.es.(*evictSlowStoreScheduler)
	c.Assert(ok, IsTrue)
	c.Assert(es2.conf.evictStore(), Equals, uint64(0))

	// check the value from storage.
	sches, vs, err := es2.conf.storage.LoadAllScheduleConfig()
	c.Assert(err, IsNil)
	valueStr := ""
	for id, sche := range sches {
		if strings.EqualFold(sche, EvictSlowStoreName) {
			valueStr = vs[id]
		}
	}

	var persistValue evictSlowStoreSchedulerConfig
	err = json.Unmarshal([]byte(valueStr), &persistValue)
	c.Assert(err, IsNil)
	c.Assert(persistValue.EvictedStores, DeepEquals, es2.conf.EvictedStores)
	c.Assert(persistValue.evictStore(), Equals, uint64(0))
}

func (s *testEvictSlowStoreSuite) TestEvictSlowStorePrepare(c *C) {
	defer s.cancel()
	es2, ok := s.es.(*evictSlowStoreScheduler)
	c.Assert(ok, IsTrue)
	c.Assert(es2.conf.evictStore(), Equals, uint64(0))
	// prepare with no evict store.
	s.es.Prepare(s.tc)

	es2.conf.setStoreAndPersist(1)
	c.Assert(es2.conf.evictStore(), Equals, uint64(1))
	// prepare with evict store.
	s.es.Prepare(s.tc)
}

func (s *testEvictSlowStoreSuite) TestEvictSlowStorePersistFail(c *C) {
	defer s.cancel()
	persisFail := "github.com/tikv/pd/server/schedulers/persistFail"
	c.Assert(failpoint.Enable(persisFail, "return(true)"), IsNil)
	storeInfo := s.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	s.tc.PutStore(newStoreInfo)
	c.Assert(s.es.IsScheduleAllowed(s.tc), IsTrue)
	// Add evict leader scheduler to store 1
	op := s.es.Schedule(s.tc)
	c.Assert(op, IsNil)
	c.Assert(failpoint.Disable(persisFail), IsNil)
	op = s.es.Schedule(s.tc)
	c.Assert(op, NotNil)
}
