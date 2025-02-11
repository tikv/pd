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
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
)

type balanceRangeSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *operator.Controller
}

func TestBalanceRangeSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceRangeSchedulerTestSuite))
}

func (suite *balanceRangeSchedulerTestSuite) SetupTest() {
	suite.cancel, _, suite.tc, suite.oc = prepareSchedulersTest()
}

func (suite *balanceRangeSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func TestGetPeers(t *testing.T) {
	re := require.New(t)
	learner := &metapb.Peer{StoreId: 1, Id: 1, Role: metapb.PeerRole_Learner}
	leader := &metapb.Peer{StoreId: 2, Id: 2}
	follower1 := &metapb.Peer{StoreId: 3, Id: 3}
	follower2 := &metapb.Peer{StoreId: 4, Id: 4}
	region := core.NewRegionInfo(&metapb.Region{Id: 100, Peers: []*metapb.Peer{
		leader, follower1, follower2, learner,
	}}, leader, core.WithLearners([]*metapb.Peer{learner}))
	for _, v := range []struct {
		role  string
		peers []*metapb.Peer
	}{
		{
			role:  "leader",
			peers: []*metapb.Peer{leader},
		},
		{
			role:  "follower",
			peers: []*metapb.Peer{follower1, follower2},
		},
		{
			role:  "learner",
			peers: []*metapb.Peer{learner},
		},
	} {
		role := NewRole(v.role)
		re.Equal(v.peers, role.getPeers(region))
	}
}

func TestBalanceRangeShouldBalance(t *testing.T) {
	re := require.New(t)
	for _, v := range []struct {
		sourceScore   int64
		targetScore   int64
		shouldBalance bool
	}{
		{
			100,
			10,
			true,
		},
		{
			10,
			10,
			false,
		},
	} {
		plan := balanceRangeSchedulerPlan{
			sourceScore: v.sourceScore,
			targetScore: v.targetScore,
		}
		re.Equal(plan.shouldBalance(balanceRangeName), v.shouldBalance)
	}
}

//func TestBalanceRangePrepare(t *testing.T) {
//	re := require.New(t)
//	cancel, _, tc, oc := prepareSchedulersTest()
//	defer cancel()
//	// args: [role, engine, timeout, range1, range2, ...]
//}

func TestBalanceRangeSchedule(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	// args: [role, engine, timeout, range1, range2, ...]
	scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader", "tikv", "1h", "100", "200"}))
	re.Nil(err)
	op, _ := scheduler.Schedule(tc, true)
	re.Empty(op)
	for i := 0; i <= 4; i++ {
		tc.AddLeaderStore(uint64(int64(i)), i*10)
	}
	tc.AddLeaderRegionWithRange(1, "100", "100", 1, 2, 3, 4)
	tc.AddLeaderRegionWithRange(2, "110", "120", 1, 2, 3, 4)
	op, _ = scheduler.Schedule(tc, true)
	re.NotEmpty(op)
}
