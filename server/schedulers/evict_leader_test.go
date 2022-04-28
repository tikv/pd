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
	"bytes"
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage"
)

var _ = Suite(&testEvictLeaderSuite{})

type testEvictLeaderSuite struct{}

func (s *testEvictLeaderSuite) TestEvictLeader(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add regions 1, 2, 3 with leaders in stores 1, 2, 3
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 1)
	tc.AddLeaderRegion(3, 3, 1)

	sl, err := schedule.CreateScheduler(EvictLeaderType, schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(EvictLeaderType, []string{"1"}))
	c.Assert(err, IsNil)
	c.Assert(sl.IsScheduleAllowed(tc), IsTrue)
	op := sl.Schedule(tc)
	testutil.CheckMultiTargetTransferLeader(c, op[0], operator.OpLeader, 1, []uint64{2, 3})
	c.Assert(op[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 1, []uint64{2, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})), IsFalse)
	c.Assert(op[0].Step(0).(operator.TransferLeader).IsFinish(tc.MockRegionInfo(1, 2, []uint64{1, 3}, []uint64{}, &metapb.RegionEpoch{ConfVer: 0, Version: 0})), IsTrue)
}

func (s *testEvictLeaderSuite) TestEvictLeaderWithUnhealthyPeer(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	sl, err := schedule.CreateScheduler(EvictLeaderType, schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(EvictLeaderType, []string{"1"}))
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add region 1, which has 3 peers. 1 is leader. 2 is healthy or pending, 3 is healthy or down.
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.MockRegionInfo(1, 1, []uint64{2, 3}, nil, nil)
	withDownPeer := core.WithDownPeers([]*pdpb.PeerStats{{
		Peer:        region.GetPeers()[2],
		DownSeconds: 1000,
	}})
	withPendingPeer := core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[1]})

	// only pending
	tc.PutRegion(region.Clone(withPendingPeer))
	op := sl.Schedule(tc)
	testutil.CheckMultiTargetTransferLeader(c, op[0], operator.OpLeader, 1, []uint64{3})
	// only down
	tc.PutRegion(region.Clone(withDownPeer))
	op = sl.Schedule(tc)
	testutil.CheckMultiTargetTransferLeader(c, op[0], operator.OpLeader, 1, []uint64{2})
	// pending + down
	tc.PutRegion(region.Clone(withPendingPeer, withDownPeer))
	c.Assert(sl.Schedule(tc), HasLen, 0)
}

func (s *testEvictLeaderSuite) TestConfigClone(c *C) {
	emptyConf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange)}
	con2 := emptyConf.Clone()
	c.Assert(emptyConf.getKeyRangesByID(1), IsNil)
	c.Assert(con2.BuildWithArgs([]string{"1"}), IsNil)
	c.Assert(con2.getKeyRangesByID(1), NotNil)
	c.Assert(emptyConf.getKeyRangesByID(1), IsNil)

	con3 := con2.Clone()
	con3.StoreIDWithRanges[1], _ = getKeyRanges([]string{"a", "b", "c", "d"})
	c.Assert(emptyConf.getKeyRangesByID(1), IsNil)
	c.Assert(len(con3.getRanges(1)) == len(con2.getRanges(1)), IsFalse)

	con4 := con3.Clone()
	c.Assert(bytes.Equal(con4.StoreIDWithRanges[1][0].StartKey, con3.StoreIDWithRanges[1][0].StartKey), IsTrue)
	con4.StoreIDWithRanges[1][0].StartKey = []byte("aaa")
	c.Assert(bytes.Equal(con4.StoreIDWithRanges[1][0].StartKey, con3.StoreIDWithRanges[1][0].StartKey), IsFalse)
}
