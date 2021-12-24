// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"context"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	_ "github.com/tikv/pd/server/schedulers"
)

var _ = Suite(&testClusterWorkerSuite{})

type testClusterWorkerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testClusterWorkerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testClusterWorkerSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func mockRegionPeer(idAllc *mockid.IDAllocator, voters []uint64) []*metapb.Peer {
	rst := make([]*metapb.Peer, len(voters))
	for i, v := range voters {
		id, _ := idAllc.Alloc()
		rst[i] = &metapb.Peer{
			Id:      id,
			StoreId: v,
			Role:    metapb.PeerRole_Voter,
		}
	}
	return rst
}

func (s *testClusterWorkerSuite) TestReportSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	idAllc := mockid.NewIDAllocator()
	storage := core.NewStorage(kv.NewMemoryKV())
	cluster := newTestRaftCluster(s.ctx, idAllc, opt, storage, core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	origin := &metapb.Region{Id: 2, StartKey: []byte("a"), EndKey: []byte("d"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}}
	origin.Peers = mockRegionPeer(idAllc, []uint64{1, 2, 3})

	c.Assert(cluster.putRegion(core.NewRegionInfo(origin, nil)), IsNil)
	old := cluster.GetRegion(2)
	c.Assert(core.HexRegionKeyStr(old.GetStartKey()), Equals, core.HexRegionKeyStr(origin.GetStartKey()))

	left := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("c"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1}}
	right := &metapb.Region{Id: 2, StartKey: []byte("c"), EndKey: []byte("d"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1}}
	c.Assert(cluster.GetRegion(1), IsNil)
	// case1: left endKey should equal to right startKey
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: proto.Clone(right).(*metapb.Region), Right: proto.Clone(left).(*metapb.Region)})
	c.Assert(err, NotNil)
	c.Assert(cluster.GetRegion(1), IsNil)
	old = cluster.GetRegion(2)
	c.Assert(old, NotNil)
	c.Assert(core.HexRegionKeyStr(old.GetStartKey()), Equals, core.HexRegionKeyStr(origin.GetStartKey()))
	// case2: peer can not be nil
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: proto.Clone(left).(*metapb.Region), Right: proto.Clone(right).(*metapb.Region)})
	c.Assert(err, IsNil)
	c.Assert(cluster.GetRegion(1), IsNil)
	old = cluster.GetRegion(2)
	c.Assert(old, NotNil)
	c.Assert(core.HexRegionKeyStr(old.GetStartKey()), Equals, core.HexRegionKeyStr(origin.GetStartKey()))

	// case3: peer can handler well
	// |--id--|--start_key--|--end_key--| --leader_id--|
	// | 1   |  "a"          |  "c"     |   1          |
	// | 2   |  "c"          |  "d"     |   1          |
	left.Peers = mockRegionPeer(idAllc, []uint64{3, 2, 1})
	right.Peers = mockRegionPeer(idAllc, []uint64{3, 2, 1})
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: proto.Clone(left).(*metapb.Region), Right: proto.Clone(right).(*metapb.Region)})
	c.Assert(err, IsNil)
	a := cluster.GetRegion(1)
	c.Assert(a, NotNil)
	c.Assert(core.HexRegionKeyStr(a.GetStartKey()), Equals, core.HexRegionKeyStr(left.GetStartKey()))
	old = cluster.GetRegion(2)
	c.Assert(old, NotNil)
	c.Assert(core.HexRegionKeyStr(old.GetStartKey()), Equals, core.HexRegionKeyStr(right.GetStartKey()))
	c.Assert(a.GetLeader().GetStoreId(), Equals, old.GetLeader().GetStoreId())

	// case4: left peer will ignore if region exist cache, but right peer will check epoch
	left.StartKey = []byte("b")
	right.RegionEpoch.ConfVer++
	right.RegionEpoch.Version++
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: proto.Clone(left).(*metapb.Region), Right: proto.Clone(right).(*metapb.Region)})
	c.Assert(err, IsNil)
	a = cluster.GetRegion(1)
	c.Assert(core.HexRegionKeyStr(a.GetStartKey()), Not(Equals), core.HexRegionKeyStr(left.GetStartKey()))
	old = cluster.GetRegion(2)
	c.Assert(old.GetRegionEpoch().GetVersion(), Equals, right.GetRegionEpoch().GetVersion())
	c.Assert(old.GetRegionEpoch().GetConfVer(), Equals, right.GetRegionEpoch().GetConfVer())
	c.Assert(a.GetLeader().GetStoreId(), Equals, old.GetLeader().GetStoreId())
}

func (s *testClusterWorkerSuite) TestReportBatchSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	idAllc := mockid.NewIDAllocator()
	cluster := newTestRaftCluster(s.ctx, idAllc, opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	regions := []*metapb.Region{
		{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), Peers: mockRegionPeer(idAllc, []uint64{1, 2, 3})},
		{Id: 2, StartKey: []byte("a"), EndKey: []byte("b"), Peers: mockRegionPeer(idAllc, []uint64{1, 2, 3})},
		{Id: 3, StartKey: []byte("b"), EndKey: []byte("c"), Peers: mockRegionPeer(idAllc, []uint64{1, 2, 3})},
		{Id: 4, StartKey: []byte("c"), EndKey: []byte("d"), Peers: mockRegionPeer(idAllc, []uint64{1, 2, 3})},
	}
	c.Assert(cluster.GetRegion(1), IsNil)
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: regions})
	c.Assert(err, IsNil)
	for _, id := range []uint64{1, 2, 3, 4} {
		c.Assert(cluster.GetRegion(id), NotNil)
	}
}
