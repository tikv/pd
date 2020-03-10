// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"go.uber.org/goleak"
)

func TestMergeChecker(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&testMergeCheckerSuite{})

type testMergeCheckerSuite struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *mockcluster.Cluster
	mc      *MergeChecker
	regions []*core.RegionInfo
}

func (s *testMergeCheckerSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.MaxMergeRegionSize = 2
	cfg.MaxMergeRegionKeys = 2
	cfg.LabelProperties = map[string][]*metapb.StoreLabel{
		opt.RejectLeader: {{Key: "reject", Value: "leader"}},
	}
	s.cluster = mockcluster.NewCluster(cfg)
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for storeID, labels := range stores {
		s.cluster.PutStoreWithLabels(storeID, labels...)
	}
	s.regions = []*core.RegionInfo{
		core.NewRegionInfo(
			&metapb.Region{
				Id:       1,
				StartKey: []byte(""),
				EndKey:   []byte("a"),
				Peers: []*metapb.Peer{
					{Id: 101, StoreId: 1},
					{Id: 102, StoreId: 2},
				},
			},
			&metapb.Peer{Id: 101, StoreId: 1},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       2,
				StartKey: []byte("a"),
				EndKey:   []byte("t"),
				Peers: []*metapb.Peer{
					{Id: 103, StoreId: 1},
					{Id: 104, StoreId: 4},
					{Id: 105, StoreId: 5},
				},
			},
			&metapb.Peer{Id: 104, StoreId: 4},
			core.SetApproximateSize(200),
			core.SetApproximateKeys(200),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       3,
				StartKey: []byte("t"),
				EndKey:   []byte("x"),
				Peers: []*metapb.Peer{
					{Id: 106, StoreId: 2},
					{Id: 107, StoreId: 5},
					{Id: 108, StoreId: 6},
				},
			},
			&metapb.Peer{Id: 108, StoreId: 6},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       4,
				StartKey: []byte("x"),
				EndKey:   []byte(""),
				Peers: []*metapb.Peer{
					{Id: 109, StoreId: 4},
				},
			},
			&metapb.Peer{Id: 109, StoreId: 4},
			core.SetApproximateSize(10),
			core.SetApproximateKeys(10),
		),
	}

	for _, region := range s.regions {
		s.cluster.PutRegion(region)
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mc = NewMergeChecker(s.ctx, s.cluster)
}

func (s *testMergeCheckerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testMergeCheckerSuite) TestBasic(c *C) {
	s.cluster.ScheduleOptions.SplitMergeInterval = 0

	// should with same peer count
	ops := s.mc.Check(s.regions[0])
	c.Assert(ops, IsNil)
	// The size should be small enough.
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, IsNil)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// Check merge with previous region.
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())

	// Enable one way merge
	s.cluster.ScheduleOptions.EnableOneWayMerge = true
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	s.cluster.ScheduleOptions.EnableOneWayMerge = false

	// Make up peers for next region.
	s.regions[3] = s.regions[3].Clone(core.WithAddPeer(&metapb.Peer{Id: 110, StoreId: 1}), core.WithAddPeer(&metapb.Peer{Id: 111, StoreId: 2}))
	s.cluster.PutRegion(s.regions[3])
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// Now it merges to next region.
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[3].GetID())

	// merge cannot across rule key.
	s.cluster.EnablePlacementRules = true
	s.cluster.RuleManager.SetRule(&placement.Rule{
		GroupID:     "pd",
		ID:          "test",
		Index:       1,
		Override:    true,
		StartKeyHex: hex.EncodeToString([]byte("x")),
		EndKeyHex:   hex.EncodeToString([]byte("z")),
		Role:        placement.Voter,
		Count:       3,
	})
	// region 2 can only merge with previous region now.
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())
	s.cluster.RuleManager.DeleteRule("test", "test")

	// Skip recently split regions.
	s.cluster.ScheduleOptions.SplitMergeInterval = time.Hour
	s.mc.RecordRegionSplit([]uint64{s.regions[2].GetID()})
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	ops = s.mc.Check(s.regions[3])
	c.Assert(ops, IsNil)
}

func (s *testMergeCheckerSuite) checkSteps(c *C, op *operator.Operator, steps []operator.OpStep) {
	c.Assert(op.Kind()&operator.OpMerge, Not(Equals), 0)
	c.Assert(steps, NotNil)
	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		switch op.Step(i).(type) {
		case operator.AddLearner:
			c.Assert(op.Step(i).(operator.AddLearner).ToStore, Equals, steps[i].(operator.AddLearner).ToStore)
		case operator.PromoteLearner:
			c.Assert(op.Step(i).(operator.PromoteLearner).ToStore, Equals, steps[i].(operator.PromoteLearner).ToStore)
		case operator.TransferLeader:
			c.Assert(op.Step(i).(operator.TransferLeader).FromStore, Equals, steps[i].(operator.TransferLeader).FromStore)
			c.Assert(op.Step(i).(operator.TransferLeader).ToStore, Equals, steps[i].(operator.TransferLeader).ToStore)
		case operator.RemovePeer:
			c.Assert(op.Step(i).(operator.RemovePeer).FromStore, Equals, steps[i].(operator.RemovePeer).FromStore)
		case operator.MergeRegion:
			c.Assert(op.Step(i).(operator.MergeRegion).IsPassive, Equals, steps[i].(operator.MergeRegion).IsPassive)
		default:
			c.Fatal("unknown operator step type")
		}
	}
}

func (s *testMergeCheckerSuite) TestMatchPeers(c *C) {
	// partial store overlap not including leader
	ops := s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	s.checkSteps(c, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.TransferLeader{FromStore: 6, ToStore: 5},
		operator.RemovePeer{FromStore: 6},
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// partial store overlap including leader
	newRegion := s.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 106, StoreId: 1},
			{Id: 107, StoreId: 5},
			{Id: 108, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 106, StoreId: 1}),
	)
	s.regions[2] = newRegion
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.RemovePeer{FromStore: 6},
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores overlap
	s.regions[2] = s.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 106, StoreId: 1},
		{Id: 107, StoreId: 5},
		{Id: 108, StoreId: 4},
	}))
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores not overlap
	s.regions[2] = s.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 109, StoreId: 2},
		{Id: 110, StoreId: 3},
		{Id: 111, StoreId: 6},
	}), core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}))
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.RemovePeer{FromStore: 6},
		operator.AddLearner{ToStore: 5},
		operator.PromoteLearner{ToStore: 5},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// no overlap with reject leader label
	s.regions[1] = s.regions[1].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 112, StoreId: 7},
			{Id: 113, StoreId: 8},
			{Id: 114, StoreId: 1},
		}),
		core.WithLeader(&metapb.Peer{Id: 114, StoreId: 1}),
	)
	s.cluster.PutRegion(s.regions[1])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},

		operator.AddLearner{ToStore: 7},
		operator.PromoteLearner{ToStore: 7},
		operator.RemovePeer{FromStore: 6},

		operator.AddLearner{ToStore: 8},
		operator.PromoteLearner{ToStore: 8},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},

		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// overlap with reject leader label
	s.regions[1] = s.regions[1].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 115, StoreId: 7},
			{Id: 116, StoreId: 8},
			{Id: 117, StoreId: 1},
		}),
		core.WithLeader(&metapb.Peer{Id: 117, StoreId: 1}),
	)
	s.regions[2] = s.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 118, StoreId: 7},
			{Id: 119, StoreId: 3},
			{Id: 120, StoreId: 2},
		}),
		core.WithLeader(&metapb.Peer{Id: 120, StoreId: 2}),
	)
	s.cluster.PutRegion(s.regions[1])
	ops = s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},
		operator.AddLearner{ToStore: 8},
		operator.PromoteLearner{ToStore: 8},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})
}

var _ = Suite(&testSplitMergeSuite{})

type testSplitMergeSuite struct{}

func (s *testMergeCheckerSuite) TestCache(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.MaxMergeRegionSize = 2
	cfg.MaxMergeRegionKeys = 2
	cfg.SplitMergeInterval = time.Hour
	s.cluster = mockcluster.NewCluster(cfg)
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
	}
	for storeID, labels := range stores {
		s.cluster.PutStoreWithLabels(storeID, labels...)
	}
	s.regions = []*core.RegionInfo{
		core.NewRegionInfo(
			&metapb.Region{
				Id:       2,
				StartKey: []byte("a"),
				EndKey:   []byte("t"),
				Peers: []*metapb.Peer{
					{Id: 103, StoreId: 1},
					{Id: 104, StoreId: 4},
					{Id: 105, StoreId: 5},
				},
			},
			&metapb.Peer{Id: 104, StoreId: 4},
			core.SetApproximateSize(200),
			core.SetApproximateKeys(200),
		),
		core.NewRegionInfo(
			&metapb.Region{
				Id:       3,
				StartKey: []byte("t"),
				EndKey:   []byte("x"),
				Peers: []*metapb.Peer{
					{Id: 106, StoreId: 2},
					{Id: 107, StoreId: 5},
					{Id: 108, StoreId: 6},
				},
			},
			&metapb.Peer{Id: 108, StoreId: 6},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
	}

	for _, region := range s.regions {
		s.cluster.PutRegion(region)
	}

	s.mc = NewMergeChecker(s.ctx, s.cluster)

	ops := s.mc.Check(s.regions[1])
	c.Assert(ops, IsNil)
	s.cluster.SplitMergeInterval = 0
	time.Sleep(time.Second)
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, NotNil)
}
