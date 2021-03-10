// Copyright 2019 TiKV Project Authors.
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

package schedule

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	"github.com/tikv/pd/pkg/mock/mockoption"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/namespace"
)

func TestChecker(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMergeCheckerSuite{})

type testMergeCheckerSuite struct {
	cluster *mockcluster.Cluster
	mc      *MergeChecker
	regions []*core.RegionInfo
}

func (s *testMergeCheckerSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.MaxMergeRegionSize = 2
	cfg.MaxMergeRegionKeys = 2
	s.cluster = mockcluster.NewCluster(cfg)
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
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
	}

	for _, region := range s.regions {
		s.cluster.PutRegion(region)
	}

	s.mc = NewMergeChecker(s.cluster, namespace.DefaultClassifier)
}

func (s *testMergeCheckerSuite) TestBasic(c *C) {
	s.cluster.ScheduleOptions.SplitMergeInterval = time.Hour

	// should with same peer count
	ops := s.mc.Check(s.regions[0])
	c.Assert(ops, IsNil)
	// The size should be small enough.
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, IsNil)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	for _, op := range ops {
		op.startTime = time.Now()
		op.startTime = op.startTime.Add(-LeaderOperatorWaitTime - time.Second)
		c.Assert(op.IsTimeout(), IsFalse)
		op.startTime = op.startTime.Add(-RegionOperatorWaitTime - time.Second)
		c.Assert(op.IsTimeout(), IsTrue)
	}
	// Check merge with previous region.
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())

	// Enable one way merge
	s.cluster.EnableOneWayMerge = true
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	s.cluster.EnableOneWayMerge = false

	// Make up peers for next region.
	s.regions[3] = s.regions[3].Clone(core.WithAddPeer(&metapb.Peer{Id: 110, StoreId: 1}), core.WithAddPeer(&metapb.Peer{Id: 111, StoreId: 2}))
	s.cluster.PutRegion(s.regions[3])
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// Now it merges to next region.
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[3].GetID())

	// Skip recently split regions.
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	ops = s.mc.Check(s.regions[3])
	c.Assert(ops, NotNil)

	s.mc.RecordRegionSplit(s.regions[2].GetID())
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	ops = s.mc.Check(s.regions[3])
	c.Assert(ops, IsNil)
}

func (s *testMergeCheckerSuite) checkSteps(c *C, op *Operator, steps []OperatorStep) {
	c.Assert(op.Kind()&OpMerge, Not(Equals), 0)
	c.Assert(steps, NotNil)
	c.Assert(op.Len(), Equals, len(steps))
	for i := range steps {
		c.Assert(op.Step(i), DeepEquals, steps[i])
	}
}

func (s *testMergeCheckerSuite) TestMatchPeers(c *C) {
	// partial store overlap not including leader
	ops := s.mc.Check(s.regions[2])
	s.checkSteps(c, ops[0], []OperatorStep{
		TransferLeader{FromStore: 6, ToStore: 5},
		AddLearner{ToStore: 1, PeerID: 1},
		PromoteLearner{ToStore: 1, PeerID: 1},
		RemovePeer{FromStore: 2},
		AddLearner{ToStore: 4, PeerID: 2},
		PromoteLearner{ToStore: 4, PeerID: 2},
		RemovePeer{FromStore: 6},
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
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
	s.checkSteps(c, ops[0], []OperatorStep{
		AddLearner{ToStore: 4, PeerID: 3},
		PromoteLearner{ToStore: 4, PeerID: 3},
		RemovePeer{FromStore: 6},
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
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
	s.checkSteps(c, ops[0], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
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
	s.checkSteps(c, ops[0], []OperatorStep{
		AddLearner{ToStore: 1, PeerID: 4},
		PromoteLearner{ToStore: 1, PeerID: 4},
		RemovePeer{FromStore: 3},
		AddLearner{ToStore: 4, PeerID: 5},
		PromoteLearner{ToStore: 4, PeerID: 5},
		RemovePeer{FromStore: 6},
		AddLearner{ToStore: 5, PeerID: 6},
		PromoteLearner{ToStore: 5, PeerID: 6},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	s.checkSteps(c, ops[1], []OperatorStep{
		MergeRegion{
			FromRegion: s.regions[2].GetMeta(),
			ToRegion:   s.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})
}

func (s *testMergeCheckerSuite) TestStorelimit(c *C) {
	oc := NewOperatorController(s.cluster, mockhbstream.NewHeartbeatStream())
	s.cluster.ScheduleOptions.SplitMergeInterval = time.Hour
	s.cluster.ScheduleOptions.StoreBalanceRate = 60
	s.regions[2] = s.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 109, StoreId: 2},
			{Id: 110, StoreId: 3},
			{Id: 111, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}),
	)
	s.cluster.PutRegion(s.regions[2])
	ops := s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// The size of Region is less or equal than 1MB.
	for i := 0; i < 50; i++ {
		c.Assert(oc.AddOperator(ops...), IsTrue)
		for _, op := range ops {
			c.Assert(oc.RemoveOperator(op), IsTrue)
		}
	}
	s.regions[2] = s.regions[2].Clone(
		core.SetApproximateSize(2),
		core.SetApproximateKeys(2),
	)
	s.cluster.PutRegion(s.regions[2])
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// The size of Region is more than 1MB but no more than 20MB.
	for i := 0; i < 5; i++ {
		c.Assert(oc.AddOperator(ops...), IsTrue)
		for _, op := range ops {
			c.Assert(oc.RemoveOperator(op), IsTrue)
		}
	}
	c.Assert(oc.AddOperator(ops...), IsFalse)
}
