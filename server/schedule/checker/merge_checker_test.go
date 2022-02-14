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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/labeler"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
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

func (s *testMergeCheckerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testMergeCheckerSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testMergeCheckerSuite) SetUpTest(c *C) {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(s.ctx, cfg)
	s.cluster.SetMaxMergeRegionSize(2)
	s.cluster.SetMaxMergeRegionKeys(2)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		config.RejectLeader: {{Key: "reject", Value: "leader"}},
	})
	s.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for storeID, labels := range stores {
		s.cluster.PutStoreWithLabels(storeID, labels...)
	}
	s.regions = []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 1, 1, []uint64{109, 4}, []uint64{109, 4}),
	}

	for _, region := range s.regions {
		s.cluster.PutRegion(region)
	}
	s.mc = NewMergeChecker(s.ctx, s.cluster)
}

func (s *testMergeCheckerSuite) TestBasic(c *C) {
	s.cluster.SetSplitMergeInterval(0)

	// should with same peer count
	ops := s.mc.Check(s.regions[0])
	c.Assert(ops, IsNil)
	// The size should be small enough.
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, IsNil)
	// target region size is too large
	s.cluster.PutRegion(s.regions[1].Clone(core.SetApproximateSize(600)))
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	// change the size back
	s.cluster.PutRegion(s.regions[1].Clone(core.SetApproximateSize(200)))
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// Check merge with previous region.
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())

	// change the max region size
	s.mc.config = config.NewImmutableConfig(config.NewConfig(), config.WithMaxRegionSize(200))
	s.cluster.PutRegion(s.regions[1].Clone(core.SetApproximateSize(200)))
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	s.mc.config = config.NewImmutableConfig(config.NewConfig(), config.WithMaxRegionSize(96))
	// Test the peer store check.
	store := s.cluster.GetStore(1)
	c.Assert(store, NotNil)
	// Test the peer store is deleted.
	s.cluster.DeleteStore(store)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	// Test the store is normal.
	s.cluster.PutStore(store)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())
	// Test the store is offline.
	s.cluster.SetStoreOffline(store.GetID())
	ops = s.mc.Check(s.regions[2])
	// Only target region have a peer on the offline store,
	// so it's not ok to merge.
	c.Assert(ops, IsNil)
	// Test the store is up.
	s.cluster.SetStoreUp(store.GetID())
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())
	store = s.cluster.GetStore(5)
	c.Assert(store, NotNil)
	// Test the peer store is deleted.
	s.cluster.DeleteStore(store)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	// Test the store is normal.
	s.cluster.PutStore(store)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())
	// Test the store is offline.
	s.cluster.SetStoreOffline(store.GetID())
	ops = s.mc.Check(s.regions[2])
	// Both regions have peers on the offline store,
	// so it's ok to merge.
	c.Assert(ops, NotNil)
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())
	// Test the store is up.
	s.cluster.SetStoreUp(store.GetID())
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[1].GetID())

	// Enable one way merge
	s.cluster.SetEnableOneWayMerge(true)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)
	s.cluster.SetEnableOneWayMerge(false)

	// Make up peers for next region.
	s.regions[3] = s.regions[3].Clone(core.WithAddPeer(&metapb.Peer{Id: 110, StoreId: 1}), core.WithAddPeer(&metapb.Peer{Id: 111, StoreId: 2}))
	s.cluster.PutRegion(s.regions[3])
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	// Now it merges to next region.
	c.Assert(ops[0].RegionID(), Equals, s.regions[2].GetID())
	c.Assert(ops[1].RegionID(), Equals, s.regions[3].GetID())

	// merge cannot across rule key.
	s.cluster.SetEnablePlacementRules(true)
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
	s.cluster.RuleManager.DeleteRule("pd", "test")

	//  check 'merge_option' label
	s.cluster.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID:       "test",
		Labels:   []labeler.RegionLabel{{Key: mergeOptionLabel, Value: mergeOptionValueDeny}},
		RuleType: labeler.KeyRange,
		Data:     makeKeyRanges("", "74"),
	})
	ops = s.mc.Check(s.regions[0])
	c.Assert(ops, HasLen, 0)
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, HasLen, 0)

	// Skip recently split regions.
	s.cluster.SetSplitMergeInterval(time.Hour)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, IsNil)

	s.mc.startTime = time.Now().Add(-2 * time.Hour)
	ops = s.mc.Check(s.regions[2])
	c.Assert(ops, NotNil)
	ops = s.mc.Check(s.regions[3])
	c.Assert(ops, NotNil)

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
	s.cluster.SetSplitMergeInterval(0)
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

func (s *testMergeCheckerSuite) TestStoreLimitWithMerge(c *C) {
	cfg := config.NewTestOptions()
	tc := mockcluster.NewCluster(s.ctx, cfg)
	tc.SetMaxMergeRegionSize(2)
	tc.SetMaxMergeRegionKeys(2)
	tc.SetSplitMergeInterval(0)
	regions := []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 10, 10, []uint64{109, 4}, []uint64{109, 4}),
	}

	for i := uint64(1); i <= 6; i++ {
		tc.AddLeaderStore(i, 10)
	}

	for _, region := range regions {
		tc.PutRegion(region)
	}

	mc := NewMergeChecker(s.ctx, tc)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := schedule.NewOperatorController(s.ctx, tc, stream)

	regions[2] = regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 109, StoreId: 2},
			{Id: 110, StoreId: 3},
			{Id: 111, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}),
	)

	// set to a small rate to reduce unstable possibility.
	tc.SetAllStoresLimit(storelimit.AddPeer, 0.0000001)
	tc.SetAllStoresLimit(storelimit.RemovePeer, 0.0000001)
	tc.PutRegion(regions[2])
	// The size of Region is less or equal than 1MB.
	for i := 0; i < 50; i++ {
		ops := mc.Check(regions[2])
		c.Assert(ops, NotNil)
		c.Assert(oc.AddOperator(ops...), IsTrue)
		for _, op := range ops {
			oc.RemoveOperator(op)
		}
	}
	regions[2] = regions[2].Clone(
		core.SetApproximateSize(2),
		core.SetApproximateKeys(2),
	)
	tc.PutRegion(regions[2])
	// The size of Region is more than 1MB but no more than 20MB.
	for i := 0; i < 5; i++ {
		ops := mc.Check(regions[2])
		c.Assert(ops, NotNil)
		c.Assert(oc.AddOperator(ops...), IsTrue)
		for _, op := range ops {
			oc.RemoveOperator(op)
		}
	}
	{
		ops := mc.Check(regions[2])
		c.Assert(ops, NotNil)
		c.Assert(oc.AddOperator(ops...), IsFalse)
	}
}

func (s *testMergeCheckerSuite) TestCache(c *C) {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(s.ctx, cfg)
	s.cluster.SetMaxMergeRegionSize(2)
	s.cluster.SetMaxMergeRegionKeys(2)
	s.cluster.SetSplitMergeInterval(time.Hour)
	s.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
	}
	for storeID, labels := range stores {
		s.cluster.PutStoreWithLabels(storeID, labels...)
	}
	s.regions = []*core.RegionInfo{
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
	}

	for _, region := range s.regions {
		s.cluster.PutRegion(region)
	}

	s.mc = NewMergeChecker(s.ctx, s.cluster)

	ops := s.mc.Check(s.regions[1])
	c.Assert(ops, IsNil)
	s.cluster.SetSplitMergeInterval(0)
	time.Sleep(time.Second)
	ops = s.mc.Check(s.regions[1])
	c.Assert(ops, NotNil)
}

func makeKeyRanges(keys ...string) []interface{} {
	var res []interface{}
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]interface{}{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}

func newRegionInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.RegionInfo {
	prs := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, &metapb.Peer{Id: peer[0], StoreId: peer[1]})
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			Peers:    prs,
		},
		&metapb.Peer{Id: leader[0], StoreId: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}
