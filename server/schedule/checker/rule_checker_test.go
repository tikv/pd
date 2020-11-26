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

package checker

import (
	"encoding/hex"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testRuleCheckerSuite{})

type testRuleCheckerSuite struct {
	cluster     *mockcluster.Cluster
	ruleManager *placement.RuleManager
	rc          *RuleChecker
}

func (s *testRuleCheckerSuite) SetUpTest(c *C) {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.DisableFeature(versioninfo.JointConsensus)
	s.cluster.SetEnablePlacementRules(true)
	s.ruleManager = s.cluster.RuleManager
	s.rc = NewRuleChecker(s.cluster, s.ruleManager)
}

func (s *testRuleCheckerSuite) TestFixRange(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test",
		StartKeyHex: "AA",
		EndKeyHex:   "FF",
		Role:        placement.Voter,
		Count:       1,
	})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Len(), Equals, 1)
	splitKeys := op.Step(0).(operator.SplitRegion).SplitKeys
	c.Assert(hex.EncodeToString(splitKeys[0]), Equals, "aa")
	c.Assert(hex.EncodeToString(splitKeys[1]), Equals, "ff")
}

func (s *testRuleCheckerSuite) TestAddRulePeer(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "add-rule-peer")
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(3))
}

func (s *testRuleCheckerSuite) TestAddRulePeerWithIsolationLevel(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "pd",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "rack", "host"},
		IsolationLevel: "zone",
	})
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "pd",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "rack", "host"},
		IsolationLevel: "rack",
	})
	op = s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "add-rule-peer")
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(4))
}

func (s *testRuleCheckerSuite) TestFixPeer(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderStore(4, 1)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
	s.cluster.SetStoreDown(2)
	r := s.cluster.GetRegion(1)
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 60000}}))
	op = s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "replace-rule-down-peer")
	var add operator.AddLearner
	c.Assert(op.Step(0), FitsTypeOf, add)
	s.cluster.SetStoreUp(2)
	s.cluster.SetStoreOffline(2)
	op = s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "replace-rule-offline-peer")
	c.Assert(op.Step(0), FitsTypeOf, add)
}

func (s *testRuleCheckerSuite) TestFixOrphanPeers(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderStore(4, 1)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3, 4)
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "remove-orphan-peer")
	c.Assert(op.Step(0).(operator.RemovePeer).FromStore, Equals, uint64(4))
}

func (s *testRuleCheckerSuite) TestFixOrphanPeers2(c *C) {
	// check orphan peers can only be handled when all rules are satisfied.
	s.cluster.AddLabelsStore(1, 1, map[string]string{"foo": "bar"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"foo": "bar"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"foo": "baz"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Leader,
		Count:    2,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "foo", Op: "in", Values: []string{"baz"}},
		},
	})
	s.cluster.SetStoreDown(2)
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testRuleCheckerSuite) TestFixRole(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 2, 1, 3)
	r := s.cluster.GetRegion(1)
	p := r.GetStorePeer(1)
	p.Role = metapb.PeerRole_Learner
	r = r.Clone(core.WithLearners([]*metapb.Peer{p}))
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "fix-peer-role")
	c.Assert(op.Step(0).(operator.PromoteLearner).ToStore, Equals, uint64(1))
}

func (s *testRuleCheckerSuite) TestFixRoleLeader(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"role": "voter"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Voter,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"voter"}},
		},
	})
	s.ruleManager.SetRule(&placement.Rule{
		GroupID: "pd",
		ID:      "r2",
		Index:   101,
		Role:    placement.Follower,
		Count:   2,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"follower"}},
		},
	})
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "fix-follower-role")
	c.Assert(op.Step(0).(operator.TransferLeader).ToStore, Equals, uint64(3))
}

func (s *testRuleCheckerSuite) TestFixRoleLeaderIssue3130(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"role": "leader"})
	s.cluster.AddLeaderRegion(1, 1, 2)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"leader"}},
		},
	})
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "fix-leader-role")
	c.Assert(op.Step(0).(operator.TransferLeader).ToStore, Equals, uint64(2))

	s.cluster.SetStoreBusy(2, true)
	op = s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
	s.cluster.SetStoreBusy(2, false)

	s.cluster.AddLeaderRegion(1, 2, 1)
	op = s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "remove-orphan-peer")
	c.Assert(op.Step(0).(operator.RemovePeer).FromStore, Equals, uint64(1))
}

func (s *testRuleCheckerSuite) TestBetterReplacement(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host3"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "pd",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "move-to-better-location")
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(4))
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3, 4)
	op = s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testRuleCheckerSuite) TestBetterReplacement2(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "host2"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "host3"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "host1"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "pd",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "host"},
	})
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "move-to-better-location")
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(4))
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3, 4)
	op = s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testRuleCheckerSuite) TestNoBetterReplacement(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "pd",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := s.rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testRuleCheckerSuite) TestIssue2419(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderStore(4, 1)
	s.cluster.SetStoreOffline(3)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	r := s.cluster.GetRegion(1)
	r = r.Clone(core.WithAddPeer(&metapb.Peer{Id: 5, StoreId: 4, Role: metapb.PeerRole_Learner}))
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "remove-orphan-peer")
	c.Assert(op.Step(0).(operator.RemovePeer).FromStore, Equals, uint64(4))

	r = r.Clone(core.WithRemoveStorePeer(4))
	op = s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "replace-rule-offline-peer")
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(1).(operator.PromoteLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(2).(operator.RemovePeer).FromStore, Equals, uint64(3))
}
