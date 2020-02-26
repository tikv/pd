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
	"encoding/hex"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/placement"
)

var _ = Suite(&testRuleCheckerSuite{})

type testRuleCheckerSuite struct {
	cluster     *mockcluster.Cluster
	ruleManager *placement.RuleManager
	rc          *RuleChecker
}

func (s *testRuleCheckerSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	cfg.EnablePlacementRules = true
	s.cluster = mockcluster.NewCluster(cfg)
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

func (s *testRuleCheckerSuite) TestFixRole(c *C) {
	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderRegionWithRange(1, "", "", 2, 1, 3)
	r := s.cluster.GetRegion(1)
	p := r.GetStorePeer(1)
	p.IsLearner = true
	r = r.Clone(core.WithLearners([]*metapb.Peer{p}))
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "fix-peer-role")
	c.Assert(op.Step(0).(operator.PromoteLearner).ToStore, Equals, uint64(1))
}

func (s *testRuleCheckerSuite) TestFixRoleLeader(c *C) {
	s.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"role": "leader"})
	s.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
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
	c.Assert(op.Desc(), Equals, "fix-peer-role")
	c.Assert(op.Step(0).(operator.TransferLeader).ToStore, Equals, uint64(3))
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
