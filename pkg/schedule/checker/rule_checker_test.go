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
	"fmt"

	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestRuleCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(ruleCheckerTestSuite))
	suite.Run(t, new(ruleCheckerTestAdvancedSuite))
}

type ruleCheckerTestSuite struct {
	suite.Suite
	cluster     *mockcluster.Cluster
	ruleManager *placement.RuleManager
	rc          *RuleChecker
	ctx         context.Context
	cancel      context.CancelFunc
}

func (suite *ruleCheckerTestSuite) SetupTest() {
	cfg := mockconfig.NewTestOptions()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
	suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.SwitchWitness))
	suite.cluster.SetEnablePlacementRules(true)
	suite.cluster.SetEnableWitness(true)
	suite.cluster.SetEnableUseJointConsensus(false)
	suite.ruleManager = suite.cluster.RuleManager
	suite.rc = NewRuleChecker(suite.ctx, suite.cluster, suite.ruleManager, cache.NewIDTTL(suite.ctx, time.Minute, 3*time.Minute))
}

func (suite *ruleCheckerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *ruleCheckerTestSuite) TestAddRulePeer() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("add-rule-peer", op.Desc())
	re.Equal(constant.High, op.GetPriorityLevel())
	re.Equal(uint64(3), op.Step(0).(operator.AddLearner).ToStore)
}

func (suite *ruleCheckerTestSuite) TestAddRulePeerWithIsolationLevel() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "rack", "host"},
		IsolationLevel: "zone",
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "rack", "host"},
		IsolationLevel: "rack",
	})
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("add-rule-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.AddLearner).ToStore)
}

func (suite *ruleCheckerTestSuite) TestReplaceDownPeerWithIsolationLevel() {
	re := suite.Require()
	suite.cluster.SetMaxStoreDownTime(100 * time.Millisecond)
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2", "host": "h3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h4"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3", "host": "h5"})
	suite.cluster.AddLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h6"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3, 5)
	suite.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "host"},
		IsolationLevel: "zone",
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
	region := suite.cluster.GetRegion(1)
	downPeer := []*pdpb.PeerStats{
		{Peer: region.GetStorePeer(5), DownSeconds: 6000},
	}
	region = region.Clone(core.WithDownPeers(downPeer))
	suite.cluster.PutRegion(region)
	suite.cluster.SetStoreDown(5)
	suite.cluster.SetStoreDown(6)
	time.Sleep(200 * time.Millisecond)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestFixPeer() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderStore(4, 1)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
	suite.cluster.SetStoreDown(2)
	r := suite.cluster.GetRegion(1)
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 60000}}))
	op = suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("fast-replace-rule-down-peer", op.Desc())
	re.Equal(constant.Urgent, op.GetPriorityLevel())
	var add operator.AddLearner
	re.IsType(add, op.Step(0))
	suite.cluster.SetStoreUp(2)
	suite.cluster.SetStoreOffline(2)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("replace-rule-offline-peer", op.Desc())
	re.Equal(constant.High, op.GetPriorityLevel())
	re.IsType(add, op.Step(0))

	suite.cluster.SetStoreUp(2)
	// leader store offline
	suite.cluster.SetStoreOffline(1)
	r1 := suite.cluster.GetRegion(1)
	nr1 := r1.Clone(core.WithPendingPeers([]*metapb.Peer{r1.GetStorePeer(3)}))
	suite.cluster.PutRegion(nr1)
	hasTransferLeader := false
	for range 100 {
		op = suite.rc.Check(suite.cluster.GetRegion(1))
		re.NotNil(op)
		if step, ok := op.Step(0).(operator.TransferLeader); ok {
			re.Equal(uint64(1), step.FromStore)
			re.NotEqual(uint64(3), step.ToStore)
			hasTransferLeader = true
		}
	}
	re.True(hasTransferLeader)
}

func (suite *ruleCheckerTestSuite) TestFixOrphanPeers() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderStore(4, 1)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3, 4)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("remove-orphan-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.RemovePeer).FromStore)
}

func (suite *ruleCheckerTestSuite) TestFixToManyOrphanPeers() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderStore(4, 1)
	suite.cluster.AddLeaderStore(5, 1)
	suite.cluster.AddLeaderStore(6, 1)
	suite.cluster.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{4, 5, 6})
	// Case1:
	// store 4, 5, 6 are orphan peers, and peer on store 3 is pending and down peer.
	region := suite.cluster.GetRegion(1)
	region = region.Clone(
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 60000}}),
		core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
	suite.cluster.PutRegion(region)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("remove-orphan-peer", op.Desc())
	re.Equal(uint64(5), op.Step(0).(operator.RemovePeer).FromStore)
	// Case2:
	// store 4, 5, 6 are orphan peers, and peer on store 3 is down peer. and peer on store 4, 5 are pending.
	region = suite.cluster.GetRegion(1)
	region = region.Clone(
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 60000}}),
		core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(4), region.GetStorePeer(5)}))
	suite.cluster.PutRegion(region)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("remove-unhealthy-orphan-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.RemovePeer).FromStore)
	// Case3:
	// store 4, 5, 6 are orphan peers, and peer on one of stores is disconnect peer
	// we should remove disconnect peer first.
	for i := uint64(4); i <= 6; i++ {
		region = suite.cluster.GetRegion(1)
		suite.cluster.SetStoreDisconnect(i)
		region = region.Clone(
			core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 60000}}),
			core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
		suite.cluster.PutRegion(region)
		op = suite.rc.Check(suite.cluster.GetRegion(1))
		re.NotNil(op)
		re.Equal("remove-orphan-peer", op.Desc())
		re.Equal(i, op.Step(0).(operator.RemovePeer).FromStore)
		suite.cluster.SetStoreUp(i)
	}
	// Case4:
	// store 4, 5, 6 are orphan peers, and peer on two of stores is disconnect peer
	// we should remove disconnect peer first.
	for i := uint64(4); i <= 6; i++ {
		region = suite.cluster.GetRegion(1)
		suite.cluster.SetStoreDisconnect(4)
		suite.cluster.SetStoreDisconnect(5)
		suite.cluster.SetStoreDisconnect(6)
		suite.cluster.SetStoreUp(i)
		region = region.Clone(
			core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 60000}}),
			core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
		suite.cluster.PutRegion(region)
		op = suite.rc.Check(suite.cluster.GetRegion(1))
		re.NotNil(op)
		re.Equal("remove-orphan-peer", op.Desc())
		removedPeerStoreID := op.Step(0).(operator.RemovePeer).FromStore
		re.NotEqual(i, removedPeerStoreID)
		region = suite.cluster.GetRegion(1)
		newRegion := region.Clone(core.WithRemoveStorePeer(removedPeerStoreID))
		suite.cluster.PutRegion(newRegion)
		op = suite.rc.Check(suite.cluster.GetRegion(1))
		re.NotNil(op)
		re.Equal("remove-orphan-peer", op.Desc())
		removedPeerStoreID = op.Step(0).(operator.RemovePeer).FromStore
		re.NotEqual(i, removedPeerStoreID)
		suite.cluster.PutRegion(region)
	}
}

func (suite *ruleCheckerTestSuite) TestFixToManyOrphanPeers2() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderStore(4, 1)
	suite.cluster.AddLeaderStore(5, 1)
	suite.cluster.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{4, 5})

	// Case1:
	// store 4, 5 are orphan peers, and peer on one of stores is disconnect peer
	// we should remove disconnect peer first.
	for i := uint64(4); i <= 5; i++ {
		region := suite.cluster.GetRegion(1)
		suite.cluster.SetStoreDisconnect(i)
		region = region.Clone(
			core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 60000}}),
			core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
		suite.cluster.PutRegion(region)
		op := suite.rc.Check(suite.cluster.GetRegion(1))
		re.NotNil(op)
		re.Equal("remove-orphan-peer", op.Desc())
		re.Equal(i, op.Step(0).(operator.RemovePeer).FromStore)
		suite.cluster.SetStoreUp(i)
	}

	// Case2:
	// store 4, 5 are orphan peers, and they are disconnect peers
	// we should remove the peer on disconnect stores at least.
	region := suite.cluster.GetRegion(1)
	suite.cluster.SetStoreDisconnect(4)
	suite.cluster.SetStoreDisconnect(5)
	region = region.Clone(
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 60000}}),
		core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(3)}))
	suite.cluster.PutRegion(region)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("remove-orphan-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.RemovePeer).FromStore)
}

func (suite *ruleCheckerTestSuite) TestFixOrphanPeers2() {
	re := suite.Require()
	// check orphan peers can only be handled when all rules are satisfied.
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"foo": "bar"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"foo": "bar"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"foo": "baz"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Leader,
		Count:    2,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "foo", Op: "in", Values: []string{"baz"}},
		},
	})
	suite.cluster.SetStoreDown(2)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestFixRole() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 2, 1, 3)
	r := suite.cluster.GetRegion(1)
	p := r.GetStorePeer(1)
	p.Role = metapb.PeerRole_Learner
	r = r.Clone(core.WithLearners([]*metapb.Peer{p}))
	op := suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("fix-peer-role", op.Desc())
	re.Equal(uint64(1), op.Step(0).(operator.PromoteLearner).ToStore)
}

func (suite *ruleCheckerTestSuite) TestFixRoleLeader() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"role": "follower"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"role": "voter"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Voter,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"voter"}},
		},
	})
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "r2",
		Index:   101,
		Role:    placement.Follower,
		Count:   2,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"follower"}},
		},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("fix-follower-role", op.Desc())
	re.Equal(uint64(3), op.Step(0).(operator.TransferLeader).ToStore)
}

func (suite *ruleCheckerTestSuite) TestFixRoleLeaderIssue3130() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"role": "leader"})
	suite.cluster.AddLeaderRegion(1, 1, 2)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"leader"}},
		},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("fix-leader-role", op.Desc())
	re.Equal(uint64(2), op.Step(0).(operator.TransferLeader).ToStore)

	suite.cluster.SetStoreBusy(2, true)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
	suite.cluster.SetStoreBusy(2, false)

	suite.cluster.AddLeaderRegion(1, 2, 1)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("remove-orphan-peer", op.Desc())
	re.Equal(uint64(1), op.Step(0).(operator.RemovePeer).FromStore)
}

func (suite *ruleCheckerTestSuite) TestFixLeaderRoleWithUnhealthyRegion() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"rule": "follower"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"rule": "follower"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"rule": "leader"})
	suite.ruleManager.SetRuleGroup(&placement.RuleGroup{
		ID:       "cluster",
		Index:    2,
		Override: true,
	})
	err := suite.ruleManager.SetRules([]*placement.Rule{
		{
			GroupID: "cluster",
			ID:      "r1",
			Index:   100,
			Role:    placement.Follower,
			Count:   2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "rule", Op: "in", Values: []string{"follower"}},
			},
		},
		{
			GroupID: "cluster",
			ID:      "r2",
			Index:   100,
			Role:    placement.Leader,
			Count:   1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "rule", Op: "in", Values: []string{"leader"}},
			},
		},
	})
	re.NoError(err)
	// no Leader
	suite.cluster.AddNoLeaderRegion(1, 1, 2, 3)
	r := suite.cluster.GetRegion(1)
	op := suite.rc.Check(r)
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestFixRuleWitness() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "follower"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "voter"})
	suite.cluster.AddLeaderRegion(1, 1)

	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Index:     100,
		Override:  true,
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "C", Op: "in", Values: []string{"voter"}},
		},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("add-rule-peer", op.Desc())
	re.Equal(uint64(3), op.Step(0).(operator.AddLearner).ToStore)
	re.True(op.Step(0).(operator.AddLearner).IsWitness)
}

func (suite *ruleCheckerTestSuite) TestFixRuleWitness2() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "voter"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "voter"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"D": "voter"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3, 4)

	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Index:     100,
		Override:  false,
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "D", Op: "in", Values: []string{"voter"}},
		},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("fix-witness-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.BecomeWitness).StoreID)
}

func (suite *ruleCheckerTestSuite) TestFixRuleWitness3() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "voter"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "voter"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	r := suite.cluster.GetRegion(1)
	// set peer3 to witness
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(3)}))
	suite.cluster.PutRegion(r)
	op := suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("fix-non-witness-peer", op.Desc())
	re.Equal(uint64(3), op.Step(0).(operator.RemovePeer).FromStore)
	re.Equal(uint64(3), op.Step(1).(operator.AddLearner).ToStore)
}

func (suite *ruleCheckerTestSuite) TestFixRuleWitness4() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "voter"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "learner"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	r := suite.cluster.GetRegion(1)
	// set peer3 to witness learner
	r = r.Clone(core.WithLearners([]*metapb.Peer{r.GetPeer(3)}))
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(3)}))

	err := suite.ruleManager.SetRules([]*placement.Rule{
		{
			GroupID:   placement.DefaultGroupID,
			ID:        placement.DefaultRuleID,
			Index:     100,
			Override:  true,
			Role:      placement.Voter,
			Count:     2,
			IsWitness: false,
		},
		{
			GroupID:   placement.DefaultGroupID,
			ID:        "r1",
			Index:     100,
			Override:  false,
			Role:      placement.Learner,
			Count:     1,
			IsWitness: false,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "C", Op: "in", Values: []string{"learner"}},
			},
		},
	})
	re.NoError(err)

	op := suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("fix-non-witness-peer", op.Desc())
	re.Equal(uint64(3), op.Step(0).(operator.BecomeNonWitness).StoreID)
}

func (suite *ruleCheckerTestSuite) TestFixRuleWitness5() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "voter"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "voter"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	err := suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Index:     100,
		Override:  true,
		Role:      placement.Voter,
		Count:     2,
		IsWitness: true,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "A", Op: "In", Values: []string{"leader"}},
		},
	})
	re.Error(err)
	re.Equal(errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("define too many witness by count %d", 2)).Error(), err.Error())
}

func (suite *ruleCheckerTestSuite) TestFixRuleWitness6() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "voter"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "voter"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	err := suite.ruleManager.SetRules([]*placement.Rule{
		{
			GroupID:   placement.DefaultGroupID,
			ID:        placement.DefaultRuleID,
			Index:     100,
			Role:      placement.Voter,
			IsWitness: false,
			Count:     2,
		},
		{
			GroupID:   placement.DefaultGroupID,
			ID:        "r1",
			Index:     100,
			Role:      placement.Voter,
			Count:     1,
			IsWitness: true,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "C", Op: "in", Values: []string{"voter"}},
			},
		},
	})
	re.NoError(err)

	suite.rc.RecordRegionPromoteToNonWitness(1)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)

	suite.rc.switchWitnessCache.Remove(1)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
}

func (suite *ruleCheckerTestSuite) TestDisableWitness() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"A": "leader"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"B": "voter"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"C": "voter"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	err := suite.ruleManager.SetRules([]*placement.Rule{
		{
			GroupID:   placement.DefaultGroupID,
			ID:        placement.DefaultRuleID,
			Index:     100,
			Role:      placement.Voter,
			IsWitness: false,
			Count:     2,
		},
		{
			GroupID:   placement.DefaultGroupID,
			ID:        "r1",
			Index:     100,
			Role:      placement.Voter,
			Count:     1,
			IsWitness: true,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "C", Op: "in", Values: []string{"voter"}},
			},
		},
	})
	re.NoError(err)

	r := suite.cluster.GetRegion(1)
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(3)}))

	op := suite.rc.Check(r)
	re.Nil(op)

	suite.cluster.SetEnableWitness(false)
	op = suite.rc.Check(r)
	re.NotNil(op)
}

func (suite *ruleCheckerTestSuite) TestBetterReplacement() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host3"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("move-to-better-location", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3, 4)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestBetterReplacement2() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "host2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "host3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "host1"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "host"},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("move-to-better-location", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 3, 4)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestNoBetterReplacement() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestIssue2419() {
	re := suite.Require()
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	suite.cluster.AddLeaderStore(4, 1)
	suite.cluster.SetStoreOffline(3)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	r := suite.cluster.GetRegion(1)
	r = r.Clone(core.WithAddPeer(&metapb.Peer{Id: 5, StoreId: 4, Role: metapb.PeerRole_Learner}))
	op := suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("remove-orphan-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.RemovePeer).FromStore)

	r = r.Clone(core.WithRemoveStorePeer(4))
	op = suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("replace-rule-offline-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	re.Equal(uint64(4), op.Step(1).(operator.PromoteLearner).ToStore)
	re.Equal(uint64(3), op.Step(2).(operator.RemovePeer).FromStore)
}

// Ref https://github.com/tikv/pd/issues/3521 https://github.com/tikv/pd/issues/5786
// The problem is when offline a store, we may add learner multiple times if
// the operator is timeout.
func (suite *ruleCheckerTestSuite) TestPriorityFixOrphanPeer() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
	var add operator.AddLearner
	var remove operator.RemovePeer
	// Ref 5786
	originRegion := suite.cluster.GetRegion(1)
	learner4 := &metapb.Peer{Id: 114, StoreId: 4, Role: metapb.PeerRole_Learner}
	testRegion := originRegion.Clone(
		core.WithAddPeer(learner4),
		core.WithAddPeer(&metapb.Peer{Id: 115, StoreId: 5, Role: metapb.PeerRole_Learner}),
		core.WithPendingPeers([]*metapb.Peer{originRegion.GetStorePeer(2), learner4}),
	)
	suite.cluster.PutRegion(testRegion)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("remove-unhealthy-orphan-peer", op.Desc())
	re.IsType(remove, op.Step(0))
	// Ref #3521
	suite.cluster.SetStoreOffline(2)
	suite.cluster.PutRegion(originRegion)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.IsType(add, op.Step(0))
	re.Equal("replace-rule-offline-peer", op.Desc())
	testRegion = suite.cluster.GetRegion(1).Clone(core.WithAddPeer(
		&metapb.Peer{
			Id:      125,
			StoreId: 4,
			Role:    metapb.PeerRole_Learner,
		}))
	suite.cluster.PutRegion(testRegion)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.IsType(remove, op.Step(0))
	re.Equal("remove-orphan-peer", op.Desc())
}

// Ref https://github.com/tikv/pd/issues/7249 https://github.com/tikv/tikv/issues/15799
func (suite *ruleCheckerTestSuite) TestFixOrphanPeerWithDisconnectedStoreAndRuleChanged() {
	re := suite.Require()
	// disconnect any two stores and change rule to 3 replicas
	stores := []uint64{1, 2, 3, 4, 5}
	testCases := [][]uint64{}
	for i := range stores {
		for j := i + 1; j < len(stores); j++ {
			testCases = append(testCases, []uint64{stores[i], stores[j]})
		}
	}
	for _, leader := range stores {
		var followers []uint64
		for i := range stores {
			if stores[i] != leader {
				followers = append(followers, stores[i])
			}
		}

		for _, testCase := range testCases {
			// init cluster with 5 replicas
			suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
			suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
			suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
			suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
			suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
			suite.cluster.AddLeaderRegionWithRange(1, "", "", leader, followers...)
			rule := &placement.Rule{
				GroupID:  placement.DefaultGroupID,
				ID:       placement.DefaultRuleID,
				Role:     placement.Voter,
				Count:    5,
				StartKey: []byte{},
				EndKey:   []byte{},
			}
			err := suite.ruleManager.SetRule(rule)
			re.NoError(err)
			op := suite.rc.Check(suite.cluster.GetRegion(1))
			re.Nil(op)

			// set two stores to disconnected
			suite.cluster.SetStoreDisconnect(testCase[0])
			suite.cluster.SetStoreDisconnect(testCase[1])

			// change rule to 3 replicas
			rule = &placement.Rule{
				GroupID:  placement.DefaultGroupID,
				ID:       placement.DefaultRuleID,
				Role:     placement.Voter,
				Count:    3,
				StartKey: []byte{},
				EndKey:   []byte{},
				Override: true,
			}
			suite.ruleManager.SetRule(rule)

			// remove peer from region 1
			for j := 1; j <= 2; j++ {
				r1 := suite.cluster.GetRegion(1)
				op = suite.rc.Check(suite.cluster.GetRegion(1))
				re.NotNil(op)
				re.Contains(op.Desc(), "orphan")
				var removedPeerStoreID uint64
				newLeaderStoreID := r1.GetLeader().GetStoreId()
				for i := range op.Len() {
					if s, ok := op.Step(i).(operator.RemovePeer); ok {
						removedPeerStoreID = s.FromStore
					}
					if s, ok := op.Step(i).(operator.TransferLeader); ok {
						newLeaderStoreID = s.ToStore
					}
				}
				re.NotZero(removedPeerStoreID)
				r1 = r1.Clone(
					core.WithLeader(r1.GetStorePeer(newLeaderStoreID)),
					core.WithRemoveStorePeer(removedPeerStoreID))
				suite.cluster.PutRegion(r1)
				r1 = suite.cluster.GetRegion(1)
				re.Len(r1.GetPeers(), 5-j)
			}

			r1 := suite.cluster.GetRegion(1)
			for _, p := range r1.GetPeers() {
				re.NotEqual(p.GetStoreId(), testCase[0])
				re.NotEqual(p.GetStoreId(), testCase[1])
			}
			suite.TearDownTest()
			suite.SetupTest()
		}
	}
}

// Ref https://github.com/tikv/pd/issues/7249 https://github.com/tikv/tikv/issues/15799
func (suite *ruleCheckerTestSuite) TestFixOrphanPeerWithDisconnectedStoreAndRuleChangedWithLearner() {
	re := suite.Require()
	// disconnect any three stores and change rule to 3 replicas
	// and there is a learner in the disconnected store.
	stores := []uint64{1, 2, 3, 4, 5, 6}
	testCases := [][]uint64{}
	for i := range stores {
		for j := i + 1; j < len(stores); j++ {
			for k := j + 1; k < len(stores); k++ {
				testCases = append(testCases, []uint64{stores[i], stores[j], stores[k]})
			}
		}
	}
	for _, leader := range stores {
		var followers []uint64
		for i := range stores {
			if stores[i] != leader {
				followers = append(followers, stores[i])
			}
		}

		for _, testCase := range testCases {
			for _, learnerStore := range testCase {
				if learnerStore == leader {
					continue
				}
				voterFollowers := []uint64{}
				for _, follower := range followers {
					if follower != learnerStore {
						voterFollowers = append(voterFollowers, follower)
					}
				}
				// init cluster with 5 voters and 1 learner
				suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
				suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
				suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
				suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
				suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
				suite.cluster.AddLabelsStore(6, 1, map[string]string{"host": "host6"})
				suite.cluster.AddLeaderRegionWithRange(1, "", "", leader, voterFollowers...)
				err := suite.ruleManager.SetRules([]*placement.Rule{
					{
						GroupID:   placement.DefaultGroupID,
						ID:        placement.DefaultRuleID,
						Index:     100,
						Override:  true,
						Role:      placement.Voter,
						Count:     5,
						IsWitness: false,
					},
					{
						GroupID:   placement.DefaultGroupID,
						ID:        "r1",
						Index:     100,
						Override:  false,
						Role:      placement.Learner,
						Count:     1,
						IsWitness: false,
						LabelConstraints: []placement.LabelConstraint{
							{Key: "host", Op: "in", Values: []string{"host" + strconv.FormatUint(learnerStore, 10)}},
						},
					},
				})
				re.NoError(err)
				r1 := suite.cluster.GetRegion(1)
				r1 = r1.Clone(core.WithAddPeer(&metapb.Peer{Id: 12, StoreId: learnerStore, Role: metapb.PeerRole_Learner}))
				suite.cluster.PutRegion(r1)
				op := suite.rc.Check(suite.cluster.GetRegion(1))
				re.Nil(op)

				// set three stores to disconnected
				suite.cluster.SetStoreDisconnect(testCase[0])
				suite.cluster.SetStoreDisconnect(testCase[1])
				suite.cluster.SetStoreDisconnect(testCase[2])

				// change rule to 3 replicas
				suite.ruleManager.DeleteRule(placement.DefaultGroupID, "r1")
				suite.ruleManager.SetRule(&placement.Rule{
					GroupID:  placement.DefaultGroupID,
					ID:       placement.DefaultRuleID,
					Role:     placement.Voter,
					Count:    3,
					StartKey: []byte{},
					EndKey:   []byte{},
					Override: true,
				})

				// remove peer from region 1
				for j := 1; j <= 3; j++ {
					r1 := suite.cluster.GetRegion(1)
					op = suite.rc.Check(suite.cluster.GetRegion(1))
					re.NotNil(op)
					re.Contains(op.Desc(), "orphan")
					var removedPeerStoreID uint64
					newLeaderStoreID := r1.GetLeader().GetStoreId()
					for i := range op.Len() {
						if s, ok := op.Step(i).(operator.RemovePeer); ok {
							removedPeerStoreID = s.FromStore
						}
						if s, ok := op.Step(i).(operator.TransferLeader); ok {
							newLeaderStoreID = s.ToStore
						}
					}
					re.NotZero(removedPeerStoreID)
					r1 = r1.Clone(
						core.WithLeader(r1.GetStorePeer(newLeaderStoreID)),
						core.WithRemoveStorePeer(removedPeerStoreID))
					suite.cluster.PutRegion(r1)
					r1 = suite.cluster.GetRegion(1)
					re.Len(r1.GetPeers(), 6-j)
				}

				r1 = suite.cluster.GetRegion(1)
				for _, p := range r1.GetPeers() {
					re.NotEqual(p.GetStoreId(), testCase[0])
					re.NotEqual(p.GetStoreId(), testCase[1])
					re.NotEqual(p.GetStoreId(), testCase[2])
				}
				suite.TearDownTest()
				suite.SetupTest()
			}
		}
	}
}

func (suite *ruleCheckerTestSuite) TestPriorityFitHealthWithDifferentRole1() {
	re := suite.Require()
	suite.cluster.SetEnableUseJointConsensus(true)
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	suite.cluster.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{4})
	r1 := suite.cluster.GetRegion(1)
	suite.cluster.GetStore(3).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()

	// set peer3 to pending and down
	r1 = r1.Clone(core.WithPendingPeers([]*metapb.Peer{r1.GetPeer(3)}))
	r1 = r1.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        r1.GetStorePeer(3),
			DownSeconds: 30000,
		},
	}))
	suite.cluster.PutRegion(r1)

	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Equal(uint64(3), op.Step(0).(operator.ChangePeerV2Enter).DemoteVoters[0].ToStore)
	re.Equal(uint64(4), op.Step(0).(operator.ChangePeerV2Enter).PromoteLearners[0].ToStore)
	re.Equal(uint64(3), op.Step(1).(operator.ChangePeerV2Leave).DemoteVoters[0].ToStore)
	re.Equal(uint64(4), op.Step(1).(operator.ChangePeerV2Leave).PromoteLearners[0].ToStore)
	re.Equal("replace-down-peer-with-orphan-peer", op.Desc())

	// set peer3 only pending
	suite.cluster.GetStore(3).GetMeta().LastHeartbeat = time.Now().UnixNano()
	r1 = r1.Clone(core.WithDownPeers(nil))
	suite.cluster.PutRegion(r1)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestPriorityFitHealthWithDifferentRole2() {
	re := suite.Require()
	suite.cluster.SetEnableUseJointConsensus(true)
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3, 4, 5)
	r1 := suite.cluster.GetRegion(1)

	// set peer3 to pending and down, and peer 3 to learner, and store 3 is down
	suite.cluster.GetStore(3).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()
	r1 = r1.Clone(core.WithLearners([]*metapb.Peer{r1.GetPeer(3)}))
	r1 = r1.Clone(
		core.WithPendingPeers([]*metapb.Peer{r1.GetPeer(3)}),
		core.WithDownPeers([]*pdpb.PeerStats{
			{
				Peer:        r1.GetStorePeer(3),
				DownSeconds: 30000,
			},
		}),
	)
	suite.cluster.PutRegion(r1)

	// default and test group => 3 voter  + 1 learner
	err := suite.ruleManager.SetRule(&placement.Rule{
		GroupID: "test",
		ID:      "10",
		Role:    placement.Learner,
		Count:   1,
	})
	re.NoError(err)

	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Equal(uint64(5), op.Step(0).(operator.ChangePeerV2Enter).DemoteVoters[0].ToStore)
	re.Equal(uint64(3), op.Step(1).(operator.RemovePeer).FromStore)
	re.Equal("replace-down-peer-with-orphan-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestPriorityFitHealthPeersAndTiFlash() {
	re := suite.Require()
	suite.cluster.SetEnableUseJointConsensus(true)
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4", "engine": "tiflash"})
	suite.cluster.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{4})
	rule := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test",
		Role:    placement.Voter,
		Count:   3,
	}
	rule2 := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test2",
		Role:    placement.Learner,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "engine",
				Op:     placement.In,
				Values: []string{"tiflash"},
			},
		},
	}
	suite.ruleManager.SetRule(rule)
	suite.ruleManager.SetRule(rule2)
	suite.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)

	r1 := suite.cluster.GetRegion(1)
	// set peer3 to pending and down
	r1 = r1.Clone(core.WithPendingPeers([]*metapb.Peer{r1.GetPeer(3)}))
	r1 = r1.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        r1.GetStorePeer(3),
			DownSeconds: 30000,
		},
	}))
	suite.cluster.PutRegion(r1)
	suite.cluster.GetStore(3).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()

	op := suite.rc.Check(suite.cluster.GetRegion(1))
	// should not promote tiflash peer
	re.Nil(op)

	// scale a node, can replace the down peer
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("fast-replace-rule-down-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestIssue3293() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)
	err := suite.ruleManager.SetRule(&placement.Rule{
		GroupID: "TiDB_DDL_51",
		ID:      "0",
		Role:    placement.Follower,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key: "host",
				Values: []string{
					"host5",
				},
				Op: placement.In,
			},
		},
	})
	re.NoError(err)
	suite.cluster.DeleteStore(suite.cluster.GetStore(5))
	err = suite.ruleManager.SetRule(&placement.Rule{
		GroupID: "TiDB_DDL_51",
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   3,
	})
	re.NoError(err)
	err = suite.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
	re.NoError(err)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("add-rule-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestIssue3299() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"dc": "sh"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)

	testCases := []struct {
		constraints []placement.LabelConstraint
		err         string
	}{
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host5"},
					Op:     placement.In,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "ho",
					Values: []string{"sh"},
					Op:     placement.In,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.NotIn,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
				{
					Key:    "host",
					Values: []string{"host3"},
					Op:     placement.In,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
			},
			err: "",
		},
	}

	for _, testCase := range testCases {
		err := suite.ruleManager.SetRule(&placement.Rule{
			GroupID:          "p",
			ID:               "0",
			Role:             placement.Follower,
			Count:            1,
			LabelConstraints: testCase.constraints,
		})
		if testCase.err != "" {
			suite.Regexp(testCase.err, err.Error())
		} else {
			re.NoError(err)
		}
	}
}

// See issue: https://github.com/tikv/pd/issues/3705
func (suite *ruleCheckerTestSuite) TestFixDownPeer() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 3, 4)
	rule := &placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone"},
	}
	suite.ruleManager.SetRule(rule)

	region := suite.cluster.GetRegion(1)
	re.Nil(suite.rc.Check(region))

	suite.cluster.SetStoreDown(4)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{Peer: region.GetStorePeer(4), DownSeconds: 6000},
	}))
	operatorutil.CheckTransferPeer(re, suite.rc.Check(region), operator.OpRegion, 4, 5)

	suite.cluster.SetStoreDown(5)
	operatorutil.CheckTransferPeer(re, suite.rc.Check(region), operator.OpRegion, 4, 2)

	rule.IsolationLevel = "zone"
	suite.ruleManager.SetRule(rule)
	re.Nil(suite.rc.Check(region))
}

func (suite *ruleCheckerTestSuite) TestFixDownPeerWithNoWitness() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	suite.cluster.SetStoreDown(2)
	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-11 * time.Minute).UnixNano()
	r := suite.cluster.GetRegion(1)
	// set peer2 to down
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 600}}))
	re.Nil(suite.rc.Check(r))
}

func (suite *ruleCheckerTestSuite) TestFixDownWitnessPeer() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	suite.cluster.SetStoreDown(2)
	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-11 * time.Minute).UnixNano()
	r := suite.cluster.GetRegion(1)
	// set peer2 to down
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 600}}))
	// set peer2 to witness
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(2)}))

	suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   2,
	})
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
	})
	re.Nil(suite.rc.Check(r))

	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()
	re.Nil(suite.rc.Check(r))
}

func (suite *ruleCheckerTestSuite) TestFixDownPeerWithAvailableWitness() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	suite.cluster.SetStoreDown(2)
	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-11 * time.Minute).UnixNano()
	r := suite.cluster.GetRegion(1)
	// set peer2 to down
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 600}}))
	// set peer3 to witness
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(3)}))

	suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   2,
	})
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
	})

	op := suite.rc.Check(r)

	re.NotNil(op)
	re.Equal("promote-witness-for-down", op.Desc())
	re.Equal(uint64(3), op.Step(0).(operator.RemovePeer).FromStore)
	re.Equal(uint64(3), op.Step(1).(operator.AddLearner).ToStore)
	re.Equal(uint64(3), op.Step(2).(operator.BecomeNonWitness).StoreID)
	re.Equal(uint64(3), op.Step(3).(operator.PromoteLearner).ToStore)
}

func (suite *ruleCheckerTestSuite) TestFixDownPeerWithAvailableWitness2() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	suite.cluster.SetStoreDown(2)
	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()
	r := suite.cluster.GetRegion(1)
	// set peer2 to down
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 6000}}))
	// set peer3 to witness
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(3)}))

	suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   2,
	})
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
	})

	op := suite.rc.Check(r)

	re.Nil(op)
}

func (suite *ruleCheckerTestSuite) TestFixDownPeerWithAvailableWitness3() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	suite.cluster.SetStoreDown(2)
	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()
	r := suite.cluster.GetRegion(1)
	// set peer2 to down
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 6000}}))
	// set peer3 to witness
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(3)}))

	suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   2,
	})
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
	})

	op := suite.rc.Check(r)

	re.NotNil(op)
	re.Equal("fast-replace-rule-down-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	re.True(op.Step(0).(operator.AddLearner).IsWitness)
	re.Equal(uint64(4), op.Step(1).(operator.PromoteLearner).ToStore)
	re.True(op.Step(1).(operator.PromoteLearner).IsWitness)
	re.Equal(uint64(2), op.Step(2).(operator.RemovePeer).FromStore)
}

func (suite *ruleCheckerTestSuite) TestFixDownPeerWithAvailableWitness4() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)

	suite.cluster.SetStoreDown(2)
	suite.cluster.GetStore(2).GetMeta().LastHeartbeat = time.Now().Add(-31 * time.Minute).UnixNano()
	r := suite.cluster.GetRegion(1)
	// set peer2 to down
	r = r.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: r.GetStorePeer(2), DownSeconds: 6000}}))

	op := suite.rc.Check(r)

	re.NotNil(op)
	re.Equal("fast-replace-rule-down-peer", op.Desc())
	re.Equal(uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	re.True(op.Step(0).(operator.AddLearner).IsWitness)
	re.Equal(uint64(4), op.Step(1).(operator.PromoteLearner).ToStore)
	re.True(op.Step(1).(operator.PromoteLearner).IsWitness)
	re.Equal(uint64(2), op.Step(2).(operator.RemovePeer).FromStore)
}

// See issue: https://github.com/tikv/pd/issues/3705
func (suite *ruleCheckerTestSuite) TestFixOfflinePeer() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 3, 4)
	rule := &placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone"},
	}
	suite.ruleManager.SetRule(rule)

	region := suite.cluster.GetRegion(1)
	re.Nil(suite.rc.Check(region))

	suite.cluster.SetStoreOffline(4)
	operatorutil.CheckTransferPeer(re, suite.rc.Check(region), operator.OpRegion, 4, 5)

	suite.cluster.SetStoreOffline(5)
	operatorutil.CheckTransferPeer(re, suite.rc.Check(region), operator.OpRegion, 4, 2)

	rule.IsolationLevel = "zone"
	suite.ruleManager.SetRule(rule)
	re.Nil(suite.rc.Check(region))
}

func (suite *ruleCheckerTestSuite) TestFixOfflinePeerWithAvailableWitness() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLeaderRegion(1, 1, 3, 4)

	r := suite.cluster.GetRegion(1)
	r = r.Clone(core.WithWitnesses([]*metapb.Peer{r.GetPeer(2)}))
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   2,
	})
	suite.ruleManager.SetRule(&placement.Rule{
		GroupID:   placement.DefaultGroupID,
		ID:        "r1",
		Role:      placement.Voter,
		Count:     1,
		IsWitness: true,
	})
	re.Nil(suite.rc.Check(r))

	suite.cluster.SetStoreOffline(4)
	op := suite.rc.Check(r)
	re.NotNil(op)
	re.Equal("replace-rule-offline-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestRuleCache() {
	re := suite.Require()
	suite.cluster.PersistOptions.SetPlacementRulesCacheEnabled(true)
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	suite.cluster.AddRegionStore(999, 1)
	suite.cluster.AddLeaderRegion(1, 1, 3, 4)
	rule := &placement.Rule{
		GroupID:        placement.DefaultGroupID,
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone"},
	}
	suite.ruleManager.SetRule(rule)
	region := suite.cluster.GetRegion(1)
	region = region.Clone(core.WithIncConfVer(), core.WithIncVersion())
	re.Nil(suite.rc.Check(region))

	testCases := []struct {
		name        string
		region      *core.RegionInfo
		stillCached bool
	}{
		{
			name:        placement.DefaultRuleID,
			region:      region,
			stillCached: true,
		},
		{
			name: "region topo changed",
			region: func() *core.RegionInfo {
				return region.Clone(core.WithAddPeer(&metapb.Peer{
					Id:      999,
					StoreId: 999,
					Role:    metapb.PeerRole_Voter,
				}), core.WithIncConfVer())
			}(),
			stillCached: false,
		},
		{
			name: "region leader changed",
			region: region.Clone(
				core.WithLeader(&metapb.Peer{Role: metapb.PeerRole_Voter, Id: 2, StoreId: 3})),
			stillCached: false,
		},
		{
			name: "region have down peers",
			region: region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
				{
					Peer:        region.GetPeer(3),
					DownSeconds: 42,
				},
			})),
			stillCached: false,
		},
	}
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		if testCase.stillCached {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/assertShouldCache", "return(true)"))
			suite.rc.Check(testCase.region)
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/assertShouldCache"))
		} else {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/assertShouldNotCache", "return(true)"))
			suite.rc.Check(testCase.region)
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/assertShouldNotCache"))
		}
	}
}

// Ref https://github.com/tikv/pd/issues/4045
func (suite *ruleCheckerTestSuite) TestSkipFixOrphanPeerIfSelectedPeerIsPendingOrDown() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3, 4)

	// set peer3 and peer4 to pending
	r1 := suite.cluster.GetRegion(1)
	r1 = r1.Clone(core.WithPendingPeers([]*metapb.Peer{r1.GetStorePeer(3), r1.GetStorePeer(4)}))
	suite.cluster.PutRegion(r1)

	// should not remove extra peer
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)

	// set peer3 to down-peer
	r1 = r1.Clone(core.WithPendingPeers([]*metapb.Peer{r1.GetStorePeer(4)}))
	r1 = r1.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        r1.GetStorePeer(3),
			DownSeconds: 42,
		},
	}))
	suite.cluster.PutRegion(r1)

	// should not remove extra peer
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)

	// set peer3 to normal
	r1 = r1.Clone(core.WithDownPeers(nil))
	suite.cluster.PutRegion(r1)

	// should remove extra peer now
	var remove operator.RemovePeer
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.IsType(remove, op.Step(0))
	re.Equal("remove-orphan-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestPriorityFitHealthPeers() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 3, 4)
	r1 := suite.cluster.GetRegion(1)

	// set peer3 to pending
	r1 = r1.Clone(core.WithPendingPeers([]*metapb.Peer{r1.GetPeer(3)}))
	suite.cluster.PutRegion(r1)

	var remove operator.RemovePeer
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.IsType(remove, op.Step(0))
	re.Equal("remove-orphan-peer", op.Desc())

	// set peer3 to down
	r1 = r1.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        r1.GetStorePeer(3),
			DownSeconds: 42,
		},
	}))
	r1 = r1.Clone(core.WithPendingPeers(nil))
	suite.cluster.PutRegion(r1)

	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.IsType(remove, op.Step(0))
	re.Equal("remove-orphan-peer", op.Desc())
}

// Ref https://github.com/tikv/pd/issues/4140
func (suite *ruleCheckerTestSuite) TestDemoteVoter() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z4"})
	region := suite.cluster.AddLeaderRegion(1, 1, 4)
	rule := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test",
		Role:    placement.Voter,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z1"},
			},
		},
	}
	rule2 := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test2",
		Role:    placement.Learner,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z4"},
			},
		},
	}
	suite.ruleManager.SetRule(rule)
	suite.ruleManager.SetRule(rule2)
	suite.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
	op := suite.rc.Check(region)
	re.NotNil(op)
	re.Equal("fix-demote-voter", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestOfflineAndDownStore() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z4"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z4"})
	region := suite.cluster.AddLeaderRegion(1, 1, 2, 3)
	op := suite.rc.Check(region)
	re.Nil(op)
	// assert rule checker should generate replace offline peer operator after cached
	suite.cluster.SetStoreOffline(1)
	op = suite.rc.Check(region)
	re.NotNil(op)
	re.Equal("replace-rule-offline-peer", op.Desc())
	// re-cache the regionFit
	suite.cluster.SetStoreUp(1)
	op = suite.rc.Check(region)
	re.Nil(op)

	// assert rule checker should generate replace down peer operator after cached
	suite.cluster.SetStoreDown(2)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(2), DownSeconds: 60000}}))
	op = suite.rc.Check(region)
	re.NotNil(op)
	re.Equal("fast-replace-rule-down-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestPendingList() {
	re := suite.Require()
	// no enough store
	suite.cluster.AddLeaderStore(1, 1)
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
	_, exist := suite.rc.pendingList.Get(1)
	re.True(exist)

	// add more stores
	suite.cluster.AddLeaderStore(2, 1)
	suite.cluster.AddLeaderStore(3, 1)
	op = suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("add-rule-peer", op.Desc())
	re.Equal(constant.High, op.GetPriorityLevel())
	re.Equal(uint64(3), op.Step(0).(operator.AddLearner).ToStore)
	_, exist = suite.rc.pendingList.Get(1)
	re.False(exist)
}

func (suite *ruleCheckerTestSuite) TestLocationLabels() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z2", "rack": "r3", "host": "h2"})
	suite.cluster.AddLabelsStore(6, 1, map[string]string{"zone": "z2", "rack": "r3", "host": "h2"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 1, 2, 5)
	rule1 := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test1",
		Role:    placement.Leader,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z1"},
			},
		},
		LocationLabels: []string{"rack"},
	}
	rule2 := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test2",
		Role:    placement.Voter,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z1"},
			},
		},
		LocationLabels: []string{"rack"},
	}
	rule3 := &placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test3",
		Role:    placement.Voter,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z2"},
			},
		},
		LocationLabels: []string{"rack"},
	}
	suite.ruleManager.SetRule(rule1)
	suite.ruleManager.SetRule(rule2)
	suite.ruleManager.SetRule(rule3)
	suite.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("move-to-better-location", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestTiFlashLocationLabels() {
	re := suite.Require()
	suite.cluster.SetEnableUseJointConsensus(true)
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z2", "rack": "r3", "host": "h2"})
	suite.cluster.AddLabelsStore(6, 1, map[string]string{"zone": "z2", "rack": "r3", "host": "h2"})
	suite.cluster.AddLabelsStore(7, 1, map[string]string{"engine": "tiflash"})
	suite.cluster.AddRegionWithLearner(1, 1, []uint64{3, 5}, []uint64{7})

	rule1 := &placement.Rule{
		GroupID: "tiflash",
		ID:      "test1",
		Role:    placement.Learner,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "engine",
				Op:     placement.In,
				Values: []string{"tiflash"},
			},
		},
	}
	suite.ruleManager.SetRule(rule1)
	rule := suite.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels = []string{"zone", "rack", "host"}
	suite.ruleManager.SetRule(rule)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.Nil(op)
}

type ruleCheckerTestAdvancedSuite struct {
	suite.Suite
	cluster     *mockcluster.Cluster
	ruleManager *placement.RuleManager
	rc          *RuleChecker
	ctx         context.Context
	cancel      context.CancelFunc
}

func (suite *ruleCheckerTestAdvancedSuite) SetupTest() {
	cfg := mockconfig.NewTestOptions()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
	suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.SwitchWitness))
	suite.cluster.SetEnablePlacementRules(true)
	suite.cluster.SetEnableWitness(true)
	suite.cluster.SetEnableUseJointConsensus(true)
	suite.ruleManager = suite.cluster.RuleManager
	suite.rc = NewRuleChecker(suite.ctx, suite.cluster, suite.ruleManager, cache.NewIDTTL(suite.ctx, time.Minute, 3*time.Minute))
}

func (suite *ruleCheckerTestAdvancedSuite) TearDownTest() {
	suite.cancel()
}

func makeStores() placement.StoreSet {
	stores := core.NewStoresInfo()
	now := time.Now()
	for region := 1; region <= 3; region++ {
		for zone := 1; zone <= 5; zone++ {
			for host := 1; host <= 5; host++ {
				id := uint64(region*100 + zone*10 + host)
				labels := map[string]string{
					"region": fmt.Sprintf("region%d", region),
					"zone":   fmt.Sprintf("zone%d", zone),
					"host":   fmt.Sprintf("host%d", host),
				}
				if host == 5 {
					labels["engine"] = "tiflash"
				}
				if zone == 1 && host == 1 {
					labels["type"] = "read"
				}
				stores.PutStore(core.NewStoreInfoWithLabel(id, labels).Clone(core.SetLastHeartbeatTS(now), core.SetStoreState(metapb.StoreState_Up)))
			}
		}
	}
	return stores
}

// example: "1111_leader,1234,2111_learner"
func makeRegion(def string) *core.RegionInfo {
	var regionMeta metapb.Region
	var leader *metapb.Peer
	for _, peerDef := range strings.Split(def, ",") {
		role, idStr := placement.Follower, peerDef
		if strings.Contains(peerDef, "_") {
			splits := strings.Split(peerDef, "_")
			idStr, role = splits[0], placement.PeerRoleType(splits[1])
		}
		id, _ := strconv.Atoi(idStr)
		peer := &metapb.Peer{Id: uint64(id), StoreId: uint64(id), Role: role.MetaPeerRole()}
		regionMeta.Peers = append(regionMeta.Peers, peer)
		if role == placement.Leader {
			leader = peer
			regionMeta.Id = peer.Id - 1
		}
	}
	return core.NewRegionInfo(&regionMeta, leader)
}

// example: "3/voter/zone=zone1+zone2,rack=rack2/zone,rack,host"
// count role constraints location_labels
func makeRule(def string) *placement.Rule {
	var rule placement.Rule
	splits := strings.Split(def, "/")
	rule.Count, _ = strconv.Atoi(splits[0])
	rule.Role = placement.PeerRoleType(splits[1])
	// only support k=v type constraint
	for _, c := range strings.Split(splits[2], ",") {
		if c == "" {
			break
		}
		kv := strings.Split(c, "=")
		rule.LabelConstraints = append(rule.LabelConstraints, placement.LabelConstraint{
			Key:    kv[0],
			Op:     "in",
			Values: strings.Split(kv[1], "+"),
		})
	}
	rule.LocationLabels = strings.Split(splits[3], ",")
	return &rule
}

// TestReplaceAnExistingPeerCases address issue: https://github.com/tikv/pd/issues/7185
func (suite *ruleCheckerTestAdvancedSuite) TestReplaceAnExistingPeerCases() {
	re := suite.Require()
	stores := makeStores()
	for _, store := range stores.GetStores() {
		suite.cluster.PutStore(store)
	}

	testCases := []struct {
		region string
		rules  []string
		opStr  string
	}{
		{"111_leader,211,311", []string{"3/voter//", "3/learner/type=read/"}, "replace-rule-swap-fit-peer {mv peer: store [111] to"},
		{"211,311_leader,151", []string{"3/voter//", "3/learner/type=read/"}, "add-rule-peer {add peer: store [111]}"},
		{"111_learner,211,311_leader,151", []string{"3/voter//", "3/learner/type=read/"}, "replace-rule-swap-fit-peer {mv peer: store [211] to"},
		{"111_learner,311_leader,151,351", []string{"3/voter//", "3/learner/type=read/"}, "add-rule-peer {add peer: store [211]}"},
		{"111_learner,211_learner,311_leader,151,351", []string{"3/voter//", "3/learner/type=read/"}, "replace-rule-swap-fit-peer {mv peer: store [311] to"},
		{"111_learner,211_learner,151_leader,252,351", []string{"3/voter//", "3/learner/type=read/"}, "add-rule-peer {add peer: store [311]}"},
		{"111_learner,211_learner,311_learner,151_leader,252,351", []string{"3/voter//", "3/learner/type=read/"}, ""},
	}
	groupName := "a_test"
	for _, cas := range testCases {
		bundle := placement.GroupBundle{
			ID:       groupName,
			Index:    1000,
			Override: true,
			Rules:    make([]*placement.Rule, 0, len(cas.rules)),
		}
		for id, r := range cas.rules {
			rule := makeRule(r)
			rule.ID = fmt.Sprintf("r%d", id)
			bundle.Rules = append(bundle.Rules, rule)
		}
		err := suite.ruleManager.SetGroupBundle(bundle)
		re.NoError(err)
		region := makeRegion(cas.region)
		suite.cluster.PutRegion(region)
		op := suite.rc.Check(region)
		if len(cas.opStr) > 0 {
			re.Contains(op.String(), cas.opStr, cas.opStr)
		}
		suite.ruleManager.DeleteGroupBundle(groupName, false)
	}
}

func (suite *ruleCheckerTestSuite) TestRemoveOrphanPeer() {
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h1"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	suite.cluster.AddLabelsStore(6, 1, map[string]string{"zone": "z2", "host": "h2"})
	rule := &placement.Rule{
		GroupID: "pd",
		ID:      "test2",
		Role:    placement.Voter,
		Count:   3,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z2"},
			},
		},
	}
	suite.ruleManager.SetRule(rule)
	suite.ruleManager.DeleteRule("pd", "default")

	// case1: regionA has 3 peers but not extra peer can be removed, so it needs to add peer first
	suite.cluster.AddLeaderRegionWithRange(1, "200", "300", 1, 2, 3)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	suite.NotNil(op)
	suite.Equal("add-rule-peer", op.Desc())

	// case2: regionB has 4 peers and one extra peer can be removed, so it needs to remove extra peer first
	suite.cluster.AddLeaderRegionWithRange(2, "300", "400", 1, 2, 3, 4)
	op = suite.rc.Check(suite.cluster.GetRegion(2))
	suite.NotNil(op)
	suite.Equal("remove-orphan-peer", op.Desc())
}

func (suite *ruleCheckerTestSuite) TestIssue7808() {
	re := suite.Require()
	suite.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1", "disk_type": "mix"})
	suite.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2", "disk_type": "mix"})
	suite.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3", "disk_type": "ssd"})
	suite.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4", "disk_type": "ssd"})
	suite.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5", "disk_type": "ssd"})
	suite.cluster.AddLeaderRegionWithRange(1, "", "", 3, 4, 1)
	err := suite.ruleManager.SetRules([]*placement.Rule{
		{
			GroupID: "pd",
			ID:      "1",
			Role:    placement.Voter,
			Count:   2,
			LabelConstraints: []placement.LabelConstraint{
				{
					Key: "disk_type",
					Values: []string{
						"ssd",
					},
					Op: placement.In,
				},
			},
			LocationLabels: []string{"host"},
			IsolationLevel: "host",
		},
		{
			GroupID: "pd",
			ID:      "2",
			Role:    placement.Follower,
			Count:   1,
			LabelConstraints: []placement.LabelConstraint{
				{
					Key: "disk_type",
					Values: []string{
						"mix",
					},
					Op: placement.In,
				},
			},
			LocationLabels: []string{"host"},
			IsolationLevel: "host",
		},
	})
	re.NoError(err)
	err = suite.ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
	re.NoError(err)
	suite.cluster.SetStoreDown(1)
	region := suite.cluster.GetRegion(1)
	downPeer := []*pdpb.PeerStats{
		{Peer: region.GetStorePeer(1), DownSeconds: 6000},
	}
	region = region.Clone(core.WithDownPeers(downPeer))
	suite.cluster.PutRegion(region)
	op := suite.rc.Check(suite.cluster.GetRegion(1))
	re.NotNil(op)
	re.Equal("fast-replace-rule-down-peer", op.Desc())
	re.Contains(op.Brief(), "mv peer: store [1] to [2]")
}
