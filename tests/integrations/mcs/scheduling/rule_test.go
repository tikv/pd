// Copyright 2023 TiKV Project Authors.
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

package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type ruleTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer  *tests.TestServer
	backendEndpoint string
}

func TestRule(t *testing.T) {
	suite.Run(t, &ruleTestSuite{})
}

func (suite *ruleTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	suite.backendEndpoint = suite.pdLeaderServer.GetAddr()
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
}

func (suite *ruleTestSuite) TearDownSuite() {
	suite.cancel()
	suite.cluster.Destroy()
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func (suite *ruleTestSuite) TestAffinityOverlapStartup() {
	re := suite.Require()
	raftCluster := suite.pdLeaderServer.GetRaftCluster()
	change := func(id, start, end string) affinity.GroupKeyRanges {
		return affinity.GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte(start), EndKey: []byte(end)}}}
	}
	a, b := change("overlap-a", "a", "d"), change("overlap-b", "b", "c")
	snapshot, watch := change("paired-snapshot", "x", "y"), change("paired-watch", "y", "z")
	ids := []string{a.GroupID, b.GroupID, snapshot.GroupID, watch.GroupID}
	// Persist the overlap before restarting PD.
	store := raftCluster.GetStorage()
	re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		for _, group := range []affinity.GroupKeyRanges{a, b} {
			id := group.GroupID
			re.NoError(store.SaveAffinityGroup(txn, id, &affinity.Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}))
			re.NoError(store.SaveRegionRule(txn, affinity.GetLabelRuleID(id), affinity.MakeLabelRule(group)))
		}
		return nil
	}))
	raftCluster.Stop()
	raftCluster.Wait()
	re.NoError(raftCluster.Start(suite.pdLeaderServer.GetServer(), false))
	pd := raftCluster.GetAffinityManager()
	defer func() { re.NoError(pd.DeleteAffinityGroups(ids, true)) }()
	re.NoError(pd.CreateAffinityGroups([]affinity.GroupKeyRanges{snapshot}))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	cluster := tc.WaitForPrimaryServing(re).GetCluster()
	mcs, rules := cluster.GetAffinityManager(), cluster.GetRegionLabeler()
	re.NoError(pd.CreateAffinityGroups([]affinity.GroupKeyRanges{watch}))
	for _, change := range []affinity.GroupKeyRanges{snapshot, watch} {
		testutil.Eventually(re, func() bool {
			state := mcs.GetAffinityGroupState(change.GroupID)
			return state != nil && state.RangeCount == 1 && rules.GetLabelRule(affinity.GetLabelRuleID(change.GroupID)) != nil
		})
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 10, StartKey: b.KeyRanges[0].StartKey, EndKey: b.KeyRanges[0].EndKey}, nil)
	for _, manager := range []*affinity.Manager{pd, mcs} {
		for _, id := range ids[:2] {
			manager.RestoreAffinityGroup(id)
			state := manager.GetAffinityGroupState(id)
			re.Equal(affinity.PhasePending, state.Phase)
			re.False(state.AffinitySchedulingAllowed)
			re.True(state.RegularSchedulingAllowed)
			re.Equal(1, state.RangeCount)
		}
		group, _ := manager.GetRegionAffinityGroupState(region)
		re.Nil(group)
	}
	re.NoError(pd.DeleteAffinityGroups(ids[:1], true))
	testutil.Eventually(re, func() bool {
		group, _ := mcs.GetRegionAffinityGroupState(region)
		return group != nil && group.ID == b.GroupID
	})
	re.NoError(pd.DeleteAffinityGroups(ids, true))
	for _, id := range ids {
		testutil.Eventually(re, func() bool {
			return !mcs.IsGroupExist(id) && rules.GetLabelRule(affinity.GetLabelRuleID(id)) == nil
		})
	}
}

func (suite *ruleTestSuite) TestRuleWatch() {
	re := suite.Require()

	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()

	tc.WaitForPrimaryServing(re)
	cluster := tc.GetPrimaryServer().GetCluster()
	ruleManager := cluster.GetRuleManager()
	// Check the default rule and rule group.
	rules := ruleManager.GetAllRules()
	re.Len(rules, 1)
	re.Equal(placement.DefaultGroupID, rules[0].GroupID)
	re.Equal(placement.DefaultRuleID, rules[0].ID)
	re.Equal(0, rules[0].Index)
	re.Empty(rules[0].StartKey)
	re.Empty(rules[0].EndKey)
	re.Equal(placement.Voter, rules[0].Role)
	re.Empty(rules[0].LocationLabels)
	ruleGroups := ruleManager.GetRuleGroups()
	re.Len(ruleGroups, 1)
	re.Equal(placement.DefaultGroupID, ruleGroups[0].ID)
	re.Equal(0, ruleGroups[0].Index)
	re.False(ruleGroups[0].Override)
	// Set a new rule via the PD.
	apiRuleManager := suite.pdLeaderServer.GetRaftCluster().GetRuleManager()
	rule := &placement.Rule{
		GroupID:     "2",
		ID:          "3",
		Role:        placement.Voter,
		Count:       1,
		StartKeyHex: "22",
		EndKeyHex:   "dd",
	}
	err = apiRuleManager.SetRule(rule)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		rules = ruleManager.GetAllRules()
		return len(rules) == 2
	})
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID > rules[j].ID
	})
	re.Len(rules, 2)
	re.Equal(rule.GroupID, rules[1].GroupID)
	re.Equal(rule.ID, rules[1].ID)
	re.Equal(rule.Role, rules[1].Role)
	re.Equal(rule.Count, rules[1].Count)
	re.Equal(rule.StartKeyHex, rules[1].StartKeyHex)
	re.Equal(rule.EndKeyHex, rules[1].EndKeyHex)
	// Delete the rule.
	err = apiRuleManager.DeleteRule(rule.GroupID, rule.ID)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		rules = ruleManager.GetAllRules()
		return len(rules) == 1
	})
	re.Len(rules, 1)
	re.Equal(placement.DefaultGroupID, rules[0].GroupID)
	// Create a new rule group.
	ruleGroup := &placement.RuleGroup{
		ID:       "2",
		Index:    100,
		Override: true,
	}
	err = apiRuleManager.SetRuleGroup(ruleGroup)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		ruleGroups = ruleManager.GetRuleGroups()
		return len(ruleGroups) == 2
	})
	re.Len(ruleGroups, 2)
	re.Equal(ruleGroup.ID, ruleGroups[1].ID)
	re.Equal(ruleGroup.Index, ruleGroups[1].Index)
	re.Equal(ruleGroup.Override, ruleGroups[1].Override)
	// Delete the rule group.
	err = apiRuleManager.DeleteRuleGroup(ruleGroup.ID)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		ruleGroups = ruleManager.GetRuleGroups()
		return len(ruleGroups) == 1
	})
	re.Len(ruleGroups, 1)

	// Test the region label rule watch.
	regionLabeler := cluster.GetRegionLabeler()
	labelRules := regionLabeler.GetAllLabelRules()
	apiRegionLabeler := suite.pdLeaderServer.GetRaftCluster().GetRegionLabeler()
	apiLabelRules := apiRegionLabeler.GetAllLabelRules()
	re.Len(labelRules, len(apiLabelRules))
	re.Equal(apiLabelRules[0].ID, labelRules[0].ID)
	re.Equal(apiLabelRules[0].Index, labelRules[0].Index)
	re.Equal(apiLabelRules[0].Labels, labelRules[0].Labels)
	re.Equal(apiLabelRules[0].RuleType, labelRules[0].RuleType)
	// Set a new region label rule.
	labelRule := &labeler.LabelRule{
		ID:       "rule1",
		Labels:   []labeler.RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     labeler.MakeKeyRanges("1234", "5678"),
	}
	err = apiRegionLabeler.SetLabelRule(labelRule)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		labelRules = regionLabeler.GetAllLabelRules()
		return len(labelRules) == 2
	})
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Len(labelRules, 2)
	re.Equal(labelRule.ID, labelRules[1].ID)
	re.Equal(labelRule.Labels, labelRules[1].Labels)
	re.Equal(labelRule.RuleType, labelRules[1].RuleType)
	// Patch the region label rule.
	labelRule = &labeler.LabelRule{
		ID:       "rule2",
		Labels:   []labeler.RegionLabel{{Key: "k2", Value: "v2"}},
		RuleType: "key-range",
		Data:     labeler.MakeKeyRanges("ab12", "cd12"),
	}
	patch := labeler.LabelRulePatch{
		SetRules:    []*labeler.LabelRule{labelRule},
		DeleteRules: []string{"rule1"},
	}
	err = apiRegionLabeler.Patch(patch)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		labelRules = regionLabeler.GetAllLabelRules()
		return len(labelRules) == 2
	})
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Len(labelRules, 2)
	re.Equal(labelRule.ID, labelRules[1].ID)
	re.Equal(labelRule.Labels, labelRules[1].Labels)
	re.Equal(labelRule.RuleType, labelRules[1].RuleType)
}

func (suite *ruleTestSuite) TestSchedulingSwitch() {
	re := suite.Require()

	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 2, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForPrimaryServing(re)

	// Add a new rule from "" to ""
	url := fmt.Sprintf("%s/pd/api/v1/config/placement-rule", suite.pdLeaderServer.GetAddr())
	respBundle := make([]placement.GroupBundle, 0)
	testutil.Eventually(re, func() bool {
		err = testutil.CheckGetJSON(tests.TestDialClient, url, nil,
			testutil.StatusOK(re), testutil.ExtractJSON(re, &respBundle))
		re.NoError(err)
		return len(respBundle) == 1 && len(respBundle[0].Rules) == 1
	})

	b2 := placement.GroupBundle{
		ID:    "pd",
		Index: 1,
		Rules: []*placement.Rule{
			{GroupID: "pd", ID: "rule0", Index: 1, Role: placement.Voter, Count: 3},
		},
	}
	data, err := json.Marshal(b2)
	re.NoError(err)

	err = testutil.CheckPostJSON(tests.TestDialClient, url+"/pd", data, testutil.StatusOK(re))
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		err = testutil.CheckGetJSON(tests.TestDialClient, url, nil,
			testutil.StatusOK(re), testutil.ExtractJSON(re, &respBundle))
		re.NoError(err)
		return len(respBundle) == 1 && len(respBundle[0].Rules) == 1
	})

	// Switch another server
	oldPrimary := tc.GetPrimaryServer()
	oldPrimary.Close()
	tc.WaitForPrimaryServing(re)
	newPrimary := tc.GetPrimaryServer()
	re.NotEqual(oldPrimary.GetAddr(), newPrimary.GetAddr())
	testutil.Eventually(re, func() bool {
		err = testutil.CheckGetJSON(tests.TestDialClient, url, nil,
			testutil.StatusOK(re), testutil.ExtractJSON(re, &respBundle))
		re.NoError(err)
		return len(respBundle) == 1 && len(respBundle[0].Rules) == 1
	})
}
