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
	rules := make([]*placement.Rule, 0)
	testutil.Eventually(re, func() bool {
		rules = ruleManager.GetAllRules()
		return len(rules) == 2
	})
	re.Len(rules, 2)
	var gotRule *placement.Rule
	for _, r := range rules {
		if r.GroupID == rule.GroupID && r.ID == rule.ID {
			gotRule = r
			break
		}
	}
	re.NotNil(gotRule)
	re.Equal(rule.Role, gotRule.Role)
	re.Equal(rule.Count, gotRule.Count)
	re.Equal(rule.StartKeyHex, gotRule.StartKeyHex)
	re.Equal(rule.EndKeyHex, gotRule.EndKeyHex)
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
	ruleGroups := make([]*placement.RuleGroup, 0)
	testutil.Eventually(re, func() bool {
		ruleGroups = ruleManager.GetRuleGroups()
		return len(ruleGroups) == 2
	})
	re.Len(ruleGroups, 2)
	var gotRuleGroup *placement.RuleGroup
	for _, rg := range ruleGroups {
		if rg.ID == ruleGroup.ID {
			gotRuleGroup = rg
			break
		}
	}
	re.NotNil(gotRuleGroup)
	re.Equal(ruleGroup.Index, gotRuleGroup.Index)
	re.Equal(ruleGroup.Override, gotRuleGroup.Override)
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
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	sort.Slice(apiLabelRules, func(i, j int) bool {
		return apiLabelRules[i].ID < apiLabelRules[j].ID
	})
	for i := range apiLabelRules {
		re.Equal(apiLabelRules[i].ID, labelRules[i].ID)
		re.Equal(apiLabelRules[i].Index, labelRules[i].Index)
		re.Equal(apiLabelRules[i].Labels, labelRules[i].Labels)
		re.Equal(apiLabelRules[i].RuleType, labelRules[i].RuleType)
	}
	baseLabelRuleCount := len(labelRules)
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
		return len(labelRules) == baseLabelRuleCount+1
	})
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Len(labelRules, baseLabelRuleCount+1)
	var gotLabelRule *labeler.LabelRule
	for _, lr := range labelRules {
		if lr.ID == labelRule.ID {
			gotLabelRule = lr
			break
		}
	}
	re.NotNil(gotLabelRule)
	re.Equal(labelRule.Labels, gotLabelRule.Labels)
	re.Equal(labelRule.RuleType, gotLabelRule.RuleType)
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
		return len(labelRules) == baseLabelRuleCount+1
	})
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Len(labelRules, baseLabelRuleCount+1)
	gotLabelRule = nil
	for _, lr := range labelRules {
		if lr.ID == labelRule.ID {
			gotLabelRule = lr
			break
		}
	}
	re.NotNil(gotLabelRule)
	re.Equal(labelRule.Labels, gotLabelRule.Labels)
	re.Equal(labelRule.RuleType, gotLabelRule.RuleType)
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
