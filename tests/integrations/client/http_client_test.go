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

package client_test

import (
	"context"
	"math"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type httpClientTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancelFunc context.CancelFunc
	cluster    *tests.TestCluster
	client     pd.Client
}

func TestHTTPClientTestSuite(t *testing.T) {
	suite.Run(t, new(httpClientTestSuite))
}

func (suite *httpClientTestSuite) SetupSuite() {
	re := suite.Require()
	var err error
	suite.ctx, suite.cancelFunc = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leader := suite.cluster.WaitLeader()
	re.NotEmpty(leader)
	err = suite.cluster.GetLeaderServer().BootstrapCluster()
	re.NoError(err)
	var (
		testServers = suite.cluster.GetServers()
		endpoints   = make([]string, 0, len(testServers))
	)
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	suite.client = pd.NewClient(endpoints)
}

func (suite *httpClientTestSuite) TearDownSuite() {
	suite.cancelFunc()
	suite.client.Close()
	suite.cluster.Destroy()
}

func (suite *httpClientTestSuite) TestGetMinResolvedTSByStoresIDs() {
	re := suite.Require()
	var (
		minResolvedTS         uint64
		storeMinResolvedTSMap map[uint64]uint64
		err                   error
	)
	// Wait for the cluster-level min resolved TS to be initialized.
	testutil.Eventually(re, func() bool {
		minResolvedTS, storeMinResolvedTSMap, err = suite.client.GetMinResolvedTSByStoresIDs(suite.ctx, nil)
		re.NoError(err)
		return minResolvedTS > 0 && len(storeMinResolvedTSMap) == 0
	})
	// Get the store-level min resolved TS.
	minResolvedTS, storeMinResolvedTSMap, err = suite.client.GetMinResolvedTSByStoresIDs(suite.ctx, []uint64{1})
	re.NoError(err)
	re.Greater(minResolvedTS, uint64(0))
	re.Len(storeMinResolvedTSMap, 1)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	// Get the store-level min resolved TS with an invalid store ID.
	minResolvedTS, storeMinResolvedTSMap, err = suite.client.GetMinResolvedTSByStoresIDs(suite.ctx, []uint64{1, 2})
	re.NoError(err)
	re.Greater(minResolvedTS, uint64(0))
	re.Len(storeMinResolvedTSMap, 2)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	re.Equal(uint64(math.MaxUint64), storeMinResolvedTSMap[2])
}

func (suite *httpClientTestSuite) TestRule() {
	re := suite.Require()
	bundles, err := suite.client.GetAllPlacementRuleBundles(suite.ctx)
	re.NoError(err)
	re.Len(bundles, 1)
	re.Equal(bundles[0].ID, placement.DefaultGroupID)
	bundle, err := suite.client.GetPlacementRuleBundleByGroup(suite.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Equal(bundles[0], bundle)
	// Check if we have the default rule.
	suite.checkRule(re, &pd.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    pd.Voter,
		Count:   3,
	}, 1, true)
	// Should be the same as the rules in the bundle.
	suite.checkRule(re, bundle.Rules[0], 1, true)
	testRule := &pd.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test",
		Role:    pd.Voter,
		Count:   3,
	}
	err = suite.client.SetPlacementRule(suite.ctx, testRule)
	re.NoError(err)
	suite.checkRule(re, testRule, 2, true)
	err = suite.client.DeletePlacementRule(suite.ctx, placement.DefaultGroupID, "test")
	re.NoError(err)
	suite.checkRule(re, testRule, 1, false)
	testRuleOp := &pd.RuleOp{
		Rule:   testRule,
		Action: pd.RuleOpAdd,
	}
	err = suite.client.SetPlacementRuleInBatch(suite.ctx, []*pd.RuleOp{testRuleOp})
	re.NoError(err)
	suite.checkRule(re, testRule, 2, true)
	testRuleOp = &pd.RuleOp{
		Rule:   testRule,
		Action: pd.RuleOpDel,
	}
	err = suite.client.SetPlacementRuleInBatch(suite.ctx, []*pd.RuleOp{testRuleOp})
	re.NoError(err)
	suite.checkRule(re, testRule, 1, false)
	err = suite.client.SetPlacementRuleBundles(suite.ctx, []*pd.GroupBundle{
		{
			ID:    placement.DefaultGroupID,
			Rules: []*pd.Rule{testRule},
		},
	}, true)
	re.NoError(err)
	suite.checkRule(re, testRule, 1, true)
	ruleGroups, err := suite.client.GetAllPlacementRuleGroups(suite.ctx)
	re.NoError(err)
	re.Len(ruleGroups, 1)
	re.Equal(placement.DefaultGroupID, ruleGroups[0].ID)
	ruleGroup, err := suite.client.GetPlacementRuleGroupByID(suite.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Equal(ruleGroups[0], ruleGroup)
	testRuleGroup := &pd.RuleGroup{
		ID:       "test-group",
		Index:    1,
		Override: true,
	}
	err = suite.client.SetPlacementRuleGroup(suite.ctx, testRuleGroup)
	re.NoError(err)
	ruleGroup, err = suite.client.GetPlacementRuleGroupByID(suite.ctx, testRuleGroup.ID)
	re.NoError(err)
	re.Equal(testRuleGroup, ruleGroup)
	err = suite.client.DeletePlacementRuleGroupByID(suite.ctx, testRuleGroup.ID)
	re.NoError(err)
	ruleGroup, err = suite.client.GetPlacementRuleGroupByID(suite.ctx, testRuleGroup.ID)
	re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	re.Empty(ruleGroup)
}

func (suite *httpClientTestSuite) checkRule(
	re *require.Assertions,
	rule *pd.Rule, totalRuleCount int, exist bool,
) {
	// Check through the `GetPlacementRulesByGroup` API.
	rules, err := suite.client.GetPlacementRulesByGroup(suite.ctx, rule.GroupID)
	re.NoError(err)
	checkRuleFunc(re, rules, rule, totalRuleCount, exist)
	// Check through the `GetPlacementRuleBundleByGroup` API.
	bundle, err := suite.client.GetPlacementRuleBundleByGroup(suite.ctx, rule.GroupID)
	re.NoError(err)
	checkRuleFunc(re, bundle.Rules, rule, totalRuleCount, exist)
}

func checkRuleFunc(
	re *require.Assertions,
	rules []*pd.Rule, rule *pd.Rule, totalRuleCount int, exist bool,
) {
	re.Len(rules, totalRuleCount)
	for _, r := range rules {
		if r.ID != rule.ID {
			continue
		}
		re.Equal(rule.GroupID, r.GroupID)
		re.Equal(rule.ID, r.ID)
		re.Equal(rule.Role, r.Role)
		re.Equal(rule.Count, r.Count)
		return
	}
	if exist {
		re.Failf("Failed to check the rule", "rule %+v not found", rule)
	}
}

func (suite *httpClientTestSuite) TestRegionLabel() {
	re := suite.Require()
	labelRules, err := suite.client.GetAllRegionLabelRules(suite.ctx)
	re.NoError(err)
	re.Len(labelRules, 1)
	re.Equal("keyspaces/0", labelRules[0].ID)
	// Set a new region label rule.
	labelRule := &pd.LabelRule{
		ID:       "rule1",
		Labels:   []pd.RegionLabel{{Key: "k1", Value: "v1"}},
		RuleType: "key-range",
		Data:     labeler.MakeKeyRanges("1234", "5678"),
	}
	err = suite.client.SetRegionLabelRule(suite.ctx, labelRule)
	re.NoError(err)
	labelRules, err = suite.client.GetAllRegionLabelRules(suite.ctx)
	re.NoError(err)
	re.Len(labelRules, 2)
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Equal(labelRule.ID, labelRules[1].ID)
	re.Equal(labelRule.Labels, labelRules[1].Labels)
	re.Equal(labelRule.RuleType, labelRules[1].RuleType)
	// Patch the region label rule.
	labelRule = &pd.LabelRule{
		ID:       "rule2",
		Labels:   []pd.RegionLabel{{Key: "k2", Value: "v2"}},
		RuleType: "key-range",
		Data:     labeler.MakeKeyRanges("ab12", "cd12"),
	}
	patch := &pd.LabelRulePatch{
		SetRules:    []*pd.LabelRule{labelRule},
		DeleteRules: []string{"rule1"},
	}
	err = suite.client.PatchRegionLabelRules(suite.ctx, patch)
	re.NoError(err)
	allLabelRules, err := suite.client.GetAllRegionLabelRules(suite.ctx)
	re.NoError(err)
	re.Len(labelRules, 2)
	sort.Slice(allLabelRules, func(i, j int) bool {
		return allLabelRules[i].ID < allLabelRules[j].ID
	})
	re.Equal(labelRule.ID, allLabelRules[1].ID)
	re.Equal(labelRule.Labels, allLabelRules[1].Labels)
	re.Equal(labelRule.RuleType, allLabelRules[1].RuleType)
	labelRules, err = suite.client.GetRegionLabelRulesByIDs(suite.ctx, []string{"keyspaces/0", "rule2"})
	re.NoError(err)
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Equal(allLabelRules, labelRules)
}

func (suite *httpClientTestSuite) TestAccelerateSchedule() {
	re := suite.Require()
	r1 := core.NewTestRegionInfo(10, 1, []byte("a1"), []byte("a2"))
	r2 := core.NewTestRegionInfo(11, 1, []byte("a2"), []byte("a3"))
	raftCluster := suite.cluster.GetLeaderServer().GetRaftCluster()
	err := raftCluster.HandleRegionHeartbeat(r1)
	re.NoError(err)
	err = raftCluster.HandleRegionHeartbeat(r2)
	re.NoError(err)
	suspectRegions := raftCluster.GetSuspectRegions()
	re.Len(suspectRegions, 0)
	err = suite.client.AccelerateSchedule(suite.ctx, []byte("a1"), []byte("a2"))
	re.NoError(err)
	suspectRegions = raftCluster.GetSuspectRegions()
	re.Len(suspectRegions, 1)
	raftCluster.ClearSuspectRegions()
	suspectRegions = raftCluster.GetSuspectRegions()
	re.Len(suspectRegions, 0)
	err = suite.client.AccelerateScheduleInBatch(suite.ctx, []struct {
		StartKey []byte `json:"start_key"`
		EndKey   []byte `json:"end_key"`
	}{
		{
			StartKey: []byte("a1"),
			EndKey:   []byte("a2"),
		},
		{
			StartKey: []byte("a2"),
			EndKey:   []byte("a3"),
		},
	})
	re.NoError(err)
	suspectRegions = raftCluster.GetSuspectRegions()
	re.Len(suspectRegions, 2)
}
