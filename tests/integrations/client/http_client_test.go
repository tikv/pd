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
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type httpClientTestSuite struct {
	suite.Suite
	// 1. Using `NewClient` will create a `DefaultPDServiceDiscovery` internal.
	// 2. Using `NewClientWithServiceDiscovery` will need a `PDServiceDiscovery` to be passed in.
	withServiceDiscovery bool
	ctx                  context.Context
	cancelFunc           context.CancelFunc
	cluster              *tests.TestCluster
	endpoints            []string
	client               pd.Client
}

func TestHTTPClientTestSuite(t *testing.T) {
	suite.Run(t, &httpClientTestSuite{
		withServiceDiscovery: false,
	})
}

func TestHTTPClientTestSuiteWithServiceDiscovery(t *testing.T) {
	suite.Run(t, &httpClientTestSuite{
		withServiceDiscovery: true,
	})
}

func (suite *httpClientTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	suite.ctx, suite.cancelFunc = context.WithCancel(context.Background())

	cluster, err := tests.NewTestCluster(suite.ctx, 2)
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetLeaderServer()

	err = leaderServer.BootstrapCluster()
	// Add 2 more stores to the cluster.
	for i := 2; i <= 4; i++ {
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		})
	}
	re.NoError(err)
	for _, region := range []*core.RegionInfo{
		core.NewTestRegionInfo(10, 1, []byte("a1"), []byte("a2")),
		core.NewTestRegionInfo(11, 1, []byte("a2"), []byte("a3")),
	} {
		err := leaderServer.GetRaftCluster().HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	var (
		testServers = cluster.GetServers()
		endpoints   = make([]string, 0, len(testServers))
	)
	for _, s := range testServers {
		addr := s.GetConfig().AdvertiseClientUrls
		url, err := url.Parse(addr)
		re.NoError(err)
		endpoints = append(endpoints, url.Host)
	}
	suite.endpoints = endpoints
	suite.cluster = cluster

	if suite.withServiceDiscovery {
		// Run test with specific service discovery.
		cli := setupCli(suite.ctx, re, suite.endpoints)
		sd := cli.GetServiceDiscovery()
		suite.client = pd.NewClientWithServiceDiscovery("pd-http-client-it-grpc", sd)
	} else {
		// Run test with default service discovery.
		suite.client = pd.NewClient("pd-http-client-it-http", suite.endpoints)
	}
}

func (suite *httpClientTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	suite.cancelFunc()
	suite.client.Close()
	suite.cluster.Destroy()
}

func (suite *httpClientTestSuite) TestMeta() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	replicateConfig, err := client.GetReplicateConfig(ctx)
	re.NoError(err)
	re.Equal(3.0, replicateConfig["max-replicas"])
	region, err := client.GetRegionByID(ctx, 10)
	re.NoError(err)
	re.Equal(int64(10), region.ID)
	re.Equal(core.HexRegionKeyStr([]byte("a1")), region.StartKey)
	re.Equal(core.HexRegionKeyStr([]byte("a2")), region.EndKey)
	region, err = client.GetRegionByKey(ctx, []byte("a2"))
	re.NoError(err)
	re.Equal(int64(11), region.ID)
	re.Equal(core.HexRegionKeyStr([]byte("a2")), region.StartKey)
	re.Equal(core.HexRegionKeyStr([]byte("a3")), region.EndKey)
	regions, err := client.GetRegions(ctx)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	regions, err = client.GetRegionsByKeyRange(ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), -1)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	regions, err = client.GetRegionsByStoreID(ctx, 1)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	regions, err = client.GetEmptyRegions(ctx)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	state, err := client.GetRegionsReplicatedStateByKeyRange(ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")))
	re.NoError(err)
	re.Equal("INPROGRESS", state)
	regionStats, err := client.GetRegionStatusByKeyRange(ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), false)
	re.NoError(err)
	re.Positive(regionStats.Count)
	re.NotEmpty(regionStats.StoreLeaderCount)
	regionStats, err = client.GetRegionStatusByKeyRange(ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), true)
	re.NoError(err)
	re.Positive(regionStats.Count)
	re.Empty(regionStats.StoreLeaderCount)
	hotReadRegions, err := client.GetHotReadRegions(ctx)
	re.NoError(err)
	re.Len(hotReadRegions.AsPeer, 4)
	re.Len(hotReadRegions.AsLeader, 4)
	hotWriteRegions, err := client.GetHotWriteRegions(ctx)
	re.NoError(err)
	re.Len(hotWriteRegions.AsPeer, 4)
	re.Len(hotWriteRegions.AsLeader, 4)

	distribution, err := client.GetRegionDistributionByKeyRange(ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), "tikv")
	re.NoError(err)
	re.Len(distribution.RegionDistributions, 4)
	sort.Slice(distribution.RegionDistributions, func(i, j int) bool {
		return distribution.RegionDistributions[i].StoreID < distribution.RegionDistributions[j].StoreID
	})
	re.Equal(2, distribution.RegionDistributions[0].RegionLeaderCount)
	re.Equal(2, distribution.RegionDistributions[0].RegionPeerCount)
	historyHorRegions, err := client.GetHistoryHotRegions(ctx, &pd.HistoryHotRegionsRequest{
		StartTime: 0,
		EndTime:   time.Now().AddDate(0, 0, 1).UnixNano() / int64(time.Millisecond),
	})
	re.NoError(err)
	re.Empty(historyHorRegions.HistoryHotRegion)
	stores, err := client.GetStores(ctx)
	re.NoError(err)
	re.Equal(4, stores.Count)
	re.Len(stores.Stores, 4)
	storeID := uint64(stores.Stores[0].Store.ID) // TODO: why type is different?
	store2, err := client.GetStore(ctx, storeID)
	re.NoError(err)
	re.EqualValues(storeID, store2.Store.ID)
	version, err := client.GetClusterVersion(ctx)
	re.NoError(err)
	re.Equal("1.0.0", version)
	rgs, err := client.GetRegionsByKeyRange(ctx, pd.NewKeyRange([]byte("a"), []byte("a1")), 100)
	re.NoError(err)
	re.Equal(int64(0), rgs.Count)
	rgs, err = client.GetRegionsByKeyRange(ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), 100)
	re.NoError(err)
	re.Equal(int64(2), rgs.Count)
	rgs, err = client.GetRegionsByKeyRange(ctx, pd.NewKeyRange([]byte("a2"), []byte("b")), 100)
	re.NoError(err)
	re.Equal(int64(1), rgs.Count)
	rgs, err = client.GetRegionsByKeyRange(ctx, pd.NewKeyRange([]byte(""), []byte("")), 100)
	re.NoError(err)
	re.Equal(int64(2), rgs.Count)
	// store 2 origin status:offline
	err = client.DeleteStore(ctx, 2)
	re.NoError(err)
	store2, err = client.GetStore(ctx, 2)
	re.NoError(err)
	re.Equal(int64(metapb.StoreState_Offline), store2.Store.State)
}

func (suite *httpClientTestSuite) TestGetMinResolvedTSByStoresIDs() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	testMinResolvedTS := tsoutil.TimeToTS(time.Now())
	raftCluster := suite.cluster.GetLeaderServer().GetRaftCluster()
	err := raftCluster.SetMinResolvedTS(1, testMinResolvedTS)
	re.NoError(err)
	// Make sure the min resolved TS is updated.
	testutil.Eventually(re, func() bool {
		minResolvedTS, _ := raftCluster.CheckAndUpdateMinResolvedTS()
		return minResolvedTS == testMinResolvedTS
	})
	// Wait for the cluster-level min resolved TS to be initialized.
	minResolvedTS, storeMinResolvedTSMap, err := client.GetMinResolvedTSByStoresIDs(ctx, nil)
	re.NoError(err)
	re.Equal(testMinResolvedTS, minResolvedTS)
	re.Empty(storeMinResolvedTSMap)
	// Get the store-level min resolved TS.
	minResolvedTS, storeMinResolvedTSMap, err = client.GetMinResolvedTSByStoresIDs(ctx, []uint64{1})
	re.NoError(err)
	re.Equal(testMinResolvedTS, minResolvedTS)
	re.Len(storeMinResolvedTSMap, 1)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	// Get the store-level min resolved TS with an invalid store ID.
	minResolvedTS, storeMinResolvedTSMap, err = client.GetMinResolvedTSByStoresIDs(ctx, []uint64{1, 2})
	re.NoError(err)
	re.Equal(testMinResolvedTS, minResolvedTS)
	re.Len(storeMinResolvedTSMap, 2)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	re.Equal(uint64(math.MaxUint64), storeMinResolvedTSMap[2])
}

func (suite *httpClientTestSuite) TestRule() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	bundles, err := client.GetAllPlacementRuleBundles(ctx)
	re.NoError(err)
	re.Len(bundles, 1)
	re.Equal(placement.DefaultGroupID, bundles[0].ID)
	bundle, err := client.GetPlacementRuleBundleByGroup(ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Equal(bundles[0], bundle)
	// Check if we have the default rule.
	suite.checkRuleResult(ctx, re, &pd.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       placement.DefaultRuleID,
		Role:     pd.Voter,
		Count:    3,
		StartKey: []byte{},
		EndKey:   []byte{},
	}, 1, true)
	// Should be the same as the rules in the bundle.
	suite.checkRuleResult(ctx, re, bundle.Rules[0], 1, true)
	testRule := &pd.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       "test",
		Role:     pd.Voter,
		Count:    3,
		StartKey: []byte{},
		EndKey:   []byte{},
	}
	err = client.SetPlacementRule(ctx, testRule)
	re.NoError(err)
	suite.checkRuleResult(ctx, re, testRule, 2, true)
	err = client.DeletePlacementRule(ctx, placement.DefaultGroupID, "test")
	re.NoError(err)
	suite.checkRuleResult(ctx, re, testRule, 1, false)
	testRuleOp := &pd.RuleOp{
		Rule:   testRule,
		Action: pd.RuleOpAdd,
	}
	err = client.SetPlacementRuleInBatch(ctx, []*pd.RuleOp{testRuleOp})
	re.NoError(err)
	suite.checkRuleResult(ctx, re, testRule, 2, true)
	testRuleOp = &pd.RuleOp{
		Rule:   testRule,
		Action: pd.RuleOpDel,
	}
	err = client.SetPlacementRuleInBatch(ctx, []*pd.RuleOp{testRuleOp})
	re.NoError(err)
	suite.checkRuleResult(ctx, re, testRule, 1, false)
	err = client.SetPlacementRuleBundles(ctx, []*pd.GroupBundle{
		{
			ID:    placement.DefaultGroupID,
			Rules: []*pd.Rule{testRule},
		},
	}, true)
	re.NoError(err)
	suite.checkRuleResult(ctx, re, testRule, 1, true)
	ruleGroups, err := client.GetAllPlacementRuleGroups(ctx)
	re.NoError(err)
	re.Len(ruleGroups, 1)
	re.Equal(placement.DefaultGroupID, ruleGroups[0].ID)
	ruleGroup, err := client.GetPlacementRuleGroupByID(ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Equal(ruleGroups[0], ruleGroup)
	testRuleGroup := &pd.RuleGroup{
		ID:       "test-group",
		Index:    1,
		Override: true,
	}
	err = client.SetPlacementRuleGroup(ctx, testRuleGroup)
	re.NoError(err)
	ruleGroup, err = client.GetPlacementRuleGroupByID(ctx, testRuleGroup.ID)
	re.NoError(err)
	re.Equal(testRuleGroup, ruleGroup)
	err = client.DeletePlacementRuleGroupByID(ctx, testRuleGroup.ID)
	re.NoError(err)
	ruleGroup, err = client.GetPlacementRuleGroupByID(ctx, testRuleGroup.ID)
	re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	re.Empty(ruleGroup)
	// Test the start key and end key.
	testRule = &pd.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       "test",
		Role:     pd.Voter,
		Count:    5,
		StartKey: []byte("a1"),
		EndKey:   []byte(""),
	}
	err = client.SetPlacementRule(ctx, testRule)
	re.NoError(err)
	suite.checkRuleResult(ctx, re, testRule, 1, true)

	// ***** Test placement rule failed passing check after transfer leader
	// Transfer the leader to another store to ensure the PD follower
	// exists stale store labels.
	suite.transferLeader(ctx, re)
	tranferLeaderRule := []*pd.GroupBundle{
		{
			ID: "test-transfer-leader",
			Rules: []*pd.Rule{
				{
					GroupID:  "test-transfer-leader",
					ID:       "readonly",
					Role:     pd.Voter,
					Count:    3,
					StartKey: []byte{},
					EndKey:   []byte{},
					LabelConstraints: []pd.LabelConstraint{
						{
							Key:    "$mode",
							Op:     pd.In,
							Values: []string{"readonly"},
						},
					},
				},
			},
		},
	}
	err = client.SetPlacementRuleBundles(ctx, tranferLeaderRule, true)
	re.Error(err)
	re.ErrorContains(err, "invalid rule content, rule 'readonly' from rule group 'test-transfer-leader' can not match any store")
	storeID := suite.setStoreLabels(ctx, re, map[string]string{
		"$mode": "readonly",
	})
	err = client.SetPlacementRuleBundles(ctx, tranferLeaderRule, true)
	re.NoError(err)
	suite.checkRuleResult(ctx, re, tranferLeaderRule[0].Rules[0], 1, true)

	suite.transferLeader(ctx, re)
	suite.checkRuleResult(ctx, re, tranferLeaderRule[0].Rules[0], 1, true)
	re.NoError(client.DeleteStoreLabel(ctx, storeID, "$mode"))
	store, err := client.GetStore(ctx, uint64(storeID))
	re.NoError(err)
	re.Empty(store.Store.Labels)
}

func (suite *httpClientTestSuite) checkRuleResult(
	ctx context.Context, re *require.Assertions,
	rule *pd.Rule, totalRuleCount int, exist bool,
) {
	client := suite.client
	if exist {
		got, err := client.GetPlacementRule(ctx, rule.GroupID, rule.ID)
		re.NoError(err)
		// skip comparison of the generated field
		got.StartKeyHex = rule.StartKeyHex
		got.EndKeyHex = rule.EndKeyHex
		re.Equal(rule, got)
	} else {
		_, err := client.GetPlacementRule(ctx, rule.GroupID, rule.ID)
		re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	}
	// Check through the `GetPlacementRulesByGroup` API.
	rules, err := client.GetPlacementRulesByGroup(ctx, rule.GroupID)
	re.NoError(err)
	checkRuleFunc(re, rules, rule, totalRuleCount, exist)
	// Check through the `GetPlacementRuleBundleByGroup` API.
	bundle, err := client.GetPlacementRuleBundleByGroup(ctx, rule.GroupID)
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
		re.Equal(rule.StartKey, r.StartKey)
		re.Equal(rule.EndKey, r.EndKey)
		return
	}
	if exist {
		re.Failf("Failed to check the rule", "rule %+v not found", rule)
	}
}

func (suite *httpClientTestSuite) TestRegionLabel() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	labelRules, err := client.GetAllRegionLabelRules(ctx)
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
	err = client.SetRegionLabelRule(ctx, labelRule)
	re.NoError(err)
	labelRules, err = client.GetAllRegionLabelRules(ctx)
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
	err = client.PatchRegionLabelRules(ctx, patch)
	re.NoError(err)
	allLabelRules, err := client.GetAllRegionLabelRules(ctx)
	re.NoError(err)
	re.Len(labelRules, 2)
	sort.Slice(allLabelRules, func(i, j int) bool {
		return allLabelRules[i].ID < allLabelRules[j].ID
	})
	re.Equal(labelRule.ID, allLabelRules[1].ID)
	re.Equal(labelRule.Labels, allLabelRules[1].Labels)
	re.Equal(labelRule.RuleType, allLabelRules[1].RuleType)
	labelRules, err = client.GetRegionLabelRulesByIDs(ctx, []string{"rule2"})
	re.NoError(err)
	re.Len(labelRules, 1)
	re.Equal(labelRule, labelRules[0])
	labelRules, err = client.GetRegionLabelRulesByIDs(ctx, []string{"keyspaces/0", "rule2"})
	re.NoError(err)
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Equal(allLabelRules, labelRules)
}

func (suite *httpClientTestSuite) TestAccelerateSchedule() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	raftCluster := suite.cluster.GetLeaderServer().GetRaftCluster()
	pendingProcessedRegions := raftCluster.GetPendingProcessedRegions()
	re.Empty(pendingProcessedRegions)
	err := client.AccelerateSchedule(ctx, pd.NewKeyRange([]byte("a1"), []byte("a2")))
	re.NoError(err)
	pendingProcessedRegions = raftCluster.GetPendingProcessedRegions()
	re.Len(pendingProcessedRegions, 1)
	for _, id := range pendingProcessedRegions {
		raftCluster.RemovePendingProcessedRegion(id)
	}
	pendingProcessedRegions = raftCluster.GetPendingProcessedRegions()
	re.Empty(pendingProcessedRegions)
	err = client.AccelerateScheduleInBatch(ctx, []*pd.KeyRange{
		pd.NewKeyRange([]byte("a1"), []byte("a2")),
		pd.NewKeyRange([]byte("a2"), []byte("a3")),
	})
	re.NoError(err)
	pendingProcessedRegions = raftCluster.GetPendingProcessedRegions()
	re.Len(pendingProcessedRegions, 2)
}

func (suite *httpClientTestSuite) TestConfig() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	config, err := client.GetConfig(ctx)
	re.NoError(err)
	re.Equal(float64(4), config["schedule"].(map[string]any)["leader-schedule-limit"])

	newConfig := map[string]any{
		"schedule.leader-schedule-limit": float64(8),
	}
	err = client.SetConfig(ctx, newConfig)
	re.NoError(err)

	config, err = client.GetConfig(ctx)
	re.NoError(err)
	re.Equal(float64(8), config["schedule"].(map[string]any)["leader-schedule-limit"])

	// Test the config with TTL.
	newConfig = map[string]any{
		"schedule.leader-schedule-limit": float64(16),
	}
	err = client.SetConfig(ctx, newConfig, 5)
	re.NoError(err)
	resp, err := suite.cluster.GetEtcdClient().Get(ctx, sc.TTLConfigPrefix+"/schedule.leader-schedule-limit")
	re.NoError(err)
	re.Equal([]byte("16"), resp.Kvs[0].Value)
	// delete the config with TTL.
	err = client.SetConfig(ctx, newConfig, 0)
	re.NoError(err)
	resp, err = suite.cluster.GetEtcdClient().Get(ctx, sc.TTLConfigPrefix+"/schedule.leader-schedule-limit")
	re.NoError(err)
	re.Empty(resp.Kvs)

	// Test the config with TTL for storing float64 as uint64.
	newConfig = map[string]any{
		"schedule.max-pending-peer-count": uint64(math.MaxInt32),
	}
	err = client.SetConfig(ctx, newConfig, 4)
	re.NoError(err)
	c := suite.cluster.GetLeaderServer().GetRaftCluster().GetOpts().GetMaxPendingPeerCount()
	re.Equal(uint64(math.MaxInt32), c)

	err = client.SetConfig(ctx, newConfig, 0)
	re.NoError(err)
	resp, err = suite.cluster.GetEtcdClient().Get(ctx, sc.TTLConfigPrefix+"/schedule.max-pending-peer-count")
	re.NoError(err)
	re.Empty(resp.Kvs)
}

func (suite *httpClientTestSuite) TestTTLConfigPersist() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	configKey := "schedule.max-pending-peer-count"

	testCases := []struct {
		inputValue        any
		expectedEtcdValue string
	}{
		{
			inputValue:        uint64(10000000),
			expectedEtcdValue: "10000000",
		},
		{
			inputValue:        int(20000000),
			expectedEtcdValue: "20000000",
		},
		{
			inputValue:        float64(2147483647),
			expectedEtcdValue: "2147483647",
		},
		{
			inputValue:        float64(1234567890.0),
			expectedEtcdValue: "1234567890",
		},
		{
			inputValue:        float64(0.0),
			expectedEtcdValue: "0",
		},
		{
			inputValue:        float64(987.65),
			expectedEtcdValue: "987.65",
		},
		{
			inputValue:        int(-1),
			expectedEtcdValue: "-1",
		},
		{
			inputValue:        int32(-2147483647),
			expectedEtcdValue: "-2147483647",
		},
	}

	for _, tc := range testCases {
		newConfig := map[string]any{
			configKey: tc.inputValue,
		}
		err := client.SetConfig(ctx, newConfig, 10000)
		re.NoError(err)
		resp, err := suite.cluster.GetEtcdClient().Get(ctx, sc.TTLConfigPrefix+"/"+configKey)
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		re.Equal([]byte(tc.expectedEtcdValue), resp.Kvs[0].Value)
		err = client.SetConfig(ctx, newConfig, 0)
		re.NoError(err)
	}
}

func (suite *httpClientTestSuite) TestScheduleConfig() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	config, err := client.GetScheduleConfig(ctx)
	re.NoError(err)
	re.Equal(float64(4), config["hot-region-schedule-limit"])
	re.Equal(float64(2048), config["region-schedule-limit"])
	config["hot-region-schedule-limit"] = float64(8)
	err = client.SetScheduleConfig(ctx, config)
	re.NoError(err)
	config, err = client.GetScheduleConfig(ctx)
	re.NoError(err)
	re.Equal(float64(8), config["hot-region-schedule-limit"])
	re.Equal(float64(2048), config["region-schedule-limit"])
}

func (suite *httpClientTestSuite) TestSchedulers() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	schedulers, err := client.GetSchedulers(ctx)
	re.NoError(err)
	const schedulerName = "evict-leader-scheduler"
	re.NotContains(schedulers, schedulerName)

	err = client.CreateScheduler(ctx, schedulerName, 1)
	re.NoError(err)
	checkScheduler := func() {
		schedulers, err = client.GetSchedulers(ctx)
		re.NoError(err)
		re.Contains(schedulers, schedulerName)
		config, err := client.GetSchedulerConfig(ctx, schedulerName)
		re.NoError(err)
		re.Contains(config, "store-id-ranges")
		re.Contains(config, "batch")
	}
	checkScheduler()
	err = client.SetSchedulerDelay(ctx, schedulerName, 100)
	re.NoError(err)
	err = client.SetSchedulerDelay(ctx, "not-exist", 100)
	re.ErrorContains(err, "500 Internal Server Error") // TODO: should return friendly error message

	re.NoError(client.DeleteScheduler(ctx, schedulerName))
	schedulers, err = client.GetSchedulers(ctx)
	re.NoError(err)
	re.NotContains(schedulers, schedulerName)

	input := map[string]any{
		"store_id": 1,
	}
	re.NoError(client.CreateSchedulerWithInput(ctx, schedulerName, input))
	checkScheduler()
	re.NoError(client.DeleteScheduler(ctx, schedulerName))
	const schedulerName2 = "balance-range-scheduler"
	input = map[string]any{
		"engine":    "tikv",
		"rule":      "leader-scatter",
		"start-key": "100",
		"end-key":   "200",
		"alias":     "test",
	}
	re.NoError(client.CreateSchedulerWithInput(ctx, schedulerName2, input))
	checkFn := func() map[string]any {
		config, err := client.GetSchedulerConfig(ctx, schedulerName2)
		re.NoError(err)
		jobs, ok := config.([]any)
		re.True(ok, config)
		res := make([]map[string]any, 0, len(jobs))
		for _, job := range jobs {
			jobMap, ok := job.(map[string]any)
			re.True(ok, config)
			res = append(res, jobMap)
		}
		return res[0]
	}
	job := checkFn()
	jobID := uint64(job["job-id"].(float64))
	re.Equal(uint64(0), jobID)
	_, ok := job["start"].(*time.Time)
	re.False(ok, job)

	// cancel one job
	re.NoError(client.CancelSchedulerJob(ctx, schedulerName2, jobID))
	job = checkFn()
	status, ok := job["status"]
	re.True(ok)
	re.Equal("cancelled", status)
}

func (suite *httpClientTestSuite) setStoreLabels(ctx context.Context, re *require.Assertions, storeLabels map[string]string) int64 {
	client := suite.client
	resp, err := client.GetStores(ctx)
	re.NoError(err)
	re.NotEmpty(resp.Stores)
	firstStore := resp.Stores[0]
	re.Empty(firstStore.Store.Labels, nil)

	err = client.SetStoreLabels(ctx, firstStore.Store.ID, storeLabels)
	re.NoError(err)

	getResp, err := client.GetStore(ctx, uint64(firstStore.Store.ID))
	re.NoError(err)

	labelsMap := make(map[string]string)
	for _, label := range getResp.Store.Labels {
		re.NotNil(label)
		labelsMap[label.Key] = label.Value
	}

	for key, value := range storeLabels {
		re.Equal(value, labelsMap[key])
	}

	return firstStore.Store.ID
}

func (suite *httpClientTestSuite) TestStoreLabels() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	storeID := suite.setStoreLabels(ctx, re, map[string]string{
		"zone": "zone1",
	})

	re.NoError(client.DeleteStoreLabel(ctx, storeID, "zone"))
	store, err := client.GetStore(ctx, uint64(storeID))
	re.NoError(err)
	re.Empty(store.Store.Labels)
}

func (suite *httpClientTestSuite) transferLeader(ctx context.Context, re *require.Assertions) {
	client := suite.client
	members, err := client.GetMembers(ctx)
	re.NoError(err)
	re.Len(members.Members, 2)

	leader, err := client.GetLeader(ctx)
	re.NoError(err)

	// Transfer leader to another pd
	for _, member := range members.Members {
		if member.GetName() != leader.GetName() {
			err = client.TransferLeader(ctx, member.GetName())
			re.NoError(err)
			break
		}
	}

	newLeader := suite.cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NoError(err)
	re.NotEqual(leader.GetName(), newLeader)
	// Force to update the members info.
	testutil.Eventually(re, func() bool {
		leader, err = client.GetLeader(ctx)
		re.NoError(err)
		return newLeader == leader.GetName()
	})
	members, err = client.GetMembers(ctx)
	re.NoError(err)
	re.Len(members.Members, 2)
	re.Equal(leader.GetName(), members.Leader.GetName())
}
func (suite *httpClientTestSuite) TestTransferLeader() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	suite.transferLeader(ctx, re)
}

func (suite *httpClientTestSuite) TestVersion() {
	re := suite.Require()
	ver, err := suite.client.GetPDVersion(suite.ctx)
	re.NoError(err)
	re.Equal(versioninfo.PDReleaseVersion, ver)
}

func (suite *httpClientTestSuite) TestStatus() {
	re := suite.Require()
	status, err := suite.client.GetStatus(suite.ctx)
	re.NoError(err)
	re.Equal(versioninfo.PDReleaseVersion, status.Version)
	re.Equal(versioninfo.PDGitHash, status.GitHash)
	re.Equal(versioninfo.PDBuildTS, status.BuildTS)
	re.GreaterOrEqual(time.Now().Unix(), status.StartTimestamp)
	re.Equal(versioninfo.PDKernelType, status.KernelType)
}

func (suite *httpClientTestSuite) TestAdmin() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	err := client.SetSnapshotRecoveringMark(ctx)
	re.NoError(err)
	err = client.ResetTS(ctx, 123, true)
	re.NoError(err)
	err = client.ResetBaseAllocID(ctx, 456)
	re.NoError(err)
	err = client.DeleteSnapshotRecoveringMark(ctx)
	re.NoError(err)
}

func (suite *httpClientTestSuite) TestWithBackoffer() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	// Should return with 404 error without backoffer.
	rule, err := client.GetPlacementRule(ctx, "non-exist-group", "non-exist-rule")
	re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	re.Nil(rule)
	// Should return with 404 error even with an infinite backoffer.
	rule, err = client.
		WithBackoffer(retry.InitialBackoffer(100*time.Millisecond, time.Second, 0)).
		GetPlacementRule(ctx, "non-exist-group", "non-exist-rule")
	re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	re.Nil(rule)
}

func (suite *httpClientTestSuite) TestRedirectWithMetrics() {
	re := suite.Require()

	cli := setupCli(suite.ctx, re, suite.endpoints)
	defer cli.Close()
	sd := cli.GetServiceDiscovery()

	metricCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "check",
		}, []string{"name", ""})
	// 1. Test all followers failed, need to send all followers.
	httpClient := pd.NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		if req.URL.Path == pd.Schedulers {
			return errors.New("mock error")
		}
		return nil
	})
	c := pd.NewClientWithServiceDiscovery("pd-http-client-it", sd, pd.WithHTTPClient(httpClient), pd.WithMetrics(metricCnt, nil))
	err := c.CreateScheduler(context.Background(), "test", 0)
	re.ErrorContains(err, "mock error")
	var out dto.Metric
	failureCnt, err := metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", "network error"}...)
	re.NoError(err)
	err = failureCnt.Write(&out)
	re.NoError(err)
	re.Equal(float64(2), out.GetCounter().GetValue())
	c.Close()

	leader := sd.GetServingURL()
	httpClient = pd.NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		// mock leader success.
		if !strings.Contains(leader, req.Host) {
			return errors.New("mock error")
		}
		return nil
	})
	c = pd.NewClientWithServiceDiscovery("pd-http-client-it", sd, pd.WithHTTPClient(httpClient), pd.WithMetrics(metricCnt, nil))
	err = c.CreateScheduler(context.Background(), "test", 0)
	re.NoError(err)
	successCnt, err := metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", ""}...)
	re.NoError(err)
	err = successCnt.Write(&out)
	re.NoError(err)
	re.Equal(float64(1), out.GetCounter().GetValue())
	c.Close()

	httpClient = pd.NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		// mock leader success.
		if strings.Contains(leader, req.Host) {
			return errors.New("mock error")
		}
		return nil
	})
	c = pd.NewClientWithServiceDiscovery("pd-http-client-it", sd, pd.WithHTTPClient(httpClient), pd.WithMetrics(metricCnt, nil))
	err = c.CreateScheduler(context.Background(), "test", 0)
	re.NoError(err)
	successCnt, err = metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", ""}...)
	re.NoError(err)
	err = successCnt.Write(&out)
	re.NoError(err)
	re.Equal(float64(2), out.GetCounter().GetValue())
	failureCnt, err = metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", "network error"}...)
	re.NoError(err)
	err = failureCnt.Write(&out)
	re.NoError(err)
	re.Equal(float64(3), out.GetCounter().GetValue())
	c.Close()
}

func (suite *httpClientTestSuite) TestUpdateKeyspaceGCManagementType() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	keyspaceName := "DEFAULT"
	expectGCManagementType := "test-type"

	keyspaceSafePointVersionConfig := pd.KeyspaceGCManagementTypeConfig{
		Config: pd.KeyspaceGCManagementType{
			GCManagementType: expectGCManagementType,
		},
	}
	err := client.UpdateKeyspaceGCManagementType(ctx, keyspaceName, &keyspaceSafePointVersionConfig)
	re.NoError(err)

	keyspaceMetaRes, err := client.GetKeyspaceMetaByName(ctx, keyspaceName)
	re.NoError(err)
	val, ok := keyspaceMetaRes.Config[keyspace.GCManagementType]

	// Check it can get expect key and value in keyspace meta config.
	re.True(ok)
	re.Equal(expectGCManagementType, val)

	// Check it doesn't support update config to keyspace.KeyspaceLevelGC now.
	keyspaceSafePointVersionConfig = pd.KeyspaceGCManagementTypeConfig{
		Config: pd.KeyspaceGCManagementType{
			GCManagementType: keyspace.KeyspaceLevelGC,
		},
	}
	err = client.UpdateKeyspaceGCManagementType(suite.ctx, keyspaceName, &keyspaceSafePointVersionConfig)
	re.Error(err)
}

func (suite *httpClientTestSuite) TestGetHealthStatus() {
	re := suite.Require()
	healths, err := suite.client.GetHealthStatus(suite.ctx)
	re.NoError(err)
	re.Len(healths, 2)
	sort.Slice(healths, func(i, j int) bool {
		return healths[i].Name < healths[j].Name
	})
	re.Equal("pd1", healths[0].Name)
	re.Equal("pd2", healths[1].Name)
	re.True(healths[0].Health && healths[1].Health)
}

func (suite *httpClientTestSuite) TestRetryOnLeaderChange() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bo := retry.InitialBackoffer(100*time.Millisecond, time.Second, 0)
		client := suite.client.WithBackoffer(bo)
		for {
			healths, err := client.GetHealthStatus(ctx)
			if err != nil && strings.Contains(err.Error(), "context canceled") {
				return
			}
			re.NoError(err)
			re.Len(healths, 2)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	leader := suite.cluster.GetLeaderServer()
	re.NotNil(leader)
	for range 3 {
		err := leader.ResignLeader()
		re.NoError(err)
		re.NotEmpty(suite.cluster.WaitLeader())
		leader = suite.cluster.GetLeaderServer()
		re.NotNil(leader)
	}

	// Cancel the context to stop the goroutine.
	cancel()
	wg.Wait()
}

func (suite *httpClientTestSuite) TestGetGCSafePoint() {
	re := suite.Require()
	client := suite.client
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	// adding some safepoints to the server
	now := time.Now().Truncate(time.Second)
	list := &api.ListServiceGCSafepoint{
		ServiceGCSafepoints: []*endpoint.ServiceSafePoint{
			{
				ServiceID: "AAA",
				ExpiredAt: now.Unix() + 10,
				SafePoint: 1,
			},
			{
				ServiceID: "BBB",
				ExpiredAt: now.Unix() + 10,
				SafePoint: 2,
			},
			{
				ServiceID: "CCC",
				ExpiredAt: now.Unix() + 10,
				SafePoint: 3,
			},
		},
		GCSafePoint:           1,
		MinServiceGcSafepoint: 1,
	}

	gcStateManager := suite.cluster.GetLeaderServer().GetServer().GetGCStateManager()
	for _, ssp := range list.ServiceGCSafepoints {
		_, _, err := gcStateManager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, ssp.ServiceID, ssp.SafePoint, ssp.ExpiredAt-now.Unix(), now)
		re.NoError(err)
	}
	_, err := gcStateManager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 1, now)
	re.NoError(err)
	_, _, err = gcStateManager.AdvanceGCSafePoint(constant.NullKeyspaceID, 1)
	re.NoError(err)

	// get the safepoints and start testing
	l, err := client.GetGCSafePoint(ctx)
	re.NoError(err)

	re.Equal(uint64(1), l.GCSafePoint)
	re.Equal(uint64(1), l.MinServiceGcSafepoint)
	re.Len(l.ServiceGCSafepoints, 3)

	// sort the gc safepoints based on order of ServiceID
	sort.Slice(l.ServiceGCSafepoints, func(i, j int) bool {
		return l.ServiceGCSafepoints[i].ServiceID < l.ServiceGCSafepoints[j].ServiceID
	})

	for i, val := range l.ServiceGCSafepoints {
		re.Equal(list.ServiceGCSafepoints[i].ServiceID, val.ServiceID)
		re.Equal(list.ServiceGCSafepoints[i].SafePoint, val.SafePoint)
	}

	// delete the safepoints
	for i := range 3 {
		msg, err := client.DeleteGCSafePoint(ctx, list.ServiceGCSafepoints[i].ServiceID)
		re.NoError(err)
		re.Equal("Delete service GC safepoint successfully.", msg)
	}

	// check that the safepoitns are indeed deleted
	l, err = client.GetGCSafePoint(ctx)
	re.NoError(err)

	re.Equal(uint64(1), l.GCSafePoint)
	re.Equal(uint64(0), l.MinServiceGcSafepoint)
	re.Empty(l.ServiceGCSafepoints)

	// Deleting "gc_worker" should result in an error in earlier version. As the service safe point becomes a
	// compatibility layer over GC barriers, it won't take any effect except that possibly deleting the residual
	// service safe point of "gc_worker" that was written by previous version.
	_, err = client.DeleteGCSafePoint(ctx, "gc_worker")
	re.NoError(err)

	// try delete some non-exist safepoints, should return normally
	var msg string
	msg, err = client.DeleteGCSafePoint(ctx, "non_exist")
	re.NoError(err)
	re.Equal("Delete service GC safepoint successfully.", msg)
}

func TestGetSiblingsRegions(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2, func(conf *config.Config, _ string) {
		conf.Replication.MaxReplicas = 1
	})
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetLeaderServer()

	err = leaderServer.BootstrapCluster()
	// Add 2 more stores to the cluster.
	for i := 2; i <= 4; i++ {
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		})
	}
	re.NoError(err)
	for _, region := range []*core.RegionInfo{
		core.NewTestRegionInfo(10, 1, []byte("a1"), []byte("a2")),
		core.NewTestRegionInfo(11, 1, []byte("a2"), []byte("a3")),
		core.NewTestRegionInfo(12, 1, []byte("a3"), []byte("a4")),
	} {
		err := leaderServer.GetRaftCluster().HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	var (
		testServers = cluster.GetServers()
		endpoints   = make([]string, 0, len(testServers))
	)
	for _, s := range testServers {
		addr := s.GetConfig().AdvertiseClientUrls
		url, err := url.Parse(addr)
		re.NoError(err)
		endpoints = append(endpoints, url.Host)
	}
	client := pd.NewClient("pd-http-client-it-http", endpoints)
	defer client.Close()
	rg, err := client.GetRegionByID(ctx, 11)
	re.NoError(err)
	re.NotNil(rg)

	rgs, err := client.GetRegionSiblingsByID(ctx, 11)
	re.NoError(err)
	re.Equal(int64(2), rgs.Count)
	re.Equal(int64(10), rgs.Regions[0].ID)
	re.Equal(int64(12), rgs.Regions[1].ID)

	rightStartKey := rgs.Regions[rgs.Count-1].GetStartKey()
	re.Zero(strings.Compare(rightStartKey, rg.EndKey))

	input := map[string]any{
		"name":             "merge-region",
		"source_region_id": 10,
		"target_region_id": 11,
	}
	err = client.CreateOperators(ctx, input)
	re.NoError(err)
	ops := leaderServer.GetRaftCluster().GetOperatorController().GetOperators()
	re.Len(ops, 2)
	re.NotZero(ops[0].Kind() & operator.OpMerge)
	re.NotZero(ops[1].Kind() & operator.OpMerge)
}
