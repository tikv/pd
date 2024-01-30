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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

type mode int

// We have two ways to create HTTP client.
// 1. using `NewClient` which created `DefaultPDServiceDiscovery`
// 2. using `NewClientWithServiceDiscovery` which pass a `PDServiceDiscovery` as parameter
// test cases should be run in both modes.
const (
	defaultServiceDiscovery mode = iota
	specificServiceDiscovery
)

type httpClientTestSuite struct {
	suite.Suite
	env map[mode]*httpClientTestEnv
}

type httpClientTestEnv struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	cluster    *tests.TestCluster
	endpoints  []string
}

func TestHTTPClientTestSuite(t *testing.T) {
	suite.Run(t, new(httpClientTestSuite))
}

func (suite *httpClientTestSuite) SetupSuite() {
	suite.env = make(map[mode]*httpClientTestEnv)
	re := suite.Require()

	for _, mode := range []mode{defaultServiceDiscovery, specificServiceDiscovery} {
		env := &httpClientTestEnv{}
		env.ctx, env.cancelFunc = context.WithCancel(context.Background())

		cluster, err := tests.NewTestCluster(env.ctx, 2)
		re.NoError(err)

		err = cluster.RunInitialServers()
		re.NoError(err)
		leader := cluster.WaitLeader()
		re.NotEmpty(leader)
		leaderServer := cluster.GetLeaderServer()
		err = leaderServer.BootstrapCluster()
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
		env.endpoints = endpoints
		env.cluster = cluster

		suite.env[mode] = env
	}
}

func (suite *httpClientTestSuite) TearDownSuite() {
	for _, env := range suite.env {
		env.cancelFunc()
		env.cluster.Destroy()
	}
}

// RunTestInTwoModes is to run test in two modes.
func (suite *httpClientTestSuite) RunTestInTwoModes(test func(mode mode, client pd.Client)) {
	// Run test with specific service discovery.
	cli := setupCli(suite.Require(), suite.env[specificServiceDiscovery].ctx, suite.env[specificServiceDiscovery].endpoints)
	sd := cli.GetServiceDiscovery()
	client := pd.NewClientWithServiceDiscovery("pd-http-client-it-grpc", sd)
	test(specificServiceDiscovery, client)
	client.Close()

	// Run test with default service discovery.
	client = pd.NewClient("pd-http-client-it-http", suite.env[defaultServiceDiscovery].endpoints)
	test(defaultServiceDiscovery, client)
	client.Close()
}

func (suite *httpClientTestSuite) TestMeta() {
	suite.RunTestInTwoModes(suite.checkMeta)
}

func (suite *httpClientTestSuite) checkMeta(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]
	replicateConfig, err := client.GetReplicateConfig(env.ctx)
	re.NoError(err)
	re.Equal(3.0, replicateConfig["max-replicas"])
	region, err := client.GetRegionByID(env.ctx, 10)
	re.NoError(err)
	re.Equal(int64(10), region.ID)
	re.Equal(core.HexRegionKeyStr([]byte("a1")), region.StartKey)
	re.Equal(core.HexRegionKeyStr([]byte("a2")), region.EndKey)
	region, err = client.GetRegionByKey(env.ctx, []byte("a2"))
	re.NoError(err)
	re.Equal(int64(11), region.ID)
	re.Equal(core.HexRegionKeyStr([]byte("a2")), region.StartKey)
	re.Equal(core.HexRegionKeyStr([]byte("a3")), region.EndKey)
	regions, err := client.GetRegions(env.ctx)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	regions, err = client.GetRegionsByKeyRange(env.ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), -1)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	regions, err = client.GetRegionsByStoreID(env.ctx, 1)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	regions, err = client.GetEmptyRegions(env.ctx)
	re.NoError(err)
	re.Equal(int64(2), regions.Count)
	re.Len(regions.Regions, 2)
	state, err := client.GetRegionsReplicatedStateByKeyRange(env.ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")))
	re.NoError(err)
	re.Equal("INPROGRESS", state)
	regionStats, err := client.GetRegionStatusByKeyRange(env.ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), false)
	re.NoError(err)
	re.Greater(regionStats.Count, 0)
	re.NotEmpty(regionStats.StoreLeaderCount)
	regionStats, err = client.GetRegionStatusByKeyRange(env.ctx, pd.NewKeyRange([]byte("a1"), []byte("a3")), true)
	re.NoError(err)
	re.Greater(regionStats.Count, 0)
	re.Empty(regionStats.StoreLeaderCount)
	hotReadRegions, err := client.GetHotReadRegions(env.ctx)
	re.NoError(err)
	re.Len(hotReadRegions.AsPeer, 1)
	re.Len(hotReadRegions.AsLeader, 1)
	hotWriteRegions, err := client.GetHotWriteRegions(env.ctx)
	re.NoError(err)
	re.Len(hotWriteRegions.AsPeer, 1)
	re.Len(hotWriteRegions.AsLeader, 1)
	historyHorRegions, err := client.GetHistoryHotRegions(env.ctx, &pd.HistoryHotRegionsRequest{
		StartTime: 0,
		EndTime:   time.Now().AddDate(0, 0, 1).UnixNano() / int64(time.Millisecond),
	})
	re.NoError(err)
	re.Empty(historyHorRegions.HistoryHotRegion)
	store, err := client.GetStores(env.ctx)
	re.NoError(err)
	re.Equal(1, store.Count)
	re.Len(store.Stores, 1)
	storeID := uint64(store.Stores[0].Store.ID) // TODO: why type is different?
	store2, err := client.GetStore(env.ctx, storeID)
	re.NoError(err)
	re.EqualValues(storeID, store2.Store.ID)
	version, err := client.GetClusterVersion(env.ctx)
	re.NoError(err)
	re.Equal("0.0.0", version)
}

func (suite *httpClientTestSuite) TestGetMinResolvedTSByStoresIDs() {
	suite.RunTestInTwoModes(suite.checkGetMinResolvedTSByStoresIDs)
}

func (suite *httpClientTestSuite) checkGetMinResolvedTSByStoresIDs(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	testMinResolvedTS := tsoutil.TimeToTS(time.Now())
	raftCluster := env.cluster.GetLeaderServer().GetRaftCluster()
	err := raftCluster.SetMinResolvedTS(1, testMinResolvedTS)
	re.NoError(err)
	// Make sure the min resolved TS is updated.
	testutil.Eventually(re, func() bool {
		minResolvedTS, _ := raftCluster.CheckAndUpdateMinResolvedTS()
		return minResolvedTS == testMinResolvedTS
	})
	// Wait for the cluster-level min resolved TS to be initialized.
	minResolvedTS, storeMinResolvedTSMap, err := client.GetMinResolvedTSByStoresIDs(env.ctx, nil)
	re.NoError(err)
	re.Equal(testMinResolvedTS, minResolvedTS)
	re.Empty(storeMinResolvedTSMap)
	// Get the store-level min resolved TS.
	minResolvedTS, storeMinResolvedTSMap, err = client.GetMinResolvedTSByStoresIDs(env.ctx, []uint64{1})
	re.NoError(err)
	re.Equal(testMinResolvedTS, minResolvedTS)
	re.Len(storeMinResolvedTSMap, 1)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	// Get the store-level min resolved TS with an invalid store ID.
	minResolvedTS, storeMinResolvedTSMap, err = client.GetMinResolvedTSByStoresIDs(env.ctx, []uint64{1, 2})
	re.NoError(err)
	re.Equal(testMinResolvedTS, minResolvedTS)
	re.Len(storeMinResolvedTSMap, 2)
	re.Equal(minResolvedTS, storeMinResolvedTSMap[1])
	re.Equal(uint64(math.MaxUint64), storeMinResolvedTSMap[2])
}

func (suite *httpClientTestSuite) TestRule() {
	suite.RunTestInTwoModes(suite.checkRule)
}

func (suite *httpClientTestSuite) checkRule(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	bundles, err := client.GetAllPlacementRuleBundles(env.ctx)
	re.NoError(err)
	re.Len(bundles, 1)
	re.Equal(placement.DefaultGroupID, bundles[0].ID)
	bundle, err := client.GetPlacementRuleBundleByGroup(env.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Equal(bundles[0], bundle)
	// Check if we have the default rule.
	suite.checkRuleResult(re, env, client, &pd.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       placement.DefaultRuleID,
		Role:     pd.Voter,
		Count:    3,
		StartKey: []byte{},
		EndKey:   []byte{},
	}, 1, true)
	// Should be the same as the rules in the bundle.
	suite.checkRuleResult(re, env, client, bundle.Rules[0], 1, true)
	testRule := &pd.Rule{
		GroupID:  placement.DefaultGroupID,
		ID:       "test",
		Role:     pd.Voter,
		Count:    3,
		StartKey: []byte{},
		EndKey:   []byte{},
	}
	err = client.SetPlacementRule(env.ctx, testRule)
	re.NoError(err)
	suite.checkRuleResult(re, env, client, testRule, 2, true)
	err = client.DeletePlacementRule(env.ctx, placement.DefaultGroupID, "test")
	re.NoError(err)
	suite.checkRuleResult(re, env, client, testRule, 1, false)
	testRuleOp := &pd.RuleOp{
		Rule:   testRule,
		Action: pd.RuleOpAdd,
	}
	err = client.SetPlacementRuleInBatch(env.ctx, []*pd.RuleOp{testRuleOp})
	re.NoError(err)
	suite.checkRuleResult(re, env, client, testRule, 2, true)
	testRuleOp = &pd.RuleOp{
		Rule:   testRule,
		Action: pd.RuleOpDel,
	}
	err = client.SetPlacementRuleInBatch(env.ctx, []*pd.RuleOp{testRuleOp})
	re.NoError(err)
	suite.checkRuleResult(re, env, client, testRule, 1, false)
	err = client.SetPlacementRuleBundles(env.ctx, []*pd.GroupBundle{
		{
			ID:    placement.DefaultGroupID,
			Rules: []*pd.Rule{testRule},
		},
	}, true)
	re.NoError(err)
	suite.checkRuleResult(re, env, client, testRule, 1, true)
	ruleGroups, err := client.GetAllPlacementRuleGroups(env.ctx)
	re.NoError(err)
	re.Len(ruleGroups, 1)
	re.Equal(placement.DefaultGroupID, ruleGroups[0].ID)
	ruleGroup, err := client.GetPlacementRuleGroupByID(env.ctx, placement.DefaultGroupID)
	re.NoError(err)
	re.Equal(ruleGroups[0], ruleGroup)
	testRuleGroup := &pd.RuleGroup{
		ID:       "test-group",
		Index:    1,
		Override: true,
	}
	err = client.SetPlacementRuleGroup(env.ctx, testRuleGroup)
	re.NoError(err)
	ruleGroup, err = client.GetPlacementRuleGroupByID(env.ctx, testRuleGroup.ID)
	re.NoError(err)
	re.Equal(testRuleGroup, ruleGroup)
	err = client.DeletePlacementRuleGroupByID(env.ctx, testRuleGroup.ID)
	re.NoError(err)
	ruleGroup, err = client.GetPlacementRuleGroupByID(env.ctx, testRuleGroup.ID)
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
	err = client.SetPlacementRule(env.ctx, testRule)
	re.NoError(err)
	suite.checkRuleResult(re, env, client, testRule, 1, true)
}

func (suite *httpClientTestSuite) checkRuleResult(
	re *require.Assertions,
	env *httpClientTestEnv,
	client pd.Client,
	rule *pd.Rule, totalRuleCount int, exist bool,
) {
	if exist {
		got, err := client.GetPlacementRule(env.ctx, rule.GroupID, rule.ID)
		re.NoError(err)
		// skip comparison of the generated field
		got.StartKeyHex = rule.StartKeyHex
		got.EndKeyHex = rule.EndKeyHex
		re.Equal(rule, got)
	} else {
		_, err := client.GetPlacementRule(env.ctx, rule.GroupID, rule.ID)
		re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	}
	// Check through the `GetPlacementRulesByGroup` API.
	rules, err := client.GetPlacementRulesByGroup(env.ctx, rule.GroupID)
	re.NoError(err)
	checkRuleFunc(re, rules, rule, totalRuleCount, exist)
	// Check through the `GetPlacementRuleBundleByGroup` API.
	bundle, err := client.GetPlacementRuleBundleByGroup(env.ctx, rule.GroupID)
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
	suite.RunTestInTwoModes(suite.checkRegionLabel)
}

func (suite *httpClientTestSuite) checkRegionLabel(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	labelRules, err := client.GetAllRegionLabelRules(env.ctx)
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
	err = client.SetRegionLabelRule(env.ctx, labelRule)
	re.NoError(err)
	labelRules, err = client.GetAllRegionLabelRules(env.ctx)
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
	err = client.PatchRegionLabelRules(env.ctx, patch)
	re.NoError(err)
	allLabelRules, err := client.GetAllRegionLabelRules(env.ctx)
	re.NoError(err)
	re.Len(labelRules, 2)
	sort.Slice(allLabelRules, func(i, j int) bool {
		return allLabelRules[i].ID < allLabelRules[j].ID
	})
	re.Equal(labelRule.ID, allLabelRules[1].ID)
	re.Equal(labelRule.Labels, allLabelRules[1].Labels)
	re.Equal(labelRule.RuleType, allLabelRules[1].RuleType)
	labelRules, err = client.GetRegionLabelRulesByIDs(env.ctx, []string{"keyspaces/0", "rule2"})
	re.NoError(err)
	sort.Slice(labelRules, func(i, j int) bool {
		return labelRules[i].ID < labelRules[j].ID
	})
	re.Equal(allLabelRules, labelRules)
}

func (suite *httpClientTestSuite) TestAccelerateSchedule() {
	suite.RunTestInTwoModes(suite.checkAccelerateSchedule)
}

func (suite *httpClientTestSuite) checkAccelerateSchedule(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	raftCluster := env.cluster.GetLeaderServer().GetRaftCluster()
	suspectRegions := raftCluster.GetSuspectRegions()
	re.Empty(suspectRegions)
	err := client.AccelerateSchedule(env.ctx, pd.NewKeyRange([]byte("a1"), []byte("a2")))
	re.NoError(err)
	suspectRegions = raftCluster.GetSuspectRegions()
	re.Len(suspectRegions, 1)
	raftCluster.ClearSuspectRegions()
	suspectRegions = raftCluster.GetSuspectRegions()
	re.Empty(suspectRegions)
	err = client.AccelerateScheduleInBatch(env.ctx, []*pd.KeyRange{
		pd.NewKeyRange([]byte("a1"), []byte("a2")),
		pd.NewKeyRange([]byte("a2"), []byte("a3")),
	})
	re.NoError(err)
	suspectRegions = raftCluster.GetSuspectRegions()
	re.Len(suspectRegions, 2)
}

func (suite *httpClientTestSuite) TestConfig() {
	suite.RunTestInTwoModes(suite.checkConfig)
}

func (suite *httpClientTestSuite) checkConfig(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	config, err := client.GetConfig(env.ctx)
	re.NoError(err)
	re.Equal(float64(4), config["schedule"].(map[string]any)["leader-schedule-limit"])

	newConfig := map[string]any{
		"schedule.leader-schedule-limit": float64(8),
	}
	err = client.SetConfig(env.ctx, newConfig)
	re.NoError(err)

	config, err = client.GetConfig(env.ctx)
	re.NoError(err)
	re.Equal(float64(8), config["schedule"].(map[string]any)["leader-schedule-limit"])

	// Test the config with TTL.
	newConfig = map[string]any{
		"schedule.leader-schedule-limit": float64(16),
	}
	err = client.SetConfig(env.ctx, newConfig, 5)
	re.NoError(err)
	resp, err := env.cluster.GetEtcdClient().Get(env.ctx, sc.TTLConfigPrefix+"/schedule.leader-schedule-limit")
	re.NoError(err)
	re.Equal([]byte("16"), resp.Kvs[0].Value)
	// delete the config with TTL.
	err = client.SetConfig(env.ctx, newConfig, 0)
	re.NoError(err)
	resp, err = env.cluster.GetEtcdClient().Get(env.ctx, sc.TTLConfigPrefix+"/schedule.leader-schedule-limit")
	re.NoError(err)
	re.Empty(resp.Kvs)
}

func (suite *httpClientTestSuite) TestScheduleConfig() {
	suite.RunTestInTwoModes(suite.checkScheduleConfig)
}

func (suite *httpClientTestSuite) checkScheduleConfig(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	config, err := client.GetScheduleConfig(env.ctx)
	re.NoError(err)
	re.Equal(float64(4), config["hot-region-schedule-limit"])
	re.Equal(float64(2048), config["region-schedule-limit"])
	config["hot-region-schedule-limit"] = float64(8)
	err = client.SetScheduleConfig(env.ctx, config)
	re.NoError(err)
	config, err = client.GetScheduleConfig(env.ctx)
	re.NoError(err)
	re.Equal(float64(8), config["hot-region-schedule-limit"])
	re.Equal(float64(2048), config["region-schedule-limit"])
}

func (suite *httpClientTestSuite) TestSchedulers() {
	suite.RunTestInTwoModes(suite.checkSchedulers)
}

func (suite *httpClientTestSuite) checkSchedulers(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	schedulers, err := client.GetSchedulers(env.ctx)
	re.NoError(err)
	re.Empty(schedulers)

	err = client.CreateScheduler(env.ctx, "evict-leader-scheduler", 1)
	re.NoError(err)
	schedulers, err = client.GetSchedulers(env.ctx)
	re.NoError(err)
	re.Len(schedulers, 1)
	err = client.SetSchedulerDelay(env.ctx, "evict-leader-scheduler", 100)
	re.NoError(err)
	err = client.SetSchedulerDelay(env.ctx, "not-exist", 100)
	re.ErrorContains(err, "500 Internal Server Error") // TODO: should return friendly error message
}

func (suite *httpClientTestSuite) TestSetStoreLabels() {
	suite.RunTestInTwoModes(suite.checkSetStoreLabels)
}

func (suite *httpClientTestSuite) checkSetStoreLabels(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	resp, err := client.GetStores(env.ctx)
	re.NoError(err)
	setStore := resp.Stores[0]
	re.Empty(setStore.Store.Labels, nil)
	storeLabels := map[string]string{
		"zone": "zone1",
	}
	err = client.SetStoreLabels(env.ctx, 1, storeLabels)
	re.NoError(err)

	resp, err = client.GetStores(env.ctx)
	re.NoError(err)
	for _, store := range resp.Stores {
		if store.Store.ID == setStore.Store.ID {
			for _, label := range store.Store.Labels {
				re.Equal(label.Value, storeLabels[label.Key])
			}
		}
	}
}

func (suite *httpClientTestSuite) TestTransferLeader() {
	suite.RunTestInTwoModes(suite.checkTransferLeader)
}

func (suite *httpClientTestSuite) checkTransferLeader(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	members, err := client.GetMembers(env.ctx)
	re.NoError(err)
	re.Len(members.Members, 2)

	leader, err := client.GetLeader(env.ctx)
	re.NoError(err)

	// Transfer leader to another pd
	for _, member := range members.Members {
		if member.GetName() != leader.GetName() {
			err = client.TransferLeader(env.ctx, member.GetName())
			re.NoError(err)
			break
		}
	}

	newLeader := env.cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NoError(err)
	re.NotEqual(leader.GetName(), newLeader)
	// Force to update the members info.
	testutil.Eventually(re, func() bool {
		leader, err = client.GetLeader(env.ctx)
		re.NoError(err)
		return newLeader == leader.GetName()
	})
	members, err = client.GetMembers(env.ctx)
	re.NoError(err)
	re.Len(members.Members, 2)
	re.Equal(leader.GetName(), members.Leader.GetName())
}

func (suite *httpClientTestSuite) TestVersion() {
	suite.RunTestInTwoModes(suite.checkVersion)
}

func (suite *httpClientTestSuite) checkVersion(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	ver, err := client.GetPDVersion(env.ctx)
	re.NoError(err)
	re.Equal(versioninfo.PDReleaseVersion, ver)
}

func (suite *httpClientTestSuite) TestAdmin() {
	suite.RunTestInTwoModes(suite.checkAdmin)
}

func (suite *httpClientTestSuite) checkAdmin(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	err := client.SetSnapshotRecoveringMark(env.ctx)
	re.NoError(err)
	err = client.ResetTS(env.ctx, 123, true)
	re.NoError(err)
	err = client.ResetBaseAllocID(env.ctx, 456)
	re.NoError(err)
	err = client.DeleteSnapshotRecoveringMark(env.ctx)
	re.NoError(err)
}

func (suite *httpClientTestSuite) TestWithBackoffer() {
	suite.RunTestInTwoModes(suite.checkWithBackoffer)
}

func (suite *httpClientTestSuite) checkWithBackoffer(mode mode, client pd.Client) {
	re := suite.Require()
	env := suite.env[mode]

	// Should return with 404 error without backoffer.
	rule, err := client.GetPlacementRule(env.ctx, "non-exist-group", "non-exist-rule")
	re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	re.Nil(rule)
	// Should return with 404 error even with an infinite backoffer.
	rule, err = client.
		WithBackoffer(retry.InitialBackoffer(100*time.Millisecond, time.Second, 0)).
		GetPlacementRule(env.ctx, "non-exist-group", "non-exist-rule")
	re.ErrorContains(err, http.StatusText(http.StatusNotFound))
	re.Nil(rule)
}

func (suite *httpClientTestSuite) TestRedirectWithMetrics() {
	re := suite.Require()
	env := suite.env[defaultServiceDiscovery]

	cli := setupCli(suite.Require(), env.ctx, env.endpoints)
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
	c.CreateScheduler(context.Background(), "test", 0)
	var out dto.Metric
	failureCnt, err := metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", "network error"}...)
	re.NoError(err)
	failureCnt.Write(&out)
	re.Equal(float64(2), out.Counter.GetValue())
	c.Close()

	leader := sd.GetServingAddr()
	httpClient = pd.NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		// mock leader success.
		if !strings.Contains(leader, req.Host) {
			return errors.New("mock error")
		}
		return nil
	})
	c = pd.NewClientWithServiceDiscovery("pd-http-client-it", sd, pd.WithHTTPClient(httpClient), pd.WithMetrics(metricCnt, nil))
	c.CreateScheduler(context.Background(), "test", 0)
	successCnt, err := metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", ""}...)
	re.NoError(err)
	successCnt.Write(&out)
	re.Equal(float64(1), out.Counter.GetValue())
	c.Close()

	httpClient = pd.NewHTTPClientWithRequestChecker(func(req *http.Request) error {
		// mock leader success.
		if strings.Contains(leader, req.Host) {
			return errors.New("mock error")
		}
		return nil
	})
	c = pd.NewClientWithServiceDiscovery("pd-http-client-it", sd, pd.WithHTTPClient(httpClient), pd.WithMetrics(metricCnt, nil))
	c.CreateScheduler(context.Background(), "test", 0)
	successCnt, err = metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", ""}...)
	re.NoError(err)
	successCnt.Write(&out)
	re.Equal(float64(2), out.Counter.GetValue())
	failureCnt, err = metricCnt.GetMetricWithLabelValues([]string{"CreateScheduler", "network error"}...)
	re.NoError(err)
	failureCnt.Write(&out)
	re.Equal(float64(3), out.Counter.GetValue())
	c.Close()
}
