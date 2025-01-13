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

package config_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/ratelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

// testDialClient used to dial http request. only used for test.
var testDialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

type testCase struct {
	name  string
	value any
	read  func(scheduleConfig *sc.ScheduleConfig) any
}

func (t *testCase) judge(re *require.Assertions, scheduleConfigs ...*sc.ScheduleConfig) {
	value := t.value
	for _, scheduleConfig := range scheduleConfigs {
		re.NotNil(scheduleConfig)
		re.IsType(value, t.read(scheduleConfig))
	}
}

type configTestSuite struct {
	suite.Suite
	env *pdTests.SchedulingTestEnvironment
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(configTestSuite))
}

func (suite *configTestSuite) SetupTest() {
	// use a new environment to avoid affecting other tests
	suite.env = pdTests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *configTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *configTestSuite) TearDownTest() {
	re := suite.Require()
	cleanFunc := func(cluster *pdTests.TestCluster) {
		def := placement.GroupBundle{
			ID: "pd",
			Rules: []*placement.Rule{
				{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
			},
		}
		data, err := json.Marshal([]placement.GroupBundle{def})
		re.NoError(err)
		leader := cluster.GetLeaderServer()
		re.NotNil(leader)
		urlPrefix := leader.GetAddr()
		err = testutil.CheckPostJSON(testDialClient, urlPrefix+"/pd/api/v1/config/placement-rule", data, testutil.StatusOK(re))
		re.NoError(err)
	}
	suite.env.RunTest(cleanFunc)
	suite.env.Cleanup()
}

func (suite *configTestSuite) TestConfig() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/dashboard/adapter/skipDashboardLoop", `return(true)`))
	suite.env.RunTest(suite.checkConfig)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/dashboard/adapter/skipDashboardLoop"))
}

func (suite *configTestSuite) checkConfig(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	svr := leaderServer.GetServer()
	pdTests.MustPutStore(re, cluster, store)

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	cfg := config.Config{}
	re.NoError(json.Unmarshal(output, &cfg))
	scheduleConfig := svr.GetScheduleConfig()

	// hidden config
	scheduleConfig.Schedulers = nil
	scheduleConfig.StoreLimit = nil
	scheduleConfig.SchedulerMaxWaitingOperator = 0
	scheduleConfig.EnableRemoveDownReplica = false
	scheduleConfig.EnableReplaceOfflineReplica = false
	scheduleConfig.EnableMakeUpReplica = false
	scheduleConfig.EnableRemoveExtraReplica = false
	scheduleConfig.EnableLocationReplacement = false
	re.Equal(uint64(0), scheduleConfig.MaxMergeRegionKeys)
	// The result of config show doesn't be 0.
	scheduleConfig.MaxMergeRegionKeys = scheduleConfig.GetMaxMergeRegionKeys()
	re.Equal(scheduleConfig, &cfg.Schedule)
	re.Equal(svr.GetReplicationConfig(), &cfg.Replication)

	// config set trace-region-flow <value>
	args = []string{"-u", pdAddr, "config", "set", "trace-region-flow", "false"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	origin := svr.GetPDServerConfig().FlowRoundByDigit
	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", "10"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(10, svr.GetPDServerConfig().FlowRoundByDigit)

	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", "-10"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.Error(err)

	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", strconv.Itoa(origin)}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "server")
		re.NoError(err)
		var conf config.PDServerConfig
		re.NoError(json.Unmarshal(output, &conf))
		return conf.FlowRoundByDigit == origin
	})

	// config show schedule
	args = []string{"-u", pdAddr, "config", "show", "schedule"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	scheduleCfg := sc.ScheduleConfig{}
	re.NoError(json.Unmarshal(output, &scheduleCfg))
	scheduleConfig = svr.GetScheduleConfig()
	scheduleConfig.MaxMergeRegionKeys = scheduleConfig.GetMaxMergeRegionKeys()
	re.Equal(scheduleConfig, &scheduleCfg)

	// After https://github.com/tikv/tikv/issues/17309, the default value is enlarged from 20 to 54,
	// to make it compatible with the default value of region size of tikv.
	re.Equal(54, int(svr.GetScheduleConfig().MaxMergeRegionSize))
	re.Equal(0, int(svr.GetScheduleConfig().MaxMergeRegionKeys))
	re.Equal(54*10000, int(svr.GetScheduleConfig().GetMaxMergeRegionKeys()))

	// set max-merge-region-size to 40MB
	args = []string{"-u", pdAddr, "config", "set", "max-merge-region-size", "40"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(40, int(svr.GetScheduleConfig().MaxMergeRegionSize))
	re.Equal(0, int(svr.GetScheduleConfig().MaxMergeRegionKeys))
	re.Equal(40*10000, int(svr.GetScheduleConfig().GetMaxMergeRegionKeys()))
	args = []string{"-u", pdAddr, "config", "set", "max-merge-region-keys", "200000"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(20*10000, int(svr.GetScheduleConfig().MaxMergeRegionKeys))
	re.Equal(20*10000, int(svr.GetScheduleConfig().GetMaxMergeRegionKeys()))

	// set store limit v2
	args = []string{"-u", pdAddr, "config", "set", "store-limit-version", "v2"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal("v2", svr.GetScheduleConfig().StoreLimitVersion)
	args = []string{"-u", pdAddr, "config", "set", "store-limit-version", "v1"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal("v1", svr.GetScheduleConfig().StoreLimitVersion)

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	replicationCfg := sc.ReplicationConfig{}
	re.NoError(json.Unmarshal(output, &replicationCfg))
	re.Equal(svr.GetReplicationConfig(), &replicationCfg)

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	clusterVersion := semver.Version{}
	re.NoError(json.Unmarshal(output, &clusterVersion))
	re.Equal(svr.GetClusterVersion(), clusterVersion)

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, err = tests.ExecuteCommand(cmd, args2...)
	re.NoError(err)
	re.NotEqual(svr.GetClusterVersion(), clusterVersion)
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	clusterVersion = semver.Version{}
	re.NoError(json.Unmarshal(output, &clusterVersion))
	re.Equal(svr.GetClusterVersion(), clusterVersion)

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	labelPropertyCfg := config.LabelPropertyConfig{}
	re.NoError(json.Unmarshal(output, &labelPropertyCfg))
	re.Equal(svr.GetLabelProperty(), labelPropertyCfg)

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, err = tests.ExecuteCommand(cmd, args2...)
	re.NoError(err)
	re.NotEqual(svr.GetLabelProperty(), labelPropertyCfg)
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	labelPropertyCfg = config.LabelPropertyConfig{}
	re.NoError(json.Unmarshal(output, &labelPropertyCfg))
	re.Equal(svr.GetLabelProperty(), labelPropertyCfg)

	// config delete label-property <type> <key> <value>
	args3 := []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, err = tests.ExecuteCommand(cmd, args3...)
	re.NoError(err)
	re.NotEqual(svr.GetLabelProperty(), labelPropertyCfg)
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	labelPropertyCfg = config.LabelPropertyConfig{}
	re.NoError(json.Unmarshal(output, &labelPropertyCfg))
	re.Equal(svr.GetLabelProperty(), labelPropertyCfg)

	// config set min-resolved-ts-persistence-interval <value>
	args = []string{"-u", pdAddr, "config", "set", "min-resolved-ts-persistence-interval", "1s"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(typeutil.NewDuration(time.Second), svr.GetPDServerConfig().MinResolvedTSPersistenceInterval)

	// config set max-store-preparing-time 10m
	args = []string{"-u", pdAddr, "config", "set", "max-store-preparing-time", "10m"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(typeutil.NewDuration(10*time.Minute), svr.GetScheduleConfig().MaxStorePreparingTime)

	args = []string{"-u", pdAddr, "config", "set", "max-store-preparing-time", "0s"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(typeutil.NewDuration(0), svr.GetScheduleConfig().MaxStorePreparingTime)

	// test config read and write
	testCases := []testCase{
		{"leader-schedule-limit", uint64(64), func(scheduleConfig *sc.ScheduleConfig) any {
			return scheduleConfig.LeaderScheduleLimit
		}}, {"hot-region-schedule-limit", uint64(64), func(scheduleConfig *sc.ScheduleConfig) any {
			return scheduleConfig.HotRegionScheduleLimit
		}}, {"hot-region-cache-hits-threshold", uint64(5), func(scheduleConfig *sc.ScheduleConfig) any {
			return scheduleConfig.HotRegionCacheHitsThreshold
		}}, {"enable-remove-down-replica", false, func(scheduleConfig *sc.ScheduleConfig) any {
			return scheduleConfig.EnableRemoveDownReplica
		}},
		{"enable-debug-metrics", true, func(scheduleConfig *sc.ScheduleConfig) any {
			return scheduleConfig.EnableDebugMetrics
		}},
		// set again
		{"enable-debug-metrics", true, func(scheduleConfig *sc.ScheduleConfig) any {
			return scheduleConfig.EnableDebugMetrics
		}},
	}
	for _, testCase := range testCases {
		// write
		args1 = []string{"-u", pdAddr, "config", "set", testCase.name, reflect.TypeOf(testCase.value).String()}
		_, err = tests.ExecuteCommand(cmd, args1...)
		re.NoError(err)
		// read
		args2 = []string{"-u", pdAddr, "config", "show"}
		output, err = tests.ExecuteCommand(cmd, args2...)
		re.NoError(err)
		cfg = config.Config{}
		re.NoError(json.Unmarshal(output, &cfg))
		// judge
		testCase.judge(re, &cfg.Schedule, svr.GetScheduleConfig())
	}

	// test error or deprecated config name
	args1 = []string{"-u", pdAddr, "config", "set", "foo-bar", "1"}
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	re.Contains(string(output), "not found")
	args1 = []string{"-u", pdAddr, "config", "set", "disable-remove-down-replica", "true"}
	output, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	re.Contains(string(output), "already been deprecated")

	// set enable-placement-rules twice, make sure it does not return error.
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, err = tests.ExecuteCommand(cmd, args1...)
	re.NoError(err)

	// test invalid value
	argsInvalid := []string{"-u", pdAddr, "config", "set", "leader-schedule-policy", "aaa"}
	output, err = tests.ExecuteCommand(cmd, argsInvalid...)
	re.NoError(err)
	re.Contains(string(output), "is invalid")
	argsInvalid = []string{"-u", pdAddr, "config", "set", "key-type", "aaa"}
	output, err = tests.ExecuteCommand(cmd, argsInvalid...)
	re.NoError(err)
	re.Contains(string(output), "is invalid")

	// config set patrol-region-worker-count
	args = []string{"-u", pdAddr, "config", "set", "patrol-region-worker-count", "8"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(8, svr.GetScheduleConfig().PatrolRegionWorkerCount)
	// the max value of patrol-region-worker-count is 8 and the min value is 1
	args = []string{"-u", pdAddr, "config", "set", "patrol-region-worker-count", "9"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "patrol-region-worker-count should be between 1 and 8")
	re.Equal(8, svr.GetScheduleConfig().PatrolRegionWorkerCount)
	args = []string{"-u", pdAddr, "config", "set", "patrol-region-worker-count", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "patrol-region-worker-count should be between 1 and 8")
	re.Equal(8, svr.GetScheduleConfig().PatrolRegionWorkerCount)
}

func (suite *configTestSuite) TestConfigForwardControl() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/dashboard/adapter/skipDashboardLoop", `return(true)`))
	suite.env.RunTest(suite.checkConfigForwardControl)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/dashboard/adapter/skipDashboardLoop"))
}

func (suite *configTestSuite) checkConfigForwardControl(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()

	f, _ := os.CreateTemp("", "pd_tests")
	fname := f.Name()
	f.Close()
	defer os.RemoveAll(fname)

	checkScheduleConfig := func(scheduleCfg *sc.ScheduleConfig, isFromPDService bool) {
		if schedulingServer := cluster.GetSchedulingPrimaryServer(); schedulingServer != nil {
			if isFromPDService {
				re.Equal(scheduleCfg.LeaderScheduleLimit, leaderServer.GetPersistOptions().GetLeaderScheduleLimit())
				re.NotEqual(scheduleCfg.LeaderScheduleLimit, schedulingServer.GetPersistConfig().GetLeaderScheduleLimit())
			} else {
				re.Equal(scheduleCfg.LeaderScheduleLimit, schedulingServer.GetPersistConfig().GetLeaderScheduleLimit())
				re.NotEqual(scheduleCfg.LeaderScheduleLimit, leaderServer.GetPersistOptions().GetLeaderScheduleLimit())
			}
		} else {
			re.Equal(scheduleCfg.LeaderScheduleLimit, leaderServer.GetPersistOptions().GetLeaderScheduleLimit())
		}
	}

	checkReplicateConfig := func(replicationCfg *sc.ReplicationConfig, isFromPDService bool) {
		if schedulingServer := cluster.GetSchedulingPrimaryServer(); schedulingServer != nil {
			if isFromPDService {
				re.Equal(replicationCfg.MaxReplicas, uint64(leaderServer.GetPersistOptions().GetMaxReplicas()))
				re.NotEqual(int(replicationCfg.MaxReplicas), schedulingServer.GetPersistConfig().GetMaxReplicas())
			} else {
				re.Equal(int(replicationCfg.MaxReplicas), schedulingServer.GetPersistConfig().GetMaxReplicas())
				re.NotEqual(replicationCfg.MaxReplicas, uint64(leaderServer.GetPersistOptions().GetMaxReplicas()))
			}
		} else {
			re.Equal(replicationCfg.MaxReplicas, uint64(leaderServer.GetPersistOptions().GetMaxReplicas()))
		}
	}

	checkRules := func(rules []*placement.Rule, isFromPDService bool) {
		apiRules := leaderServer.GetRaftCluster().GetRuleManager().GetAllRules()
		if schedulingServer := cluster.GetSchedulingPrimaryServer(); schedulingServer != nil {
			schedulingRules := schedulingServer.GetCluster().GetRuleManager().GetAllRules()
			if isFromPDService {
				re.Len(apiRules, len(rules))
				re.NotEqual(len(schedulingRules), len(rules))
			} else {
				re.Len(schedulingRules, len(rules))
				re.NotEqual(len(apiRules), len(rules))
			}
		} else {
			re.Len(apiRules, len(rules))
		}
	}

	checkGroup := func(group placement.RuleGroup, isFromPDService bool) {
		apiGroup := leaderServer.GetRaftCluster().GetRuleManager().GetRuleGroup(placement.DefaultGroupID)
		if schedulingServer := cluster.GetSchedulingPrimaryServer(); schedulingServer != nil {
			schedulingGroup := schedulingServer.GetCluster().GetRuleManager().GetRuleGroup(placement.DefaultGroupID)
			if isFromPDService {
				re.Equal(apiGroup.Index, group.Index)
				re.NotEqual(schedulingGroup.Index, group.Index)
			} else {
				re.Equal(schedulingGroup.Index, group.Index)
				re.NotEqual(apiGroup.Index, group.Index)
			}
		} else {
			re.Equal(apiGroup.Index, group.Index)
		}
	}

	testConfig := func(options ...string) {
		for _, isFromPDService := range []bool{true, false} {
			cmd := ctl.GetRootCmd()
			args := []string{"-u", pdAddr, "config", "show"}
			args = append(args, options...)
			if isFromPDService {
				args = append(args, "--from_pd")
			}
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			if len(options) == 0 || options[0] == "all" {
				cfg := config.Config{}
				re.NoError(json.Unmarshal(output, &cfg))
				checkReplicateConfig(&cfg.Replication, isFromPDService)
				checkScheduleConfig(&cfg.Schedule, isFromPDService)
			} else if options[0] == "replication" {
				replicationCfg := &sc.ReplicationConfig{}
				re.NoError(json.Unmarshal(output, replicationCfg))
				checkReplicateConfig(replicationCfg, isFromPDService)
			} else if options[0] == "schedule" {
				scheduleCfg := &sc.ScheduleConfig{}
				re.NoError(json.Unmarshal(output, scheduleCfg))
				checkScheduleConfig(scheduleCfg, isFromPDService)
			} else {
				re.Fail("no implement")
			}
		}
	}

	testRules := func(options ...string) {
		for _, isFromPDService := range []bool{true, false} {
			cmd := ctl.GetRootCmd()
			args := []string{"-u", pdAddr, "config", "placement-rules"}
			args = append(args, options...)
			if isFromPDService {
				args = append(args, "--from_pd")
			}
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			if options[0] == "show" {
				var rules []*placement.Rule
				re.NoError(json.Unmarshal(output, &rules))
				checkRules(rules, isFromPDService)
			} else if options[0] == "load" {
				var rules []*placement.Rule
				b, _ := os.ReadFile(fname)
				re.NoError(json.Unmarshal(b, &rules))
				checkRules(rules, isFromPDService)
			} else if options[0] == "rule-group" {
				var group placement.RuleGroup
				re.NoError(json.Unmarshal(output, &group), string(output))
				checkGroup(group, isFromPDService)
			} else if options[0] == "rule-bundle" && options[1] == "get" {
				var bundle placement.GroupBundle
				re.NoError(json.Unmarshal(output, &bundle), string(output))
				checkRules(bundle.Rules, isFromPDService)
			} else if options[0] == "rule-bundle" && options[1] == "load" {
				var bundles []placement.GroupBundle
				b, _ := os.ReadFile(fname)
				re.NoError(json.Unmarshal(b, &bundles), string(output))
				checkRules(bundles[0].Rules, isFromPDService)
			} else {
				re.Fail("no implement")
			}
		}
	}

	// Test Config
	// inject different config to scheduling server
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		scheCfg := sche.GetPersistConfig().GetScheduleConfig().Clone()
		scheCfg.LeaderScheduleLimit = 233
		sche.GetPersistConfig().SetScheduleConfig(scheCfg)
		repCfg := sche.GetPersistConfig().GetReplicationConfig().Clone()
		repCfg.MaxReplicas = 7
		sche.GetPersistConfig().SetReplicationConfig(repCfg)
		re.Equal(uint64(233), sche.GetPersistConfig().GetLeaderScheduleLimit())
		re.Equal(7, sche.GetPersistConfig().GetMaxReplicas())
	}
	// show config from PD rather than scheduling server
	testConfig()
	// show all config from PD rather than scheduling server
	testConfig("all")
	// show replication config from PD rather than scheduling server
	testConfig("replication")
	// show schedule config from PD rather than scheduling server
	testConfig("schedule")

	// Test Rule
	// inject different rule to scheduling server
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		ruleManager := sche.GetCluster().GetRuleManager()
		ruleManager.SetAllGroupBundles([]placement.GroupBundle{{
			ID:       placement.DefaultGroupID,
			Index:    233,
			Override: true,
			Rules: []*placement.Rule{
				{
					GroupID: placement.DefaultGroupID,
					ID:      "test",
					Index:   100,
					Role:    placement.Voter,
					Count:   5,
				},
				{
					GroupID: placement.DefaultGroupID,
					ID:      "pd",
					Index:   101,
					Role:    placement.Voter,
					Count:   3,
				},
			},
		}}, true)
		re.Len(ruleManager.GetAllRules(), 2)
	}

	// show placement rules
	testRules("show")
	// load placement rules
	testRules("load", "--out="+fname)
	// show placement rules group
	testRules("rule-group", "show", placement.DefaultGroupID)
	// show placement rules group bundle
	testRules("rule-bundle", "get", placement.DefaultGroupID)
	// load placement rules bundle
	testRules("rule-bundle", "load", "--out="+fname)
}

func (suite *configTestSuite) TestPlacementRules() {
	suite.env.RunTest(suite.checkPlacementRules)
}

func (suite *configTestSuite) checkPlacementRules(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	pdTests.MustPutStore(re, cluster, store)

	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// test show
	checkShowRuleKey(re, pdAddr, [][2]string{{placement.DefaultGroupID, placement.DefaultRuleID}})

	f, _ := os.CreateTemp("", "pd_tests")
	fname := f.Name()
	f.Close()
	defer os.RemoveAll(fname)

	// test load
	rules := checkLoadRule(re, pdAddr, fname, [][2]string{{placement.DefaultGroupID, placement.DefaultRuleID}})

	// test save
	rules = append(rules, placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "test1",
		Role:    placement.Voter,
		Count:   1,
	}, placement.Rule{
		GroupID: "test-group",
		ID:      "test2",
		Role:    placement.Voter,
		Count:   2,
	})
	b, _ := json.Marshal(rules)
	os.WriteFile(fname, b, 0600)
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	re.NoError(err)

	// test show group
	checkShowRuleKey(re, pdAddr, [][2]string{{placement.DefaultGroupID, placement.DefaultRuleID}, {placement.DefaultGroupID, "test1"}}, "--group=pd")

	// test rule region detail
	pdTests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"))
	checkShowRuleKey(re, pdAddr, [][2]string{{placement.DefaultGroupID, placement.DefaultRuleID}}, "--region=1", "--detail")

	// test delete
	// need clear up args, so create new a cobra.Command. Otherwise gourp still exists.
	rules[0].Count = 0
	b, _ = json.Marshal(rules)
	os.WriteFile(fname, b, 0600)
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	re.NoError(err)
	checkShowRuleKey(re, pdAddr, [][2]string{{placement.DefaultGroupID, "test1"}}, "--group=pd")
}

func (suite *configTestSuite) TestPlacementRuleGroups() {
	suite.env.RunTest(suite.checkPlacementRuleGroups)
}

func (suite *configTestSuite) checkPlacementRuleGroups(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	pdTests.MustPutStore(re, cluster, store)
	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// test show
	var group placement.RuleGroup
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", placement.DefaultGroupID)
		re.NoError(err)
		return !strings.Contains(string(output), "404")
	})
	re.NoError(json.Unmarshal(output, &group), string(output))
	re.Equal(placement.RuleGroup{ID: placement.DefaultGroupID}, group)

	// test set
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", placement.DefaultGroupID, "42", "true")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "group2", "100", "false")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "group3", "200", "false")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// show all
	var groups []placement.RuleGroup
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show")
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &groups))
		return reflect.DeepEqual([]placement.RuleGroup{
			{ID: placement.DefaultGroupID, Index: 42, Override: true},
			{ID: "group2", Index: 100, Override: false},
			{ID: "group3", Index: 200, Override: false},
		}, groups)
	})

	// delete
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "delete", "group2")
	re.NoError(err)
	re.Contains(string(output), "Delete group and rules successfully.")

	// show again
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "group2")
		re.NoError(err)
		return strings.Contains(string(output), "404")
	})

	// delete using regex
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "delete", "--regexp", ".*3")
	re.NoError(err)

	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "group3")
		re.NoError(err)
		return strings.Contains(string(output), "404")
	})
}

func (suite *configTestSuite) TestPlacementRuleBundle() {
	suite.env.RunTest(suite.checkPlacementRuleBundle)
}

func (suite *configTestSuite) checkPlacementRuleBundle(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	pdTests.MustPutStore(re, cluster, store)

	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// test get
	var bundle placement.GroupBundle
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "get", placement.DefaultGroupID)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &bundle))
	expect := placement.GroupBundle{ID: placement.DefaultGroupID, Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}}
	expect.Rules[0].CreateTimestamp = bundle.Rules[0].CreateTimestamp // skip create timestamp in mcs
	re.Equal(expect, bundle)

	f, err := os.CreateTemp("", "pd_tests")
	re.NoError(err)
	fname := f.Name()
	f.Close()
	defer os.RemoveAll(fname)

	// test load
	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: placement.DefaultGroupID, Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})

	// test set
	bundle.ID = "pe"
	bundle.Rules[0].GroupID = "pe"
	b, err := json.Marshal(bundle)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	re.NoError(err)
	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: placement.DefaultGroupID, Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})

	// test delete
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", placement.DefaultGroupID)
	re.NoError(err)

	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})

	// test delete regexp
	bundle.ID = "pf"
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}
	b, err = json.Marshal(bundle)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	re.NoError(err)
	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "--regexp", ".*f")
	re.NoError(err)

	bundles := []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	}
	checkLoadRuleBundle(re, pdAddr, fname, bundles)

	// test save
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}
	bundles = append(bundles, bundle)
	b, err = json.Marshal(bundles)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname)
	re.NoError(err)
	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})

	// partial update, so still one group is left, no error
	bundles = []placement.GroupBundle{{ID: "pe", Rules: []*placement.Rule{}}}
	b, err = json.Marshal(bundles)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname, "--partial")
	re.NoError(err)

	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})

	// set default rule only
	bundles = []placement.GroupBundle{{
		ID: "pd",
		Rules: []*placement.Rule{
			{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
		},
	}}
	b, err = json.Marshal(bundles)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname)
	re.NoError(err)
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "--regexp", ".*f")
	re.NoError(err)

	checkLoadRuleBundle(re, pdAddr, fname, []placement.GroupBundle{
		{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3}}},
	})
}

func checkLoadRuleBundle(re *require.Assertions, pdAddr string, fname string, expectValues []placement.GroupBundle) {
	var bundles []placement.GroupBundle
	cmd := ctl.GetRootCmd()
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		_, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
		re.NoError(err)
		b, _ := os.ReadFile(fname)
		re.NoError(json.Unmarshal(b, &bundles))
		return len(bundles) == len(expectValues)
	})
	assertBundles(re, bundles, expectValues)
}

func checkLoadRule(re *require.Assertions, pdAddr string, fname string, expectValues [][2]string) []placement.Rule {
	var rules []placement.Rule
	cmd := ctl.GetRootCmd()
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		_, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "load", "--out="+fname)
		re.NoError(err)
		b, _ := os.ReadFile(fname)
		re.NoError(json.Unmarshal(b, &rules))
		return len(rules) == len(expectValues)
	})
	for i, v := range expectValues {
		re.Equal(v, rules[i].Key())
	}
	return rules
}

func checkShowRuleKey(re *require.Assertions, pdAddr string, expectValues [][2]string, opts ...string) {
	var (
		rules []placement.Rule
		fit   placement.RegionFit
	)
	cmd := ctl.GetRootCmd()
	testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
		args := []string{"-u", pdAddr, "config", "placement-rules", "show"}
		output, err := tests.ExecuteCommand(cmd, append(args, opts...)...)
		re.NoError(err)
		err = json.Unmarshal(output, &rules)
		if err == nil {
			return len(rules) == len(expectValues)
		}
		re.NoError(json.Unmarshal(output, &fit))
		return len(fit.RuleFits) != 0
	})
	if len(rules) != 0 {
		for i, v := range expectValues {
			re.Equal(v, rules[i].Key())
		}
	}
	if len(fit.RuleFits) != 0 {
		for i, v := range expectValues {
			re.Equal(v, fit.RuleFits[i].Rule.Key())
		}
	}
}

func TestReplicationMode(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdTests.MustPutStore(re, cluster, store)

	conf := config.ReplicationModeConfig{
		ReplicationMode: "majority",
		DRAutoSync: config.DRAutoSyncReplicationConfig{
			WaitStoreTimeout: typeutil.NewDuration(time.Minute),
		},
	}
	check := func() {
		output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "replication-mode")
		re.NoError(err)
		var conf2 config.ReplicationModeConfig
		re.NoError(json.Unmarshal(output, &conf2))
		re.Equal(conf, conf2)
	}

	check()

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync")
	re.NoError(err)
	conf.ReplicationMode = "dr-auto-sync"
	check()

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "label-key", "foobar")
	re.NoError(err)
	conf.DRAutoSync.LabelKey = "foobar"
	check()

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "primary-replicas", "5")
	re.NoError(err)
	conf.DRAutoSync.PrimaryReplicas = 5
	check()

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "wait-store-timeout", "10m")
	re.NoError(err)
	conf.DRAutoSync.WaitStoreTimeout = typeutil.NewDuration(time.Minute * 10)
	check()
}

func TestServiceMiddlewareConfig(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdTests.MustPutStore(re, cluster, store)

	conf := config.ServiceMiddlewareConfig{
		AuditConfig: config.AuditConfig{
			EnableAudit: true,
		},
		RateLimitConfig: config.RateLimitConfig{
			EnableRateLimit: true,
			LimiterConfig:   make(map[string]ratelimit.DimensionConfig),
		},
		GRPCRateLimitConfig: config.GRPCRateLimitConfig{
			EnableRateLimit: true,
			LimiterConfig:   make(map[string]ratelimit.DimensionConfig),
		},
	}

	check := func() {
		output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "service-middleware")
		re.NoError(err)
		var conf2 config.ServiceMiddlewareConfig
		re.NoError(json.Unmarshal(output, &conf2))
		re.Equal(conf, conf2)
	}

	check()

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "audit", "enable-audit", "false")
	re.NoError(err)
	conf.AuditConfig.EnableAudit = false
	check()
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "rate-limit", "GetRegion", "qps", "100.1")
	re.NoError(err)
	conf.RateLimitConfig.LimiterConfig["GetRegion"] = ratelimit.DimensionConfig{QPS: 100.1, QPSBurst: 100}
	check()
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "grpc-rate-limit", "GetRegion", "qps", "101.1")
	re.NoError(err)
	conf.GRPCRateLimitConfig.LimiterConfig["GetRegion"] = ratelimit.DimensionConfig{QPS: 101.1, QPSBurst: 101}
	check()
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "rate-limit", "GetRegion", "concurrency", "10")
	re.NoError(err)
	conf.RateLimitConfig.LimiterConfig["GetRegion"] = ratelimit.DimensionConfig{QPS: 100.1, QPSBurst: 100, ConcurrencyLimit: 10}
	check()
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "grpc-rate-limit", "GetRegion", "concurrency", "11")
	re.NoError(err)
	conf.GRPCRateLimitConfig.LimiterConfig["GetRegion"] = ratelimit.DimensionConfig{QPS: 101.1, QPSBurst: 101, ConcurrencyLimit: 11}
	check()
	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "xxx", "GetRegion", "qps", "1000")
	re.NoError(err)
	re.Contains(string(output), "correct type")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "grpc-rate-limit", "xxx", "qps", "1000")
	re.NoError(err)
	re.Contains(string(output), "There is no label matched.")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "grpc-rate-limit", "GetRegion", "xxx", "1000")
	re.NoError(err)
	re.Contains(string(output), "Input is invalid")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "grpc-rate-limit", "GetRegion", "qps", "xxx")
	re.NoError(err)
	re.Contains(string(output), "strconv.ParseFloat")
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "grpc-rate-limit", "enable-grpc-rate-limit", "false")
	re.NoError(err)
	conf.GRPCRateLimitConfig.EnableRateLimit = false
	check()
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "rate-limit", "GetRegion", "concurrency", "0")
	re.NoError(err)
	conf.RateLimitConfig.LimiterConfig["GetRegion"] = ratelimit.DimensionConfig{QPS: 100.1, QPSBurst: 100}
	check()
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "service-middleware", "rate-limit", "GetRegion", "qps", "0")
	re.NoError(err)
	delete(conf.RateLimitConfig.LimiterConfig, "GetRegion")
	check()
}

func (suite *configTestSuite) TestUpdateDefaultReplicaConfig() {
	suite.env.RunTest(suite.checkUpdateDefaultReplicaConfig)
}

func (suite *configTestSuite) checkUpdateDefaultReplicaConfig(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	pdTests.MustPutStore(re, cluster, store)
	checkMaxReplicas := func(expect uint64) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			replicationCfg := sc.ReplicationConfig{}
			re.NoError(json.Unmarshal(output, &replicationCfg))
			return replicationCfg.MaxReplicas == expect
		})
	}

	checkLocationLabels := func(expect int) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			replicationCfg := sc.ReplicationConfig{}
			re.NoError(json.Unmarshal(output, &replicationCfg))
			return len(replicationCfg.LocationLabels) == expect
		})
	}

	checkIsolationLevel := func(expect string) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			replicationCfg := sc.ReplicationConfig{}
			re.NoError(json.Unmarshal(output, &replicationCfg))
			return replicationCfg.IsolationLevel == expect
		})
	}

	checkRuleCount := func(expect int) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", placement.DefaultGroupID, "--id", placement.DefaultRuleID}
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			rule := placement.Rule{}
			re.NoError(json.Unmarshal(output, &rule))
			return rule.Count == expect
		})
	}

	checkRuleLocationLabels := func(expect int) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", placement.DefaultGroupID, "--id", placement.DefaultRuleID}
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			rule := placement.Rule{}
			re.NoError(json.Unmarshal(output, &rule))
			return len(rule.LocationLabels) == expect
		})
	}

	checkRuleIsolationLevel := func(expect string) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", placement.DefaultGroupID, "--id", placement.DefaultRuleID}
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			rule := placement.Rule{}
			re.NoError(json.Unmarshal(output, &rule))
			return rule.IsolationLevel == expect
		})
	}

	// update successfully when placement rules is not enabled.
	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "2")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkMaxReplicas(2)
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "location-labels", "zone,host")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "isolation-level", "zone")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkLocationLabels(2)
	checkRuleLocationLabels(2)
	checkIsolationLevel("zone")
	checkRuleIsolationLevel("zone")

	// update successfully when only one default rule exists.
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "3")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkMaxReplicas(3)
	checkRuleCount(3)

	// We need to change isolation first because we will validate
	// if the location label contains the isolation level when setting location labels.
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "isolation-level", "host")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "location-labels", "host")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkLocationLabels(1)
	checkRuleLocationLabels(1)
	checkIsolationLevel("host")
	checkRuleIsolationLevel("host")

	// update unsuccessfully when many rule exists.
	fname := suite.T().TempDir()
	rules := []placement.Rule{
		{
			GroupID: placement.DefaultGroupID,
			ID:      "test1",
			Role:    "voter",
			Count:   1,
		},
	}
	b, err := json.Marshal(rules)
	re.NoError(err)
	os.WriteFile(fname, b, 0600)
	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	re.NoError(err)
	checkMaxReplicas(3)
	checkRuleCount(3)

	_, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "4")
	re.NoError(err)
	checkMaxReplicas(4)
	checkRuleCount(4)
	checkLocationLabels(1)
	checkRuleLocationLabels(1)
	checkIsolationLevel("host")
	checkRuleIsolationLevel("host")
}

func (suite *configTestSuite) TestMaxReplicaChanged() {
	suite.env.RunTest(suite.checkMaxReplicaChanged)
}

func (suite *configTestSuite) checkMaxReplicaChanged(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	pdTests.MustPutStore(re, cluster, store)

	// test set max-replicas with invalid value
	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "z")
	re.NoError(err)
	re.NotContains(string(output), "Success!")
	// test set max-replicas with less value
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "2")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	re.Contains(string(output), "which is less than the current replicas")
	// test set max-replicas with greater value
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "3")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	re.NotContains(string(output), "which is less than the current replicas")
	// test meet error when get config failed
	failpoint.Enable("github.com/tikv/pd/server/api/getReplicationConfigFailed", `return(200)`)
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "3")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	re.Contains(string(output), "Failed to unmarshal config when checking config")
	failpoint.Disable("github.com/tikv/pd/server/api/getReplicationConfigFailed")
	failpoint.Enable("github.com/tikv/pd/server/api/getReplicationConfigFailed", `return(500)`)
	output, err = tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "3")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	re.Contains(string(output), "Failed to get config when checking config")
	failpoint.Disable("github.com/tikv/pd/server/api/getReplicationConfigFailed")
}

func (suite *configTestSuite) TestPDServerConfig() {
	suite.env.RunTest(suite.checkPDServerConfig)
}

func (suite *configTestSuite) checkPDServerConfig(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	pdTests.MustPutStore(re, cluster, store)

	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "server")
	re.NoError(err)
	var conf config.PDServerConfig
	re.NoError(json.Unmarshal(output, &conf))

	re.True(conf.UseRegionStorage)
	re.Equal(24*time.Hour, conf.MaxResetTSGap.Duration)
	re.Equal("table", conf.KeyType)
	re.Equal(typeutil.StringSlice([]string{}), conf.RuntimeServices)
	re.Equal("", conf.MetricStorage)
	if conf.DashboardAddress != "auto" { // dashboard has been assigned
		re.Equal(leaderServer.GetAddr(), conf.DashboardAddress)
	}
	re.Equal(int(3), conf.FlowRoundByDigit)
}

func (suite *configTestSuite) TestMicroserviceConfig() {
	suite.env.RunTest(suite.checkMicroserviceConfig)
}

func (suite *configTestSuite) checkMicroserviceConfig(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	pdTests.MustPutStore(re, cluster, store)
	svr := leaderServer.GetServer()
	output, err := tests.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "all")
	re.NoError(err)
	cfg := config.Config{}
	re.NoError(json.Unmarshal(output, &cfg))
	re.True(svr.GetMicroserviceConfig().EnableSchedulingFallback)
	re.True(cfg.Microservice.EnableSchedulingFallback)
	// config set enable-scheduling-fallback <value>
	args := []string{"-u", pdAddr, "config", "set", "enable-scheduling-fallback", "false"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.False(svr.GetMicroserviceConfig().EnableSchedulingFallback)
}

func (suite *configTestSuite) TestRegionRules() {
	suite.env.RunTest(suite.checkRegionRules)
}

func (suite *configTestSuite) checkRegionRules(cluster *pdTests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := ctl.GetRootCmd()

	storeID, regionID := uint64(1), uint64(2)
	store := &metapb.Store{
		Id:    storeID,
		State: metapb.StoreState_Up,
	}
	pdTests.MustPutStore(re, cluster, store)
	pdTests.MustPutRegion(re, cluster, regionID, storeID, []byte{}, []byte{})

	args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--region=" + strconv.Itoa(int(regionID)), "--detail"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	fit := &placement.RegionFit{}
	re.NoError(json.Unmarshal(output, fit))
	re.Len(fit.RuleFits, 1)
	re.Equal(placement.DefaultGroupID, fit.RuleFits[0].Rule.GroupID)
	re.Equal(placement.DefaultRuleID, fit.RuleFits[0].Rule.ID)
}

func assertBundles(re *require.Assertions, a, b []placement.GroupBundle) {
	re.Len(b, len(a))
	for i := range a {
		assertBundle(re, a[i], b[i])
	}
}

func assertBundle(re *require.Assertions, a, b placement.GroupBundle) {
	re.Equal(a.ID, b.ID)
	re.Equal(a.Index, b.Index)
	re.Equal(a.Override, b.Override)
	re.Len(b.Rules, len(a.Rules))
	for i := range a.Rules {
		assertRule(re, a.Rules[i], b.Rules[i])
	}
}

func assertRule(re *require.Assertions, a, b *placement.Rule) {
	re.Equal(a.GroupID, b.GroupID)
	re.Equal(a.ID, b.ID)
	re.Equal(a.Index, b.Index)
	re.Equal(a.Override, b.Override)
	re.Equal(a.StartKey, b.StartKey)
	re.Equal(a.EndKey, b.EndKey)
	re.Equal(a.Role, b.Role)
	re.Equal(a.Count, b.Count)
	re.Equal(a.LabelConstraints, b.LabelConstraints)
	re.Equal(a.LocationLabels, b.LocationLabels)
	re.Equal(a.IsolationLevel, b.IsolationLevel)
}
