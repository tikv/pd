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
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

type testCase struct {
	name  string
	value interface{}
	read  func(scheduleConfig *sc.ScheduleConfig) interface{}
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
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(configTestSuite))
}

func (suite *configTestSuite) TestConfig() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfig)
}

func (suite *configTestSuite) checkConfig(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	svr := leaderServer.GetServer()
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	cfg := config.Config{}
	re.NoError(json.Unmarshal(output, &cfg))
	scheduleConfig := svr.GetScheduleConfig()

	// hidden config
	scheduleConfig.Schedulers = nil
	scheduleConfig.SchedulersPayload = nil
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
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.False(svr.GetPDServerConfig().TraceRegionFlow)

	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", "10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(10, svr.GetPDServerConfig().FlowRoundByDigit)

	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", "-10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.Error(err)

	// config show schedule
	args = []string{"-u", pdAddr, "config", "show", "schedule"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	scheduleCfg := sc.ScheduleConfig{}
	re.NoError(json.Unmarshal(output, &scheduleCfg))
	scheduleConfig = svr.GetScheduleConfig()
	scheduleConfig.MaxMergeRegionKeys = scheduleConfig.GetMaxMergeRegionKeys()
	re.Equal(scheduleConfig, &scheduleCfg)

	re.Equal(20, int(svr.GetScheduleConfig().MaxMergeRegionSize))
	re.Equal(0, int(svr.GetScheduleConfig().MaxMergeRegionKeys))
	re.Equal(20*10000, int(svr.GetScheduleConfig().GetMaxMergeRegionKeys()))

	// set max-merge-region-size to 40MB
	args = []string{"-u", pdAddr, "config", "set", "max-merge-region-size", "40"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(40, int(svr.GetScheduleConfig().MaxMergeRegionSize))
	re.Equal(0, int(svr.GetScheduleConfig().MaxMergeRegionKeys))
	re.Equal(40*10000, int(svr.GetScheduleConfig().GetMaxMergeRegionKeys()))
	args = []string{"-u", pdAddr, "config", "set", "max-merge-region-keys", "200000"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(20*10000, int(svr.GetScheduleConfig().MaxMergeRegionKeys))
	re.Equal(20*10000, int(svr.GetScheduleConfig().GetMaxMergeRegionKeys()))

	// set store limit v2
	args = []string{"-u", pdAddr, "config", "set", "store-limit-version", "v2"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal("v2", svr.GetScheduleConfig().StoreLimitVersion)
	args = []string{"-u", pdAddr, "config", "set", "store-limit-version", "v1"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal("v1", svr.GetScheduleConfig().StoreLimitVersion)

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	replicationCfg := sc.ReplicationConfig{}
	re.NoError(json.Unmarshal(output, &replicationCfg))
	re.Equal(svr.GetReplicationConfig(), &replicationCfg)

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	clusterVersion := semver.Version{}
	re.NoError(json.Unmarshal(output, &clusterVersion))
	re.Equal(svr.GetClusterVersion(), clusterVersion)

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, err = pdctl.ExecuteCommand(cmd, args2...)
	re.NoError(err)
	re.NotEqual(svr.GetClusterVersion(), clusterVersion)
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	clusterVersion = semver.Version{}
	re.NoError(json.Unmarshal(output, &clusterVersion))
	re.Equal(svr.GetClusterVersion(), clusterVersion)

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	labelPropertyCfg := config.LabelPropertyConfig{}
	re.NoError(json.Unmarshal(output, &labelPropertyCfg))
	re.Equal(svr.GetLabelProperty(), labelPropertyCfg)

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, err = pdctl.ExecuteCommand(cmd, args2...)
	re.NoError(err)
	re.NotEqual(svr.GetLabelProperty(), labelPropertyCfg)
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	labelPropertyCfg = config.LabelPropertyConfig{}
	re.NoError(json.Unmarshal(output, &labelPropertyCfg))
	re.Equal(svr.GetLabelProperty(), labelPropertyCfg)

	// config delete label-property <type> <key> <value>
	args3 := []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, err = pdctl.ExecuteCommand(cmd, args3...)
	re.NoError(err)
	re.NotEqual(svr.GetLabelProperty(), labelPropertyCfg)
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	labelPropertyCfg = config.LabelPropertyConfig{}
	re.NoError(json.Unmarshal(output, &labelPropertyCfg))
	re.Equal(svr.GetLabelProperty(), labelPropertyCfg)

	// config set min-resolved-ts-persistence-interval <value>
	args = []string{"-u", pdAddr, "config", "set", "min-resolved-ts-persistence-interval", "1s"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(typeutil.NewDuration(time.Second), svr.GetPDServerConfig().MinResolvedTSPersistenceInterval)

	// config set max-store-preparing-time 10m
	args = []string{"-u", pdAddr, "config", "set", "max-store-preparing-time", "10m"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(typeutil.NewDuration(10*time.Minute), svr.GetScheduleConfig().MaxStorePreparingTime)

	args = []string{"-u", pdAddr, "config", "set", "max-store-preparing-time", "0s"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(typeutil.NewDuration(0), svr.GetScheduleConfig().MaxStorePreparingTime)

	// test config read and write
	testCases := []testCase{
		{"leader-schedule-limit", uint64(64), func(scheduleConfig *sc.ScheduleConfig) interface{} {
			return scheduleConfig.LeaderScheduleLimit
		}}, {"hot-region-schedule-limit", uint64(64), func(scheduleConfig *sc.ScheduleConfig) interface{} {
			return scheduleConfig.HotRegionScheduleLimit
		}}, {"hot-region-cache-hits-threshold", uint64(5), func(scheduleConfig *sc.ScheduleConfig) interface{} {
			return scheduleConfig.HotRegionCacheHitsThreshold
		}}, {"enable-remove-down-replica", false, func(scheduleConfig *sc.ScheduleConfig) interface{} {
			return scheduleConfig.EnableRemoveDownReplica
		}},
		{"enable-debug-metrics", true, func(scheduleConfig *sc.ScheduleConfig) interface{} {
			return scheduleConfig.EnableDebugMetrics
		}},
		// set again
		{"enable-debug-metrics", true, func(scheduleConfig *sc.ScheduleConfig) interface{} {
			return scheduleConfig.EnableDebugMetrics
		}},
	}
	for _, testCase := range testCases {
		// write
		args1 = []string{"-u", pdAddr, "config", "set", testCase.name, reflect.TypeOf(testCase.value).String()}
		_, err = pdctl.ExecuteCommand(cmd, args1...)
		re.NoError(err)
		// read
		args2 = []string{"-u", pdAddr, "config", "show"}
		output, err = pdctl.ExecuteCommand(cmd, args2...)
		re.NoError(err)
		cfg = config.Config{}
		re.NoError(json.Unmarshal(output, &cfg))
		// judge
		testCase.judge(re, &cfg.Schedule, svr.GetScheduleConfig())
	}

	// test error or deprecated config name
	args1 = []string{"-u", pdAddr, "config", "set", "foo-bar", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	re.Contains(string(output), "not found")
	args1 = []string{"-u", pdAddr, "config", "set", "disable-remove-down-replica", "true"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	re.Contains(string(output), "already been deprecated")

	// set enable-placement-rules twice, make sure it does not return error.
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, err = pdctl.ExecuteCommand(cmd, args1...)
	re.NoError(err)

	// test invalid value
	argsInvalid := []string{"-u", pdAddr, "config", "set", "leader-schedule-policy", "aaa"}
	output, err = pdctl.ExecuteCommand(cmd, argsInvalid...)
	re.NoError(err)
	re.Contains(string(output), "is invalid")
	argsInvalid = []string{"-u", pdAddr, "config", "set", "key-type", "aaa"}
	output, err = pdctl.ExecuteCommand(cmd, argsInvalid...)
	re.NoError(err)
	re.Contains(string(output), "is invalid")
}

func (suite *configTestSuite) TestPlacementRules() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkPlacementRules)
}

func (suite *configTestSuite) checkPlacementRules(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// test show
	var rules []placement.Rule
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "show")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &rules))
	re.Len(rules, 1)
	re.Equal([2]string{"pd", "default"}, rules[0].Key())

	f, _ := os.CreateTemp("/tmp", "pd_tests")
	fname := f.Name()
	f.Close()
	defer os.RemoveAll(fname)

	// test load
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "load", "--out="+fname)
	re.NoError(err)
	b, _ := os.ReadFile(fname)
	re.NoError(json.Unmarshal(b, &rules))
	re.Len(rules, 1)
	re.Equal([2]string{"pd", "default"}, rules[0].Key())

	// test save
	rules = append(rules, placement.Rule{
		GroupID: "pd",
		ID:      "test1",
		Role:    "voter",
		Count:   1,
	}, placement.Rule{
		GroupID: "test-group",
		ID:      "test2",
		Role:    "voter",
		Count:   2,
	})
	b, _ = json.Marshal(rules)
	os.WriteFile(fname, b, 0600)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	re.NoError(err)

	// test show group
	var rules2 []placement.Rule
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "show", "--group=pd")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &rules2))
	re.Len(rules2, 2)
	re.Equal([2]string{"pd", "default"}, rules2[0].Key())
	re.Equal([2]string{"pd", "test1"}, rules2[1].Key())

	// test rule region detail
	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"))
	fit := &placement.RegionFit{}
	// need clear up args, so create new a cobra.Command. Otherwise gourp still exists.
	cmd2 := pdctlCmd.GetRootCmd()
	output, err = pdctl.ExecuteCommand(cmd2, "-u", pdAddr, "config", "placement-rules", "show", "--region=1", "--detail")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, fit))
	re.Len(fit.RuleFits, 3)
	re.Equal([2]string{"pd", "default"}, fit.RuleFits[0].Rule.Key())

	// test delete
	rules[0].Count = 0
	b, _ = json.Marshal(rules)
	os.WriteFile(fname, b, 0600)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	re.NoError(err)
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "show", "--group=pd")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &rules))
	re.Len(rules, 1)
	re.Equal([2]string{"pd", "test1"}, rules[0].Key())
}

func (suite *configTestSuite) TestPlacementRuleGroups() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkPlacementRuleGroups)
}

func (suite *configTestSuite) checkPlacementRuleGroups(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// test show
	var group placement.RuleGroup
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "pd")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &group))
	re.Equal(placement.RuleGroup{ID: "pd"}, group)

	// test set
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "pd", "42", "true")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "group2", "100", "false")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "group3", "200", "false")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// show all
	var groups []placement.RuleGroup
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &groups))
	re.Equal([]placement.RuleGroup{
		{ID: "pd", Index: 42, Override: true},
		{ID: "group2", Index: 100, Override: false},
		{ID: "group3", Index: 200, Override: false},
	}, groups)

	// delete
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "delete", "group2")
	re.NoError(err)
	re.Contains(string(output), "Delete group and rules successfully.")

	// show again
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "group2")
	re.NoError(err)
	re.Contains(string(output), "404")

	// delete using regex
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "delete", "--regexp", ".*3")
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "group3")
	re.NoError(err)
	re.Contains(string(output), "404")
}

func (suite *configTestSuite) TestPlacementRuleBundle() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkPlacementRuleBundle)
}

func (suite *configTestSuite) checkPlacementRuleBundle(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	// test get
	var bundle placement.GroupBundle
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "get", "pd")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &bundle))
	re.Equal(placement.GroupBundle{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}}, bundle)

	f, err := os.CreateTemp("/tmp", "pd_tests")
	re.NoError(err)
	fname := f.Name()
	f.Close()
	defer os.RemoveAll(fname)

	// test load
	var bundles []placement.GroupBundle
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	re.NoError(err)
	b, _ := os.ReadFile(fname)
	re.NoError(json.Unmarshal(b, &bundles))
	re.Len(bundles, 1)
	re.Equal(placement.GroupBundle{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}}, bundles[0])

	// test set
	bundle.ID = "pe"
	bundle.Rules[0].GroupID = "pe"
	b, err = json.Marshal(bundle)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	re.NoError(err)
	b, _ = os.ReadFile(fname)
	re.NoError(json.Unmarshal(b, &bundles))
	assertBundles(re, bundles, []placement.GroupBundle{
		{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}},
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	})

	// test delete
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "pd")
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	re.NoError(err)
	b, _ = os.ReadFile(fname)
	re.NoError(json.Unmarshal(b, &bundles))
	assertBundles(re, bundles, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	})

	// test delete regexp
	bundle.ID = "pf"
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}
	b, err = json.Marshal(bundle)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "--regexp", ".*f")
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	re.NoError(err)
	b, _ = os.ReadFile(fname)
	re.NoError(json.Unmarshal(b, &bundles))
	assertBundles(re, bundles, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	})

	// test save
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}
	bundles = append(bundles, bundle)
	b, err = json.Marshal(bundles)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname)
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	re.NoError(err)
	b, err = os.ReadFile(fname)
	re.NoError(err)
	re.NoError(json.Unmarshal(b, &bundles))
	assertBundles(re, bundles, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}},
	})

	// partial update, so still one group is left, no error
	bundles = []placement.GroupBundle{{ID: "pe", Rules: []*placement.Rule{}}}
	b, err = json.Marshal(bundles)
	re.NoError(err)
	re.NoError(os.WriteFile(fname, b, 0600))
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname, "--partial")
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	re.NoError(err)
	b, err = os.ReadFile(fname)
	re.NoError(err)
	re.NoError(json.Unmarshal(b, &bundles))
	assertBundles(re, bundles, []placement.GroupBundle{
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}},
	})
}

func TestReplicationMode(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	conf := config.ReplicationModeConfig{
		ReplicationMode: "majority",
		DRAutoSync: config.DRAutoSyncReplicationConfig{
			WaitStoreTimeout: typeutil.NewDuration(time.Minute),
		},
	}
	check := func() {
		output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "replication-mode")
		re.NoError(err)
		var conf2 config.ReplicationModeConfig
		re.NoError(json.Unmarshal(output, &conf2))
		re.Equal(conf, conf2)
	}

	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync")
	re.NoError(err)
	conf.ReplicationMode = "dr-auto-sync"
	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "label-key", "foobar")
	re.NoError(err)
	conf.DRAutoSync.LabelKey = "foobar"
	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "primary-replicas", "5")
	re.NoError(err)
	conf.DRAutoSync.PrimaryReplicas = 5
	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "wait-store-timeout", "10m")
	re.NoError(err)
	conf.DRAutoSync.WaitStoreTimeout = typeutil.NewDuration(time.Minute * 10)
	check()
}

func (suite *configTestSuite) TestUpdateDefaultReplicaConfig() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkUpdateDefaultReplicaConfig)
}

func (suite *configTestSuite) checkUpdateDefaultReplicaConfig(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	checkMaxReplicas := func(expect uint64) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		replicationCfg := sc.ReplicationConfig{}
		re.NoError(json.Unmarshal(output, &replicationCfg))
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			return replicationCfg.MaxReplicas == expect
		})
	}

	checkLocationLabels := func(expect int) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		replicationCfg := sc.ReplicationConfig{}
		re.NoError(json.Unmarshal(output, &replicationCfg))
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			return len(replicationCfg.LocationLabels) == expect
		})
	}

	checkIsolationLevel := func(expect string) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		replicationCfg := sc.ReplicationConfig{}
		re.NoError(json.Unmarshal(output, &replicationCfg))
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			return replicationCfg.IsolationLevel == expect
		})
	}

	checkRuleCount := func(expect int) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", "pd", "--id", "default"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		rule := placement.Rule{}
		re.NoError(json.Unmarshal(output, &rule))
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			return rule.Count == expect
		})
	}

	checkRuleLocationLabels := func(expect int) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", "pd", "--id", "default"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		rule := placement.Rule{}
		re.NoError(json.Unmarshal(output, &rule))
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			return len(rule.LocationLabels) == expect
		})
	}

	checkRuleIsolationLevel := func(expect string) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", "pd", "--id", "default"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		rule := placement.Rule{}
		re.NoError(json.Unmarshal(output, &rule))
		testutil.Eventually(re, func() bool { // wait for the config to be synced to the scheduling server
			return rule.IsolationLevel == expect
		})
	}

	// update successfully when placement rules is not enabled.
	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "2")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkMaxReplicas(2)
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "location-labels", "zone,host")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "isolation-level", "zone")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkLocationLabels(2)
	checkRuleLocationLabels(2)
	checkIsolationLevel("zone")
	checkRuleIsolationLevel("zone")

	// update successfully when only one default rule exists.
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "3")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	checkMaxReplicas(3)
	checkRuleCount(3)

	// We need to change isolation first because we will validate
	// if the location label contains the isolation level when setting location labels.
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "isolation-level", "host")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "location-labels", "host")
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
			GroupID: "pd",
			ID:      "test1",
			Role:    "voter",
			Count:   1,
		},
	}
	b, err := json.Marshal(rules)
	re.NoError(err)
	os.WriteFile(fname, b, 0600)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	re.NoError(err)
	checkMaxReplicas(3)
	checkRuleCount(3)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "4")
	re.NoError(err)
	checkMaxReplicas(4)
	checkRuleCount(4)
	checkLocationLabels(1)
	checkRuleLocationLabels(1)
	checkIsolationLevel("host")
	checkRuleIsolationLevel("host")
}

func (suite *configTestSuite) TestPDServerConfig() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkPDServerConfig)
}

func (suite *configTestSuite) checkPDServerConfig(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	pdAddr := leaderServer.GetAddr()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	tests.MustPutStore(re, cluster, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "server")
	re.NoError(err)
	var conf config.PDServerConfig
	re.NoError(json.Unmarshal(output, &conf))

	re.True(conf.UseRegionStorage)
	re.Equal(24*time.Hour, conf.MaxResetTSGap.Duration)
	re.Equal("table", conf.KeyType)
	re.Equal(typeutil.StringSlice([]string{}), conf.RuntimeServices)
	re.Equal("", conf.MetricStorage)
	re.Equal("auto", conf.DashboardAddress)
	re.Equal(int(3), conf.FlowRoundByDigit)
}

func assertBundles(re *require.Assertions, a, b []placement.GroupBundle) {
	re.Len(b, len(a))
	for i := 0; i < len(a); i++ {
		assertBundle(re, a[i], b[i])
	}
}

func assertBundle(re *require.Assertions, a, b placement.GroupBundle) {
	re.Equal(a.ID, b.ID)
	re.Equal(a.Index, b.Index)
	re.Equal(a.Override, b.Override)
	re.Len(b.Rules, len(a.Rules))
	for i := 0; i < len(a.Rules); i++ {
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
