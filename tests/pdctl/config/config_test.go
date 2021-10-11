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
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&configTestSuite{})

type configTestSuite struct{}

func (s *configTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

type testItem struct {
	name  string
	value interface{}
	read  func(scheduleConfig *config.ScheduleConfig) interface{}
}

func (t *testItem) judge(c *C, scheduleConfigs ...*config.ScheduleConfig) {
	value := t.value
	for _, scheduleConfig := range scheduleConfigs {
		c.Assert(scheduleConfig, NotNil)
		c.Assert(reflect.TypeOf(t.read(scheduleConfig)), Equals, reflect.TypeOf(value))
	}
}

func (s *configTestSuite) TestConfig(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	cfg := config.Config{}
	c.Assert(json.Unmarshal(output, &cfg), IsNil)
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
	scheduleConfig.StoreLimitMode = ""

	c.Assert(&cfg.Schedule, DeepEquals, scheduleConfig)
	c.Assert(&cfg.Replication, DeepEquals, svr.GetReplicationConfig())

	// config set trace-region-flow <value>
	args = []string{"-u", pdAddr, "config", "set", "trace-region-flow", "false"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(svr.GetPDServerConfig().TraceRegionFlow, IsFalse)

	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", "10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(svr.GetPDServerConfig().FlowRoundByDigit, Equals, 10)

	args = []string{"-u", pdAddr, "config", "set", "flow-round-by-digit", "-10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, NotNil)

	// config show schedule
	args = []string{"-u", pdAddr, "config", "show", "schedule"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	scheduleCfg := config.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(&scheduleCfg, DeepEquals, svr.GetScheduleConfig())

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	replicationCfg := config.ReplicationConfig{}
	c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
	c.Assert(&replicationCfg, DeepEquals, svr.GetReplicationConfig())

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion := semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, err = pdctl.ExecuteCommand(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(clusterVersion, Not(DeepEquals), svr.GetClusterVersion())
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion = semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg := config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, err = pdctl.ExecuteCommand(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config delete label-property <type> <key> <value>
	args3 := []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, err = pdctl.ExecuteCommand(cmd, args3...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// test config read and write
	testItems := []testItem{
		{"leader-schedule-limit", uint64(64), func(scheduleConfig *config.ScheduleConfig) interface{} {
			return scheduleConfig.LeaderScheduleLimit
		}}, {"hot-region-schedule-limit", uint64(64), func(scheduleConfig *config.ScheduleConfig) interface{} {
			return scheduleConfig.HotRegionScheduleLimit
		}}, {"hot-region-cache-hits-threshold", uint64(5), func(scheduleConfig *config.ScheduleConfig) interface{} {
			return scheduleConfig.HotRegionCacheHitsThreshold
		}}, {"enable-remove-down-replica", false, func(scheduleConfig *config.ScheduleConfig) interface{} {
			return scheduleConfig.EnableRemoveDownReplica
		}},
		{"enable-debug-metrics", true, func(scheduleConfig *config.ScheduleConfig) interface{} {
			return scheduleConfig.EnableDebugMetrics
		}},
		// set again
		{"enable-debug-metrics", true, func(scheduleConfig *config.ScheduleConfig) interface{} {
			return scheduleConfig.EnableDebugMetrics
		}},
	}
	for _, item := range testItems {
		// write
		args1 = []string{"-u", pdAddr, "config", "set", item.name, reflect.TypeOf(item.value).String()}
		_, err = pdctl.ExecuteCommand(cmd, args1...)
		c.Assert(err, IsNil)
		// read
		args2 = []string{"-u", pdAddr, "config", "show"}
		output, err = pdctl.ExecuteCommand(cmd, args2...)
		c.Assert(err, IsNil)
		cfg = config.Config{}
		c.Assert(json.Unmarshal(output, &cfg), IsNil)
		// judge
		item.judge(c, &cfg.Schedule, svr.GetScheduleConfig())
	}

	// test error or deprecated config name
	args1 = []string{"-u", pdAddr, "config", "set", "foo-bar", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "not found"), IsTrue)
	args1 = []string{"-u", pdAddr, "config", "set", "disable-remove-down-replica", "true"}
	output, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "already been deprecated"), IsTrue)

	// set enable-placement-rules twice, make sure it does not return error.
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, err = pdctl.ExecuteCommand(cmd, args1...)
	c.Assert(err, IsNil)
}

func (s *configTestSuite) TestPlacementRules(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// test show
	var rules []placement.Rule
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "show")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &rules)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	c.Assert(rules[0].Key(), Equals, [2]string{"pd", "default"})

	f, _ := os.CreateTemp("/tmp", "pd_tests")
	fname := f.Name()
	f.Close()

	// test load
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ := os.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &rules), IsNil)
	c.Assert(rules, HasLen, 1)
	c.Assert(rules[0].Key(), Equals, [2]string{"pd", "default"})

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
	os.WriteFile(fname, b, 0644)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	c.Assert(err, IsNil)

	// test show group
	var rules2 []placement.Rule
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "show", "--group=pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &rules2)
	c.Assert(err, IsNil)
	c.Assert(rules2, HasLen, 2)
	c.Assert(rules2[0].Key(), Equals, [2]string{"pd", "default"})
	c.Assert(rules2[1].Key(), Equals, [2]string{"pd", "test1"})

	// test delete
	rules[0].Count = 0
	b, _ = json.Marshal(rules)
	os.WriteFile(fname, b, 0644)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	c.Assert(err, IsNil)
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "show", "--group=pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &rules)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	c.Assert(rules[0].Key(), Equals, [2]string{"pd", "test1"})
}

func (s *configTestSuite) TestPlacementRuleGroups(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// test show
	var group placement.RuleGroup
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &group)
	c.Assert(err, IsNil)
	c.Assert(group, DeepEquals, placement.RuleGroup{ID: "pd"})

	// test set
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "pd", "42", "true")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "group2", "100", "false")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// show all
	var groups []placement.RuleGroup
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &groups)
	c.Assert(err, IsNil)
	c.Assert(groups, DeepEquals, []placement.RuleGroup{
		{ID: "pd", Index: 42, Override: true},
		{ID: "group2", Index: 100, Override: false},
	})

	// delete
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "delete", "group2")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// show again
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "group2")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "404"), IsTrue)
}

func (s *configTestSuite) TestPlacementRuleBundle(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// test get
	var bundle placement.GroupBundle
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "get", "pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &bundle)
	c.Assert(err, IsNil)
	c.Assert(bundle, DeepEquals, placement.GroupBundle{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}})

	f, err := os.CreateTemp("/tmp", "pd_tests")
	c.Assert(err, IsNil)
	fname := f.Name()
	f.Close()
	defer func() {
		os.RemoveAll(fname)
	}()

	// test load
	var bundles []placement.GroupBundle
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ := os.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, HasLen, 1)
	c.Assert(bundles[0], DeepEquals, placement.GroupBundle{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}})

	// test set
	bundle.ID = "pe"
	bundle.Rules[0].GroupID = "pe"
	b, err = json.Marshal(bundle)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(fname, b, 0644), IsNil)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	c.Assert(err, IsNil)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ = os.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	assertBundles(bundles, []placement.GroupBundle{
		{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}},
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	}, c)

	// test delete
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "pd")
	c.Assert(err, IsNil)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ = os.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	assertBundles(bundles, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	}, c)

	// test delete regexp
	bundle.ID = "pf"
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}
	b, err = json.Marshal(bundle)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(fname, b, 0644), IsNil)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	c.Assert(err, IsNil)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "--regexp", ".*f")
	c.Assert(err, IsNil)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ = os.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	assertBundles(bundles, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	}, c)

	// test save
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}
	bundles = append(bundles, bundle)
	b, err = json.Marshal(bundles)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(fname, b, 0644), IsNil)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname)
	c.Assert(err, IsNil)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, err = os.ReadFile(fname)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	assertBundles(bundles, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}},
	}, c)

	// partial update, so still one group is left, no error
	bundles = []placement.GroupBundle{{ID: "pe", Rules: []*placement.Rule{}}}
	b, err = json.Marshal(bundles)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(fname, b, 0644), IsNil)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname, "--partial")
	c.Assert(err, IsNil)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, err = os.ReadFile(fname)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	assertBundles(bundles, []placement.GroupBundle{
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}},
	}, c)
}

func (s *configTestSuite) TestReplicationMode(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	conf := config.ReplicationModeConfig{
		ReplicationMode: "majority",
		DRAutoSync: config.DRAutoSyncReplicationConfig{
			WaitStoreTimeout: typeutil.NewDuration(time.Minute),
			WaitSyncTimeout:  typeutil.NewDuration(time.Minute),
			WaitAsyncTimeout: typeutil.NewDuration(2 * time.Minute),
		},
	}
	check := func() {
		output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "show", "replication-mode")
		c.Assert(err, IsNil)
		var conf2 config.ReplicationModeConfig
		json.Unmarshal(output, &conf2)
		c.Assert(conf2, DeepEquals, conf)
	}

	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync")
	c.Assert(err, IsNil)
	conf.ReplicationMode = "dr-auto-sync"
	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "label-key", "foobar")
	c.Assert(err, IsNil)
	conf.DRAutoSync.LabelKey = "foobar"
	check()

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "primary-replicas", "5")
	c.Assert(err, IsNil)
	conf.DRAutoSync.PrimaryReplicas = 5
	check()
}

func (s *configTestSuite) TestUpdateDefaultReplicaConfig(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	checkMaxReplicas := func(expect uint64) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		c.Assert(err, IsNil)
		replicationCfg := config.ReplicationConfig{}
		c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
		c.Assert(replicationCfg.MaxReplicas, Equals, expect)
	}

	checkLocaltionLabels := func(expect int) {
		args := []string{"-u", pdAddr, "config", "show", "replication"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		c.Assert(err, IsNil)
		replicationCfg := config.ReplicationConfig{}
		c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
		c.Assert(replicationCfg.LocationLabels, HasLen, expect)
	}

	checkRuleCount := func(expect int) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", "pd", "--id", "default"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		c.Assert(err, IsNil)
		rule := placement.Rule{}
		c.Assert(json.Unmarshal(output, &rule), IsNil)
		c.Assert(rule.Count, Equals, expect)
	}

	checkRuleLocationLabels := func(expect int) {
		args := []string{"-u", pdAddr, "config", "placement-rules", "show", "--group", "pd", "--id", "default"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		c.Assert(err, IsNil)
		rule := placement.Rule{}
		c.Assert(json.Unmarshal(output, &rule), IsNil)
		c.Assert(rule.LocationLabels, HasLen, expect)
	}

	// update successfully when placement rules is not enabled.
	output, err := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "2")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)
	checkMaxReplicas(2)
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "location-labels", "zone,host")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)
	checkLocaltionLabels(2)
	checkRuleLocationLabels(2)

	// update successfully when only one default rule exists.
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "3")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)
	checkMaxReplicas(3)
	checkRuleCount(3)

	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "location-labels", "host")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)
	checkLocaltionLabels(1)
	checkRuleLocationLabels(1)

	// update unsuccessfully when many rule exists.
	f, _ := os.CreateTemp("/tmp", "pd_tests")
	fname := f.Name()
	f.Close()
	defer func() {
		os.RemoveAll(fname)
	}()

	rules := []placement.Rule{
		{
			GroupID: "pd",
			ID:      "test1",
			Role:    "voter",
			Count:   1,
		},
	}
	b, err := json.Marshal(rules)
	c.Assert(err, IsNil)
	os.WriteFile(fname, b, 0644)
	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	c.Assert(err, IsNil)
	checkMaxReplicas(3)
	checkRuleCount(3)

	_, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "config", "set", "max-replicas", "4")
	c.Assert(err, IsNil)
	checkMaxReplicas(4)
	checkRuleCount(4)
	checkLocaltionLabels(1)
	checkRuleLocationLabels(1)
}

func assertBundles(a, b []placement.GroupBundle, c *C) {
	c.Assert(len(a), Equals, len(b))
	for i := 0; i < len(a); i++ {
		assertBundle(a[i], b[i], c)
	}
}

func assertBundle(a, b placement.GroupBundle, c *C) {
	c.Assert(a.ID, Equals, b.ID)
	c.Assert(a.Index, Equals, b.Index)
	c.Assert(a.Override, Equals, b.Override)
	c.Assert(len(a.Rules), Equals, len(b.Rules))
	for i := 0; i < len(a.Rules); i++ {
		assertRule(a.Rules[i], b.Rules[i], c)
	}
}

func assertRule(a, b *placement.Rule, c *C) {
	c.Assert(a.GroupID, Equals, b.GroupID)
	c.Assert(a.ID, Equals, b.ID)
	c.Assert(a.Index, Equals, b.Index)
	c.Assert(a.Override, Equals, b.Override)
	c.Assert(bytes.Equal(a.StartKey, b.StartKey), IsTrue)
	c.Assert(bytes.Equal(a.EndKey, b.EndKey), IsTrue)
	c.Assert(a.Role, Equals, b.Role)
	c.Assert(a.Count, Equals, b.Count)
	c.Assert(a.LabelConstraints, DeepEquals, b.LabelConstraints)
	c.Assert(a.LocationLabels, DeepEquals, b.LocationLabels)
	c.Assert(a.IsolationLevel, Equals, b.IsolationLevel)
}
