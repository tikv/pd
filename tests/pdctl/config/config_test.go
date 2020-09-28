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
// See the License for the specific language governing permissions and
// limitations under the License.

package config_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
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
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	cfg := config.Config{}
	c.Assert(json.Unmarshal(output, &cfg), IsNil)
	scheduleConfig := svr.GetScheduleConfig()
	scheduleConfig.Schedulers = nil
	scheduleConfig.SchedulersPayload = nil
	scheduleConfig.StoreLimit = nil
	c.Assert(&cfg.Schedule, DeepEquals, scheduleConfig)
	c.Assert(&cfg.Replication, DeepEquals, svr.GetReplicationConfig())

	// config set trace-region-flow <value>
	args = []string{"-u", pdAddr, "config", "set", "trace-region-flow", "false"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(svr.GetPDServerConfig().TraceRegionFlow, Equals, false)

	// config show schedule
	args = []string{"-u", pdAddr, "config", "show", "schedule"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	scheduleCfg := config.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(&scheduleCfg, DeepEquals, svr.GetScheduleConfig())

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	replicationCfg := config.ReplicationConfig{}
	c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
	c.Assert(&replicationCfg, DeepEquals, svr.GetReplicationConfig())

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion := semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(clusterVersion, Not(DeepEquals), svr.GetClusterVersion())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion = semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg := config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = config.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config delete label-property <type> <key> <value>
	args3 := []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args3...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
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
		_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
		c.Assert(err, IsNil)
		// read
		args2 = []string{"-u", pdAddr, "config", "show"}
		_, output, err = pdctl.ExecuteCommandC(cmd, args2...)
		c.Assert(err, IsNil)
		cfg = config.Config{}
		c.Assert(json.Unmarshal(output, &cfg), IsNil)
		// judge
		item.judge(c, &cfg.Schedule, svr.GetScheduleConfig())
	}

	// test error or deprecated config name
	args1 = []string{"-u", pdAddr, "config", "set", "foo-bar", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "not found"), IsTrue)
	args1 = []string{"-u", pdAddr, "config", "set", "disable-remove-down-replica", "true"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "already been deprecated"), IsTrue)

	// set enable-placement-rules twice, make sure it does not return error.
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "enable-placement-rules", "true"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args1...)
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
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	_, output, err := pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// test show
	var rules []placement.Rule
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "show")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &rules)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	c.Assert(rules[0].Key(), Equals, [2]string{"pd", "default"})

	f, _ := ioutil.TempFile("/tmp", "pd_tests")
	fname := f.Name()
	f.Close()

	// test load
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ := ioutil.ReadFile(fname)
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
	ioutil.WriteFile(fname, b, 0644)
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	c.Assert(err, IsNil)

	// test show group
	var rules2 []placement.Rule
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "show", "--group=pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &rules2)
	c.Assert(err, IsNil)
	c.Assert(rules2, HasLen, 2)
	c.Assert(rules2[0].Key(), Equals, [2]string{"pd", "default"})
	c.Assert(rules2[1].Key(), Equals, [2]string{"pd", "test1"})

	// test delete
	rules[0].Count = 0
	b, _ = json.Marshal(rules)
	ioutil.WriteFile(fname, b, 0644)
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "save", "--in="+fname)
	c.Assert(err, IsNil)
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "show", "--group=pd")
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
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	_, output, err := pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// test show
	var group placement.RuleGroup
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &group)
	c.Assert(err, IsNil)
	c.Assert(group, DeepEquals, placement.RuleGroup{ID: "pd"})

	// test set
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "pd", "42", "true")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "set", "group2", "100", "false")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// show all
	var groups []placement.RuleGroup
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &groups)
	c.Assert(err, IsNil)
	c.Assert(groups, DeepEquals, []placement.RuleGroup{
		{ID: "pd", Index: 42, Override: true},
		{ID: "group2", Index: 100, Override: false},
	})

	// delete
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "delete", "group2")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	// show again
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-group", "show", "group2")
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
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	_, output, err := pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "enable")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success!"), IsTrue)

	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "set", "max-replicas", "1")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "please update rule instead"), IsTrue)

	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "set", "location-labels", "dc,rack")
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "please update rule instead"), IsTrue)

	// test get
	var bundle placement.GroupBundle
	_, output, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "get", "pd")
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, &bundle)
	c.Assert(err, IsNil)
	c.Assert(bundle, DeepEquals, placement.GroupBundle{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}})

	f, err := ioutil.TempFile("/tmp", "pd_tests")
	c.Assert(err, IsNil)
	fname := f.Name()
	f.Close()
	defer func() {
		os.RemoveAll(fname)
	}()

	// test load
	var bundles []placement.GroupBundle
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ := ioutil.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, HasLen, 1)
	c.Assert(bundles[0], DeepEquals, placement.GroupBundle{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}})

	// test set
	bundle.ID = "pe"
	bundle.Rules[0].GroupID = "pe"
	b, err = json.Marshal(bundle)
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(fname, b, 0644), IsNil)
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	c.Assert(err, IsNil)

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ = ioutil.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, DeepEquals, []placement.GroupBundle{
		{ID: "pd", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pd", ID: "default", Role: "voter", Count: 3}}},
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	})

	// test delete
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "pd")
	c.Assert(err, IsNil)

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ = ioutil.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, DeepEquals, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	})

	// test delete regexp
	bundle.ID = "pf"
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}
	b, err = json.Marshal(bundle)
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(fname, b, 0644), IsNil)
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "set", "--in="+fname)
	c.Assert(err, IsNil)

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "delete", "--regexp", ".*f")
	c.Assert(err, IsNil)

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, _ = ioutil.ReadFile(fname)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, DeepEquals, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
	})

	// test save
	bundle.Rules = []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}
	bundles = append(bundles, bundle)
	b, err = json.Marshal(bundles)
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(fname, b, 0644), IsNil)
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname)
	c.Assert(err, IsNil)

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, err = ioutil.ReadFile(fname)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, DeepEquals, []placement.GroupBundle{
		{ID: "pe", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pe", ID: "default", Role: "voter", Count: 3}}},
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}},
	})

	// partial update, so still one group is left, no error
	bundles = []placement.GroupBundle{{ID: "pe", Rules: []*placement.Rule{}}}
	b, err = json.Marshal(bundles)
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(fname, b, 0644), IsNil)
	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "save", "--in="+fname, "--partial")
	c.Assert(err, IsNil)

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "placement-rules", "rule-bundle", "load", "--out="+fname)
	c.Assert(err, IsNil)
	b, err = ioutil.ReadFile(fname)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(b, &bundles), IsNil)
	c.Assert(bundles, DeepEquals, []placement.GroupBundle{
		{ID: "pf", Index: 0, Override: false, Rules: []*placement.Rule{{GroupID: "pf", ID: "default", Role: "voter", Count: 3}}},
	})
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
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
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
		_, output, err := pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "show", "replication-mode")
		c.Assert(err, IsNil)
		var conf2 config.ReplicationModeConfig
		json.Unmarshal([]byte(output), &conf2)
		c.Assert(conf2, DeepEquals, conf)
	}

	check()

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync")
	c.Assert(err, IsNil)
	conf.ReplicationMode = "dr-auto-sync"
	check()

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "label-key", "foobar")
	c.Assert(err, IsNil)
	conf.DRAutoSync.LabelKey = "foobar"
	check()

	_, _, err = pdctl.ExecuteCommandC(cmd, "-u", pdAddr, "config", "set", "replication-mode", "dr-auto-sync", "primary-replicas", "5")
	c.Assert(err, IsNil)
	conf.DRAutoSync.PrimaryReplicas = 5
	check()
}
