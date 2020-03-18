// Copyright 2019 PingCAP, Inc.
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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&configTestSuite{})

type configTestSuite struct{}

func (s *configTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
	server.ConfigCheckInterval = 10 * time.Millisecond
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
	c.Assert(&cfg.Schedule, DeepEquals, scheduleConfig)
	c.Assert(&cfg.Replication, DeepEquals, svr.GetReplicationConfig())

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
	time.Sleep(20 * time.Millisecond)
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
	time.Sleep(20 * time.Millisecond)
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
	time.Sleep(20 * time.Millisecond)
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
	c.Assert(strings.Contains(string(output), "config item not found"), IsTrue)
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
