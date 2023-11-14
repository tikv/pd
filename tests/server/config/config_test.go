// Copyright 2022 TiKV Project Authors.
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

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/ratelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// testDialClient used to dial http request.
var testDialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func TestRateLimitConfigReload(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NotNil(leader)
	re.Empty(leader.GetServer().GetServiceMiddlewareConfig().RateLimitConfig.LimiterConfig)
	limitCfg := make(map[string]ratelimit.DimensionConfig)
	limitCfg["GetRegions"] = ratelimit.DimensionConfig{QPS: 1}

	input := map[string]interface{}{
		"enable-rate-limit": "true",
		"limiter-config":    limitCfg,
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := testDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)

	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	var servers []*server.Server
	for _, s := range cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	server.MustWaitLeader(re, servers)
	leader = cluster.GetLeaderServer()
	re.NotNil(leader)
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)
}

type configTestSuite struct {
	suite.Suite
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(configTestSuite))
}

func (suite *configTestSuite) TestConfigAll() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfigAll)
}

func (suite *configTestSuite) checkConfigAll(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := fmt.Sprintf("%s/pd/api/v1/config", urlPrefix)
	cfg := &config.Config{}
	tu.Eventually(re, func() bool {
		err := tu.ReadGetJSON(re, testDialClient, addr, cfg)
		suite.NoError(err)
		return cfg.PDServerCfg.DashboardAddress != "auto"
	})

	// the original way
	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
	l := map[string]interface{}{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	l = map[string]interface{}{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	newCfg := &config.Config{}
	err = tu.ReadGetJSON(re, testDialClient, addr, newCfg)
	suite.NoError(err)
	cfg.Replication.MaxReplicas = 5
	cfg.Replication.LocationLabels = []string{"zone", "rack"}
	cfg.Schedule.RegionScheduleLimit = 10
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:9090"
	suite.Equal(newCfg, cfg)

	// the new way
	l = map[string]interface{}{
		"schedule.tolerant-size-ratio":            2.5,
		"schedule.enable-tikv-split-region":       "false",
		"replication.location-labels":             "idc,host",
		"pd-server.metric-storage":                "http://127.0.0.1:1234",
		"log.level":                               "warn",
		"cluster-version":                         "v4.0.0-beta",
		"replication-mode.replication-mode":       "dr-auto-sync",
		"replication-mode.dr-auto-sync.label-key": "foobar",
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
	newCfg1 := &config.Config{}
	err = tu.ReadGetJSON(re, testDialClient, addr, newCfg1)
	suite.NoError(err)
	cfg.Schedule.EnableTiKVSplitRegion = false
	cfg.Schedule.TolerantSizeRatio = 2.5
	cfg.Replication.LocationLabels = []string{"idc", "host"}
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:1234"
	cfg.Log.Level = "warn"
	cfg.ReplicationMode.DRAutoSync.LabelKey = "foobar"
	cfg.ReplicationMode.ReplicationMode = "dr-auto-sync"
	v, err := versioninfo.ParseVersion("v4.0.0-beta")
	suite.NoError(err)
	cfg.ClusterVersion = *v
	suite.Equal(cfg, newCfg1)

	// revert this to avoid it affects TestConfigTTL
	l["schedule.enable-tikv-split-region"] = "true"
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	// illegal prefix
	l = map[string]interface{}{
		"replicate.max-replicas": 1,
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData,
		tu.StatusNotOK(re),
		tu.StringContain(re, "not found"))
	suite.NoError(err)

	// update prefix directly
	l = map[string]interface{}{
		"replication-mode": nil,
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData,
		tu.StatusNotOK(re),
		tu.StringContain(re, "cannot update config prefix"))
	suite.NoError(err)

	// config item not found
	l = map[string]interface{}{
		"schedule.region-limit": 10,
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(re), tu.StringContain(re, "not found"))
	suite.NoError(err)
}

func (suite *configTestSuite) TestConfigSchedule() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfigSchedule)
}

func (suite *configTestSuite) checkConfigSchedule(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := fmt.Sprintf("%s/pd/api/v1/config/schedule", urlPrefix)

	scheduleConfig := &sc.ScheduleConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, scheduleConfig))
	scheduleConfig.MaxStoreDownTime.Duration = time.Second
	postData, err := json.Marshal(scheduleConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	scheduleConfig1 := &sc.ScheduleConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, scheduleConfig1))
	suite.Equal(*scheduleConfig1, *scheduleConfig)
}

func (suite *configTestSuite) TestConfigReplication() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfigReplication)
}

func (suite *configTestSuite) checkConfigReplication(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := fmt.Sprintf("%s/pd/api/v1/config/replicate", urlPrefix)
	rc := &sc.ReplicationConfig{}
	err := tu.ReadGetJSON(re, testDialClient, addr, rc)
	suite.NoError(err)

	rc.MaxReplicas = 5
	rc1 := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(rc1)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	rc.LocationLabels = []string{"zone", "rack"}
	rc2 := map[string]string{"location-labels": "zone,rack"}
	postData, err = json.Marshal(rc2)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	rc.IsolationLevel = "zone"
	rc3 := map[string]string{"isolation-level": "zone"}
	postData, err = json.Marshal(rc3)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	rc4 := &sc.ReplicationConfig{}
	err = tu.ReadGetJSON(re, testDialClient, addr, rc4)
	suite.NoError(err)

	suite.Equal(*rc4, *rc)
}

func (suite *configTestSuite) TestConfigLabelProperty() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfigLabelProperty)
}

func (suite *configTestSuite) checkConfigLabelProperty(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := urlPrefix + "/pd/api/v1/config/label-property"
	loadProperties := func() config.LabelPropertyConfig {
		var cfg config.LabelPropertyConfig
		err := tu.ReadGetJSON(re, testDialClient, addr, &cfg)
		suite.NoError(err)
		return cfg
	}

	cfg := loadProperties()
	suite.Empty(cfg)

	cmds := []string{
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn2"}`,
		`{"type": "bar", "action": "set", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := tu.CheckPostJSON(testDialClient, addr, []byte(cmd), tu.StatusOK(re))
		suite.NoError(err)
	}

	cfg = loadProperties()
	suite.Len(cfg, 2)
	suite.Equal([]config.StoreLabel{
		{Key: "zone", Value: "cn1"},
		{Key: "zone", Value: "cn2"},
	}, cfg["foo"])
	suite.Equal([]config.StoreLabel{{Key: "host", Value: "h1"}}, cfg["bar"])

	cmds = []string{
		`{"type": "foo", "action": "delete", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "bar", "action": "delete", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := tu.CheckPostJSON(testDialClient, addr, []byte(cmd), tu.StatusOK(re))
		suite.NoError(err)
	}

	cfg = loadProperties()
	suite.Len(cfg, 1)
	suite.Equal([]config.StoreLabel{{Key: "zone", Value: "cn2"}}, cfg["foo"])
}

func (suite *configTestSuite) TestConfigDefault() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfigDefault)
}

func (suite *configTestSuite) checkConfigDefault(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := urlPrefix + "/pd/api/v1/config"

	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
	l := map[string]interface{}{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	l = map[string]interface{}{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)

	addr = fmt.Sprintf("%s/pd/api/v1/config/default", urlPrefix)
	defaultCfg := &config.Config{}
	err = tu.ReadGetJSON(re, testDialClient, addr, defaultCfg)
	suite.NoError(err)

	suite.Equal(uint64(3), defaultCfg.Replication.MaxReplicas)
	suite.Equal(typeutil.StringSlice([]string{}), defaultCfg.Replication.LocationLabels)
	suite.Equal(uint64(2048), defaultCfg.Schedule.RegionScheduleLimit)
	suite.Equal("", defaultCfg.PDServerCfg.MetricStorage)
}

func (suite *configTestSuite) TestConfigPDServer() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkConfigPDServer)
}

func (suite *configTestSuite) checkConfigPDServer(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addrPost := urlPrefix + "/pd/api/v1/config"
	ms := map[string]interface{}{
		"metric-storage": "",
	}
	postData, err := json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addrPost, postData, tu.StatusOK(re)))
	addrGet := fmt.Sprintf("%s/pd/api/v1/config/pd-server", urlPrefix)
	sc := &config.PDServerConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addrGet, sc))
	suite.Equal(bool(true), sc.UseRegionStorage)
	suite.Equal("table", sc.KeyType)
	suite.Equal(typeutil.StringSlice([]string{}), sc.RuntimeServices)
	suite.Equal("", sc.MetricStorage)
	suite.Equal("auto", sc.DashboardAddress)
	suite.Equal(int(3), sc.FlowRoundByDigit)
	suite.Equal(typeutil.NewDuration(time.Second), sc.MinResolvedTSPersistenceInterval)
	suite.Equal(24*time.Hour, sc.MaxResetTSGap.Duration)
}

var ttlConfig = map[string]interface{}{
	"schedule.max-snapshot-count":             999,
	"schedule.enable-location-replacement":    false,
	"schedule.max-merge-region-size":          999,
	"schedule.max-merge-region-keys":          999,
	"schedule.scheduler-max-waiting-operator": 999,
	"schedule.leader-schedule-limit":          999,
	"schedule.region-schedule-limit":          999,
	"schedule.hot-region-schedule-limit":      999,
	"schedule.replica-schedule-limit":         999,
	"schedule.merge-schedule-limit":           999,
	"schedule.enable-tikv-split-region":       false,
}

var invalidTTLConfig = map[string]interface{}{
	"schedule.invalid-ttl-config": 0,
}

type ttlConfigInterface interface {
	GetMaxSnapshotCount() uint64
	IsLocationReplacementEnabled() bool
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetSchedulerMaxWaitingOperator() uint64
	GetLeaderScheduleLimit() uint64
	GetRegionScheduleLimit() uint64
	GetHotRegionScheduleLimit() uint64
	GetReplicaScheduleLimit() uint64
	GetMergeScheduleLimit() uint64
	IsTikvRegionSplitEnabled() bool
}

func (suite *configTestSuite) assertTTLConfig(
	cluster *tests.TestCluster,
	expectedEqual bool,
) {
	equality := suite.Equal
	if !expectedEqual {
		equality = suite.NotEqual
	}
	checkfunc := func(options ttlConfigInterface) {
		equality(uint64(999), options.GetMaxSnapshotCount())
		equality(false, options.IsLocationReplacementEnabled())
		equality(uint64(999), options.GetMaxMergeRegionSize())
		equality(uint64(999), options.GetMaxMergeRegionKeys())
		equality(uint64(999), options.GetSchedulerMaxWaitingOperator())
		equality(uint64(999), options.GetLeaderScheduleLimit())
		equality(uint64(999), options.GetRegionScheduleLimit())
		equality(uint64(999), options.GetHotRegionScheduleLimit())
		equality(uint64(999), options.GetReplicaScheduleLimit())
		equality(uint64(999), options.GetMergeScheduleLimit())
		equality(false, options.IsTikvRegionSplitEnabled())
	}
	checkfunc(cluster.GetLeaderServer().GetServer().GetPersistOptions())
	if cluster.GetSchedulingPrimaryServer() != nil {
		// wait for the scheduling primary server to be synced
		options := cluster.GetSchedulingPrimaryServer().GetPersistConfig()
		tu.Eventually(suite.Require(), func() bool {
			if expectedEqual {
				return uint64(999) == options.GetMaxSnapshotCount()
			}
			return uint64(999) != options.GetMaxSnapshotCount()
		})
		checkfunc(options)
	}
}

func (suite *configTestSuite) assertTTLConfigItemEqaul(
	cluster *tests.TestCluster,
	item string,
	expectedValue interface{},
) {
	checkfunc := func(options ttlConfigInterface) bool {
		switch item {
		case "max-merge-region-size":
			return expectedValue.(uint64) == options.GetMaxMergeRegionSize()
		case "max-merge-region-keys":
			return expectedValue.(uint64) == options.GetMaxMergeRegionKeys()
		case "enable-tikv-split-region":
			return expectedValue.(bool) == options.IsTikvRegionSplitEnabled()
		}
		return false
	}
	suite.True(checkfunc(cluster.GetLeaderServer().GetServer().GetPersistOptions()))
	if cluster.GetSchedulingPrimaryServer() != nil {
		// wait for the scheduling primary server to be synced
		tu.Eventually(suite.Require(), func() bool {
			return checkfunc(cluster.GetSchedulingPrimaryServer().GetPersistConfig())
		})
	}
}

func createTTLUrl(url string, ttl int) string {
	return fmt.Sprintf("%s/pd/api/v1/config?ttlSecond=%d", url, ttl)
}

func (suite *configTestSuite) TestConfigTTL() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	// FIXME: enable this test in two modes after ttl config is supported.
	env.RunTestInPDMode(suite.checkConfigTTL)
}

func (suite *configTestSuite) checkConfigTTL(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()
	postData, err := json.Marshal(ttlConfig)
	suite.NoError(err)

	// test no config and cleaning up
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 0), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfig(cluster, false)

	// test time goes by
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfig(cluster, true)
	time.Sleep(2 * time.Second)
	suite.assertTTLConfig(cluster, false)

	// test cleaning up
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfig(cluster, true)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 0), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfig(cluster, false)

	postData, err = json.Marshal(invalidTTLConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 1), postData,
		tu.StatusNotOK(re), tu.StringEqual(re, "\"unsupported ttl config schedule.invalid-ttl-config\"\n"))
	suite.NoError(err)

	// only set max-merge-region-size
	mergeConfig := map[string]interface{}{
		"schedule.max-merge-region-size": 999,
	}
	postData, err = json.Marshal(mergeConfig)
	suite.NoError(err)

	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfigItemEqaul(cluster, "max-merge-region-size", uint64(999))
	// max-merge-region-keys should keep consistence with max-merge-region-size.
	suite.assertTTLConfigItemEqaul(cluster, "max-merge-region-keys", uint64(999*10000))

	// on invalid value, we use default config
	mergeConfig = map[string]interface{}{
		"schedule.enable-tikv-split-region": "invalid",
	}
	postData, err = json.Marshal(mergeConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 10), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfigItemEqaul(cluster, "enable-tikv-split-region", true)
}

func (suite *configTestSuite) TestTTLConflict() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	// FIXME: enable this test in two modes after ttl config is supported.
	env.RunTestInPDMode(suite.checkTTLConflict)
}

func (suite *configTestSuite) checkTTLConflict(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()
	addr := createTTLUrl(urlPrefix, 1)
	postData, err := json.Marshal(ttlConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.assertTTLConfig(cluster, true)

	cfg := map[string]interface{}{"max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	suite.NoError(err)
	addr = fmt.Sprintf("%s/pd/api/v1/config", urlPrefix)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(re), tu.StringEqual(re, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	suite.NoError(err)
	addr = fmt.Sprintf("%s/pd/api/v1/config/schedule", urlPrefix)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(re), tu.StringEqual(re, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	suite.NoError(err)
	cfg = map[string]interface{}{"schedule.max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(urlPrefix, 0), postData, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
}
