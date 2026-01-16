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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/ratelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
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

	input := map[string]any{
		"enable-rate-limit": "true",
		"limiter-config":    limitCfg,
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, err := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)

	oldLeaderName := leader.GetServer().Name()
	err = leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leader = cluster.GetLeaderServer()
	re.NotNil(leader)
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)
}

type configTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(configTestSuite))
}

func (suite *configTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *configTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *configTestSuite) TestConfigAll() {
	suite.env.RunTest(suite.checkConfigAll)
}

func (suite *configTestSuite) checkConfigAll(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := fmt.Sprintf("%s/pd/api/v1/config", urlPrefix)
	cfg := &config.Config{}
	testutil.Eventually(re, func() bool {
		err := testutil.ReadGetJSON(re, tests.TestDialClient, addr, cfg)
		re.NoError(err)
		return cfg.PDServerCfg.DashboardAddress != "auto"
	})

	// the original way
	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)
	l := map[string]any{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	l = map[string]any{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)
	cfg.Replication.MaxReplicas = 5
	cfg.Replication.LocationLabels = []string{"zone", "rack"}
	cfg.Schedule.RegionScheduleLimit = 10
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:9090"

	testutil.Eventually(re, func() bool {
		newCfg := &config.Config{}
		err = testutil.ReadGetJSON(re, tests.TestDialClient, addr, newCfg)
		re.NoError(err)
		return suite.Equal(newCfg, cfg)
	})
	// the new way
	l = map[string]any{
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
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)
	cfg.Schedule.EnableTiKVSplitRegion = false
	cfg.Schedule.TolerantSizeRatio = 2.5
	cfg.Replication.LocationLabels = []string{"idc", "host"}
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:1234"
	cfg.Log.Level = "warn"
	cfg.ReplicationMode.DRAutoSync.LabelKey = "foobar"
	cfg.ReplicationMode.ReplicationMode = "dr-auto-sync"
	v, err := versioninfo.ParseVersion("v4.0.0-beta")
	re.NoError(err)
	cfg.ClusterVersion = *v
	testutil.Eventually(re, func() bool {
		newCfg1 := &config.Config{}
		err = testutil.ReadGetJSON(re, tests.TestDialClient, addr, newCfg1)
		re.NoError(err)
		return suite.Equal(cfg, newCfg1)
	})

	// revert this to avoid it affects TestConfigTTL
	l["schedule.enable-tikv-split-region"] = "true"
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	// illegal prefix
	l = map[string]any{
		"replicate.max-replicas": 1,
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData,
		testutil.StatusNotOK(re),
		testutil.StringContain(re, "not found"))
	re.NoError(err)

	// update prefix directly
	l = map[string]any{
		"replication-mode": nil,
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData,
		testutil.StatusNotOK(re),
		testutil.StringContain(re, "cannot update config prefix"))
	re.NoError(err)

	// config item not found
	l = map[string]any{
		"schedule.region-limit": 10,
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusNotOK(re), testutil.StringContain(re, "not found"))
	re.NoError(err)
}

func (suite *configTestSuite) TestConfigSchedule() {
	suite.env.RunTest(suite.checkConfigSchedule)
}

func (suite *configTestSuite) checkConfigSchedule(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := fmt.Sprintf("%s/pd/api/v1/config/schedule", urlPrefix)

	scheduleConfig := &sc.ScheduleConfig{}
	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, addr, scheduleConfig))
	scheduleConfig.MaxStoreDownTime.Duration = time.Second
	postData, err := json.Marshal(scheduleConfig)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		scheduleConfig1 := &sc.ScheduleConfig{}
		re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, addr, scheduleConfig1))
		return reflect.DeepEqual(*scheduleConfig1, *scheduleConfig)
	})
}

func (suite *configTestSuite) TestConfigReplication() {
	suite.env.RunTest(suite.checkConfigReplication)
}

func (suite *configTestSuite) checkConfigReplication(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := fmt.Sprintf("%s/pd/api/v1/config/replicate", urlPrefix)
	rc := &sc.ReplicationConfig{}
	err := testutil.ReadGetJSON(re, tests.TestDialClient, addr, rc)
	re.NoError(err)

	rc.MaxReplicas = 5
	rc1 := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(rc1)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	rc.LocationLabels = []string{"zone", "rack"}
	rc2 := map[string]string{"location-labels": "zone,rack"}
	postData, err = json.Marshal(rc2)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	rc.IsolationLevel = "zone"
	rc3 := map[string]string{"isolation-level": "zone"}
	postData, err = json.Marshal(rc3)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	rc4 := &sc.ReplicationConfig{}
	testutil.Eventually(re, func() bool {
		err = testutil.ReadGetJSON(re, tests.TestDialClient, addr, rc4)
		re.NoError(err)
		return reflect.DeepEqual(*rc4, *rc)
	})
}

func (suite *configTestSuite) TestConfigLabelProperty() {
	suite.env.RunTest(suite.checkConfigLabelProperty)
}

func (suite *configTestSuite) checkConfigLabelProperty(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := urlPrefix + "/pd/api/v1/config/label-property"
	loadProperties := func() config.LabelPropertyConfig {
		var cfg config.LabelPropertyConfig
		err := testutil.ReadGetJSON(re, tests.TestDialClient, addr, &cfg)
		re.NoError(err)
		return cfg
	}

	cfg := loadProperties()
	re.Empty(cfg)

	cmds := []string{
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn2"}`,
		`{"type": "bar", "action": "set", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := testutil.CheckPostJSON(tests.TestDialClient, addr, []byte(cmd), testutil.StatusOK(re))
		re.NoError(err)
	}

	cfg = loadProperties()
	re.Len(cfg, 2)
	re.Equal([]config.StoreLabel{
		{Key: "zone", Value: "cn1"},
		{Key: "zone", Value: "cn2"},
	}, cfg["foo"])
	re.Equal([]config.StoreLabel{{Key: "host", Value: "h1"}}, cfg["bar"])

	cmds = []string{
		`{"type": "foo", "action": "delete", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "bar", "action": "delete", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := testutil.CheckPostJSON(tests.TestDialClient, addr, []byte(cmd), testutil.StatusOK(re))
		re.NoError(err)
	}

	cfg = loadProperties()
	re.Len(cfg, 1)
	re.Equal([]config.StoreLabel{{Key: "zone", Value: "cn2"}}, cfg["foo"])
}

func (suite *configTestSuite) TestConfigDefault() {
	suite.env.RunTest(suite.checkConfigDefault)
}

func (suite *configTestSuite) checkConfigDefault(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addr := urlPrefix + "/pd/api/v1/config"

	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)
	l := map[string]any{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	l = map[string]any{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)

	addr = fmt.Sprintf("%s/pd/api/v1/config/default", urlPrefix)
	defaultCfg := &config.Config{}
	err = testutil.ReadGetJSON(re, tests.TestDialClient, addr, defaultCfg)
	re.NoError(err)

	re.Equal(uint64(3), defaultCfg.Replication.MaxReplicas)
	re.Equal(typeutil.StringSlice([]string{}), defaultCfg.Replication.LocationLabels)
	re.Equal(uint64(2048), defaultCfg.Schedule.RegionScheduleLimit)
	re.Empty(defaultCfg.PDServerCfg.MetricStorage)
}

func (suite *configTestSuite) TestConfigPDServer() {
	suite.env.RunTest(suite.checkConfigPDServer)
}

func (suite *configTestSuite) checkConfigPDServer(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()

	addrPost := urlPrefix + "/pd/api/v1/config"
	ms := map[string]any{
		"metric-storage": "",
	}
	postData, err := json.Marshal(ms)
	re.NoError(err)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addrPost, postData, testutil.StatusOK(re)))
	addrGet := fmt.Sprintf("%s/pd/api/v1/config/pd-server", urlPrefix)
	sc := &config.PDServerConfig{}
	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, addrGet, sc))
	re.Equal(bool(true), sc.UseRegionStorage)
	re.Equal("table", sc.KeyType)
	re.Equal(typeutil.StringSlice([]string{}), sc.RuntimeServices)
	re.Empty(sc.MetricStorage)
	if sc.DashboardAddress != "auto" { // dashboard has been assigned
		re.Equal(leaderServer.GetAddr(), sc.DashboardAddress)
	}
	re.Equal(int(3), sc.FlowRoundByDigit)
	re.Equal(typeutil.NewDuration(time.Second), sc.MinResolvedTSPersistenceInterval)
	re.Equal(24*time.Hour, sc.MaxResetTSGap.Duration)
}

var ttlConfig = map[string]any{
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

var invalidTTLConfig = map[string]any{
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

func assertTTLConfig(
	re *require.Assertions,
	cluster *tests.TestCluster,
	expectedEqual bool,
) {
	equalFunc := func(options ttlConfigInterface) bool {
		return uint64(999) == options.GetMaxSnapshotCount() &&
			!options.IsLocationReplacementEnabled() &&
			uint64(999) == options.GetMaxMergeRegionSize() &&
			uint64(999) == options.GetMaxMergeRegionKeys() &&
			uint64(999) == options.GetSchedulerMaxWaitingOperator() &&
			uint64(999) == options.GetLeaderScheduleLimit() &&
			uint64(999) == options.GetRegionScheduleLimit() &&
			uint64(999) == options.GetHotRegionScheduleLimit() &&
			uint64(999) == options.GetReplicaScheduleLimit() &&
			uint64(999) == options.GetMergeScheduleLimit() &&
			!options.IsTikvRegionSplitEnabled()
	}
	re.Equal(expectedEqual, equalFunc(cluster.GetLeaderServer().GetServer().GetPersistOptions()))
	if cluster.GetSchedulingPrimaryServer() != nil {
		testutil.Eventually(re, func() bool {
			// wait for the scheduling primary server to be synced
			return expectedEqual == equalFunc(cluster.GetSchedulingPrimaryServer().GetPersistConfig())
		})
		equalFunc(cluster.GetSchedulingPrimaryServer().GetPersistConfig())
	}
}

func assertTTLConfigItemEqual(
	re *require.Assertions,
	cluster *tests.TestCluster,
	item string,
	expectedValue any,
) {
	checkFunc := func(options ttlConfigInterface) bool {
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
	re.True(checkFunc(cluster.GetLeaderServer().GetServer().GetPersistOptions()))
	if cluster.GetSchedulingPrimaryServer() != nil {
		// wait for the scheduling primary server to be synced
		testutil.Eventually(re, func() bool {
			return checkFunc(cluster.GetSchedulingPrimaryServer().GetPersistConfig())
		})
	}
}

func createTTLUrl(url string, ttl int) string {
	return fmt.Sprintf("%s/pd/api/v1/config?ttlSecond=%d", url, ttl)
}

func (suite *configTestSuite) TestConfigTTL() {
	suite.env.RunTest(suite.checkConfigTTL)
}

func (suite *configTestSuite) checkConfigTTL(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()
	postData, err := json.Marshal(ttlConfig)
	re.NoError(err)

	// test no config and cleaning up
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 0), postData, testutil.StatusOK(re))
	re.NoError(err)
	assertTTLConfig(re, cluster, false)

	// test time goes by
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 5), postData, testutil.StatusOK(re))
	re.NoError(err)
	assertTTLConfig(re, cluster, true)
	time.Sleep(5 * time.Second)
	assertTTLConfig(re, cluster, false)

	// test cleaning up
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 5), postData,
		testutil.StatusOK(re), testutil.StringEqual(re, "\"The ttl config is updated.\"\n"))
	re.NoError(err)
	assertTTLConfig(re, cluster, true)
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 0), postData,
		testutil.StatusOK(re), testutil.StatusOK(re), testutil.StringEqual(re, "\"The ttl config is deleted.\"\n"))
	re.NoError(err)
	assertTTLConfig(re, cluster, false)

	postData, err = json.Marshal(invalidTTLConfig)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 1), postData,
		testutil.StatusNotOK(re), testutil.StringEqual(re, "\"unsupported ttl config schedule.invalid-ttl-config\"\n"))
	re.NoError(err)

	// only set max-merge-region-size
	mergeConfig := map[string]any{
		"schedule.max-merge-region-size": 999,
	}
	postData, err = json.Marshal(mergeConfig)
	re.NoError(err)

	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 1), postData, testutil.StatusOK(re))
	re.NoError(err)
	assertTTLConfigItemEqual(re, cluster, "max-merge-region-size", uint64(999))
	// max-merge-region-keys should keep consistence with max-merge-region-size.
	assertTTLConfigItemEqual(re, cluster, "max-merge-region-keys", uint64(999*10000))

	// on invalid value, we use default config
	mergeConfig = map[string]any{
		"schedule.enable-tikv-split-region": "invalid",
	}
	postData, err = json.Marshal(mergeConfig)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 10), postData, testutil.StatusOK(re))
	re.NoError(err)
	assertTTLConfigItemEqual(re, cluster, "enable-tikv-split-region", true)
}

func (suite *configTestSuite) TestTTLConflict() {
	suite.env.RunTest(suite.checkTTLConflict)
}

func (suite *configTestSuite) checkTTLConflict(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	urlPrefix := leaderServer.GetAddr()
	addr := createTTLUrl(urlPrefix, 1)
	postData, err := json.Marshal(ttlConfig)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)
	assertTTLConfig(re, cluster, true)

	cfg := map[string]any{"max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	re.NoError(err)
	addr = fmt.Sprintf("%s/pd/api/v1/config", urlPrefix)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusNotOK(re), testutil.StringEqual(re, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	re.NoError(err)
	addr = fmt.Sprintf("%s/pd/api/v1/config/schedule", urlPrefix)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusNotOK(re), testutil.StringEqual(re, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	re.NoError(err)
	cfg = map[string]any{"schedule.max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, createTTLUrl(urlPrefix, 0), postData, testutil.StatusOK(re))
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re))
	re.NoError(err)
}
