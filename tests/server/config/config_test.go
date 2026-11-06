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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/log"

	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	cfg "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/ratelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestRateLimitConfigReload(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
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

func TestGCConcurrencyConfigUpdateAndReload(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	cluster, err := tests.NewTestCluster(ctx, 3, func(cfg *config.Config, _ string) {
		cfg.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	configURL := leader.GetAddr() + "/pd/api/v1/config"
	getConfig := func() *config.PDServerConfig {
		result := &config.PDServerConfig{}
		re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, configURL+"/pd-server", result))
		return result
	}
	re.Equal(4, getConfig().GCKeyspaceIndexLoadConcurrency)
	re.Equal(4, getConfig().GCStateCacheWarmupConcurrency)
	data := []byte(`{"gc-keyspace-index-load-concurrency":16,"pd-server.gc-state-cache-warmup-concurrency":16}`)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, configURL, data, testutil.StatusOK(re)))
	re.Equal(16, getConfig().GCKeyspaceIndexLoadConcurrency)
	re.Equal(16, getConfig().GCStateCacheWarmupConcurrency)
	data = []byte(`{"gc-keyspace-index-load-concurrency":1,"pd-server.gc-state-cache-warmup-concurrency":2}`)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, configURL, data, testutil.StatusOK(re)))
	for _, name := range []string{"gc-keyspace-index-load-concurrency", "gc-state-cache-warmup-concurrency"} {
		for _, value := range []int{0, -1, 17, 128, 1 << 30} {
			data, err := json.Marshal(map[string]int{name: value})
			re.NoError(err)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, configURL, data,
				testutil.StatusNotOK(re), testutil.StringContain(re, name)))
			re.Equal(1, getConfig().GCKeyspaceIndexLoadConcurrency)
			re.Equal(2, getConfig().GCStateCacheWarmupConcurrency)
		}
	}
	// Leadership reload must retain successful updates and exclude rejected ones.
	core, logs := observer.New(zap.InfoLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	oldLeader := leader.GetServer().Name()
	re.NoError(leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeader, ""))
	re.NotEmpty(cluster.WaitLeader())
	leader = cluster.GetLeaderServer()
	re.NotEqual(oldLeader, leader.GetServer().Name())
	configURL = leader.GetAddr() + "/pd/api/v1/config"
	re.Equal(1, getConfig().GCKeyspaceIndexLoadConcurrency)
	re.Equal(2, getConfig().GCStateCacheWarmupConcurrency)
	if kerneltype.IsNextGen() {
		// These executions must consume the reloaded options through the server's
		// manager wiring, even though its original startup configuration was 4.
		testutil.Eventually(re, func() bool {
			return logs.FilterMessage("start loading GC keyspace index").FilterField(zap.Int("concurrency", 1)).Len() > 0 &&
				logs.FilterMessage("start warming GC state cache").FilterField(zap.Int("concurrency", 2)).Len() > 0
		})
	}
}

func TestMetricStorageValidationAtQueryTime(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NotNil(leader)

	configURL := leader.GetAddr() + "/pd/api/v1/config"
	queryURL := leader.GetAddr() + "/pd/api/v1/metric/query"
	for _, metricStorage := range []string{
		"file:///tmp/metrics",
		"http://127.0.0.1:9090",
		"http://user:pass@metrics.example",
		"not a URL",
	} {
		postData, err := json.Marshal(map[string]any{"metric-storage": metricStorage})
		re.NoError(err)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, configURL, postData, testutil.StatusOK(re)))
		re.Equal(metricStorage, leader.GetServer().GetConfig().PDServerCfg.MetricStorage)

		queryResp, err := tests.TestDialClient.Get(queryURL)
		re.NoError(err)
		queryBody, err := io.ReadAll(queryResp.Body)
		re.NoError(err)
		re.NoError(queryResp.Body.Close())
		re.Equal(http.StatusBadGateway, queryResp.StatusCode)
		re.JSONEq(`{"status":"error","errorType":"proxy","error":"metric query failed"}`, string(queryBody))
	}
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

func (suite *configTestSuite) TestConfigBatchValidation() {
	suite.env.RunTest(suite.checkConfigBatchValidation)
}

func (suite *configTestSuite) checkConfigBatchValidation(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	svr := leader.GetServer()
	addr := leader.GetAddr() + "/pd/api/v1/config"
	seed := fmt.Appendf(nil, `{"schedule.merge-schedule-limit":%d}`, svr.GetScheduleConfig().MergeScheduleLimit)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, seed, testutil.StatusOK(re)))
	before := svr.GetConfig()
	ruleManager := svr.GetRaftCluster().GetRuleManager()
	rulesBefore := ruleManager.GetAllRules()
	var persistedBefore map[string]any
	exists, err := svr.GetStorage().LoadConfig(&persistedBefore)
	re.NoError(err)
	re.True(exists)

	for _, testCase := range []struct {
		key   string
		value any
		err   string
	}{
		{"schedule.schedulers", []map[string]string{{"type": "balance-leader"}}, "config item schedulers not found"},
		{"unknown-item", true, "not found"},
		{"replication.unknown-item", 1, "not found"},
		{"replication.location-labels", 123, "invalid syntax"},
		{"max-replicas", "invalid", "cannot unmarshal"},
		{"replication.max-replicas", 0, "invalid count 0"},
		{"replication.isolation-level", "unknown-label", "isolation-level must be one of location-labels"},
		{"schedule.high-space-ratio", 1, "low-space-ratio should be larger"},
		{"pd-server.gc-state-cache-warmup-concurrency", 0, "gc-state-cache-warmup-concurrency must be between"},
		{"pd-server.dashboard-address", "http://127.0.0.1:1", "is not the client url of any member"},
		{"replication-mode.replication-mode", "invalid", "invalid replication mode"},
		{"keyspace.check-region-split-interval", "0s", "check-region-split-interval should between"},
		{"keyspace.meta-service-groups", map[string]string{"": "http://127.0.0.1:2379"}, "group ID cannot be empty"},
		{"micro-service.enable-resource-manager-fallback", strconv.FormatBool(!before.Microservice.EnableResourceManagerFallback), "cannot update config item"},
		{"log.level", "invalid", "log level invalid is illegal"},
		{"cluster-version", 1, "input value 1 is illegal"},
		{"controller.enable-controller-trace-log", true, "cannot unmarshal bool"},
	} {
		attempts := 1
		if testCase.key == "schedule.schedulers" {
			attempts = 8 // Exercise map iteration order for the original reproducer.
		}
		for range attempts {
			patch := map[string]any{
				"replication.max-replicas":      before.Replication.MaxReplicas + 1,
				"pd-server.metric-storage":      "http://127.0.0.1:9999",
				"schedule.merge-schedule-limit": before.Schedule.MergeScheduleLimit + 1,
				"log.level":                     "debug",
			}
			patch[testCase.key] = testCase.value
			data, err := json.Marshal(patch)
			re.NoError(err)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, data,
				testutil.Status(re, http.StatusBadRequest), testutil.StringContain(re, testCase.err)))
			re.Equal(before, svr.GetConfig(), "rejected item: %s", testCase.key)
			re.Equal(rulesBefore, ruleManager.GetAllRules(), "rejected item: %s", testCase.key)
			var persistedAfter map[string]any
			exists, err := svr.GetStorage().LoadConfig(&persistedAfter)
			re.NoError(err)
			re.True(exists)
			re.Equal(persistedBefore, persistedAfter, "rejected item: %s", testCase.key)
		}
	}

	ttlData := []byte(`{"schedule.max-merge-region-size":100}`)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr+"?ttlSecond=60", ttlData, testutil.StatusOK(re)))
	defer func() {
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr+"?ttlSecond=0", ttlData, testutil.StatusOK(re)))
	}()
	data, err := json.Marshal(map[string]any{
		"replication.max-replicas":       before.Replication.MaxReplicas + 1,
		"schedule.max-merge-region-size": before.Schedule.MaxMergeRegionSize + 1,
	})
	re.NoError(err)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, data,
		testutil.Status(re, http.StatusBadRequest), testutil.StringContain(re, "need to clean up TTL first")))
	re.Equal(before, svr.GetConfig())
	re.Equal(rulesBefore, ruleManager.GetAllRules())
	var persistedAfter map[string]any
	exists, err = svr.GetStorage().LoadConfig(&persistedAfter)
	re.NoError(err)
	re.True(exists)
	re.Equal(persistedBefore, persistedAfter)
}

func (suite *configTestSuite) TestConfigBatchSchedulerArgs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		svr := cluster.GetLeaderServer().GetServer()
		before := svr.GetScheduleConfig()
		defer func() { re.NoError(svr.SetScheduleConfig(*before)) }()
		seed := before.Clone()
		seed.Schedulers = sc.SchedulerConfigs{{Type: "evict-leader", Args: []string{"1"}}}
		re.NoError(svr.SetScheduleConfig(*seed))
		var persistedBefore map[string]any
		exists, err := svr.GetStorage().LoadConfig(&persistedBefore)
		re.NoError(err)
		re.True(exists)
		for _, invalid := range []map[string]any{
			{"schedule.high-space-ratio": 1},
			{"controller.ru-version-policy": map[string]int{"default": 0}},
		} {
			invalid["schedule.schedulers-v2"] = sc.SchedulerConfigs{{Type: "evict-leader", Args: []string{"2"}}}
			data, err := json.Marshal(invalid)
			re.NoError(err)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, cluster.GetLeaderServer().GetAddr()+"/pd/api/v1/config", data,
				testutil.Status(re, http.StatusBadRequest)))
			// Compare with an independent value, not another potentially aliased config.
			re.Equal([]string{"1"}, svr.GetScheduleConfig().Schedulers[0].Args)
			var persistedAfter map[string]any
			exists, err := svr.GetStorage().LoadConfig(&persistedAfter)
			re.NoError(err)
			re.True(exists)
			re.Equal(persistedBefore, persistedAfter)
		}
	})
}

func (suite *configTestSuite) TestConfigBatchObjectAliases() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		svr := leader.GetServer()
		addr := leader.GetAddr() + "/pd/api/v1/config"
		before := svr.GetConfig().Keyspace
		re.Empty(before.MetaServiceGroups)
		defer func() {
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, []byte(`{"keyspace.meta-service-groups":null}`), testutil.StatusOK(re)))
		}()
		for _, invalid := range []string{
			`{"keyspace.meta-service-groups":123,"meta-service-groups":{"alias-b":"http://127.0.0.1:2379"}}`,
			`{"keyspace.meta-service-groups":{"alias-a":"http://127.0.0.1:2379"},"meta-service-groups":123}`,
			`{"schedule.merge-schedule-limit":1,"merge-schedule-limit":2}`,
		} {
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, []byte(invalid), testutil.Status(re, http.StatusBadRequest)))
			re.Equal(before, svr.GetConfig().Keyspace)
		}
		data := fmt.Appendf(nil, `{"keyspace.meta-service-groups":{"alias-a":%q},"meta-service-groups":{"alias-b":%q}}`, leader.GetAddr(), leader.GetAddr())
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, data, testutil.StatusOK(re)))
		groups := svr.GetConfig().Keyspace.MetaServiceGroups
		re.Equal(leader.GetAddr(), groups["alias-a"])
		re.Equal(leader.GetAddr(), groups["alias-b"])
		var persisted config.Config
		exists, err := svr.GetStorage().LoadConfig(&persisted)
		re.NoError(err)
		re.True(exists)
		re.Equal(groups, persisted.Keyspace.MetaServiceGroups)

		controllerURL := leader.GetAddr() + "/resource-manager/api/v1/config/controller"
		var controller rm.ControllerConfig
		re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, controllerURL, &controller))
		defer func() {
			data, err := json.Marshal(map[string]any{"controller.ru-version-policy": controller.RUVersionPolicy})
			re.NoError(err)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, data, testutil.StatusOK(re)))
		}()
		data = []byte(`{"controller.ru-version-policy":{"default":2},"ru-version-policy":{"overrides":{"42":1}}}`)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, data, testutil.StatusOK(re)))
		var updated rm.ControllerConfig
		re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, controllerURL, &updated))
		re.Equal(rm.RUVersion(2), updated.RUVersionPolicy.Default)
		re.Equal(rm.RUVersion(1), updated.RUVersionPolicy.Overrides[42])
	})
}

func TestConfigBatchEnablePlacementRules(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		witness     bool
		storedCount int
	}{
		{name: "new-rules"},
		{name: "witness-rules", witness: true},
		{name: "stored-default", storedCount: 3},
		{name: "custom-default", storedCount: 5},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			cluster, err := tests.NewTestCluster(t.Context(), 1, func(cfg *config.Config, _ string) {
				cfg.Keyspace.WaitRegionSplit = false
				cfg.Replication.EnablePlacementRules = false
				cfg.Schedule.EnableWitness = testCase.witness
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			re.NotEmpty(cluster.WaitLeader())
			leader := cluster.GetLeaderServer()
			re.NoError(leader.BootstrapCluster())
			svr := leader.GetServer()
			manager := svr.GetRaftCluster().GetRuleManager()
			re.False(manager.IsInitialized())
			storage := svr.GetStorage()
			if testCase.storedCount != 0 {
				rule := &placement.Rule{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: testCase.storedCount}
				re.NoError(storage.RunInTxn(t.Context(), func(txn kv.Txn) error { return storage.SaveRule(txn, rule.StoreKey(), rule) }))
			}
			re.NoError(svr.SetScheduleConfig(*svr.GetScheduleConfig()))
			before := svr.GetConfig()
			var persistedBefore map[string]any
			exists, err := storage.LoadConfig(&persistedBefore)
			re.NoError(err)
			re.True(exists)
			rulesBefore := make(map[string]string)
			re.NoError(storage.LoadRules(func(k, v string) { rulesBefore[k] = v }))
			post := func(count int, status int) {
				data := fmt.Appendf(nil, `{"pd-server.metric-storage":"http://127.0.0.1:9999","replication.enable-placement-rules":"true","replication.max-replicas":%d,"location-labels":"zone","replication.isolation-level":"zone"}`, count)
				re.NoError(testutil.CheckPostJSON(tests.TestDialClient, leader.GetAddr()+"/pd/api/v1/config", data, testutil.Status(re, status)))
			}
			assertUnchanged := func() {
				re.False(manager.IsInitialized())
				re.Equal(before, svr.GetConfig())
				var persistedAfter map[string]any
				exists, err := storage.LoadConfig(&persistedAfter)
				re.NoError(err)
				re.True(exists)
				re.Equal(persistedBefore, persistedAfter)
				rulesAfter := make(map[string]string)
				re.NoError(storage.LoadRules(func(k, v string) { rulesAfter[k] = v }))
				re.Equal(rulesBefore, rulesAfter)
			}
			post(0, http.StatusBadRequest)
			assertUnchanged()
			post(4, http.StatusOK)
			after := svr.GetConfig()
			re.True(after.Replication.EnablePlacementRules)
			re.Equal(uint64(4), after.Replication.MaxReplicas)
			re.Equal("http://127.0.0.1:9999", after.PDServerCfg.MetricStorage)
			count := 0
			for _, rule := range manager.GetAllRules() {
				count += rule.Count
				if testCase.storedCount == 0 {
					re.Equal([]string{"zone"}, rule.LocationLabels)
					re.Equal("zone", rule.IsolationLevel)
				}
			}
			if testCase.storedCount == 0 {
				re.Equal(4, count)
			} else {
				// Enabling preserves existing rules, including custom replica counts.
				re.Equal(testCase.storedCount, count)
				rulesAfter := make(map[string]string)
				re.NoError(storage.LoadRules(func(k, v string) { rulesAfter[k] = v }))
				re.Equal(rulesBefore, rulesAfter)
			}
			var persisted config.Config
			exists, err = storage.LoadConfig(&persisted)
			re.NoError(err)
			re.True(exists)
			re.Equal(after.Replication, persisted.Replication)
			if testCase.name == "new-rules" {
				data := []byte(`{"replication.enable-placement-rules":"false"}`)
				re.NoError(testutil.CheckPostJSON(tests.TestDialClient, leader.GetAddr()+"/pd/api/v1/config", data, testutil.StatusOK(re)))
				post(5, http.StatusOK)
				re.Equal(uint64(5), svr.GetReplicationConfig().MaxReplicas)
				re.Equal(4, manager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID).Count)
			}
		})
	}
}

func (suite *configTestSuite) TestConfigBatchRelatedFields() {
	suite.env.RunTest(suite.checkConfigBatchRelatedFields)
}

func (suite *configTestSuite) checkConfigBatchRelatedFields(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	svr := leader.GetServer()
	addr := leader.GetAddr() + "/pd/api/v1/config"
	before := svr.GetConfig()
	post := func(patch map[string]any) {
		data, err := json.Marshal(patch)
		re.NoError(err)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, data, testutil.StatusOK(re)))
	}
	defer func() {
		post(map[string]any{
			"replication.location-labels":          before.Replication.LocationLabels,
			"replication.isolation-level":          before.Replication.IsolationLevel,
			"schedule.high-space-ratio":            before.Schedule.HighSpaceRatio,
			"schedule.low-space-ratio":             before.Schedule.LowSpaceRatio,
			"schedule.merge-schedule-limit":        before.Schedule.MergeScheduleLimit,
			"keyspace.wait-region-split-timeout":   before.Keyspace.WaitRegionSplitTimeout.String(),
			"keyspace.check-region-split-interval": before.Keyspace.CheckRegionSplitInterval.String(),
		})
	}()
	// Alternate complete, valid combinations. Applying isolation-level or
	// the interval individually can fail against the previous combination.
	for _, label := range []string{"batch-zone", "batch-rack"} {
		patch := map[string]any{
			"location-labels":                      label,
			"replication.isolation-level":          label,
			"high-space-ratio":                     0.9,
			"schedule.low-space-ratio":             0.95,
			"merge-schedule-limit":                 0,
			"keyspace.wait-region-split-timeout":   "100ms",
			"keyspace.check-region-split-interval": "50ms",
		}
		if label == "batch-rack" {
			delete(patch, "location-labels")
			patch["replication.LOCATION-LABELS"] = label
		}
		post(patch)
		after := svr.GetConfig()
		re.Equal([]string{label}, []string(after.Replication.LocationLabels))
		re.Equal(label, after.Replication.IsolationLevel)
		re.Equal(0.9, after.Schedule.HighSpaceRatio)
		re.Equal(0.95, after.Schedule.LowSpaceRatio)
		re.Zero(after.Schedule.MergeScheduleLimit)
		re.Equal(100*time.Millisecond, after.Keyspace.WaitRegionSplitTimeout.Duration)
		re.Equal(50*time.Millisecond, after.Keyspace.CheckRegionSplitInterval.Duration)
		var persisted config.Config
		exists, err := svr.GetStorage().LoadConfig(&persisted)
		re.NoError(err)
		re.True(exists)
		re.Equal(after.Replication, persisted.Replication)
		re.Equal(after.Keyspace, persisted.Keyspace)
	}
}

func (suite *configTestSuite) TestKeyspaceConfigUpdate() {
	suite.env.RunTest(suite.checkKeyspaceConfigUpdate)
}

func (suite *configTestSuite) checkKeyspaceConfigUpdate(cluster *tests.TestCluster) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	addr := fmt.Sprintf("%s/pd/api/v1/config", leaderServer.GetAddr())

	postData, err := json.Marshal(map[string]any{
		"keyspace.wait-region-split": false,
	})
	re.NoError(err)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re)))

	testutil.Eventually(re, func() bool {
		return !leaderServer.GetServer().GetPersistOptions().GetKeyspaceConfig().ToWaitRegionSplit()
	})

	postData, err = json.Marshal(map[string]any{
		"keyspace.wait-region-split": false,
	})
	re.NoError(err)
	re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re)))
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

	invalidDefaultStoreLimit := map[string]any{
		"default-store-limit": map[string]any{
			"add-peer":    -1,
			"remove-peer": scheduleConfig.DefaultStoreLimit.RemovePeer,
		},
	}
	postData, err = json.Marshal(invalidDefaultStoreLimit)
	re.NoError(err)
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, postData,
		testutil.StatusNotOK(re),
		testutil.StringContain(re, "default-store-limit.add-peer should be finite and non-negative"))
	re.NoError(err)
	re.Equal(scheduleConfig.DefaultStoreLimit, leaderServer.GetPersistOptions().GetScheduleConfig().DefaultStoreLimit)

	for _, endpoint := range []string{"config", "config/schedule"} {
		addr := fmt.Sprintf("%s/pd/api/v1/%s", urlPrefix, endpoint)
		postData = []byte(`{"store-limit":{"1":{"add-peer":0,"remove-peer":0,"transfer-leader-in":0},"2":{"add-peer":10,"remove-peer":20,"transfer-leader-in":30}}}`)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, postData, testutil.StatusOK(re)))
		current := leaderServer.GetPersistOptions().GetScheduleConfig().Clone()
		re.Equal(sc.StoreLimitConfig{}, current.StoreLimit[1])
		re.Equal(sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 30}, current.StoreLimit[2])
		re.Equal(scheduleConfig.DefaultStoreLimit, current.DefaultStoreLimit)
		re.Equal(scheduleConfig.MaxStoreDownTime, current.MaxStoreDownTime)
		persisted := &config.Config{}
		exists, err := leaderServer.GetServer().GetStorage().LoadConfig(persisted)
		re.NoError(err)
		re.True(exists)
		re.Equal(current.StoreLimit, persisted.Schedule.StoreLimit)

		for _, field := range []string{"add-peer", "remove-peer", "transfer-leader-in"} {
			postData, err = json.Marshal(map[string]any{
				"store-limit": map[string]any{"1": map[string]float64{field: -1}},
			})
			re.NoError(err)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, postData,
				testutil.StatusNotOK(re),
				testutil.StringContain(re, "store-limit[1]."+field+" should be finite and non-negative")))
			re.Equal(current, leaderServer.GetPersistOptions().GetScheduleConfig())
			after := &config.Config{}
			exists, err = leaderServer.GetServer().GetStorage().LoadConfig(after)
			re.NoError(err)
			re.True(exists)
			re.Equal(persisted.Schedule, after.Schedule)
		}
	}
}

func (suite *configTestSuite) TestStoreLimitPartialUpdates() {
	suite.env.RunTest(suite.checkStoreLimitPartialUpdates)
}

func (suite *configTestSuite) checkStoreLimitPartialUpdates(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	for _, endpoint := range []string{"config", "config/schedule"} {
		addr := leader.GetAddr() + "/pd/api/v1/" + endpoint
		for _, testCase := range []struct {
			patch    string
			expected sc.StoreLimitConfig
		}{
			{`{"add-peer":30,"remove-peer":40}`, sc.StoreLimitConfig{AddPeer: 30, RemovePeer: 40, TransferLeaderIn: 300}},
			{`{"transfer-leader-in":120}`, sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 120}},
			{`{"transfer-leader-in":0}`, sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20}},
			{`{"TRANSFER-LEADER-IN":120}`, sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 120}},
			{`{"add-peer":null,"remove-peer":null,"transfer-leader-in":null}`, sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 300}},
			{`{}`, sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 300}},
			{`null`, sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: 300}},
		} {
			seed := []byte(`{"store-limit":{"1":{"add-peer":10,"remove-peer":20,"transfer-leader-in":300}}}`)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, seed, testutil.StatusOK(re)))
			before := leader.GetPersistOptions().GetScheduleConfig().Clone()
			patch := []byte(`{"store-limit":{"1":` + testCase.patch + `}}`)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, patch, testutil.StatusOK(re)))
			before.StoreLimit[1] = testCase.expected
			re.Equal(before, leader.GetPersistOptions().GetScheduleConfig())
			persisted := &config.Config{}
			exists, err := leader.GetServer().GetStorage().LoadConfig(persisted)
			re.NoError(err)
			re.True(exists)
			re.Equal(before.StoreLimit, persisted.Schedule.StoreLimit)
		}

		// Each request adds a fresh store so an earlier iteration cannot mask a
		// default/store ordering bug. Exercise both legacy key spellings as well.
		for i := range 20 {
			prefix := ""
			if endpoint == "config" && i%2 == 0 {
				prefix = "schedule."
			}
			storeID := uint64(9000 + i)
			if endpoint == "config/schedule" {
				storeID += 1000
			}
			patch := fmt.Appendf(nil, `{"%sstore-limit":{"%d":{"transfer-leader-in":300},"%d":{}},"%sdefault-store-limit":{"add-peer":%d,"remove-peer":70,"transfer-leader-in":120}}`,
				prefix, storeID, storeID+100, prefix, 60+i)
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, patch, testutil.StatusOK(re)))
			current := leader.GetPersistOptions().GetScheduleConfig()
			re.Equal(sc.StoreLimitConfig{AddPeer: float64(60 + i), RemovePeer: 70, TransferLeaderIn: 300}, current.StoreLimit[storeID])
			re.Equal(current.DefaultStoreLimit, current.StoreLimit[storeID+100])
		}
		before := leader.GetPersistOptions().GetScheduleConfig().Clone()
		invalidPatches := []string{
			`{"default-store-limit":{"add-peer":99},"store-limit":{"1":{"transfer-leader-in":-1}}}`,
			`{"default-store-limit":{"add-peer":99},"store-limit":{"1":{"transfer-leader-in":"invalid"}}}`,
		}
		if endpoint == "config" {
			invalidPatches = append(invalidPatches,
				`{"default-store-limit":{"add-peer":99},"schedule.unknown-option":1}`,
				`{"default-store-limit":{"add-peer":99},"schedule.default-store-limit":{"add-peer":100}}`)
		}
		for _, patch := range invalidPatches {
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, addr, []byte(patch), testutil.StatusNotOK(re)))
			re.Equal(before, leader.GetPersistOptions().GetScheduleConfig())
			persisted := &config.Config{}
			exists, err := leader.GetServer().GetStorage().LoadConfig(persisted)
			re.NoError(err)
			re.True(exists)
			re.Equal(before.StoreLimit, persisted.Schedule.StoreLimit)
			re.Equal(before.DefaultStoreLimit, persisted.Schedule.DefaultStoreLimit)
		}
	}
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
	"schedule.split-scatter-schedule-limit":   999,
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
	GetSplitScatterScheduleLimit() uint64
	IsTikvRegionSplitEnabled() bool
}

func assertTTLConfig(
	re *require.Assertions,
	cluster *tests.TestCluster,
	expectedEqual bool,
) {
	equality := re.Equal
	if !expectedEqual {
		equality = re.NotEqual
	}
	checkFunc := func(options ttlConfigInterface) {
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
		equality(uint64(999), options.GetSplitScatterScheduleLimit())
		equality(false, options.IsTikvRegionSplitEnabled())
	}
	checkFunc(cluster.GetLeaderServer().GetServer().GetPersistOptions())
	if cluster.GetSchedulingPrimaryServer() != nil {
		var options *cfg.PersistConfig
		testutil.Eventually(re, func() bool {
			// wait for the scheduling primary server to be synced
			options = cluster.GetSchedulingPrimaryServer().GetPersistConfig()
			if expectedEqual {
				return uint64(999) == options.GetMaxSnapshotCount()
			}
			return uint64(999) != options.GetMaxSnapshotCount()
		})
		checkFunc(options)
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
