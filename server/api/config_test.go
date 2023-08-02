// Copyright 2016 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	sc "github.com/tikv/pd/pkg/schedule/config"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type configTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(configTestSuite))
}

func (suite *configTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = false
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (suite *configTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *configTestSuite) TestConfigAll() {
	re := suite.Require()
	addr := fmt.Sprintf("%s/config", suite.urlPrefix)
	cfg := &config.Config{}
	err := tu.ReadGetJSON(re, testDialClient, addr, cfg)
	suite.NoError(err)

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
	re := suite.Require()
	addr := fmt.Sprintf("%s/config/schedule", suite.urlPrefix)
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
	re := suite.Require()
	addr := fmt.Sprintf("%s/config/replicate", suite.urlPrefix)
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
	re := suite.Require()
	addr := suite.svr.GetAddr() + apiPrefix + "/api/v1/config/label-property"
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
	addr := fmt.Sprintf("%s/config", suite.urlPrefix)

	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	suite.NoError(err)
	re := suite.Require()
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

	addr = fmt.Sprintf("%s/config/default", suite.urlPrefix)
	defaultCfg := &config.Config{}
	err = tu.ReadGetJSON(re, testDialClient, addr, defaultCfg)
	suite.NoError(err)

	suite.Equal(uint64(3), defaultCfg.Replication.MaxReplicas)
	suite.Equal(typeutil.StringSlice([]string{}), defaultCfg.Replication.LocationLabels)
	suite.Equal(uint64(2048), defaultCfg.Schedule.RegionScheduleLimit)
	suite.Equal("", defaultCfg.PDServerCfg.MetricStorage)
}

func (suite *configTestSuite) TestConfigPDServer() {
	re := suite.Require()
	addrPost := fmt.Sprintf("%s/config", suite.urlPrefix)
	ms := map[string]interface{}{
		"metric-storage": "",
	}
	postData, err := json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addrPost, postData, tu.StatusOK(re)))
	addrGet := fmt.Sprintf("%s/config/pd-server", suite.urlPrefix)
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

func assertTTLConfig(
	options *config.PersistOptions,
	equality func(interface{}, interface{}, ...interface{}) bool,
) {
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

func createTTLUrl(url string, ttl int) string {
	return fmt.Sprintf("%s/config?ttlSecond=%d", url, ttl)
}

func (suite *configTestSuite) TestConfigTTL() {
	postData, err := json.Marshal(ttlConfig)
	suite.NoError(err)

	// test no config and cleaning up
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 0), postData, tu.StatusOK(re))
	suite.NoError(err)
	assertTTLConfig(suite.svr.GetPersistOptions(), suite.NotEqual)

	// test time goes by
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	assertTTLConfig(suite.svr.GetPersistOptions(), suite.Equal)
	time.Sleep(2 * time.Second)
	assertTTLConfig(suite.svr.GetPersistOptions(), suite.NotEqual)

	// test cleaning up
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	assertTTLConfig(suite.svr.GetPersistOptions(), suite.Equal)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 0), postData, tu.StatusOK(re))
	suite.NoError(err)
	assertTTLConfig(suite.svr.GetPersistOptions(), suite.NotEqual)

	postData, err = json.Marshal(invalidTTLConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 1), postData,
		tu.StatusNotOK(re), tu.StringEqual(re, "\"unsupported ttl config schedule.invalid-ttl-config\"\n"))
	suite.NoError(err)

	// only set max-merge-region-size
	mergeConfig := map[string]interface{}{
		"schedule.max-merge-region-size": 999,
	}
	postData, err = json.Marshal(mergeConfig)
	suite.NoError(err)

	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.Equal(uint64(999), suite.svr.GetPersistOptions().GetMaxMergeRegionSize())
	// max-merge-region-keys should keep consistence with max-merge-region-size.
	suite.Equal(uint64(999*10000), suite.svr.GetPersistOptions().GetMaxMergeRegionKeys())

	// on invalid value, we use default config
	mergeConfig = map[string]interface{}{
		"schedule.enable-tikv-split-region": "invalid",
	}
	postData, err = json.Marshal(mergeConfig)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 1), postData, tu.StatusOK(re))
	suite.NoError(err)
	suite.True(suite.svr.GetPersistOptions().IsTikvRegionSplitEnabled())
}

func (suite *configTestSuite) TestTTLConflict() {
	addr := createTTLUrl(suite.urlPrefix, 1)
	postData, err := json.Marshal(ttlConfig)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
	assertTTLConfig(suite.svr.GetPersistOptions(), suite.Equal)

	cfg := map[string]interface{}{"max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	suite.NoError(err)
	addr = fmt.Sprintf("%s/config", suite.urlPrefix)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(re), tu.StringEqual(re, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	suite.NoError(err)
	addr = fmt.Sprintf("%s/config/schedule", suite.urlPrefix)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(re), tu.StringEqual(re, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	suite.NoError(err)
	cfg = map[string]interface{}{"schedule.max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(suite.urlPrefix, 0), postData, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re))
	suite.NoError(err)
}
