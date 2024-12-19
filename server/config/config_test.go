// Copyright 2017 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/ratelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/logutil"
)

func TestSecurity(t *testing.T) {
	re := require.New(t)
	cfg := NewConfig()
	re.Equal(logutil.RedactInfoLogOFF, cfg.Security.RedactInfoLog)
}

func TestTLS(t *testing.T) {
	re := require.New(t)
	cfg := NewConfig()
	tls, err := cfg.Security.ToTLSConfig()
	re.NoError(err)
	re.Nil(tls)
}

func TestBadFormatJoinAddr(t *testing.T) {
	re := require.New(t)
	cfg := NewConfig()
	cfg.Join = "127.0.0.1:2379" // Wrong join addr without scheme.
	re.Error(cfg.Adjust(nil, false))
}

func TestReloadConfig(t *testing.T) {
	re := require.New(t)
	opt, err := newTestScheduleOption()
	re.NoError(err)
	storage := storage.NewStorageWithMemoryBackend()
	scheduleCfg := opt.GetScheduleConfig()
	scheduleCfg.MaxSnapshotCount = 10
	opt.SetMaxReplicas(5)
	opt.GetPDServerConfig().UseRegionStorage = true
	re.NoError(opt.Persist(storage))

	newOpt, err := newTestScheduleOption()
	re.NoError(err)
	re.NoError(newOpt.Reload(storage))

	re.Equal(5, newOpt.GetMaxReplicas())
	re.Equal(uint64(10), newOpt.GetMaxSnapshotCount())
	re.Equal(int64(512), newOpt.GetMaxMovableHotPeerSize())
}

func TestReloadUpgrade(t *testing.T) {
	re := require.New(t)
	opt, err := newTestScheduleOption()
	re.NoError(err)

	// Simulate an old configuration that only contains 2 fields.
	type OldConfig struct {
		Schedule    sc.ScheduleConfig    `toml:"schedule" json:"schedule"`
		Replication sc.ReplicationConfig `toml:"replication" json:"replication"`
	}
	old := &OldConfig{
		Schedule:    *opt.GetScheduleConfig(),
		Replication: *opt.GetReplicationConfig(),
	}
	storage := storage.NewStorageWithMemoryBackend()
	re.NoError(storage.SaveConfig(old))

	newOpt, err := newTestScheduleOption()
	re.NoError(err)
	re.NoError(newOpt.Reload(storage))
	re.Equal(defaultKeyType, newOpt.GetPDServerConfig().KeyType) // should be set to default value.
}

func TestReloadUpgrade2(t *testing.T) {
	re := require.New(t)
	opt, err := newTestScheduleOption()
	re.NoError(err)

	// Simulate an old configuration that does not contain ScheduleConfig.
	type OldConfig struct {
		Replication sc.ReplicationConfig `toml:"replication" json:"replication"`
	}
	old := &OldConfig{
		Replication: *opt.GetReplicationConfig(),
	}
	storage := storage.NewStorageWithMemoryBackend()
	re.NoError(storage.SaveConfig(old))

	newOpt, err := newTestScheduleOption()
	re.NoError(err)
	re.NoError(newOpt.Reload(storage))
	re.Equal("", newOpt.GetScheduleConfig().RegionScoreFormulaVersion) // formulaVersion keep old value when reloading.
}

func TestValidation(t *testing.T) {
	re := require.New(t)
	cfg := NewConfig()
	re.NoError(cfg.Adjust(nil, false))

	cfg.Log.File.Filename = filepath.Join(cfg.DataDir, "test")
	re.Error(cfg.Validate())

	// check schedule config
	cfg.Schedule.HighSpaceRatio = -0.1
	re.Error(cfg.Schedule.Validate())
	cfg.Schedule.HighSpaceRatio = 0.6
	re.NoError(cfg.Schedule.Validate())
	cfg.Schedule.LowSpaceRatio = 1.1
	re.Error(cfg.Schedule.Validate())
	cfg.Schedule.LowSpaceRatio = 0.4
	re.Error(cfg.Schedule.Validate())
	cfg.Schedule.LowSpaceRatio = 0.8
	re.NoError(cfg.Schedule.Validate())
	cfg.Schedule.TolerantSizeRatio = -0.6
	re.Error(cfg.Schedule.Validate())
	// check quota
	re.Equal(defaultQuotaBackendBytes, cfg.QuotaBackendBytes)
	// check request bytes
	re.Equal(defaultMaxRequestBytes, cfg.MaxRequestBytes)

	re.Equal(defaultLogFormat, cfg.Log.Format)
}

func TestAdjust(t *testing.T) {
	re := require.New(t)
	cfgData := `
name = ""
lease = 0
max-request-bytes = 20000000

[pd-server]
metric-storage = "http://127.0.0.1:9090"

[schedule]
max-merge-region-size = 0
enable-one-way-merge = true
leader-schedule-limit = 0
`

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flagSet.StringP("log-level", "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	flagSet.Parse(nil)
	cfg := NewConfig()
	err := cfg.Parse(flagSet)
	re.NoError(err)
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)

	// When invalid, use default values.
	host, err := os.Hostname()
	re.NoError(err)
	re.Equal(fmt.Sprintf("%s-%s", defaultName, host), cfg.Name)
	re.Equal(defaultLeaderLease, cfg.LeaderLease)
	re.Equal(uint(20000000), cfg.MaxRequestBytes)
	// When defined, use values from config file.
	re.Equal(0*10000, int(cfg.Schedule.GetMaxMergeRegionKeys()))
	re.Equal(uint64(0), cfg.Schedule.MaxMergeRegionSize)
	re.True(cfg.Schedule.EnableOneWayMerge)
	re.Equal(uint64(0), cfg.Schedule.LeaderScheduleLimit)
	// When undefined, use default values.
	re.True(cfg.PreVote)
	re.Equal("info", cfg.Log.Level)
	re.Equal(uint64(0), cfg.Schedule.MaxMergeRegionKeys)
	re.Equal("http://127.0.0.1:9090", cfg.PDServerCfg.MetricStorage)

	re.Equal(defaultTSOUpdatePhysicalInterval, cfg.TSOUpdatePhysicalInterval.Duration)

	// Check undefined config fields
	cfgData = `
type = "pd"
name = ""
lease = 0

[schedule]
type = "random-merge"
max-merge-region-keys = 400000
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)
	re.Contains(cfg.WarningMsgs[0], "Config contains undefined item")
	re.Equal(40*10000, int(cfg.Schedule.GetMaxMergeRegionKeys()))

	cfgData = `
[metric]
interval = "35s"
address = "localhost:9090"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)

	re.Equal(35*time.Second, cfg.Metric.PushInterval.Duration)
	re.Equal("localhost:9090", cfg.Metric.PushAddress)

	// Test clamping TSOUpdatePhysicalInterval value
	cfgData = `
tso-update-physical-interval = "500ns"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)

	re.Equal(minTSOUpdatePhysicalInterval, cfg.TSOUpdatePhysicalInterval.Duration)

	cfgData = `
tso-update-physical-interval = "15s"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)

	re.Equal(maxTSOUpdatePhysicalInterval, cfg.TSOUpdatePhysicalInterval.Duration)

	cfgData = `
[log]
level = "debug"
`
	flagSet = pflag.NewFlagSet("testlog", pflag.ContinueOnError)
	flagSet.StringP("log-level", "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	flagSet.Parse(nil)
	cfg = NewConfig()
	err = cfg.Parse(flagSet)
	re.NoError(err)
	meta, err = toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)
	re.Equal("debug", cfg.Log.Level)
}

func TestMigrateFlags(t *testing.T) {
	re := require.New(t)
	load := func(s string) (*Config, error) {
		cfg := NewConfig()
		meta, err := toml.Decode(s, &cfg)
		re.NoError(err)
		err = cfg.Adjust(&meta, false)
		return cfg, err
	}
	cfg, err := load(`
[pd-server]
trace-region-flow = false
[schedule]
disable-remove-down-replica = true
enable-make-up-replica = false
disable-remove-extra-replica = true
enable-remove-extra-replica = false
`)
	re.NoError(err)
	re.Equal(math.MaxInt8, cfg.PDServerCfg.FlowRoundByDigit)
	re.True(cfg.Schedule.EnableReplaceOfflineReplica)
	re.False(cfg.Schedule.EnableRemoveDownReplica)
	re.False(cfg.Schedule.EnableMakeUpReplica)
	re.False(cfg.Schedule.EnableRemoveExtraReplica)
	b, err := json.Marshal(cfg)
	re.NoError(err)
	re.NotContains(string(b), "disable-replace-offline-replica")
	re.NotContains(string(b), "disable-remove-down-replica")

	_, err = load(`
[schedule]
enable-make-up-replica = false
disable-make-up-replica = false
`)
	re.Error(err)
}

func TestPDServerConfig(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		cfgData          string
		hasErr           bool
		dashboardAddress string
	}{
		{
			`
[pd-server]
dashboard-address = "http://127.0.0.1:2379"
`,
			false,
			"http://127.0.0.1:2379",
		},
		{
			`
[pd-server]
dashboard-address = "auto"
`,
			false,
			"auto",
		},
		{
			`
[pd-server]
dashboard-address = "none"
`,
			false,
			"none",
		},
		{
			"",
			false,
			"auto",
		},
		{
			`
[pd-server]
dashboard-address = "127.0.0.1:2379"
`,
			true,
			"",
		},
		{
			`
[pd-server]
dashboard-address = "foo"
`,
			true,
			"",
		},
	}

	for _, test := range tests {
		cfg := NewConfig()
		meta, err := toml.Decode(test.cfgData, &cfg)
		re.NoError(err)
		err = cfg.Adjust(&meta, false)
		re.Equal(test.hasErr, err != nil)
		if !test.hasErr {
			re.Equal(test.dashboardAddress, cfg.PDServerCfg.DashboardAddress)
		}
	}
}

func TestDashboardConfig(t *testing.T) {
	re := require.New(t)
	cfgData := `
[dashboard]
tidb-cacert-path = "/path/ca.pem"
tidb-key-path = "/path/client-key.pem"
tidb-cert-path = "/path/client.pem"
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)
	re.Equal("/path/ca.pem", cfg.Dashboard.TiDBCAPath)
	re.Equal("/path/client-key.pem", cfg.Dashboard.TiDBKeyPath)
	re.Equal("/path/client.pem", cfg.Dashboard.TiDBCertPath)

	// Test different editions
	tests := []struct {
		Edition         string
		EnableTelemetry bool
	}{
		{"Community", true},
		{"Enterprise", false},
	}
	originalDefaultEnableTelemetry := defaultEnableTelemetry
	for _, test := range tests {
		defaultEnableTelemetry = true
		initByLDFlags(test.Edition)
		cfg = NewConfig()
		meta, err = toml.Decode(cfgData, &cfg)
		re.NoError(err)
		err = cfg.Adjust(&meta, false)
		re.NoError(err)
		re.Equal(test.EnableTelemetry, cfg.Dashboard.EnableTelemetry)
	}
	defaultEnableTelemetry = originalDefaultEnableTelemetry
}

func TestReplicationMode(t *testing.T) {
	re := require.New(t)
	cfgData := `
[replication-mode]
replication-mode = "dr-auto-sync"
[replication-mode.dr-auto-sync]
label-key = "zone"
primary = "zone1"
dr = "zone2"
primary-replicas = 2
dr-replicas = 1
wait-store-timeout = "120s"
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)

	re.Equal("dr-auto-sync", cfg.ReplicationMode.ReplicationMode)
	re.Equal("zone", cfg.ReplicationMode.DRAutoSync.LabelKey)
	re.Equal("zone1", cfg.ReplicationMode.DRAutoSync.Primary)
	re.Equal("zone2", cfg.ReplicationMode.DRAutoSync.DR)
	re.Equal(2, cfg.ReplicationMode.DRAutoSync.PrimaryReplicas)
	re.Equal(1, cfg.ReplicationMode.DRAutoSync.DRReplicas)
	re.Equal(2*time.Minute, cfg.ReplicationMode.DRAutoSync.WaitStoreTimeout.Duration)

	cfg = NewConfig()
	meta, err = toml.Decode("", &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)
	re.Equal("majority", cfg.ReplicationMode.ReplicationMode)
}

func TestHotHistoryRegionConfig(t *testing.T) {
	re := require.New(t)
	cfgData := `
[schedule]
hot-regions-reserved-days= 30
hot-regions-write-interval= "30m"
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)
	re.Equal(30*time.Minute, cfg.Schedule.HotRegionsWriteInterval.Duration)
	re.Equal(uint64(30), cfg.Schedule.HotRegionsReservedDays)
	// Verify default value
	cfg = NewConfig()
	err = cfg.Adjust(nil, false)
	re.NoError(err)
	re.Equal(10*time.Minute, cfg.Schedule.HotRegionsWriteInterval.Duration)
	re.Equal(uint64(7), cfg.Schedule.HotRegionsReservedDays)
}

func TestConfigClone(t *testing.T) {
	re := require.New(t)
	cfg := &Config{}
	cfg.Adjust(nil, false)
	re.Equal(cfg, cfg.Clone())

	emptyConfigMetaData := configutil.NewConfigMetadata(nil)

	schedule := &sc.ScheduleConfig{}
	schedule.Adjust(emptyConfigMetaData, false)
	re.Equal(schedule, schedule.Clone())

	replication := &sc.ReplicationConfig{}
	replication.Adjust(emptyConfigMetaData)
	re.Equal(replication, replication.Clone())

	pdServer := &PDServerConfig{}
	pdServer.adjust(emptyConfigMetaData)
	re.Equal(pdServer, pdServer.Clone())

	replicationMode := &ReplicationModeConfig{}
	replicationMode.adjust(emptyConfigMetaData)
	re.Equal(replicationMode, replicationMode.Clone())
}

func newTestScheduleOption() (*PersistOptions, error) {
	cfg := NewConfig()
	if err := cfg.Adjust(nil, false); err != nil {
		return nil, err
	}
	opt := NewPersistOptions(cfg)
	return opt, nil
}

func TestRateLimitClone(t *testing.T) {
	re := require.New(t)
	cfg := &RateLimitConfig{
		EnableRateLimit: defaultEnableRateLimitMiddleware,
		LimiterConfig:   make(map[string]ratelimit.DimensionConfig),
	}
	clone := cfg.Clone()
	clone.LimiterConfig["test"] = ratelimit.DimensionConfig{
		ConcurrencyLimit: 200,
	}
	dc := cfg.LimiterConfig["test"]
	re.Zero(dc.ConcurrencyLimit)

	gCfg := &GRPCRateLimitConfig{
		EnableRateLimit: defaultEnableGRPCRateLimitMiddleware,
		LimiterConfig:   make(map[string]ratelimit.DimensionConfig),
	}
	gClone := gCfg.Clone()
	gClone.LimiterConfig["test"] = ratelimit.DimensionConfig{
		ConcurrencyLimit: 300,
	}
	gdc := gCfg.LimiterConfig["test"]
	re.Zero(gdc.ConcurrencyLimit)
}
