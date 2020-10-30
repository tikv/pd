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
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) SetUpSuite(c *C) {
	for _, d := range DefaultSchedulers {
		RegisterScheduler(d.Type)
	}
	RegisterScheduler("random-merge")
	RegisterScheduler("shuffle-leader")
}

func (s *testConfigSuite) TestSecurity(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Security.RedactInfoLog, Equals, false)
}

func (s *testConfigSuite) TestTLS(c *C) {
	cfg := NewConfig()
	tls, err := cfg.Security.ToTLSConfig()
	c.Assert(err, IsNil)
	c.Assert(tls, IsNil)
}

func (s *testConfigSuite) TestBadFormatJoinAddr(c *C) {
	cfg := NewConfig()
	cfg.Join = "127.0.0.1:2379" // Wrong join addr without scheme.
	c.Assert(cfg.Adjust(nil), NotNil)
}

func (s *testConfigSuite) TestReloadConfig(c *C) {
	opt, err := newTestScheduleOption()
	c.Assert(err, IsNil)
	storage := core.NewStorage(kv.NewMemoryKV())
	scheduleCfg := opt.GetScheduleConfig()
	scheduleCfg.MaxSnapshotCount = 10
	opt.SetMaxReplicas(5)
	opt.GetPDServerConfig().UseRegionStorage = true
	c.Assert(opt.Persist(storage), IsNil)

	// Add a new default enable scheduler "shuffle-leader"
	DefaultSchedulers = append(DefaultSchedulers, SchedulerConfig{Type: "shuffle-leader"})
	defer func() {
		DefaultSchedulers = DefaultSchedulers[:len(DefaultSchedulers)-1]
	}()

	newOpt, err := newTestScheduleOption()
	c.Assert(err, IsNil)
	c.Assert(newOpt.Reload(storage), IsNil)
	schedulers := newOpt.GetSchedulers()
	c.Assert(schedulers, HasLen, 5)
	c.Assert(newOpt.IsUseRegionStorage(), IsTrue)
	for i, s := range schedulers {
		c.Assert(s.Type, Equals, DefaultSchedulers[i].Type)
		c.Assert(s.Disable, IsFalse)
	}
	c.Assert(newOpt.GetMaxReplicas(), Equals, 5)
	c.Assert(newOpt.GetMaxSnapshotCount(), Equals, uint64(10))
}

func (s *testConfigSuite) TestReloadUpgrade(c *C) {
	opt, err := newTestScheduleOption()
	c.Assert(err, IsNil)

	// Simulate an old configuration that only contains 2 fields.
	type OldConfig struct {
		Schedule    ScheduleConfig    `toml:"schedule" json:"schedule"`
		Replication ReplicationConfig `toml:"replication" json:"replication"`
	}
	old := &OldConfig{
		Schedule:    *opt.GetScheduleConfig(),
		Replication: *opt.GetReplicationConfig(),
	}
	storage := core.NewStorage(kv.NewMemoryKV())
	c.Assert(storage.SaveConfig(old), IsNil)

	newOpt, err := newTestScheduleOption()
	c.Assert(err, IsNil)
	c.Assert(newOpt.Reload(storage), IsNil)
	c.Assert(newOpt.GetPDServerConfig().KeyType, Equals, defaultKeyType) // should be set to default value.
}

func (s *testConfigSuite) TestValidation(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Adjust(nil), IsNil)

	cfg.Log.File.Filename = path.Join(cfg.DataDir, "test")
	c.Assert(cfg.Validate(), NotNil)

	// check schedule config
	cfg.Schedule.HighSpaceRatio = -0.1
	c.Assert(cfg.Schedule.Validate(), NotNil)
	cfg.Schedule.HighSpaceRatio = 0.6
	c.Assert(cfg.Schedule.Validate(), IsNil)
	cfg.Schedule.LowSpaceRatio = 1.1
	c.Assert(cfg.Schedule.Validate(), NotNil)
	cfg.Schedule.LowSpaceRatio = 0.4
	c.Assert(cfg.Schedule.Validate(), NotNil)
	cfg.Schedule.LowSpaceRatio = 0.8
	c.Assert(cfg.Schedule.Validate(), IsNil)
	cfg.Schedule.TolerantSizeRatio = -0.6
	c.Assert(cfg.Schedule.Validate(), NotNil)
	// check quota
	c.Assert(cfg.QuotaBackendBytes, Equals, defaultQuotaBackendBytes)
}

func (s *testConfigSuite) TestAdjust(c *C) {
	cfgData := `
name = ""
lease = 0

[pd-server]
metric-storage = "http://127.0.0.1:9090"

[schedule]
max-merge-region-size = 0
enable-one-way-merge = true
leader-schedule-limit = 0
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)

	// When invalid, use default values.
	host, err := os.Hostname()
	c.Assert(err, IsNil)
	c.Assert(cfg.Name, Equals, fmt.Sprintf("%s-%s", defaultName, host))
	c.Assert(cfg.LeaderLease, Equals, defaultLeaderLease)
	// When defined, use values from config file.
	c.Assert(cfg.Schedule.MaxMergeRegionSize, Equals, uint64(0))
	c.Assert(cfg.Schedule.EnableOneWayMerge, Equals, true)
	c.Assert(cfg.Schedule.LeaderScheduleLimit, Equals, uint64(0))
	// When undefined, use default values.
	c.Assert(cfg.PreVote, IsTrue)
	c.Assert(cfg.Schedule.MaxMergeRegionKeys, Equals, uint64(defaultMaxMergeRegionKeys))
	c.Assert(cfg.PDServerCfg.MetricStorage, Equals, "http://127.0.0.1:9090")

	c.Assert(cfg.TSOUpdatePhysicalInterval.Duration, Equals, DefaultTSOUpdatePhysicalInterval)

	// Check undefined config fields
	cfgData = `
type = "pd"
name = ""
lease = 0

[schedule]
type = "random-merge"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(cfg.WarningMsgs[0], "Config contains undefined item"), IsTrue)

	// Check misspelled schedulers name
	cfgData = `
name = ""
lease = 0

[[schedule.schedulers]]
type = "random-merge-schedulers"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, NotNil)

	// Check correct schedulers name
	cfgData = `
name = ""
lease = 0

[[schedule.schedulers]]
type = "random-merge"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)

	cfgData = `
[metric]
interval = "35s"
address = "localhost:9090"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)

	c.Assert(cfg.Metric.PushInterval.Duration, Equals, 35*time.Second)
	c.Assert(cfg.Metric.PushAddress, Equals, "localhost:9090")

	// Test clamping TSOUpdatePhysicalInterval value
	cfgData = `
tso-update-physical-interval = "10ms"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)

	c.Assert(cfg.TSOUpdatePhysicalInterval.Duration, Equals, minTSOUpdatePhysicalInterval)

	cfgData = `
tso-update-physical-interval = "15s"
`
	cfg = NewConfig()
	meta, err = toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)

	c.Assert(cfg.TSOUpdatePhysicalInterval.Duration, Equals, maxTSOUpdatePhysicalInterval)
}

func (s *testConfigSuite) TestMigrateFlags(c *C) {
	load := func(s string) (*Config, error) {
		cfg := NewConfig()
		meta, err := toml.Decode(s, &cfg)
		c.Assert(err, IsNil)
		err = cfg.Adjust(&meta)
		return cfg, err
	}
	cfg, err := load(`
[schedule]
disable-remove-down-replica = true
enable-make-up-replica = false
disable-remove-extra-replica = true
enable-remove-extra-replica = false
`)
	c.Assert(err, IsNil)
	c.Assert(cfg.Schedule.EnableReplaceOfflineReplica, IsTrue)
	c.Assert(cfg.Schedule.EnableRemoveDownReplica, IsFalse)
	c.Assert(cfg.Schedule.EnableMakeUpReplica, IsFalse)
	c.Assert(cfg.Schedule.EnableRemoveExtraReplica, IsFalse)
	b, err := json.Marshal(cfg)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(b), "disable-replace-offline-replica"), IsFalse)
	c.Assert(strings.Contains(string(b), "disable-remove-down-replica"), IsFalse)

	_, err = load(`
[schedule]
enable-make-up-replica = false
disable-make-up-replica = false
`)
	c.Assert(err, NotNil)
}

func newTestScheduleOption() (*PersistOptions, error) {
	cfg := NewConfig()
	if err := cfg.Adjust(nil); err != nil {
		return nil, err
	}
	opt := NewPersistOptions(cfg)
	return opt, nil
}

func (s *testConfigSuite) TestPDServerConfig(c *C) {
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

	for _, t := range tests {
		cfg := NewConfig()
		meta, err := toml.Decode(t.cfgData, &cfg)
		c.Assert(err, IsNil)
		err = cfg.Adjust(&meta)
		c.Assert(err != nil, Equals, t.hasErr)
		if !t.hasErr {
			c.Assert(cfg.PDServerCfg.DashboardAddress, Equals, t.dashboardAddress)
		}
	}
}

func (s *testConfigSuite) TestDashboardConfig(c *C) {
	cfgData := `
[dashboard]
tidb-cacert-path = "/path/ca.pem"
tidb-key-path = "/path/client-key.pem"
tidb-cert-path = "/path/client.pem"
`
	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)
	c.Assert(cfg.Dashboard.TiDBCAPath, Equals, "/path/ca.pem")
	c.Assert(cfg.Dashboard.TiDBKeyPath, Equals, "/path/client-key.pem")
	c.Assert(cfg.Dashboard.TiDBCertPath, Equals, "/path/client.pem")

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
		c.Assert(err, IsNil)
		err = cfg.Adjust(&meta)
		c.Assert(err, IsNil)
		c.Assert(cfg.Dashboard.EnableTelemetry, Equals, test.EnableTelemetry)
	}
	defaultEnableTelemetry = originalDefaultEnableTelemetry
}

func (s *testConfigSuite) TestReplicationMode(c *C) {
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
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)

	c.Assert(cfg.ReplicationMode.ReplicationMode, Equals, "dr-auto-sync")
	c.Assert(cfg.ReplicationMode.DRAutoSync.LabelKey, Equals, "zone")
	c.Assert(cfg.ReplicationMode.DRAutoSync.Primary, Equals, "zone1")
	c.Assert(cfg.ReplicationMode.DRAutoSync.DR, Equals, "zone2")
	c.Assert(cfg.ReplicationMode.DRAutoSync.PrimaryReplicas, Equals, 2)
	c.Assert(cfg.ReplicationMode.DRAutoSync.DRReplicas, Equals, 1)
	c.Assert(cfg.ReplicationMode.DRAutoSync.WaitStoreTimeout.Duration, Equals, 2*time.Minute)
	c.Assert(cfg.ReplicationMode.DRAutoSync.WaitSyncTimeout.Duration, Equals, time.Minute)

	cfg = NewConfig()
	meta, err = toml.Decode("", &cfg)
	c.Assert(err, IsNil)
	err = cfg.Adjust(&meta)
	c.Assert(err, IsNil)
	c.Assert(cfg.ReplicationMode.ReplicationMode, Equals, "majority")
}

func (s *testConfigSuite) TestConfigClone(c *C) {
	cfg := &Config{}
	cfg.Adjust(nil)
	c.Assert(cfg.Clone(), DeepEquals, cfg)

	emptyConfigMetaData := newConfigMetadata(nil)

	schedule := &ScheduleConfig{}
	schedule.adjust(emptyConfigMetaData)
	c.Assert(schedule.Clone(), DeepEquals, schedule)

	replication := &ReplicationConfig{}
	replication.adjust(emptyConfigMetaData)
	c.Assert(replication.Clone(), DeepEquals, replication)

	pdServer := &PDServerConfig{}
	pdServer.adjust(emptyConfigMetaData)
	c.Assert(pdServer.Clone(), DeepEquals, pdServer)

	replicationMode := &ReplicationModeConfig{}
	replicationMode.adjust(emptyConfigMetaData)
	c.Assert(replicationMode.Clone(), DeepEquals, replicationMode)
}
