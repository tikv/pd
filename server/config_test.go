// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"fmt"
	"os"
	"path"

	"github.com/BurntSushi/toml"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestTLS(c *C) {
	cfg := NewConfig()
	tls, err := cfg.Security.ToTLSConfig()
	c.Assert(err, IsNil)
	c.Assert(tls, IsNil)
}

func (s *testConfigSuite) TestBadFormatJoinAddr(c *C) {
	cfg := NewTestSingleConfig()
	cfg.Join = "127.0.0.1:2379" // Wrong join addr without scheme.
	c.Assert(cfg.Adjust(nil), NotNil)
}

func (s *testConfigSuite) TestReloadConfig(c *C) {
	_, opt := newTestScheduleConfig()
	kv := core.NewKV(core.NewMemoryKV())
	scheduleCfg := opt.load()
	scheduleCfg.MaxSnapshotCount = 10
	opt.SetMaxReplicas(5)
	opt.loadPDServerConfig().UseRegionStorage = true
	opt.persist(kv)

	// suppose we add a new default enable scheduler "adjacent-region"
	defaultSchedulers := []string{"balance-region", "balance-leader", "hot-region", "label", "adjacent-region"}
	_, newOpt := newTestScheduleConfig()
	newOpt.AddSchedulerCfg("adjacent-region", []string{})
	newOpt.reload(kv)
	schedulers := newOpt.GetSchedulers()
	c.Assert(schedulers, HasLen, 5)
	c.Assert(newOpt.loadPDServerConfig().UseRegionStorage, IsTrue)
	for i, s := range schedulers {
		c.Assert(s.Type, Equals, defaultSchedulers[i])
		c.Assert(s.Disable, IsFalse)
	}
	c.Assert(newOpt.GetMaxReplicas("default"), Equals, 5)
	c.Assert(newOpt.GetMaxSnapshotCount(), Equals, uint64(10))
}

func (s *testConfigSuite) TestValidation(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Adjust(nil), IsNil)

	cfg.Log.File.Filename = path.Join(cfg.DataDir, "test")
	c.Assert(cfg.validate(), NotNil)

	// check schedule config
	cfg.Schedule.HighSpaceRatio = -0.1
	c.Assert(cfg.Schedule.validate(), NotNil)
	cfg.Schedule.HighSpaceRatio = 0.6
	c.Assert(cfg.Schedule.validate(), IsNil)
	cfg.Schedule.LowSpaceRatio = 1.1
	c.Assert(cfg.Schedule.validate(), NotNil)
	cfg.Schedule.LowSpaceRatio = 0.4
	c.Assert(cfg.Schedule.validate(), NotNil)
	cfg.Schedule.LowSpaceRatio = 0.8
	c.Assert(cfg.Schedule.validate(), IsNil)
	cfg.Schedule.TolerantSizeRatio = -0.6
	c.Assert(cfg.Schedule.validate(), NotNil)
}

func (s *testConfigSuite) TestAdjust(c *C) {
	cfgData := `
name = ""
lease = 0

[schedule]
max-merge-region-size = 0
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
	c.Assert(cfg.Schedule.LeaderScheduleLimit, Equals, uint64(0))
	// When undefined, use default values.
	c.Assert(cfg.PreVote, IsTrue)
	c.Assert(cfg.Schedule.MaxMergeRegionKeys, Equals, uint64(defaultMaxMergeRegionKeys))

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
	c.Assert(err, NotNil)
}
