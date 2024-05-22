// Copyright 2024 TiKV Project Authors.
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

package heartbeatbench

import (
	"sync/atomic"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/tools/pd-dev/util"
)

const (
	defaultStoreCount        = 50
	defaultRegionCount       = 1000000
	defaultHotStoreCount     = 0
	defaultReplica           = 3
	defaultLeaderUpdateRatio = 0.06
	defaultEpochUpdateRatio  = 0.0
	defaultSpaceUpdateRatio  = 0.0
	defaultFlowUpdateRatio   = 0.0
	defaultReportRatio       = 1
	defaultRound             = 0
	defaultSample            = false
	defaultInitialVersion    = 1
)

// config is the heartbeat-bench configuration.
type config struct {
	*util.GeneralConfig

	InitEpochVer      uint64  `toml:"epoch-ver" json:"epoch-ver"`
	StoreCount        int     `toml:"store-count" json:"store-count"`
	HotStoreCount     int     `toml:"hot-store-count" json:"hot-store-count"`
	RegionCount       int     `toml:"region-count" json:"region-count"`
	Replica           int     `toml:"replica" json:"replica"`
	LeaderUpdateRatio float64 `toml:"leader-update-ratio" json:"leader-update-ratio"`
	EpochUpdateRatio  float64 `toml:"epoch-update-ratio" json:"epoch-update-ratio"`
	SpaceUpdateRatio  float64 `toml:"space-update-ratio" json:"space-update-ratio"`
	FlowUpdateRatio   float64 `toml:"flow-update-ratio" json:"flow-update-ratio"`
	ReportRatio       float64 `toml:"report-ratio" json:"report-ratio"`
	Sample            bool    `toml:"sample" json:"sample"`
	Round             int     `toml:"round" json:"round"`
}

// newConfig return a set of settings.
func newConfig() *config {
	cfg := &config{
		GeneralConfig: &util.GeneralConfig{},
	}
	cfg.FlagSet = flag.NewFlagSet("heartbeat-bench", flag.ContinueOnError)
	fs := cfg.FlagSet
	cfg.GeneralConfig = util.NewGeneralConfig(fs)
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.Uint64Var(&cfg.InitEpochVer, "epoch-ver", 1, "the initial epoch version value")

	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	var meta *toml.MetaData
	if c.ConfigFile != "" {
		meta, err = configutil.ConfigFromFile(c, c.ConfigFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	c.Adjust(meta)
	return c.Validate()
}

// Adjust is used to adjust configurations
func (c *config) Adjust(meta *toml.MetaData) {
	c.GeneralConfig.Adjust()

	if !meta.IsDefined("round") {
		configutil.AdjustInt(&c.Round, defaultRound)
	}

	if !meta.IsDefined("store-count") {
		configutil.AdjustInt(&c.StoreCount, defaultStoreCount)
	}
	if !meta.IsDefined("region-count") {
		configutil.AdjustInt(&c.RegionCount, defaultRegionCount)
	}

	if !meta.IsDefined("hot-store-count") {
		configutil.AdjustInt(&c.HotStoreCount, defaultHotStoreCount)
	}
	if !meta.IsDefined("replica") {
		configutil.AdjustInt(&c.Replica, defaultReplica)
	}

	if !meta.IsDefined("leader-update-ratio") {
		configutil.AdjustFloat64(&c.LeaderUpdateRatio, defaultLeaderUpdateRatio)
	}
	if !meta.IsDefined("epoch-update-ratio") {
		configutil.AdjustFloat64(&c.EpochUpdateRatio, defaultEpochUpdateRatio)
	}
	if !meta.IsDefined("space-update-ratio") {
		configutil.AdjustFloat64(&c.SpaceUpdateRatio, defaultSpaceUpdateRatio)
	}
	if !meta.IsDefined("flow-update-ratio") {
		configutil.AdjustFloat64(&c.FlowUpdateRatio, defaultFlowUpdateRatio)
	}
	if !meta.IsDefined("report-ratio") {
		configutil.AdjustFloat64(&c.ReportRatio, defaultReportRatio)
	}
	if !meta.IsDefined("sample") {
		c.Sample = defaultSample
	}
	if !meta.IsDefined("epoch-ver") {
		c.InitEpochVer = defaultInitialVersion
	}
}

// Validate is used to validate configurations
func (c *config) Validate() error {
	if c.HotStoreCount < 0 || c.HotStoreCount > c.StoreCount {
		return errors.Errorf("hot-store-count must be in [0, store-count]")
	}
	if c.ReportRatio < 0 || c.ReportRatio > 1 {
		return errors.Errorf("report-ratio must be in [0, 1]")
	}
	if c.LeaderUpdateRatio > c.ReportRatio || c.LeaderUpdateRatio < 0 {
		return errors.Errorf("leader-update-ratio can not be negative or larger than report-ratio")
	}
	if c.EpochUpdateRatio > c.ReportRatio || c.EpochUpdateRatio < 0 {
		return errors.Errorf("epoch-update-ratio can not be negative or larger than report-ratio")
	}
	if c.SpaceUpdateRatio > c.ReportRatio || c.SpaceUpdateRatio < 0 {
		return errors.Errorf("space-update-ratio can not be negative or larger than report-ratio")
	}
	if c.FlowUpdateRatio > c.ReportRatio || c.FlowUpdateRatio < 0 {
		return errors.Errorf("flow-update-ratio can not be negative or larger than report-ratio")
	}
	return nil
}

// Clone creates a copy of current config.
func (c *config) Clone() *config {
	cfg := &config{}
	*cfg = *c
	return cfg
}

// Options is the option of the heartbeat-bench.
type options struct {
	hotStoreCount atomic.Value
	reportRatio   atomic.Value

	leaderUpdateRatio atomic.Value
	epochUpdateRatio  atomic.Value
	spaceUpdateRatio  atomic.Value
	flowUpdateRatio   atomic.Value
}

// NewOptions creates a new option.
func newOptions(cfg *config) *options {
	o := &options{}
	o.hotStoreCount.Store(cfg.HotStoreCount)
	o.leaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.epochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.spaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.flowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.reportRatio.Store(cfg.ReportRatio)
	return o
}

// GetHotStoreCount returns the hot store count.
func (o *options) GetHotStoreCount() int {
	return o.hotStoreCount.Load().(int)
}

// GetLeaderUpdateRatio returns the leader update ratio.
func (o *options) GetLeaderUpdateRatio() float64 {
	return o.leaderUpdateRatio.Load().(float64)
}

// GetEpochUpdateRatio returns the epoch update ratio.
func (o *options) GetEpochUpdateRatio() float64 {
	return o.epochUpdateRatio.Load().(float64)
}

// GetSpaceUpdateRatio returns the space update ratio.
func (o *options) GetSpaceUpdateRatio() float64 {
	return o.spaceUpdateRatio.Load().(float64)
}

// GetFlowUpdateRatio returns the flow update ratio.
func (o *options) GetFlowUpdateRatio() float64 {
	return o.flowUpdateRatio.Load().(float64)
}

// GetReportRatio returns the report ratio.
func (o *options) GetReportRatio() float64 {
	return o.reportRatio.Load().(float64)
}

// SetOptions sets the option.
func (o *options) SetOptions(cfg *config) {
	o.hotStoreCount.Store(cfg.HotStoreCount)
	o.leaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.epochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.spaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.flowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.reportRatio.Store(cfg.ReportRatio)
}
