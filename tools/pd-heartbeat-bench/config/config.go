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
	"sync/atomic"

	"github.com/BurntSushi/toml"
	"github.com/docker/go-units"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
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
	defaultRegionKeys        = 960000
	defaultRandomSeed        = 1
	defaultRegionSize        = typeutil.ByteSize(96 * units.MiB)
	defaultStoreCapacity     = typeutil.ByteSize(8 * units.TiB)

	defaultLogFormat = "text"
)

// Config is the heartbeat-bench configuration.
type Config struct {
	flagSet    *flag.FlagSet
	configFile string
	PDAddr     string
	StatusAddr string

	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	InitEpochVer      uint64            `toml:"epoch-ver" json:"epoch-ver"`
	RegionSize        typeutil.ByteSize `toml:"region-size" json:"region-size"`
	RegionKeys        uint64            `toml:"region-keys" json:"region-keys"`
	StoreCapacity     typeutil.ByteSize `toml:"store-capacity" json:"store-capacity"`
	RandomSeed        uint64            `toml:"random-seed" json:"random-seed"`
	StoreCount        int               `toml:"store-count" json:"store-count"`
	HotStoreCount     int               `toml:"hot-store-count" json:"hot-store-count"`
	RegionCount       int               `toml:"region-count" json:"region-count"`
	Replica           int               `toml:"replica" json:"replica"`
	LeaderUpdateRatio float64           `toml:"leader-update-ratio" json:"leader-update-ratio"`
	EpochUpdateRatio  float64           `toml:"epoch-update-ratio" json:"epoch-update-ratio"`
	SpaceUpdateRatio  float64           `toml:"space-update-ratio" json:"space-update-ratio"`
	FlowUpdateRatio   float64           `toml:"flow-update-ratio" json:"flow-update-ratio"`
	ReportRatio       float64           `toml:"report-ratio" json:"report-ratio"`
	Sample            bool              `toml:"sample" json:"sample"`
	Round             int               `toml:"round" json:"round"`
	MetricsAddr       string            `toml:"metrics-addr" json:"metrics-addr"`
	DeleteOperators   bool              `toml:"delete-operators" json:"delete-operators"`
}

// NewConfig return a set of settings.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("heartbeat-bench", flag.ContinueOnError)
	fs := cfg.flagSet
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.StringVar(&cfg.PDAddr, "pd-endpoints", "127.0.0.1:2379", "pd leader address")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "127.0.0.1:20180", "status address")
	fs.StringVar(&cfg.Security.CAPath, "cacert", "", "path of file that contains list of trusted TLS CAs")
	fs.StringVar(&cfg.Security.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.Security.KeyPath, "key", "", "path of file that contains X509 key in PEM format")
	fs.Uint64Var(&cfg.InitEpochVer, "epoch-ver", 1, "the initial epoch version value")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "127.0.0.1:9090", "the address to pull metrics")

	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	var meta *toml.MetaData
	if c.configFile != "" {
		meta, err = configutil.ConfigFromFile(c, c.configFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	c.Adjust(meta)
	return c.Validate()
}

// Adjust is used to adjust configurations
func (c *Config) Adjust(meta *toml.MetaData) {
	isDefined := func(key string) bool {
		return meta != nil && meta.IsDefined(key)
	}
	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}
	if !isDefined("round") {
		configutil.AdjustInt(&c.Round, defaultRound)
	}

	if !isDefined("store-count") {
		configutil.AdjustInt(&c.StoreCount, defaultStoreCount)
	}
	if !isDefined("region-count") {
		configutil.AdjustInt(&c.RegionCount, defaultRegionCount)
	}

	if !isDefined("hot-store-count") {
		configutil.AdjustInt(&c.HotStoreCount, defaultHotStoreCount)
	}
	if !isDefined("replica") {
		configutil.AdjustInt(&c.Replica, defaultReplica)
	}

	if !isDefined("leader-update-ratio") {
		configutil.AdjustFloat64(&c.LeaderUpdateRatio, defaultLeaderUpdateRatio)
	}
	if !isDefined("epoch-update-ratio") {
		configutil.AdjustFloat64(&c.EpochUpdateRatio, defaultEpochUpdateRatio)
	}
	if !isDefined("space-update-ratio") {
		configutil.AdjustFloat64(&c.SpaceUpdateRatio, defaultSpaceUpdateRatio)
	}
	if !isDefined("flow-update-ratio") {
		configutil.AdjustFloat64(&c.FlowUpdateRatio, defaultFlowUpdateRatio)
	}
	if !isDefined("report-ratio") {
		configutil.AdjustFloat64(&c.ReportRatio, defaultReportRatio)
	}
	if !isDefined("sample") {
		c.Sample = defaultSample
	}
	if !isDefined("epoch-ver") {
		c.InitEpochVer = defaultInitialVersion
	}
	if !isDefined("region-size") {
		configutil.AdjustByteSize(&c.RegionSize, defaultRegionSize)
	}
	if !isDefined("region-keys") {
		configutil.AdjustUint64(&c.RegionKeys, defaultRegionKeys)
	}
	if !isDefined("store-capacity") {
		configutil.AdjustByteSize(&c.StoreCapacity, defaultStoreCapacity)
	}
	if !isDefined("random-seed") {
		configutil.AdjustUint64(&c.RandomSeed, defaultRandomSeed)
	}
}

// Validate is used to validate configurations
func (c *Config) Validate() error {
	if c.Round < 0 {
		return errors.Errorf("round must be greater than or equal to 0")
	}
	if c.InitEpochVer == 0 {
		return errors.Errorf("epoch-ver must be greater than 0")
	}
	if c.StoreCount <= 0 {
		return errors.Errorf("store-count must be greater than 0")
	}
	if c.RegionCount <= 0 {
		return errors.Errorf("region-count must be greater than 0")
	}
	if c.Replica <= 0 || c.Replica > c.StoreCount {
		return errors.Errorf("replica must be in [1, store-count]")
	}
	if c.RegionSize == 0 {
		return errors.Errorf("region-size must be greater than 0")
	}
	if c.RegionKeys == 0 {
		return errors.Errorf("region-keys must be greater than 0")
	}
	if c.StoreCapacity == 0 {
		return errors.Errorf("store-capacity must be greater than 0")
	}
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
func (c *Config) Clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

// WorkloadOptions is an immutable snapshot of the dynamically configurable
// heartbeat workload.
type WorkloadOptions struct {
	HotStoreCount     int
	ReportRatio       float64
	LeaderUpdateRatio float64
	EpochUpdateRatio  float64
	SpaceUpdateRatio  float64
	FlowUpdateRatio   float64
}

// Options stores the dynamically configurable heartbeat workload.
type Options struct {
	value atomic.Value
}

// NewOptions creates a new option.
func NewOptions(cfg *Config) *Options {
	o := &Options{}
	o.value.Store(workloadOptionsFromConfig(cfg))
	return o
}

func workloadOptionsFromConfig(cfg *Config) WorkloadOptions {
	return WorkloadOptions{
		HotStoreCount:     cfg.HotStoreCount,
		ReportRatio:       cfg.ReportRatio,
		LeaderUpdateRatio: cfg.LeaderUpdateRatio,
		EpochUpdateRatio:  cfg.EpochUpdateRatio,
		SpaceUpdateRatio:  cfg.SpaceUpdateRatio,
		FlowUpdateRatio:   cfg.FlowUpdateRatio,
	}
}

// Snapshot returns one consistent workload configuration.
func (o *Options) Snapshot() WorkloadOptions {
	return o.value.Load().(WorkloadOptions)
}

// GetHotStoreCount returns the hot store count.
func (o *Options) GetHotStoreCount() int {
	return o.Snapshot().HotStoreCount
}

// GetLeaderUpdateRatio returns the leader update ratio.
func (o *Options) GetLeaderUpdateRatio() float64 {
	return o.Snapshot().LeaderUpdateRatio
}

// GetEpochUpdateRatio returns the epoch update ratio.
func (o *Options) GetEpochUpdateRatio() float64 {
	return o.Snapshot().EpochUpdateRatio
}

// GetSpaceUpdateRatio returns the space update ratio.
func (o *Options) GetSpaceUpdateRatio() float64 {
	return o.Snapshot().SpaceUpdateRatio
}

// GetFlowUpdateRatio returns the flow update ratio.
func (o *Options) GetFlowUpdateRatio() float64 {
	return o.Snapshot().FlowUpdateRatio
}

// GetReportRatio returns the report ratio.
func (o *Options) GetReportRatio() float64 {
	return o.Snapshot().ReportRatio
}

// SetOptions sets the option.
func (o *Options) SetOptions(cfg *Config) {
	o.value.Store(workloadOptionsFromConfig(cfg))
}
