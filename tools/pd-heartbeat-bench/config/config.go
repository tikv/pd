package config

import (
	"math"
	"sync/atomic"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
	"go.uber.org/zap"
)

const (
	defaultStoreCount        = 50
	defaultRegionCount       = 1000000
	defaultKeyLength         = 56
	defaultReplica           = 3
	defaultLeaderUpdateRatio = 0.06
	defaultEpochUpdateRatio  = 0.04
	defaultSpaceUpdateRatio  = 0.15
	defaultFlowUpdateRatio   = 0.35
	defaultNoUpdateRatio     = 0
	defaultRound             = 0
	defaultSample            = false

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

	StoreCount        int     `toml:"store-count" json:"store-count"`
	RegionCount       int     `toml:"region-count" json:"region-count"`
	KeyLength         int     `toml:"key-length" json:"key-length"`
	Replica           int     `toml:"replica" json:"replica"`
	LeaderUpdateRatio float64 `toml:"leader-update-ratio" json:"leader-update-ratio"`
	EpochUpdateRatio  float64 `toml:"epoch-update-ratio" json:"epoch-update-ratio"`
	SpaceUpdateRatio  float64 `toml:"space-update-ratio" json:"space-update-ratio"`
	FlowUpdateRatio   float64 `toml:"flow-update-ratio" json:"flow-update-ratio"`
	NoUpdateRatio     float64 `toml:"no-update-ratio" json:"no-update-ratio"`
	Sample            bool    `toml:"sample" json:"sample"`
	Round             int     `toml:"round" json:"round"`
}

// NewConfig return a set of settings.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("heartbeat-bench", flag.ContinueOnError)
	fs := cfg.flagSet
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.StringVar(&cfg.PDAddr, "pd-endpoints", "127.0.0.1:2379", "pd address")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "127.0.0.1:20180", "status address")

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
	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}
	if !meta.IsDefined("round") {
		configutil.AdjustInt(&c.Round, defaultRound)
	}

	if !meta.IsDefined("store-count") {
		configutil.AdjustInt(&c.StoreCount, defaultStoreCount)
	}
	if !meta.IsDefined("region-count") {
		configutil.AdjustInt(&c.RegionCount, defaultRegionCount)
	}

	if !meta.IsDefined("key-length") {
		configutil.AdjustInt(&c.KeyLength, defaultKeyLength)
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
	if !meta.IsDefined("no-update-ratio") {
		configutil.AdjustFloat64(&c.NoUpdateRatio, defaultNoUpdateRatio)
	}
	if !meta.IsDefined("sample") {
		c.Sample = defaultSample
	}
}

// Validate is used to validate configurations
func (c *Config) Validate() error {
	if c.LeaderUpdateRatio < 0 || c.LeaderUpdateRatio > 1 {
		return errors.Errorf("leader-update-ratio must be in [0, 1]")
	}
	if c.EpochUpdateRatio < 0 || c.EpochUpdateRatio > 1 {
		return errors.Errorf("epoch-update-ratio must be in [0, 1]")
	}
	if c.SpaceUpdateRatio < 0 || c.SpaceUpdateRatio > 1 {
		return errors.Errorf("space-update-ratio must be in [0, 1]")
	}
	if c.FlowUpdateRatio < 0 || c.FlowUpdateRatio > 1 {
		return errors.Errorf("flow-update-ratio must be in [0, 1]")
	}
	if c.NoUpdateRatio < 0 || c.NoUpdateRatio > 1 {
		return errors.Errorf("no-update-ratio must be in [0, 1]")
	}
	max := math.Max(c.LeaderUpdateRatio, math.Max(c.EpochUpdateRatio, math.Max(c.SpaceUpdateRatio, c.FlowUpdateRatio)))
	if max+c.NoUpdateRatio > 1 {
		return errors.Errorf("sum of update-ratio must be in [0, 1]")
	}
	return nil
}

// Clone creates a copy of current config.
func (c *Config) Clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

// Options is the option of the heartbeat-bench.
type Options struct {
	LeaderUpdateRatio atomic.Value
	EpochUpdateRatio  atomic.Value
	SpaceUpdateRatio  atomic.Value
	FlowUpdateRatio   atomic.Value
	NoUpdateRatio     atomic.Value
}

// NewOptions creates a new option.
func NewOptions(cfg *Config) *Options {
	o := &Options{}
	o.LeaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.EpochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.SpaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.FlowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.NoUpdateRatio.Store(cfg.NoUpdateRatio)
	return o
}

// GetLeaderUpdateRatio returns the leader update ratio.
func (o *Options) GetLeaderUpdateRatio() float64 {
	return o.LeaderUpdateRatio.Load().(float64)
}

// GetEpochUpdateRatio returns the epoch update ratio.
func (o *Options) GetEpochUpdateRatio() float64 {
	return o.EpochUpdateRatio.Load().(float64)
}

// GetSpaceUpdateRatio returns the space update ratio.
func (o *Options) GetSpaceUpdateRatio() float64 {
	return o.SpaceUpdateRatio.Load().(float64)
}

// GetFlowUpdateRatio returns the flow update ratio.
func (o *Options) GetFlowUpdateRatio() float64 {
	return o.FlowUpdateRatio.Load().(float64)
}

// GetNoUpdateRatio returns the no update ratio.
func (o *Options) GetNoUpdateRatio() float64 {
	return o.NoUpdateRatio.Load().(float64)
}

// SetOptions sets the option.
func (o *Options) SetOptions(cfg *Config) {
	o.LeaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.EpochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.SpaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.FlowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.NoUpdateRatio.Store(cfg.NoUpdateRatio)
}
