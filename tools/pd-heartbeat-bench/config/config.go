package config

import (
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
	MetricsAddr       string  `toml:"metrics-addr" json:"metrics-addr"`
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
func (c *Config) Validate() error {
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

// Options is the option of the heartbeat-bench.
type Options struct {
	HotStoreCount atomic.Value
	ReportRatio   atomic.Value

	LeaderUpdateRatio atomic.Value
	EpochUpdateRatio  atomic.Value
	SpaceUpdateRatio  atomic.Value
	FlowUpdateRatio   atomic.Value
}

// NewOptions creates a new option.
func NewOptions(cfg *Config) *Options {
	o := &Options{}
	o.HotStoreCount.Store(cfg.HotStoreCount)
	o.LeaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.EpochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.SpaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.FlowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.ReportRatio.Store(cfg.ReportRatio)
	return o
}

// GetHotStoreCount returns the hot store count.
func (o *Options) GetHotStoreCount() int {
	return o.HotStoreCount.Load().(int)
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

// GetReportRatio returns the report ratio.
func (o *Options) GetReportRatio() float64 {
	return o.ReportRatio.Load().(float64)
}

// SetOptions sets the option.
func (o *Options) SetOptions(cfg *Config) {
	o.HotStoreCount.Store(cfg.HotStoreCount)
	o.LeaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.EpochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.SpaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.FlowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.ReportRatio.Store(cfg.ReportRatio)
}
