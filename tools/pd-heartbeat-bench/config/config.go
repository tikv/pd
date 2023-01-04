package config

import (
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	logger   *zap.Logger
	logProps *log.ZapProperties

	StoreCount        int     `toml:"store-count" json:"store-count"`
	RegionCount       int     `toml:"region-count" json:"region-count"`
	KeyLength         int     `toml:"key-length" json:"key-length"`
	Replica           int     `toml:"replica" json:"replica"`
	LeaderUpdateRatio float64 `toml:"leader-update-ratio" json:"leader-update-ratio"`
	EpochUpdateRatio  float64 `toml:"epoch-update-ratio" json:"epoch-update-ratio"`
	SpaceUpdateRatio  float64 `toml:"space-update-ratio" json:"space-update-ratio"`
	FlowUpdateRatio   float64 `toml:"flow-update-ratio" json:"flow-update-ratio"`
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
	fs.StringVar(&cfg.PDAddr, "pd", "http://127.0.0.1:2379", "pd address")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "http://127.0.0.1:20180", "status address")

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
		meta, err = c.configFromFile(c.configFile)
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
	return nil
}

// Adjust is used to adjust configurations
func (c *Config) Adjust(meta *toml.MetaData) {
	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}
	if !meta.IsDefined("round") {
		adjustInt(&c.Round, defaultRound)
	}

	if !meta.IsDefined("store-count") {
		adjustInt(&c.StoreCount, defaultStoreCount)
	}
	if !meta.IsDefined("region-count") {
		adjustInt(&c.RegionCount, defaultRegionCount)
	}

	if !meta.IsDefined("key-length") {
		adjustInt(&c.KeyLength, defaultKeyLength)
	}

	if !meta.IsDefined("replica") {
		adjustInt(&c.Replica, defaultReplica)
	}

	if !meta.IsDefined("leader-update-ratio") {
		adjustFloat64(&c.LeaderUpdateRatio, defaultLeaderUpdateRatio)
	}
	if !meta.IsDefined("epoch-update-ratio") {
		adjustFloat64(&c.EpochUpdateRatio, defaultEpochUpdateRatio)
	}
	if !meta.IsDefined("space-update-ratio") {
		adjustFloat64(&c.SpaceUpdateRatio, defaultSpaceUpdateRatio)
	}
	if !meta.IsDefined("flow-update-ratio") {
		adjustFloat64(&c.FlowUpdateRatio, defaultFlowUpdateRatio)
	}
	if !meta.IsDefined("sample") {
		c.Sample = defaultSample
	}
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, c)
	return &meta, err
}

// SetupLogger setup the logger.
func (c *Config) SetupLogger() error {
	lg, p, err := log.InitLogger(&c.Log, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return err
	}
	c.logger = lg
	c.logProps = p
	return nil
}

// GetZapLogger gets the created zap logger.
func (c *Config) GetZapLogger() *zap.Logger {
	return c.logger
}

// GetZapLogProperties gets properties of the zap logger.
func (c *Config) GetZapLogProperties() *log.ZapProperties {
	return c.logProps
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt(v *int, defValue int) {
	if *v == 0 {
		*v = defValue
	}
}
