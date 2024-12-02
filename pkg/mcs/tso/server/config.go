// Copyright 2023 TiKV Project Authors.
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

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

const (
	defaultMaxResetTSGap = 24 * time.Hour

	defaultName             = "tso"
	defaultBackendEndpoints = "http://127.0.0.1:2379"
	defaultListenAddr       = "http://127.0.0.1:3379"

	defaultTSOSaveInterval           = time.Duration(constant.DefaultLeaderLease) * time.Second
	defaultTSOUpdatePhysicalInterval = 50 * time.Millisecond
	maxTSOUpdatePhysicalInterval     = 10 * time.Second
	minTSOUpdatePhysicalInterval     = 1 * time.Millisecond
)

var _ tso.ServiceConfig = (*Config)(nil)

// Config is the configuration for the TSO.
type Config struct {
	BackendEndpoints    string `toml:"backend-endpoints" json:"backend-endpoints"`
	ListenAddr          string `toml:"listen-addr" json:"listen-addr"`
	AdvertiseListenAddr string `toml:"advertise-listen-addr" json:"advertise-listen-addr"`

	Name              string `toml:"name" json:"name"`
	DataDir           string `toml:"data-dir" json:"data-dir"`
	EnableGRPCGateway bool   `json:"enable-grpc-gateway"`

	// LeaderLease defines the time within which a TSO primary/leader must update its TTL
	// in etcd, otherwise etcd will expire the leader key and other servers can campaign
	// the primary/leader again. Etcd only supports seconds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// EnableLocalTSO is used to enable the Local TSO Allocator feature,
	// which allows the PD server to generate Local TSO for certain DC-level transactions.
	// To make this feature meaningful, user has to set the "zone" label for the PD server
	// to indicate which DC this PD belongs to.
	EnableLocalTSO bool `toml:"enable-local-tso" json:"enable-local-tso"`

	// TSOSaveInterval is the interval to save timestamp.
	TSOSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// The interval to update physical part of timestamp. Usually, this config should not be set.
	// At most 1<<18 (262144) TSOs can be generated in the interval. The smaller the value, the
	// more TSOs provided, and at the same time consuming more CPU time.
	// This config is only valid in 1ms to 10s. If it's configured too long or too short, it will
	// be automatically clamped to the range.
	TSOUpdatePhysicalInterval typeutil.Duration `toml:"tso-update-physical-interval" json:"tso-update-physical-interval"`

	// MaxResetTSGap is the max gap to reset the TSO.
	MaxResetTSGap typeutil.Duration `toml:"max-gap-reset-ts" json:"max-gap-reset-ts"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	// WarningMsgs contains all warnings during parsing.
	WarningMsgs []string

	// Log related config.
	Log log.Config `toml:"log" json:"log"`

	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security configutil.SecurityConfig `toml:"security" json:"security"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
}

// GetName returns the Name
func (c *Config) GetName() string {
	return c.Name
}

// GeBackendEndpoints returns the BackendEndpoints
func (c *Config) GeBackendEndpoints() string {
	return c.BackendEndpoints
}

// GetListenAddr returns the ListenAddr
func (c *Config) GetListenAddr() string {
	return c.ListenAddr
}

// GetAdvertiseListenAddr returns the AdvertiseListenAddr
func (c *Config) GetAdvertiseListenAddr() string {
	return c.AdvertiseListenAddr
}

// GetLeaderLease returns the leader lease.
func (c *Config) GetLeaderLease() int64 {
	return c.LeaderLease
}

// GetTSOUpdatePhysicalInterval returns TSO update physical interval.
func (c *Config) GetTSOUpdatePhysicalInterval() time.Duration {
	return c.TSOUpdatePhysicalInterval.Duration
}

// GetTSOSaveInterval returns TSO save interval.
func (c *Config) GetTSOSaveInterval() time.Duration {
	return c.TSOSaveInterval.Duration
}

// GetMaxResetTSGap returns the MaxResetTSGap.
func (c *Config) GetMaxResetTSGap() time.Duration {
	return c.MaxResetTSGap.Duration
}

// GetTLSConfig returns the TLS config.
func (c *Config) GetTLSConfig() *grpcutil.TLSConfig {
	return &c.Security.TLSConfig
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(flagSet *pflag.FlagSet) error {
	// Load config file if specified.
	var (
		meta *toml.MetaData
		err  error
	)
	if configFile, _ := flagSet.GetString("config"); configFile != "" {
		meta, err = configutil.ConfigFromFile(c, configFile)
		if err != nil {
			return err
		}
	}

	// Ignore the error check here
	configutil.AdjustCommandLineString(flagSet, &c.Name, "name")
	configutil.AdjustCommandLineString(flagSet, &c.Log.Level, "log-level")
	configutil.AdjustCommandLineString(flagSet, &c.Log.File.Filename, "log-file")
	configutil.AdjustCommandLineString(flagSet, &c.Metric.PushAddress, "metrics-addr")
	configutil.AdjustCommandLineString(flagSet, &c.Security.CAPath, "cacert")
	configutil.AdjustCommandLineString(flagSet, &c.Security.CertPath, "cert")
	configutil.AdjustCommandLineString(flagSet, &c.Security.KeyPath, "key")
	configutil.AdjustCommandLineString(flagSet, &c.BackendEndpoints, "backend-endpoints")
	configutil.AdjustCommandLineString(flagSet, &c.ListenAddr, "listen-addr")
	configutil.AdjustCommandLineString(flagSet, &c.AdvertiseListenAddr, "advertise-listen-addr")

	return c.Adjust(meta)
}

// Adjust is used to adjust the TSO configurations.
func (c *Config) Adjust(meta *toml.MetaData) error {
	configMetaData := configutil.NewConfigMetadata(meta)
	if err := configMetaData.CheckUndecoded(); err != nil {
		c.WarningMsgs = append(c.WarningMsgs, err.Error())
	}
	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		configutil.AdjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	configutil.AdjustString(&c.DataDir, fmt.Sprintf("default-datadir.%s", c.Name))
	configutil.AdjustPath(&c.DataDir)

	if err := c.Validate(); err != nil {
		return err
	}

	configutil.AdjustString(&c.BackendEndpoints, defaultBackendEndpoints)
	configutil.AdjustString(&c.ListenAddr, defaultListenAddr)
	configutil.AdjustString(&c.AdvertiseListenAddr, c.ListenAddr)

	configutil.AdjustDuration(&c.MaxResetTSGap, defaultMaxResetTSGap)
	configutil.AdjustInt64(&c.LeaderLease, constant.DefaultLeaderLease)
	configutil.AdjustDuration(&c.TSOSaveInterval, defaultTSOSaveInterval)
	configutil.AdjustDuration(&c.TSOUpdatePhysicalInterval, defaultTSOUpdatePhysicalInterval)

	if c.TSOUpdatePhysicalInterval.Duration > maxTSOUpdatePhysicalInterval {
		c.TSOUpdatePhysicalInterval.Duration = maxTSOUpdatePhysicalInterval
	} else if c.TSOUpdatePhysicalInterval.Duration < minTSOUpdatePhysicalInterval {
		c.TSOUpdatePhysicalInterval.Duration = minTSOUpdatePhysicalInterval
	}
	if c.TSOUpdatePhysicalInterval.Duration != defaultTSOUpdatePhysicalInterval {
		log.Warn("tso update physical interval is non-default",
			zap.Duration("update-physical-interval", c.TSOUpdatePhysicalInterval.Duration))
	}

	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = constant.DefaultEnableGRPCGateway
	}

	c.adjustLog(configMetaData.Child("log"))
	return c.Security.Encryption.Adjust()
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = constant.DefaultDisableErrorVerbose
	}
	configutil.AdjustString(&c.Log.Format, constant.DefaultLogFormat)
	configutil.AdjustString(&c.Log.Level, constant.DefaultLogLevel)
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	logFile, err := filepath.Abs(c.Log.File.Filename)
	if err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(dataDir, filepath.Dir(logFile))
	if err != nil {
		return errors.WithStack(err)
	}
	if !strings.HasPrefix(rel, "..") {
		return errors.New("log directory shouldn't be the subdirectory of data directory")
	}

	return nil
}
