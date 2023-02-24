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

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"go.uber.org/zap"
)

const (
	defaultName              = "Resource Manager"
	defaultBackendEndpoints  = "127.0.0.1:2379"
	defaultListenAddr        = "127.0.0.1:3380"
	defaultEnableGRPCGateway = true

	defaultLogFormat           = "text"
	defaultDisableErrorVerbose = true

	defaultReadBaseCost  = 0.25
	defaultWriteBaseCost = 1
	// 1 RU = 64 KiB read bytes
	defaultReadCostPerByte = 1. / (64 * 1024)
	// 1 RU = 1 KiB written bytes
	defaultWriteCostPerByte = 1. / 1024
	// 1 RU = 3 millisecond CPU time
	defaultCPUMsCost = 1. / 3
)

// Config is the configuration for the resource manager.
type Config struct {
	BackendEndpoints  string `toml:"backend-endpoints" json:"backend-endpoints"`
	ListenAddr        string `toml:"listen-addr" json:"listen-addr"`
	Name              string `toml:"name" json:"name"`
	DataDir           string `toml:"data-dir" json:"data-dir"` // TODO: remove this after refactoring
	EnableGRPCGateway bool   `json:"enable-grpc-gateway"`      // TODO: use it

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	// Log related config.
	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	// RequestUnit is the configuration determines the coefficients of the RRU and WRU cost.
	// This configuration should be modified carefully.
	RequestUnit RequestUnitConfig
}

// RequestUnitConfig is the configuration of the request units, which determines the coefficients of
// the RRU and WRU cost.
type RequestUnitConfig struct {
	// ReadBaseCost is the base cost for a read request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	ReadBaseCost float64 `toml:"read-base-cost" json:"read-base-cost"`
	// ReadCostPerByte is the cost for each byte read. It's 1 RU = 64 KiB by default.
	ReadCostPerByte float64 `toml:"read-cost-per-byte" json:"read-cost-per-byte"`
	// WriteBaseCost is the base cost for a write request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	WriteBaseCost float64 `toml:"write-base-cost" json:"write-base-cost"`
	// WriteCostPerByte is the cost for each byte written. It's 1 RU = 1 KiB by default.
	WriteCostPerByte float64 `toml:"write-cost-per-byte" json:"write-cost-per-byte"`
	// CPUMsCost is the cost for each millisecond of CPU time taken.
	// It's 1 RU = 3 millisecond by default.
	CPUMsCost float64 `toml:"read-cpu-ms-cost" json:"read-cpu-ms-cost"`
}

// Adjust adjusts the configuration and initializes it with the default value if necessary.
func (ruc *RequestUnitConfig) Adjust() {
	if ruc == nil {
		return
	}
	if ruc.ReadBaseCost == 0 {
		ruc.ReadBaseCost = defaultReadBaseCost
	}
	if ruc.ReadCostPerByte == 0 {
		ruc.ReadCostPerByte = defaultReadCostPerByte
	}
	if ruc.WriteBaseCost == 0 {
		ruc.WriteBaseCost = defaultWriteBaseCost
	}
	if ruc.WriteCostPerByte == 0 {
		ruc.WriteCostPerByte = defaultWriteCostPerByte
	}
	if ruc.CPUMsCost == 0 {
		ruc.CPUMsCost = defaultCPUMsCost
	}
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
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
	configutil.AdjustCommandlineString(flagSet, &c.Log.Level, "log-level")
	configutil.AdjustCommandlineString(flagSet, &c.Log.File.Filename, "log-file")
	configutil.AdjustCommandlineString(flagSet, &c.Metric.PushAddress, "metrics-addr")
	configutil.AdjustCommandlineString(flagSet, &c.Security.CAPath, "cacert")
	configutil.AdjustCommandlineString(flagSet, &c.Security.CertPath, "cert")
	configutil.AdjustCommandlineString(flagSet, &c.Security.KeyPath, "key")
	configutil.AdjustCommandlineString(flagSet, &c.BackendEndpoints, "backend-endpoints")
	configutil.AdjustCommandlineString(flagSet, &c.ListenAddr, "listen-addr")

	return c.Adjust(meta, false)
}

// Adjust is used to adjust the resource manager configurations.
func (c *Config) Adjust(meta *toml.MetaData, reloading bool) error {
	configMetaData := configutil.NewConfigMetadata(meta)
	warningMsgs := make([]string, 0)
	if err := configMetaData.CheckUndecoded(); err != nil {
		warningMsgs = append(warningMsgs, err.Error())
	}
	configutil.PrintConfigCheckMsg(os.Stdout, warningMsgs)

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		configutil.AdjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	configutil.AdjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))
	c.adjustPath()

	if err := c.Validate(); err != nil {
		return err
	}

	configutil.AdjustString(&c.BackendEndpoints, defaultBackendEndpoints)
	configutil.AdjustString(&c.ListenAddr, defaultListenAddr)

	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = defaultEnableGRPCGateway
	}

	c.adjustLog(configMetaData.Child("log"))
	c.Security.Encryption.Adjust()

	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}

	c.RequestUnit.Adjust()

	return nil
}

func (c *Config) adjustPath() {
	absPath, err := filepath.Abs(c.DataDir)
	if err == nil {
		c.DataDir = absPath
	}
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = defaultDisableErrorVerbose
	}
}

// GetTLSConfig returns the TLS config.
func (c *Config) GetTLSConfig() *grpcutil.TLSConfig {
	return &c.Security.TLSConfig
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
