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
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	defaultName             = "resource manager"
	defaultBackendEndpoints = "http://127.0.0.1:2379"
	defaultListenAddr       = "http://127.0.0.1:3379"

	// 1 RU = 8 storage read requests
	defaultReadBaseCost = 1. / 8
	// 1 RU = 2 storage read batch requests
	defaultReadPerBatchBaseCost = 1. / 2
	// 1 RU = 1 storage write request
	defaultWriteBaseCost = 1
	// 1 RU = 1 storage write batch request
	defaultWritePerBatchBaseCost = 1
	// 1 RU = 64 KiB read bytes
	defaultReadCostPerByte = 1. / (64 * 1024)
	// 1 RU = 1 KiB written bytes
	defaultWriteCostPerByte = 1. / 1024
	// 1 RU = 3 millisecond CPU time
	defaultCPUMsCost = 1. / 3

	// Because the resource manager has not been deployed in microservice mode,
	// do not enable this function.
	defaultDegradedModeWaitDuration = time.Second * 0
	// defaultMaxWaitDuration is the max duration to wait for the token before throwing error.
	defaultMaxWaitDuration = 30 * time.Second
	// defaultLTBTokenRPCMaxDelay is the upper bound of backoff delay for local token bucket RPC.
	defaultLTBTokenRPCMaxDelay = 1 * time.Second
)

// Config is the configuration for the resource manager.
type Config struct {
	BackendEndpoints    string `toml:"backend-endpoints" json:"backend-endpoints"`
	ListenAddr          string `toml:"listen-addr" json:"listen-addr"`
	AdvertiseListenAddr string `toml:"advertise-listen-addr" json:"advertise-listen-addr"`
	Name                string `toml:"name" json:"name"`
	DataDir             string `toml:"data-dir" json:"data-dir"` // TODO: remove this after refactoring
	EnableGRPCGateway   bool   `json:"enable-grpc-gateway"`      // TODO: use it

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	// Log related config.
	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	// WarningMsgs contains all warnings during parsing.
	WarningMsgs []string

	// LeaderLease defines the time within which a Resource Manager primary/leader must
	// update its TTL in etcd, otherwise etcd will expire the leader key and other servers
	// can campaign the primary/leader again. Etcd only supports seconds TTL, so here is
	// second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	Controller ControllerConfig `toml:"controller" json:"controller"`
}

// ControllerConfig is the configuration of the resource manager controller which includes some option for client needed.
type ControllerConfig struct {
	// EnableDegradedMode is to control whether resource control client enable degraded mode when server is disconnect.
	DegradedModeWaitDuration typeutil.Duration `toml:"degraded-mode-wait-duration" json:"degraded-mode-wait-duration"`

	// LTBMaxWaitDuration is the max wait time duration for local token bucket.
	LTBMaxWaitDuration typeutil.Duration `toml:"ltb-max-wait-duration" json:"ltb-max-wait-duration"`

	// LTBTokenRPCMaxDelay is the upper bound of backoff delay for local token bucket RPC.
	LTBTokenRPCMaxDelay typeutil.Duration `toml:"ltb-token-rpc-max-delay" json:"ltb-token-rpc-max-delay"`

	// RequestUnit is the configuration determines the coefficients of the RRU and WRU cost.
	// This configuration should be modified carefully.
	RequestUnit RequestUnitConfig `toml:"request-unit" json:"request-unit"`

	// EnableControllerTraceLog is to control whether resource control client enable trace.
	EnableControllerTraceLog bool `toml:"enable-controller-trace-log" json:"enable-controller-trace-log,string"`
}

// Adjust adjusts the configuration and initializes it with the default value if necessary.
func (rmc *ControllerConfig) Adjust(meta *configutil.ConfigMetaData) {
	if rmc == nil {
		return
	}
	rmc.RequestUnit.Adjust(meta.Child("request-unit"))
	if !meta.IsDefined("degraded-mode-wait-duration") {
		configutil.AdjustDuration(&rmc.DegradedModeWaitDuration, defaultDegradedModeWaitDuration)
	}
	if !meta.IsDefined("ltb-max-wait-duration") {
		configutil.AdjustDuration(&rmc.LTBMaxWaitDuration, defaultMaxWaitDuration)
	}
	if !meta.IsDefined("ltb-token-rpc-max-delay") {
		configutil.AdjustDuration(&rmc.LTBTokenRPCMaxDelay, defaultLTBTokenRPCMaxDelay)
	}
	failpoint.Inject("enableDegradedModeAndTraceLog", func() {
		configutil.AdjustDuration(&rmc.DegradedModeWaitDuration, time.Second)
		configutil.AdjustBool(&rmc.EnableControllerTraceLog, true)
	})
}

// RequestUnitConfig is the configuration of the request units, which determines the coefficients of
// the RRU and WRU cost. This configuration should be modified carefully.
type RequestUnitConfig struct {
	// ReadBaseCost is the base cost for a read request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	ReadBaseCost float64 `toml:"read-base-cost" json:"read-base-cost"`
	// ReadPerBatchBaseCost is the base cost for a read request with batch.
	ReadPerBatchBaseCost float64 `toml:"read-per-batch-base-cost" json:"read-per-batch-base-cost"`
	// ReadCostPerByte is the cost for each byte read. It's 1 RU = 64 KiB by default.
	ReadCostPerByte float64 `toml:"read-cost-per-byte" json:"read-cost-per-byte"`
	// WriteBaseCost is the base cost for a write request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	WriteBaseCost float64 `toml:"write-base-cost" json:"write-base-cost"`
	// WritePerBatchBaseCost is the base cost for a write request with batch.
	WritePerBatchBaseCost float64 `toml:"write-per-batch-base-cost" json:"write-per-batch-base-cost"`
	// WriteCostPerByte is the cost for each byte written. It's 1 RU = 1 KiB by default.
	WriteCostPerByte float64 `toml:"write-cost-per-byte" json:"write-cost-per-byte"`
	// CPUMsCost is the cost for each millisecond of CPU time taken.
	// It's 1 RU = 3 millisecond by default.
	CPUMsCost float64 `toml:"read-cpu-ms-cost" json:"read-cpu-ms-cost"`
}

// Adjust adjusts the configuration and initializes it with the default value if necessary.
func (ruc *RequestUnitConfig) Adjust(meta *configutil.ConfigMetaData) {
	if ruc == nil {
		return
	}
	if !meta.IsDefined("read-base-cost") {
		configutil.AdjustFloat64(&ruc.ReadBaseCost, defaultReadBaseCost)
	}
	if !meta.IsDefined("read-per-batch-base-cost") {
		configutil.AdjustFloat64(&ruc.ReadPerBatchBaseCost, defaultReadPerBatchBaseCost)
	}
	if !meta.IsDefined("read-cost-per-byte") {
		configutil.AdjustFloat64(&ruc.ReadCostPerByte, defaultReadCostPerByte)
	}
	if !meta.IsDefined("write-base-cost") {
		configutil.AdjustFloat64(&ruc.WriteBaseCost, defaultWriteBaseCost)
	}
	if !meta.IsDefined("write-per-batch-base-cost") {
		configutil.AdjustFloat64(&ruc.WritePerBatchBaseCost, defaultWritePerBatchBaseCost)
	}
	if !meta.IsDefined("write-cost-per-byte") {
		configutil.AdjustFloat64(&ruc.WriteCostPerByte, defaultWriteCostPerByte)
	}
	if !meta.IsDefined("read-cpu-ms-cost") {
		configutil.AdjustFloat64(&ruc.CPUMsCost, defaultCPUMsCost)
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

// Adjust is used to adjust the resource manager configurations.
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
	configutil.AdjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))
	configutil.AdjustPath(&c.DataDir)

	if err := c.Validate(); err != nil {
		return err
	}

	configutil.AdjustString(&c.BackendEndpoints, defaultBackendEndpoints)
	configutil.AdjustString(&c.ListenAddr, defaultListenAddr)
	configutil.AdjustString(&c.AdvertiseListenAddr, c.ListenAddr)

	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = constant.DefaultEnableGRPCGateway
	}

	c.adjustLog(configMetaData.Child("log"))
	if err := c.Security.Encryption.Adjust(); err != nil {
		return err
	}

	c.Controller.Adjust(configMetaData.Child("controller"))
	configutil.AdjustInt64(&c.LeaderLease, constant.DefaultLeaderLease)

	return nil
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = constant.DefaultDisableErrorVerbose
	}
	configutil.AdjustString(&c.Log.Format, constant.DefaultLogFormat)
	configutil.AdjustString(&c.Log.Level, constant.DefaultLogLevel)
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
