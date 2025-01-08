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

package controller

import (
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

var (
	requestUnitLimitTypeList map[rmpb.RequestUnitType]struct{} = map[rmpb.RequestUnitType]struct{}{
		rmpb.RequestUnitType_RU: {},
	}
	requestResourceLimitTypeList map[rmpb.RawResourceType]struct{} = map[rmpb.RawResourceType]struct{}{
		rmpb.RawResourceType_IOReadFlow:  {},
		rmpb.RawResourceType_IOWriteFlow: {},
		rmpb.RawResourceType_CPU:         {},
	}
)

const (
	// movingAvgFactor is the weight applied to a new "sample" of RU usage (with one
	// sample per mainLoopUpdateInterval).
	//
	// If we want a factor of 0.5 per second, this should be:
	//
	//	0.5^(1 second / mainLoopUpdateInterval)
	movingAvgFactor                = 0.5
	notifyFraction                 = 0.1
	tokenReserveFraction           = 0.8
	consumptionsReportingThreshold = 100
	extendedReportingPeriodFactor  = 4
	// defaultGroupCleanupInterval is the interval to clean up the deleted resource groups in memory.
	defaultGroupCleanupInterval = 5 * time.Minute
	// defaultGroupStateUpdateInterval is the interval to update the state of the resource groups.
	defaultGroupStateUpdateInterval = 1 * time.Second
	// targetPeriod indicate how long it is expected to cost token when acquiring token.
	// According to the resource control Grafana panel and Prometheus sampling period, the period should be the factor of 15.
	defaultTargetPeriod = 5 * time.Second
	// defaultMaxWaitDuration is the max duration to wait for the token before throwing error.
	defaultMaxWaitDuration = 30 * time.Second
	// defaultLTBTokenRPCMaxDelay is the upper bound of backoff delay for local token bucket RPC.
	defaultLTBTokenRPCMaxDelay = 1 * time.Second
	// defaultWaitRetryTimes is the times to retry when waiting for the token.
	defaultWaitRetryTimes = 20
	// defaultWaitRetryInterval is the interval to retry when waiting for the token.
	defaultWaitRetryInterval = 50 * time.Millisecond
)

const (

	// 1 RU = 8 storage read requests
	defaultReadBaseCost = 1. / 8 // 0.125
	// 1 RU = 2 storage read batch requests
	defaultReadPerBatchBaseCost = 1. / 2 // 0.5
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
	defaultDegradedModeWaitDuration = time.Duration(0)
	defaultAvgBatchProportion       = 0.7
)

// TokenRPCParams is the parameters for local bucket RPC.
type TokenRPCParams struct {
	// WaitRetryInterval is the interval to retry when waiting for the token.
	WaitRetryInterval Duration `toml:"wait-retry-interval" json:"wait-retry-interval"`

	// WaitRetryTimes is the times to retry when waiting for the token.
	WaitRetryTimes int `toml:"wait-retry-times" json:"wait-retry-times"`
}

// LocalBucketConfig is the configuration for local bucket. not export to server side.
type LocalBucketConfig struct {
	TokenRPCParams `toml:"token-rpc-params" json:"token-rpc-params"`
}

// BaseConfig is the configuration of the resource manager controller which includes some option for client needed.
// TODO: unified the configuration for client and server, server side in pkg/mcs/resourcemanger/config.go.
type BaseConfig struct {
	// EnableDegradedMode is to control whether resource control client enable degraded mode when server is disconnect.
	DegradedModeWaitDuration Duration `toml:"degraded-mode-wait-duration" json:"degraded-mode-wait-duration"`

	// LTBMaxWaitDuration is the max wait time duration for local token bucket.
	LTBMaxWaitDuration Duration `toml:"ltb-max-wait-duration" json:"ltb-max-wait-duration"`

	// LTBTokenRPCMaxDelay is the upper bound of backoff delay for local token bucket RPC.
	LTBTokenRPCMaxDelay Duration `toml:"ltb-token-rpc-max-delay" json:"ltb-token-rpc-max-delay"`

	// RequestUnit is the configuration determines the coefficients of the RRU and WRU cost.
	// This configuration should be modified carefully.
	RequestUnit RequestUnitConfig `toml:"request-unit" json:"request-unit"`

	// EnableControllerTraceLog is to control whether resource control client enable trace.
	EnableControllerTraceLog bool `toml:"enable-controller-trace-log" json:"enable-controller-trace-log,string"`
}

// Config is the configuration of the resource manager controller.
type Config struct {
	BaseConfig
	LocalBucketConfig
}

// Adjust adjusts the configuration.
func (c *Config) Adjust() {
	// valid the configuration, TODO: separately add the valid function.
	if c.BaseConfig.LTBMaxWaitDuration.Duration == 0 {
		c.BaseConfig.LTBMaxWaitDuration = NewDuration(defaultMaxWaitDuration)
	}
	if c.LocalBucketConfig.WaitRetryInterval.Duration == 0 {
		c.LocalBucketConfig.WaitRetryInterval = NewDuration(defaultWaitRetryInterval)
	}
	// adjust the client settings. calculate the retry times.
	if int(c.BaseConfig.LTBTokenRPCMaxDelay.Duration) != int(c.LocalBucketConfig.WaitRetryInterval.Duration)*c.LocalBucketConfig.WaitRetryTimes {
		c.LocalBucketConfig.WaitRetryTimes = int(c.BaseConfig.LTBTokenRPCMaxDelay.Duration / c.LocalBucketConfig.WaitRetryInterval.Duration)
	}
}

// DefaultConfig returns the default resource manager controller configuration.
func DefaultConfig() *Config {
	return &Config{
		BaseConfig: BaseConfig{
			DegradedModeWaitDuration: NewDuration(defaultDegradedModeWaitDuration),
			RequestUnit:              DefaultRequestUnitConfig(),
			EnableControllerTraceLog: false,
			LTBMaxWaitDuration:       NewDuration(defaultMaxWaitDuration),
			LTBTokenRPCMaxDelay:      NewDuration(defaultLTBTokenRPCMaxDelay),
		},
		LocalBucketConfig: LocalBucketConfig{
			TokenRPCParams: TokenRPCParams{
				WaitRetryInterval: NewDuration(defaultWaitRetryInterval),
				WaitRetryTimes:    defaultWaitRetryTimes,
			},
		},
	}
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

// DefaultRequestUnitConfig returns the default request unit configuration.
func DefaultRequestUnitConfig() RequestUnitConfig {
	return RequestUnitConfig{
		ReadBaseCost:          defaultReadBaseCost,
		ReadPerBatchBaseCost:  defaultReadPerBatchBaseCost,
		ReadCostPerByte:       defaultReadCostPerByte,
		WriteBaseCost:         defaultWriteBaseCost,
		WritePerBatchBaseCost: defaultWritePerBatchBaseCost,
		WriteCostPerByte:      defaultWriteCostPerByte,
		CPUMsCost:             defaultCPUMsCost,
	}
}

// RUConfig is the configuration of the resource units, which gives the read/write request
// units or request resource cost standards. It should be calculated by a given `RequestUnitConfig`
// or `RequestResourceConfig`.
type RUConfig struct {
	// RU model config
	ReadBaseCost          RequestUnit
	ReadPerBatchBaseCost  RequestUnit
	ReadBytesCost         RequestUnit
	WriteBaseCost         RequestUnit
	WritePerBatchBaseCost RequestUnit
	WriteBytesCost        RequestUnit
	CPUMsCost             RequestUnit
	// The CPU statistics need to distinguish between different environments.
	isSingleGroupByKeyspace bool

	// some config for client
	LTBMaxWaitDuration       time.Duration
	WaitRetryInterval        time.Duration
	WaitRetryTimes           int
	DegradedModeWaitDuration time.Duration
}

// DefaultRUConfig returns the default configuration.
func DefaultRUConfig() *RUConfig {
	return GenerateRUConfig(
		DefaultConfig(),
	)
}

// GenerateRUConfig generates the configuration by the given request unit configuration.
func GenerateRUConfig(config *Config) *RUConfig {
	return &RUConfig{
		ReadBaseCost:             RequestUnit(config.RequestUnit.ReadBaseCost),
		ReadPerBatchBaseCost:     RequestUnit(config.RequestUnit.ReadPerBatchBaseCost),
		ReadBytesCost:            RequestUnit(config.RequestUnit.ReadCostPerByte),
		WriteBaseCost:            RequestUnit(config.RequestUnit.WriteBaseCost),
		WritePerBatchBaseCost:    RequestUnit(config.RequestUnit.WritePerBatchBaseCost),
		WriteBytesCost:           RequestUnit(config.RequestUnit.WriteCostPerByte),
		CPUMsCost:                RequestUnit(config.RequestUnit.CPUMsCost),
		LTBMaxWaitDuration:       config.LTBMaxWaitDuration.Duration,
		WaitRetryInterval:        config.WaitRetryInterval.Duration,
		WaitRetryTimes:           config.WaitRetryTimes,
		DegradedModeWaitDuration: config.DegradedModeWaitDuration.Duration,
	}
}
