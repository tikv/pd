// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

var (
	requestUnitList map[rmpb.RequestUnitType]struct{} = map[rmpb.RequestUnitType]struct{}{
		rmpb.RequestUnitType_RRU: {},
		rmpb.RequestUnitType_WRU: {},
	}
	requestResourceList map[rmpb.RawResourceType]struct{} = map[rmpb.RawResourceType]struct{}{
		rmpb.RawResourceType_IOReadFlow:  {},
		rmpb.RawResourceType_IOWriteFlow: {},
		rmpb.RawResourceType_CPU:         {},
	}
)

const (
	initialRequestUnits = 10000
	bufferRUs           = 2000
	// movingAvgFactor is the weight applied to a new "sample" of RU usage (with one
	// sample per mainLoopUpdateInterval).
	//
	// If we want a factor of 0.5 per second, this should be:
	//
	//	0.5^(1 second / mainLoopUpdateInterval)
	movingAvgFactor                = 0.5
	notifyFraction                 = 0.1
	consumptionsReportingThreshold = 100
	extendedReportingPeriodFactor  = 4
	defaultGroupLoopUpdateInterval = 1 * time.Second
	defaultTargetPeriod            = 10 * time.Second
	defaultMaxRequestTokens        = 1e8
)

const (
	defaultReadBaseCost     = 1
	defaultReadCostPerByte  = 1. / 1024 / 1024
	defaultReadCPUMsCost    = 1
	defaultWriteBaseCost    = 3
	defaultWriteCostPerByte = 5. / 1024 / 1024
)

// RequestUnitConfig is the configuration of the request units, which determines the coefficients of
// the RRU and WRU cost. This configuration should be modified carefully.
type RequestUnitConfig struct {
	// ReadBaseCost is the base cost for a read request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	ReadBaseCost float64 `toml:"read-base-cost" json:"read-base-cost"`
	// ReadCostPerByte is the cost for each byte read. It's 1 MiB = 1 RRU by default.
	ReadCostPerByte float64 `toml:"read-cost-per-byte" json:"read-cost-per-byte"`
	// ReadCPUMsCost is the cost for each millisecond of CPU time taken by a read request.
	// It's 1 millisecond = 1 RRU by default.
	ReadCPUMsCost float64 `toml:"read-cpu-ms-cost" json:"read-cpu-ms-cost"`
	// WriteBaseCost is the base cost for a write request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	WriteBaseCost float64 `toml:"write-base-cost" json:"write-base-cost"`
	// WriteCostPerByte is the cost for each byte written. It's 1 MiB = 5 WRU by default.
	WriteCostPerByte float64 `toml:"write-cost-per-byte" json:"write-cost-per-byte"`
}

// DefaultRequestUnitConfig returns the default request unit configuration.
func DefaultRequestUnitConfig() *RequestUnitConfig {
	return &RequestUnitConfig{
		ReadBaseCost:     defaultReadBaseCost,
		ReadCostPerByte:  defaultReadCostPerByte,
		ReadCPUMsCost:    defaultReadCPUMsCost,
		WriteBaseCost:    defaultWriteBaseCost,
		WriteCostPerByte: defaultWriteCostPerByte,
	}
}

// Config is the configuration of the resource units, which gives the read/write request
// units or request resource cost standards. It should be calculated by a given `RequestUnitConfig`
// or `RequestResourceConfig`.
type Config struct {
	groupLoopUpdateInterval time.Duration
	targetPeriod            time.Duration
	maxRequestTokens        float64

	ReadBaseCost   RequestUnit
	ReadBytesCost  RequestUnit
	ReadCPUMsCost  RequestUnit
	WriteBaseCost  RequestUnit
	WriteBytesCost RequestUnit
	// TODO: add SQL computing CPU cost.
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	cfg := generateConfig(
		DefaultRequestUnitConfig(),
	)
	return cfg
}

func generateConfig(ruConfig *RequestUnitConfig) *Config {
	cfg := &Config{
		ReadBaseCost:   RequestUnit(ruConfig.ReadBaseCost),
		ReadBytesCost:  RequestUnit(ruConfig.ReadCostPerByte),
		ReadCPUMsCost:  RequestUnit(ruConfig.ReadCPUMsCost),
		WriteBaseCost:  RequestUnit(ruConfig.WriteBaseCost),
		WriteBytesCost: RequestUnit(ruConfig.WriteCostPerByte),
	}
	cfg.groupLoopUpdateInterval = defaultGroupLoopUpdateInterval
	cfg.targetPeriod = defaultTargetPeriod
	cfg.maxRequestTokens = defaultMaxRequestTokens
	return cfg
}
