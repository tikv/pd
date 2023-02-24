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
	// defaultGroupCleanupInterval is the interval to clean up the deleted resource groups in memory.
	defaultGroupCleanupInterval = 10 * time.Minute
	// defaultGroupStateUpdateInterval is the interval to update the state of the resource groups.
	defaultGroupStateUpdateInterval = 1 * time.Second
	// targetPeriod indicate how long it is expected to cost token when acquiring token.
	defaultTargetPeriod = 10 * time.Second
)

const (
	defaultReadBaseCost  = 0.25
	defaultWriteBaseCost = 1
	// 1 RU = 64 KiB read bytes
	defaultReadCostPerByte = 1. / (64 * 1024)
	// 1 RU = 1 KiB written bytes
	defaultWriteCostPerByte = 1. / 1024
	// 1 RU = 3 millisecond CPU time
	defaultCPUMsCost = 1. / 3
)

// RequestUnitConfig is the configuration of the request units, which determines the coefficients of
// the RRU and WRU cost. This configuration should be modified carefully.
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

// defaultRequestUnitConfig returns the default request unit configuration.
func defaultRequestUnitConfig() *RequestUnitConfig {
	return &RequestUnitConfig{
		ReadBaseCost:     defaultReadBaseCost,
		ReadCostPerByte:  defaultReadCostPerByte,
		WriteBaseCost:    defaultWriteBaseCost,
		WriteCostPerByte: defaultWriteCostPerByte,
		CPUMsCost:        defaultCPUMsCost,
	}
}

// Config is the configuration of the resource units, which gives the read/write request
// units or request resource cost standards. It should be calculated by a given `RequestUnitConfig`
// or `RequestResourceConfig`.
type Config struct {
	// RU model config
	ReadBaseCost   RequestUnit
	ReadBytesCost  RequestUnit
	WriteBaseCost  RequestUnit
	WriteBytesCost RequestUnit
	CPUMsCost      RequestUnit
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	cfg := generateConfig(
		defaultRequestUnitConfig(),
	)
	return cfg
}

func generateConfig(ruConfig *RequestUnitConfig) *Config {
	cfg := &Config{
		ReadBaseCost:   RequestUnit(ruConfig.ReadBaseCost),
		ReadBytesCost:  RequestUnit(ruConfig.ReadCostPerByte),
		WriteBaseCost:  RequestUnit(ruConfig.WriteBaseCost),
		WriteBytesCost: RequestUnit(ruConfig.WriteCostPerByte),
		CPUMsCost:      RequestUnit(ruConfig.CPUMsCost),
	}
	return cfg
}
