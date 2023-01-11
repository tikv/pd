// Copyright 2022 TiKV Project Authors.
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

package tenantclient

import (
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

var (
	ruLen                                             = len(rmpb.RequestUnitType_name)
	resourceLen                                       = len(rmpb.ResourceType_name)
	requestUnitList map[rmpb.RequestUnitType]struct{} = map[rmpb.RequestUnitType]struct{}{
		rmpb.RequestUnitType_RRU: {},
		rmpb.RequestUnitType_WRU: {},
	}
	requestResourceList map[rmpb.ResourceType]struct{} = map[rmpb.ResourceType]struct{}{
		rmpb.ResourceType_ReadBytes:      {},
		rmpb.ResourceType_WriteBytes:     {},
		rmpb.ResourceType_TotalCPUTimeMs: {},
	}
)

const (
	initialRequestUnits = 10000
	bufferRUs           = 5000
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
)

const (
	defaultReadBaseCost     = 1
	defaultReadCostPerByte  = 1. / 1024 / 1024
	defaultWriteBaseCost    = 5
	defaultWriteCostPerByte = 10. / 1024 / 1024
	defaultWriteCPUMsCost   = 1
)

// RequestUnitConfig is the configuration of the request units, which determines the coefficients of
// the RRU and WRU cost.
type RequestUnitConfig struct {
	// ReadBaseCost is the base cost for a read request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	ReadBaseCost float64 `toml:"read-base-cost" json:"read-base-cost"`
	// ReadCostPerByte is the cost for each byte read. It's 1 MiB = 1 RRU by default.
	ReadCostPerByte float64 `toml:"read-cost-per-byte" json:"read-cost-per-byte"`
	// WriteBaseCost is the base cost for a write request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	WriteBaseCost float64 `toml:"write-base-cost" json:"write-base-cost"`
	// WriteCostPerByte is the cost for each byte written. It's 1 MiB = 10 WRU by default.
	WriteCostPerByte float64 `toml:"write-cost-per-byte" json:"write-cost-per-byte"`
	// WriteCPUMsCost is the cost for each millisecond of CPU time taken by a write request.
	// It's 1 millisecond = 1 WRU by default.
	WriteCPUMsCost float64 `toml:"write-cpu-ms-cost" json:"write-cpu-ms-cost"`
}

// DefaultRequestUnitConfig returns the default request unit configuration.
func DefaultRequestUnitConfig() *RequestUnitConfig {
	return &RequestUnitConfig{
		ReadBaseCost:     defaultReadBaseCost,
		ReadCostPerByte:  defaultReadCostPerByte,
		WriteBaseCost:    defaultWriteBaseCost,
		WriteCostPerByte: defaultWriteCostPerByte,
		WriteCPUMsCost:   defaultWriteCPUMsCost,
	}
}

// Config is the configuration of the resource units, which gives the read/write request
// units or request resource cost standards. It should be calculated by a given `RequestUnitConfig`
// or `RequestResourceConfig`.
type Config struct {
	groupLoopUpdateInterval time.Duration
	targetPeriod            time.Duration

	ReadBaseCost   RequestUnit
	ReadBytesCost  RequestUnit
	WriteBaseCost  RequestUnit
	WriteBytesCost RequestUnit
	WriteCPUMsCost RequestUnit
	// TODO: add SQL computing CPU cost.
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	cfg := generateConfig(
		DefaultRequestUnitConfig(),
	)
	cfg.groupLoopUpdateInterval = defaultGroupLoopUpdateInterval
	cfg.targetPeriod = defaultTargetPeriod
	return cfg
}

func generateConfig(ruConfig *RequestUnitConfig) *Config {
	return &Config{
		ReadBaseCost:   RequestUnit(ruConfig.ReadBaseCost),
		ReadBytesCost:  RequestUnit(ruConfig.ReadCostPerByte),
		WriteBaseCost:  RequestUnit(ruConfig.WriteBaseCost),
		WriteBytesCost: RequestUnit(ruConfig.WriteCostPerByte),
		WriteCPUMsCost: RequestUnit(ruConfig.WriteCPUMsCost),
	}
}
