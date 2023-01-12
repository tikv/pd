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

package client

import (
	"time"

	"github.com/pingcap/kvproto/pkg/resource_manager"
)

var requestUnitList map[resource_manager.RequestUnitType]struct{} = map[resource_manager.RequestUnitType]struct{}{
	resource_manager.RequestUnitType_RRU: {},
	resource_manager.RequestUnitType_WRU: {},
}

var requestResourceList map[resource_manager.ResourceType]struct{} = map[resource_manager.ResourceType]struct{}{
	resource_manager.ResourceType_IOReadFlow:  {},
	resource_manager.ResourceType_IOWriteFlow: {},
	resource_manager.ResourceType_CPU:         {},
}

const initialRquestUnits = 10000

const bufferRUs = 5000

// movingAvgFactor is the weight applied to a new "sample" of RU usage (with one
// sample per mainLoopUpdateInterval).
//
// If we want a factor of 0.5 per second, this should be:
//
//	0.5^(1 second / mainLoopUpdateInterval)
const movingAvgFactor = 0.5

const notifyFraction = 0.1

const consumptionsReportingThreshold = 100

const extendedReportingPeriodFactor = 4

const defaultGroupLoopUpdateInterval = 1 * time.Second
const defaultTargetPeriod = 10 * time.Second
const (
	readRequestCost  = 1
	readCostPerByte  = 0.5 / 1024 / 1024
	writeRequestCost = 5
	writeCostPerByte = 200. / 1024 / 1024
	kvCPUMsCost      = 1
	sqlCPUSecondCost = 0
)

type Config struct {
	groupLoopUpdateInterval time.Duration
	targetPeriod            time.Duration

	ReadRequestCost  RequestUnit
	ReadBytesCost    RequestUnit
	WriteRequestCost RequestUnit
	WriteBytesCost   RequestUnit
	KVCPUMsCost      RequestUnit
	SQLCPUSecondCost RequestUnit
}

func DefaultConfig() *Config {
	cfg := &Config{
		groupLoopUpdateInterval: defaultGroupLoopUpdateInterval,
		targetPeriod:            defaultTargetPeriod,
		ReadRequestCost:         RequestUnit(readRequestCost),
		ReadBytesCost:           RequestUnit(readCostPerByte),
		WriteRequestCost:        RequestUnit(writeRequestCost),
		WriteBytesCost:          RequestUnit(writeCostPerByte),
		KVCPUMsCost:             RequestUnit(kvCPUMsCost),
		SQLCPUSecondCost:        RequestUnit(sqlCPUSecondCost),
	}
	return cfg
}
