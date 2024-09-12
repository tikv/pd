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

package utils

import (
	"github.com/docker/go-units"
)

const (
	// RegionHeartBeatReportInterval indicates the interval between write interval, the value is the heartbeat report interval of a region.
	RegionHeartBeatReportInterval = 60
	// StoreHeartBeatReportInterval indicates the interval between read stats report, the value is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10

	// HotRegionAntiCount is default value for antiCount
	HotRegionAntiCount = 2

	// DefaultAotSize is default size of average over time.
	DefaultAotSize = 1
	// DefaultWriteMfSize is default size of write median filter.
	DefaultWriteMfSize = 5
	// DefaultReadMfSize is default size of read median filter.
	DefaultReadMfSize = 5
)

// MinHotThresholds is the threshold at which this dimension is recorded as a hot spot.
var MinHotThresholds = [RegionStatCount]float64{
	RegionReadBytes:     8 * units.KiB,
	RegionReadKeys:      128,
	RegionReadQueryNum:  128,
	RegionWriteBytes:    1 * units.KiB,
	RegionWriteKeys:     32,
	RegionWriteQueryNum: 32,
}
