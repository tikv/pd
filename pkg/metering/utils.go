// Copyright 2025 TiKV Project Authors.
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

package metering

import (
	"github.com/pingcap/metering_sdk/common"
)

const (
	// SourceNamePD is the source name of the PD.
	SourceNamePD = "pd"

	// UnitRU is the unit of the metering RU.
	UnitRU = "RU"
	// UnitBytes is the unit of the metering bytes.
	UnitBytes = "Bytes"
	// UnitRequests is the unit of the metering requests.
	UnitRequests = "Requests"

	// DataVersionField is the version field of the metering data.
	DataVersionField = "version"
	// DataClusterIDField is the cluster ID field of the metering data.
	DataClusterIDField = "cluster_id"
	// DataSourceNameField is the source name field of the metering data.
	DataSourceNameField = "source_name"
)

// NewRUValue creates a new metering RU value.
func NewRUValue(value float64) common.MeteringValue {
	return common.MeteringValue{Value: uint64(value), Unit: UnitRU}
}

// NewBytesValue creates a new metering bytes value.
func NewBytesValue(value uint64) common.MeteringValue {
	return common.MeteringValue{Value: value, Unit: UnitBytes}
}

// NewRequestsValue creates a new metering requests value.
func NewRequestsValue(value uint64) common.MeteringValue {
	return common.MeteringValue{Value: value, Unit: UnitRequests}
}
