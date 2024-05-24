// Copyright 2020 TiKV Project Authors.
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

const (
	// BytePriority indicates hot-region-scheduler prefer byte dim
	BytePriority = "byte"
	// KeyPriority indicates hot-region-scheduler prefer key dim
	KeyPriority = "key"
	// QueryPriority indicates hot-region-scheduler prefer query dim
	QueryPriority = "query"
)

// Indicator dims.
const (
	ByteDim int = iota
	KeyDim
	QueryDim
	DimLen
)

// StringToDim return dim according to string.
func StringToDim(name string) int {
	switch name {
	case BytePriority:
		return ByteDim
	case KeyPriority:
		return KeyDim
	case QueryPriority:
		return QueryDim
	}
	return ByteDim
}

// DimToString return string according to dim.
func DimToString(dim int) string {
	switch dim {
	case ByteDim:
		return BytePriority
	case KeyDim:
		return KeyPriority
	case QueryDim:
		return QueryPriority
	default:
		return ""
	}
}

// RegionStatKind represents the statistics type of region.
type RegionStatKind int

// Different region statistics kinds.
const (
	RegionReadBytes RegionStatKind = iota
	RegionReadKeys
	RegionReadQueryNum
	RegionWriteBytes
	RegionWriteKeys
	RegionWriteQueryNum

	RegionStatCount
)

func (k RegionStatKind) String() string {
	switch k {
	case RegionReadBytes:
		return "read_bytes"
	case RegionReadKeys:
		return "read_keys"
	case RegionWriteBytes:
		return "write_bytes"
	case RegionWriteKeys:
		return "write_keys"
	case RegionReadQueryNum:
		return "read_query"
	case RegionWriteQueryNum:
		return "write_query"
	}
	return "unknown RegionStatKind"
}

// StoreStatKind represents the statistics type of store.
type StoreStatKind int

// Different store statistics kinds.
const (
	StoreReadBytes StoreStatKind = iota
	StoreReadKeys
	StoreWriteBytes
	StoreWriteKeys
	StoreReadQuery
	StoreWriteQuery
	StoreCPUUsage
	StoreDiskReadRate
	StoreDiskWriteRate

	StoreRegionsWriteBytes // Same as StoreWriteBytes, but it is counted by RegionHeartbeat.
	StoreRegionsWriteKeys  // Same as StoreWriteKeys, but it is counted by RegionHeartbeat.

	StoreStatCount
)

func (k StoreStatKind) String() string {
	switch k {
	case StoreReadBytes:
		return "store_read_bytes"
	case StoreReadKeys:
		return "store_read_keys"
	case StoreWriteBytes:
		return "store_write_bytes"
	case StoreReadQuery:
		return "store_read_query"
	case StoreWriteQuery:
		return "store_write_query"
	case StoreWriteKeys:
		return "store_write_keys"
	case StoreCPUUsage:
		return "store_cpu_usage"
	case StoreDiskReadRate:
		return "store_disk_read_rate"
	case StoreDiskWriteRate:
		return "store_disk_write_rate"
	case StoreRegionsWriteBytes:
		return "store_regions_write_bytes"
	case StoreRegionsWriteKeys:
		return "store_regions_write_keys"
	}

	return "unknown StoreStatKind"
}

// SourceKind represents the statistics item source.
type SourceKind int

// Different statistics item sources.
const (
	Direct  SourceKind = iota // there is a corresponding peer in this store.
	Inherit                   // there is no corresponding peer in this store and we need to copy from other stores.
)

func (k SourceKind) String() string {
	switch k {
	case Direct:
		return "direct"
	case Inherit:
		return "inherit"
	}
	return "unknown"
}

// RWType is a identify hot region types.
type RWType int

// Flags for r/w type.
const (
	Write RWType = iota
	Read
	RWTypeLen
)

func (rw RWType) String() string {
	switch rw {
	case Write:
		return "write"
	case Read:
		return "read"
	}
	return "unimplemented"
}

var (
	writeRegionStats = []RegionStatKind{RegionWriteBytes, RegionWriteKeys, RegionWriteQueryNum}
	readRegionStats  = []RegionStatKind{RegionReadBytes, RegionReadKeys, RegionReadQueryNum}
)

// RegionStats returns hot items according to kind
func (rw RWType) RegionStats() []RegionStatKind {
	switch rw {
	case Write:
		return writeRegionStats
	case Read:
		return readRegionStats
	}
	return nil
}

// Inverse returns the opposite of kind.
func (rw RWType) Inverse() RWType {
	switch rw {
	case Write:
		return Read
	default: // Case Read
		return Write
	}
}

// ReportInterval returns the report interval of read or write.
func (rw RWType) ReportInterval() int {
	switch rw {
	case Write:
		return RegionHeartBeatReportInterval
	default: // Case Read
		return StoreHeartBeatReportInterval
	}
}

// DefaultAntiCount returns the default anti count of read or write.
func (rw RWType) DefaultAntiCount() int {
	switch rw {
	case Write:
		return HotRegionAntiCount
	default: // Case Read
		return HotRegionAntiCount * (RegionHeartBeatReportInterval / StoreHeartBeatReportInterval)
	}
}

// GetLoadRates gets the load rates of the read or write type.
func (rw RWType) GetLoadRates(deltaLoads []float64, interval uint64) []float64 {
	loads := make([]float64, DimLen)
	for dim, k := range rw.RegionStats() {
		loads[dim] = deltaLoads[k] / float64(interval)
	}
	return loads
}

// SetFullLoadRates set load rates to full as read or write type.
func (rw RWType) SetFullLoadRates(full []float64, loads []float64) {
	for dim, k := range rw.RegionStats() {
		full[k] = loads[dim]
	}
}

// ForeachRegionStats foreach all region stats of read and write.
func ForeachRegionStats(f func(RWType, int, RegionStatKind)) {
	for _, rwTy := range []RWType{Read, Write} {
		for dim, kind := range rwTy.RegionStats() {
			f(rwTy, dim, kind)
		}
	}
}

// ActionType indicates the action type for the stat item.
type ActionType int

// Flags for action type.
const (
	Add ActionType = iota
	Remove
	Update
	ActionTypeLen
)

func (t ActionType) String() string {
	switch t {
	case Add:
		return "add"
	case Remove:
		return "remove"
	case Update:
		return "update"
	}
	return "unimplemented"
}
