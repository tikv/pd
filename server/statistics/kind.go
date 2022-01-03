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

package statistics

// RegionStatKind represents the statistics type of region.
type RegionStatKind int

// Different region statistics kinds.
const (
	RegionReadBytes RegionStatKind = iota
	RegionReadKeys
	RegionReadQuery
	RegionWriteBytes
	RegionWriteKeys
	RegionWriteQuery

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
	case RegionReadQuery:
		return "read_query"
	case RegionWriteQuery:
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

// sourceKind represents the statistics item source.
type sourceKind int

const (
	direct  sourceKind = iota // there is a corresponding peer in this store.
	inherit                   // there is no a corresponding peer in this store and there is a peer just deleted.
	adopt                     // there is no corresponding peer in this store and there is no peer just deleted, we need to copy from other stores.
)

func (k sourceKind) String() string {
	switch k {
	case direct:
		return "direct"
	case inherit:
		return "inherit"
	case adopt:
		return "adopt"
	}
	return "unknown"
}

// RWType is a identify hot region types.
type RWType int

// Flags for r/w type.
const (
	Write RWType = iota
	Read
)

func (k RWType) String() string {
	switch k {
	case Write:
		return "write"
	case Read:
		return "read"
	}
	return "unimplemented"
}

// RegionStats returns hot items according to kind
func (k RWType) RegionStats() []RegionStatKind {
	switch k {
	case Write:
		return []RegionStatKind{RegionWriteBytes, RegionWriteKeys, RegionWriteQuery}
	case Read:
		return []RegionStatKind{RegionReadBytes, RegionReadKeys, RegionReadQuery}
	}
	return nil
}

// ActionType indicates the action type for the stat item.
type ActionType int

// Flags for action type.
const (
	Add ActionType = iota
	Remove
	Update
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
