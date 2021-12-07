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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

// RegionStatKind represents the statistics type of region.
type RegionStatKind int

// Different region statistics kinds.
const (
	RegionReadBytes RegionStatKind = iota
	RegionReadKeys
	RegionWriteBytes
	RegionWriteKeys

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
	StoreCPUUsage
	StoreDiskReadRate
	StoreDiskWriteRate

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
	case StoreWriteKeys:
		return "store_write_keys"
	case StoreCPUUsage:
		return "store_cpu_usage"
	case StoreDiskReadRate:
		return "store_disk_read_rate"
	case StoreDiskWriteRate:
		return "store_disk_write_rate"
	}

	return "unknown StoreStatKind"
}
<<<<<<< HEAD
=======

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
type RWType uint32

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
>>>>>>> 2246ef626 (statistics: fix the problem that the hot cache cannot be emptied when the interval is less than 60 (#4396))
