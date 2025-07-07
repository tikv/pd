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

package constant

const (
	// DefaultKeyspaceName is the name reserved for default keyspace.
	DefaultKeyspaceName = "DEFAULT"

	// ZeroKeyspaceID is the start keyspace for keyspace id.
	ZeroKeyspaceID = uint32(0)

	// DefaultKeyspaceID is the default key space id.
	// 0 is reserved for default keyspace with the name "DEFAULT". It's initialized when PD bootstraps
	// and reserved for users who haven't been assigned keyspace.
	DefaultKeyspaceID = uint32(0)
	// MaxValidKeyspaceID is the max valid keyspace id.
	// Valid keyspace id range is [0, 0xFFFFFF](uint24max, or 16777215)
	// In kv encode, the first byte is represented by r/x, which means txn kv/raw kv, so there are 24 bits left.
	MaxValidKeyspaceID = uint32(0xFFFFFF)

	// ValidKeyspaceIDMask is the mask of valid bits for keyspace ID. If any bit outside the mask is set, the keyspace
	// ID is considered invalid and regarded as the same as NullKeyspaceID.
	ValidKeyspaceIDMask = uint32(0xFFFFFF)
	// NullKeyspaceID is used for api v1 or legacy path where is keyspace agnostic.
	NullKeyspaceID = uint32(0xFFFFFFFF)
	// DefaultKeyspaceGroupID is the default key space group id.
	// We also reserved 0 for the keyspace group for the same purpose.
	DefaultKeyspaceGroupID = uint32(0)
)

// only for next gen
const (
	// ReservedKeyspaceIDCount is the reserved count for keyspace id.
	ReservedKeyspaceIDCount = uint64(1024)
	// ReservedKeyspaceIDStart is the start id for reserved keyspace id.
	// The reserved keyspace id range is [0xFFFFFF - 1024, 0xFFFFFF)
	ReservedKeyspaceIDStart = uint64(MaxValidKeyspaceID) - ReservedKeyspaceIDCount
	// SystemKeyspaceID is the system keyspace ID.
	SystemKeyspaceID = MaxValidKeyspaceID - 1
	// SystemKeyspaceName is the system keyspace name.
	SystemKeyspaceName = "SYSTEM"
)
