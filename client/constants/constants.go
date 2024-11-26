// Copyright 2024 TiKV Project Authors.
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

package constants

import "time"

const (
	// DefaultKeyspaceID is the default keyspace ID.
	// Valid keyspace id range is [0, 0xFFFFFF](uint24max, or 16777215)
	// ​0 is reserved for default keyspace with the name "DEFAULT", It's initialized
	// when PD bootstrap and reserved for users who haven't been assigned keyspace.
	DefaultKeyspaceID = uint32(0)
	// MaxKeyspaceID is the maximum keyspace ID.
	MaxKeyspaceID = uint32(0xFFFFFF)
	// NullKeyspaceID is used for API v1 or legacy path where is keyspace agnostic.
	NullKeyspaceID = uint32(0xFFFFFFFF)
	// DefaultKeyspaceGroupID is the default key space group id.
	// We also reserved 0 for the keyspace group for the same purpose.
	DefaultKeyspaceGroupID = uint32(0)
	// DefaultKeyspaceName is the default keyspace name.
	DefaultKeyspaceName = "DEFAULT"

	// RetryInterval is the base retry interval.
	RetryInterval = 500 * time.Millisecond
	// MaxRetryTimes is the max retry times.
	MaxRetryTimes = 6
)
