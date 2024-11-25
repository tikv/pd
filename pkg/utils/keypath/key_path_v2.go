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

package keypath

import (
	"fmt"
	"path"
)

const (
	leaderPathFormat          = "/pd/%d/leader"             // "/pd/{cluster_id}/leader"
	allocIDPathFormat         = "/pd/%d/alloc_id"           // "/pd/{cluster_id}/alloc_id"
	keyspaceAllocIDPathFormat = "/pd/%d/keyspaces/alloc_id" // "/pd/{cluster_id}/keyspaces/alloc_id"
)

// Prefix returns the parent directory of the given path.
func Prefix(str string) string {
	return path.Dir(str)
}

// LeaderPath returns the leader path.
func LeaderPath() string {

	return fmt.Sprintf(leaderPathFormat, ClusterID())
}

// AllocIDPath returns the alloc id path.
func AllocIDPath() string {
	return fmt.Sprintf(allocIDPathFormat, ClusterID())
}

// KeyspaceAllocIDPath returns the keyspace alloc id path.
func KeyspaceAllocIDPath() string {
	return fmt.Sprintf(keyspaceAllocIDPathFormat, ClusterID())
}
