// Copyright 2022 TiKV Project Authors.
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

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

const (
	// Config is the path to save the PD config.
	Config       = "config"
	schedulePath = "schedule"

	replicationPath = "replication_mode"
	// GCWorkerServiceSafePointID is the service id of GC worker.
	GCWorkerServiceSafePointID = "gc_worker"

	globalTSOAllocatorEtcdPrefix = "gta"
	// TimestampKey is the key of timestamp oracle used for the suffix.
	TimestampKey = "timestamp"
)

// StoreLeaderWeightPath returns the store leader weight key path with the given store ID.
func StoreLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

// StoreRegionWeightPath returns the store region weight key path with the given store ID.
func StoreRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// ReplicationModePath returns the path to save the replication mode with the given mode.
func ReplicationModePath(mode string) string {
	return path.Join(replicationPath, mode)
}

// KeyspaceGroupGlobalTSPath constructs the timestampOracle path prefix for Global TSO, which is:
//  1. for the default keyspace group:
//     "" in /pd/{cluster_id}/timestamp
//  2. for the non-default keyspace groups:
//     {group}/gta in /ms/{cluster_id}/tso/{group}/gta/timestamp
func KeyspaceGroupGlobalTSPath(groupID uint32) string {
	if groupID == constant.DefaultKeyspaceGroupID {
		return ""
	}
	return path.Join(fmt.Sprintf("%05d", groupID), globalTSOAllocatorEtcdPrefix)
}

// TimestampPath returns the timestamp path for the given timestamp oracle path prefix.
func TimestampPath(tsPath string) string {
	return path.Join(tsPath, TimestampKey)
}
