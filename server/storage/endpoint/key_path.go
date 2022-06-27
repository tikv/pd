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

package endpoint

import (
	"fmt"
	"path"
	"strconv"
)

const (
	clusterPath                = "raft"
	configPath                 = "config"
	serviceMiddlewarePath      = "service_middleware"
	schedulePath               = "schedule"
	gcPath                     = "gc"
	rulesPath                  = "rules"
	ruleGroupPath              = "rule_group"
	regionLabelPath            = "region_label"
	replicationPath            = "replication_mode"
	customScheduleConfigPath   = "scheduler_config"
	gcWorkerServiceSafePointID = "gc_worker"
	minResolvedTS              = "min_resolved_ts"
	keyspaceSafePointPath      = "keyspace/gc_safe_point"
	keyspaceGCSafePointSuffix  = "gc"
)

// AppendToRootPath appends the given key to the rootPath.
func AppendToRootPath(rootPath string, key string) string {
	return path.Join(rootPath, key)
}

// ClusterRootPath appends the `clusterPath` to the rootPath.
func ClusterRootPath(rootPath string) string {
	return AppendToRootPath(rootPath, clusterPath)
}

// ClusterBootstrapTimeKey returns the path to save the cluster bootstrap timestamp.
func ClusterBootstrapTimeKey() string {
	return path.Join(clusterPath, "status", "raft_bootstrap_time")
}

func scheduleConfigPath(scheduleName string) string {
	return path.Join(customScheduleConfigPath, scheduleName)
}

// StorePath returns the store meta info key path with the given store ID.
func StorePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// RegionPath returns the region meta info key path with the given region ID.
func RegionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

func ruleKeyPath(ruleKey string) string {
	return path.Join(rulesPath, ruleKey)
}

func ruleGroupIDPath(groupID string) string {
	return path.Join(ruleGroupPath, groupID)
}

func regionLabelKeyPath(ruleKey string) string {
	return path.Join(regionLabelPath, ruleKey)
}

func replicationModePath(mode string) string {
	return path.Join(replicationPath, mode)
}

func gcSafePointPath() string {
	return path.Join(gcPath, "safe_point")
}

// GCSafePointServicePrefixPath returns the GC safe point service key path prefix.
func GCSafePointServicePrefixPath() string {
	return path.Join(gcSafePointPath(), "service") + "/"
}

func gcSafePointServicePath(serviceID string) string {
	return path.Join(gcSafePointPath(), "service", serviceID)
}

// MinResolvedTSPath returns the min resolved ts path
func MinResolvedTSPath() string {
	return path.Join(clusterPath, minResolvedTS)
}

// KeyspaceSafePointPrefix returns prefix for given keyspace's safe points
// Prefix: /keyspace/gc_safe_point/{space_id}
func KeyspaceSafePointPrefix(spaceID uint32) string {
	spaceIDStr := strconv.FormatUint(uint64(spaceID), 10)
	return path.Join(keyspaceSafePointPath, spaceIDStr)
}

// KeyspaceGCSafePointPath returns the gc safe point's path of the given keyspace.
// Path: /keyspace/gc_safe_point/{space_id}/gc
func KeyspaceGCSafePointPath(spaceID uint32) string {
	return path.Join(KeyspaceSafePointPrefix(spaceID), keyspaceGCSafePointSuffix)
}

// KeyspaceServiceSafePointPrefix returns the prefix of given service's service safe point.
// Prefix: /keyspace/gc_safe_point/{space_id}/service/
func KeyspaceServiceSafePointPrefix(spaceID uint32) string {
	return path.Join(KeyspaceSafePointPrefix(spaceID), "service") + "/"
}

// KeyspaceServiceSafePointPath returns the path of given service's service safe point.
// Path: /keyspace/gc_safe_point/{space_id}/service/{service_id}
func KeyspaceServiceSafePointPath(spaceID uint32, serviceID string) string {
	return path.Join(KeyspaceServiceSafePointPrefix(spaceID), serviceID)
}

// KeyspaceSafePointPath returns the path to keyspace safe point storage.
// Path: keyspace/gc_safe_point/
func KeyspaceSafePointPath() string {
	return keyspaceSafePointPath + "/"
}

// KeySpaceGCSafePointSuffix returns the suffix for any gc safe point.
// Postfix: /gc
func KeySpaceGCSafePointSuffix() string {
	return "/" + keyspaceGCSafePointSuffix
}
