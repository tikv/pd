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
)

const (
	clusterPath                = "raft"
	configPath                 = "config"
	schedulePath               = "schedule"
	gcPath                     = "gc"
	rulesPath                  = "rules"
	ruleGroupPath              = "rule_group"
	regionLabelPath            = "region_label"
	replicationPath            = "replication_mode"
	customScheduleConfigPath   = "scheduler_config"
	gcWorkerServiceSafePointID = "gc_worker"
	minResolvedTS              = "min_resolved_ts"
	keySpacePath               = "keySpace"
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

// AllKeySpacePrefix returns prefix for all key-spaces
// Path: /key-space/
func AllKeySpacePrefix() string {
	return keySpacePath + "/"
}

// keySpacePrefix returns path prefix for given key-space
// Prefix: /key-space/{space-id}
func keySpacePrefix(spaceID string) string {
	return path.Join(keySpacePath, spaceID)
}

// safePointPrefix returns path prefix for given key-space's safe point
// Prefix: /key-space/{space-id}/gc/safepoint
func safePointPrefix(spaceID string) string {
	return path.Join(keySpacePrefix(spaceID), "gc", "safepoint")
}

// ServiceSafePointPrefix returns the prefix of given service's service safe point
// It ends with a "/" for more precise searching
// Prefix: /key-space/{space-id}/gc/safepoint/service/
func ServiceSafePointPrefix(spaceID string) string {
	return path.Join(safePointPrefix(spaceID), "service") + "/"
}

// KeySpaceGCSafePointPath returns the gc safe point's path of the given key-space
// Path: /key-space/{space-id}/gc/safepoint/gc
func KeySpaceGCSafePointPath(spaceID string) string {
	return path.Join(safePointPrefix(spaceID), "gc")
}

// KeySpaceGCSafePointSuffix returns the postfix for any gc safepoint path
// Postfix: /gc/safepoint/gc
func KeySpaceGCSafePointSuffix() string {
	return "/" + path.Join("gc", "safepoint", "gc")
}

// ServiceSafePointPath returns the path of given service's service safe point
// Path: /key-space/{space-id}/gc/safepoint/service/{service-id}
func ServiceSafePointPath(spaceID, serviceID string) string {
	return path.Join(safePointPrefix(spaceID), "service", serviceID)
}
