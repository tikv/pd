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
	"strings"
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
	externalTimeStamp          = "external_timestamp"
	keyspaceSafePointPrefix    = "keyspaces/gc_safepoint"
	keyspaceGCSafePointSuffix  = "gc"
	keyspacePrefix             = "keyspaces"
	keyspaceMetaInfix          = "meta"
	keyspaceIDInfix            = "id"
	keyspaceAllocID            = "alloc_id"
	regionPathPrefix           = "raft/r"
	// resource group storage endpoint has prefix `resource_group`
	resourceGroupSettingsPath = "settings"
	resourceGroupStatesPath   = "states"
	requestUnitConfigPath     = "ru_config"
	// tso storage endpoint has prefix `tso`
	microserviceKey = "microservice"
	tsoServiceKey   = "tso"
	timestampKey    = "timestamp"
	// localTSOSuffix is the same as `localTSOSuffixEtcdPrefix` defined in `pkg/tso/allocator_manager.go`.
	localTSOSuffix = "lts"

	// we use uint64 to represent ID, the max length of uint64 is 20.
	keyLen = 20
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
	var buf strings.Builder
	buf.WriteString(regionPathPrefix)
	buf.WriteString("/")
	s := strconv.FormatUint(regionID, 10)
	if len(s) > keyLen {
		s = s[len(s)-keyLen:]
	} else {
		b := make([]byte, keyLen)
		diff := keyLen - len(s)
		for i := 0; i < keyLen; i++ {
			if i < diff {
				b[i] = 48
			} else {
				b[i] = s[i-diff]
			}
		}
		s = string(b)
	}
	buf.WriteString(s)

	return buf.String()
}

func resourceGroupSettingKeyPath(groupName string) string {
	return path.Join(resourceGroupSettingsPath, groupName)
}

func resourceGroupStateKeyPath(groupName string) string {
	return path.Join(resourceGroupStatesPath, groupName)
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

// MinResolvedTSPath returns the min resolved ts path.
func MinResolvedTSPath() string {
	return path.Join(clusterPath, minResolvedTS)
}

// ExternalTimestampPath returns the external timestamp path.
func ExternalTimestampPath() string {
	return path.Join(clusterPath, externalTimeStamp)
}

// KeyspaceServiceSafePointPrefix returns the prefix of given service's service safe point.
// Prefix: /keyspaces/gc_safepoint/{space_id}/service/
func KeyspaceServiceSafePointPrefix(spaceID string) string {
	return path.Join(keyspaceSafePointPrefix, spaceID, "service") + "/"
}

// KeyspaceGCSafePointPath returns the gc safe point's path of the given key-space.
// Path: /keyspaces/gc_safepoint/{space_id}/gc
func KeyspaceGCSafePointPath(spaceID string) string {
	return path.Join(keyspaceSafePointPrefix, spaceID, keyspaceGCSafePointSuffix)
}

// KeyspaceServiceSafePointPath returns the path of given service's service safe point.
// Path: /keyspaces/gc_safepoint/{space_id}/service/{service_id}
func KeyspaceServiceSafePointPath(spaceID, serviceID string) string {
	return path.Join(KeyspaceServiceSafePointPrefix(spaceID), serviceID)
}

// KeyspaceSafePointPrefix returns prefix for all key-spaces' safe points.
// Path: /keyspaces/gc_safepoint/
func KeyspaceSafePointPrefix() string {
	return keyspaceSafePointPrefix + "/"
}

// KeyspaceGCSafePointSuffix returns the suffix for any gc safepoint.
// Postfix: /gc
func KeyspaceGCSafePointSuffix() string {
	return "/" + keyspaceGCSafePointSuffix
}

// KeyspaceMetaPrefix returns the prefix of keyspaces' metadata.
// Prefix: keyspaces/meta/
func KeyspaceMetaPrefix() string {
	return path.Join(keyspacePrefix, keyspaceMetaInfix) + "/"
}

// KeyspaceMetaPath returns the path to the given keyspace's metadata.
// Path: keyspaces/meta/{space_id}
func KeyspaceMetaPath(spaceID uint32) string {
	idStr := encodeKeyspaceID(spaceID)
	return path.Join(KeyspaceMetaPrefix(), idStr)
}

// KeyspaceIDPath returns the path to keyspace id from the given name.
// Path: keyspaces/id/{name}
func KeyspaceIDPath(name string) string {
	return path.Join(keyspacePrefix, keyspaceIDInfix, name)
}

// KeyspaceIDAlloc returns the path of the keyspace id's persistent window boundary.
// Path: keyspaces/alloc_id
func KeyspaceIDAlloc() string {
	return path.Join(keyspacePrefix, keyspaceAllocID)
}

// encodeKeyspaceID from uint32 to string.
// It adds extra padding to make encoded ID ordered.
// Encoded ID can be decoded directly with strconv.ParseUint.
// Width of the padded keyspaceID is 8 (decimal representation of uint24max is 16777215).
func encodeKeyspaceID(spaceID uint32) string {
	return fmt.Sprintf("%08d", spaceID)
}

func timestampPath(keyspaceGroupName string, dcLocationKey ...string) string {
	if len(dcLocationKey) != 0 {
		return buildPath(false, []string{microserviceKey, tsoServiceKey, keyspaceGroupName, localTSOSuffix, dcLocationKey[0], timestampKey}...)
	}
	return buildPath(false, []string{microserviceKey, tsoServiceKey, keyspaceGroupName, timestampKey}...)
}

func timestampPrefix(keyspaceGroupName string, dcLocationKey ...string) string {
	if len(dcLocationKey) != 0 {
		return buildPath(true, []string{microserviceKey, tsoServiceKey, keyspaceGroupName, localTSOSuffix, dcLocationKey[0]}...)
	}
	return buildPath(true, []string{microserviceKey, tsoServiceKey, keyspaceGroupName}...)
}

func buildPath(withSuffix bool, str ...string) string {
	var sb strings.Builder
	for i := 0; i < len(str); i++ {
		if i != 0 {
			sb.WriteString("/")
		}
		sb.WriteString(str[i])
	}
	if withSuffix {
		sb.WriteString("/")
	}
	return sb.String()
}
