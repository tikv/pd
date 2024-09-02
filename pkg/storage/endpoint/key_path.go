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
	"regexp"
	"strconv"
	"strings"

	"github.com/tikv/pd/pkg/global"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

const (
	pdRootPath                = "/pd"
	clusterPath               = "raft"
	configPath                = "config"
	serviceMiddlewarePath     = "service_middleware"
	schedulePath              = "schedule"
	gcPath                    = "gc"
	ruleCommonPath            = "rule"
	rulesPath                 = "rules"
	ruleGroupPath             = "rule_group"
	regionLabelPath           = "region_label"
	replicationPath           = "replication_mode"
	customSchedulerConfigPath = "scheduler_config"
	// GCWorkerServiceSafePointID is the service id of GC worker.
	GCWorkerServiceSafePointID = "gc_worker"
	minResolvedTS              = "min_resolved_ts"
	externalTimeStamp          = "external_timestamp"
	keyspaceSafePointPrefix    = "keyspaces/gc_safepoint"
	keyspaceGCSafePointSuffix  = "gc"
	keyspacePrefix             = "keyspaces"
	keyspaceMetaInfix          = "meta"
	keyspaceIDInfix            = "id"
	keyspaceAllocID            = "alloc_id"
	gcSafePointInfix           = "gc_safe_point"
	serviceSafePointInfix      = "service_safe_point"
	regionPathPrefix           = "raft/r"
	// resource group storage endpoint has prefix `resource_group`
	resourceGroupSettingsPath = "settings"
	resourceGroupStatesPath   = "states"
	controllerConfigPath      = "controller"
	// tso storage endpoint has prefix `tso`
	tsoServiceKey                = constant.TSOServiceName
	globalTSOAllocatorEtcdPrefix = "gta"
	// TimestampKey is the key of timestamp oracle used for the suffix.
	TimestampKey = "timestamp"

	tsoKeyspaceGroupPrefix      = tsoServiceKey + "/" + constant.KeyspaceGroupsKey
	keyspaceGroupsMembershipKey = "membership"
	keyspaceGroupsElectionKey   = "election"

	// we use uint64 to represent ID, the max length of uint64 is 20.
	keyLen = 20

	// /pd/{cluster_id}
	rootPathFormat = "/pd/%d"

	configPathFormat                = "/pd/%d/config"
	customSchedulerConfigPathFormat = "/pd/%d/scheduler_config"

	clusterRootPathFormat         = "/pd/%d/raft"
	clusterBootstrapTimeKeyFormat = "/pd/%d/raft/status/raft_bootstrap_time"

	recoveringMarkPath = "/pd/%d/cluster/markers/snapshot-recovering"

	// raft/s/{store_id}
	storePathFormat       = "/pd/%d/raft/s/%020d"
	storePathPrefixFormat = "/pd/%d/raft/s"

	regionPathFormat = "/pd/%d/raft/r/%d"

	// schedule/store_weight/{store_id}/leader
	storeLeaderWeightPathFormat = "schedule/store_weight/%020d/leader"
	// schedule/store_weight/{store_id}/region
	storeRegionWeightPathFormat = "schedule/store_weight/%020d/region"

	// replication_mode/{mode}
	replicationModePathFormat = "replication_mode/%s"
	// region_label/{rule_key}
	regionLabelPathFormat = "region_label/%s"
	// rules/{rule_key}
	ruleKeyPathFormat = "rules/%s"
	// rule_group/{group_id}
	ruleGroupIDPathFormat   = "rule_group/%s"
	rulesPathFormat         = "/pd/%d/rules"
	rulesCommonPathFormat   = "/pd/%d/rule"
	ruleGroupPathFormat     = "/pd/%d/rule_group"
	regionLabelPrefixFormat = "/pd/%d/region_label"

	// resource_group/states/{group_name}
	resourceGroupStatePathFormat = "resource_group/states/%s"
	// resource_group/settings/{group_name}
	resourceGroupSettingPathFormat    = "resource_group/settings/%s"
	resourceGroupControllerConfigPath = "resource_group/controller"

	gcSafePointPathPrefixFormat        = "/pd/%d/gc/safe_point"
	gcSafePointServicePrefixPathFormat = "/pd/%d/gc/safe_point/service/"
	gcSafePointServicePathFormat       = "/pd/%d/gc/safe_point/service/%s"
	minResolvedTSPathFormat            = "/pd/%d/raft/min_resolved_ts"
	externalTimestampPathFormat        = "/pd/%d/raft/external_timestamp"
	gcSafePointV2PathFormat            = "/pd/%d/keyspaces/gc_safe_point/%08d"
	gcSafePointV2PrefixFormat          = "/pd/%d/keyspaces/gc_safe_point"
	serviceSafePointV2PathFormat       = "/pd/%d/keyspaces/service_safe_point/%08d/%s"
	serviceSafePointV2PrefixFormat     = "/pd/%d/keyspaces/service_safe_point/%08d/"
	keyspaceMetaPrefixFormat           = "/pd/%d/keyspaces/meta/"
	keyspaceMetaPathFormat             = "/pd/%d/keyspaces/meta/%08d"
	keyspaceIDPathFormat               = "/pd/%d/keyspaces/id/%s"

	tsoSvcRootPathFormat        = "/ms/%d/tso"
	schedulingSvcRootPathFormat = "/ms/%d/scheduling"
	resourceManagerSvcRootPath  = "/ms/%d/resource_manager"
	schedulingPrimaryPathFormat = "/ms/%d/scheduling/primary"

	defaultKeyspaceGroupsElectionPath      = "/ms/%d/tso/00000"
	keyspaceGroupsElectionKeyFormat        = "/ms/%d/tso/keyspace_groups/election/%05d"
	defaultKeyspaceGroupPrimaryFormat      = "/ms/%d/tso/00000/primary"
	keyspaceGroupPrimaryFormat             = "/ms/%d/tso/keyspace_groups/election/%05d/primary"
	defaultKeyspaceGroupGlobalTSPathFormat = "/pd/%d/timestamp"
	keyspaceGroupGlobalTSPathFormat        = "/ms/%d/tso/%05d/gta/timestamp"

	compiledNonDefaultIDRegexpFormat = `^/ms/%d/tso/keyspace_groups/election/(\d{5})/primary$`
)

// PDRootPath returns the PD root path.
func PDRootPath() string {
	return fmt.Sprintf(rootPathFormat, global.ClusterID())
}

// AppendToRootPath appends the given key to the rootPath.
func AppendToRootPath(rootPath string, key string) string {
	return path.Join(rootPath, key)
}

func RecoveringMarkPath() string {
	return fmt.Sprintf(recoveringMarkPath, global.ClusterID())
}

// ClusterRootPath appends the `clusterPath` to the rootPath.
func ClusterRootPath() string {
	return fmt.Sprintf(clusterRootPathFormat, global.ClusterID())
}

// ClusterBootstrapTimeKey returns the path to save the cluster bootstrap timestamp.
func ClusterBootstrapTimeKey() string {
	return fmt.Sprintf(clusterBootstrapTimeKeyFormat, global.ClusterID())
}

// ConfigPath returns the path to save the PD config.
func ConfigPath() string {
	return fmt.Sprintf(configPathFormat, global.ClusterID())
}

// SchedulerConfigPathPrefix returns the path prefix to save the scheduler config.
func SchedulerConfigPathPrefix() string {
	return fmt.Sprintf(customSchedulerConfigPathFormat, global.ClusterID())
}

// RulesPathPrefix returns the path prefix to save the placement rules.
func RulesPathPrefix() string {
	return fmt.Sprintf(rulesPathFormat, global.ClusterID())
}

// RuleCommonPathPrefix returns the path prefix to save the placement rule common config.
func RuleCommonPathPrefix() string {
	return fmt.Sprintf(rulesCommonPathFormat, global.ClusterID())
}

// RuleGroupPathPrefix returns the path prefix to save the placement rule groups.
func RuleGroupPathPrefix() string {
	return fmt.Sprintf(ruleGroupPathFormat, global.ClusterID())
}

// RegionLabelPathPrefix returns the path prefix to save the region label.
func RegionLabelPathPrefix() string {
	return fmt.Sprintf(regionLabelPrefixFormat, global.ClusterID())
}

func schedulerConfigPath(schedulerName string) string {
	return path.Join(customSchedulerConfigPath, schedulerName)
}

// StorePath returns the store meta info key path with the given store ID.
func StorePath(storeID uint64) string {
	return fmt.Sprintf(storePathFormat, storeID)
}

// StorePathPrefix returns the store meta info key path prefix.
func StorePathPrefix() string {
	return fmt.Sprintf(storePathPrefixFormat, global.ClusterID())
}

// ExtractStoreIDFromPath extracts the store ID from the given path.
func ExtractStoreIDFromPath(path string) (uint64, error) {
	idStr := strings.TrimLeft(strings.TrimPrefix(path, StorePathPrefix()), "0")
	return strconv.ParseUint(idStr, 10, 64)
}

func storeLeaderWeightPath(storeID uint64) string {
	return fmt.Sprintf(storeLeaderWeightPathFormat, storeID)
}

func storeRegionWeightPath(storeID uint64) string {
	return fmt.Sprintf(storeRegionWeightPathFormat, storeID)
}

// RegionPath returns the region meta info key path with the given region ID.
func RegionPath(regionID uint64) string {
	return fmt.Sprintf(regionPathFormat, global.ClusterID(), regionID)
}

func resourceGroupSettingKeyPath(groupName string) string {
	return fmt.Sprintf(resourceGroupSettingPathFormat, groupName)
}

func resourceGroupStateKeyPath(groupName string) string {
	return fmt.Sprintf(resourceGroupStatePathFormat, groupName)
}

func ruleKeyPath(ruleKey string) string {
	return fmt.Sprintf(ruleKeyPathFormat, ruleKey)
}

func ruleGroupIDPath(groupID string) string {
	return fmt.Sprintf(ruleGroupIDPathFormat, groupID)
}

func regionLabelKeyPath(ruleKey string) string {
	return fmt.Sprintf(regionLabelPathFormat, ruleKey)
}

func replicationModePath(mode string) string {
	return fmt.Sprintf(replicationModePathFormat, mode)
}

func gcSafePointPath() string {
	return fmt.Sprintf(gcSafePointPathPrefixFormat, global.ClusterID())
}

// GCSafePointServicePrefixPath returns the GC safe point service key path prefix.
func GCSafePointServicePrefixPath() string {
	return fmt.Sprintf(gcSafePointServicePrefixPathFormat, global.ClusterID())
}

func gcSafePointServicePath(serviceID string) string {
	return fmt.Sprintf(gcSafePointServicePathFormat, global.ClusterID(), serviceID)
}

// MinResolvedTSPath returns the min resolved ts path.
func MinResolvedTSPath() string {
	return fmt.Sprintf(minResolvedTSPathFormat, global.ClusterID())
}

// ExternalTimestampPath returns the external timestamp path.
func ExternalTimestampPath() string {
	return fmt.Sprintf(externalTimestampPathFormat, global.ClusterID())
}

// GCSafePointV2Path is the storage path of gc safe point v2.
// Path: keyspaces/gc_safe_point/{keyspaceID}
func GCSafePointV2Path(keyspaceID uint32) string {
	return fmt.Sprintf(gcSafePointV2PathFormat, global.ClusterID(), keyspaceID)
}

// GCSafePointV2Prefix is the path prefix to all gc safe point v2.
// Prefix: keyspaces/gc_safe_point/
func GCSafePointV2Prefix() string {
	return fmt.Sprintf(gcSafePointV2PrefixFormat, global.ClusterID())
}

// ServiceSafePointV2Path is the storage path of service safe point v2.
// Path: keyspaces/service_safe_point/{spaceID}/{serviceID}
func ServiceSafePointV2Path(keyspaceID uint32, serviceID string) string {
	return fmt.Sprintf(serviceSafePointV2PathFormat, global.ClusterID(), keyspaceID, serviceID)
}

// ServiceSafePointV2Prefix is the path prefix of all service safe point that belongs to a specific keyspace.
// Can be used to retrieve keyspace's service safe point at once.
// Path: keyspaces/service_safe_point/{spaceID}/
func ServiceSafePointV2Prefix(keyspaceID uint32) string {
	return fmt.Sprintf(serviceSafePointV2PrefixFormat, global.ClusterID(), keyspaceID)
}

// KeyspaceMetaPrefix returns the prefix of keyspaces' metadata.
// Prefix: keyspaces/meta/
func KeyspaceMetaPrefix() string {
	return fmt.Sprintf(keyspaceMetaPrefixFormat, global.ClusterID())
}

// KeyspaceMetaPath returns the path to the given keyspace's metadata.
// Path: keyspaces/meta/{space_id}
func KeyspaceMetaPath(spaceID uint32) string {
	return fmt.Sprintf(keyspaceMetaPathFormat, global.ClusterID(), spaceID)
}

// KeyspaceIDPath returns the path to keyspace id from the given name.
// Path: keyspaces/id/{name}
func KeyspaceIDPath(name string) string {
	return fmt.Sprintf(keyspaceIDPathFormat, global.ClusterID(), name)
}

// KeyspaceIDAlloc returns the path of the keyspace id's persistent window boundary.
// Path: keyspaces/alloc_id
func KeyspaceIDAlloc() string {
	return path.Join(keyspacePrefix, keyspaceAllocID)
}

// KeyspaceGroupIDPrefix returns the prefix of keyspace group id.
// Path: tso/keyspace_groups/membership
func KeyspaceGroupIDPrefix() string {
	return path.Join(tsoKeyspaceGroupPrefix, keyspaceGroupsMembershipKey)
}

// KeyspaceGroupIDPath returns the path to keyspace id from the given name.
// Path: tso/keyspace_groups/membership/{id}
func KeyspaceGroupIDPath(id uint32) string {
	return path.Join(tsoKeyspaceGroupPrefix, keyspaceGroupsMembershipKey, encodeKeyspaceGroupID(id))
}

// GetCompiledKeyspaceGroupIDRegexp returns the compiled regular expression for matching keyspace group id.
func GetCompiledKeyspaceGroupIDRegexp() *regexp.Regexp {
	pattern := strings.Join([]string{KeyspaceGroupIDPrefix(), `(\d{5})$`}, "/")
	return regexp.MustCompile(pattern)
}

// ResourceManagerSvcRootPath returns the root path of resource manager service.
// Path: /ms/{cluster_id}/resource_manager
func ResourceManagerSvcRootPath() string {
	return fmt.Sprintf(resourceManagerSvcRootPath, global.ClusterID())
}

// SchedulingSvcRootPath returns the root path of scheduling service.
// Path: /ms/{cluster_id}/scheduling
func SchedulingSvcRootPath() string {
	return fmt.Sprintf(schedulingSvcRootPathFormat, global.ClusterID())
}

// TSOSvcRootPath returns the root path of tso service.
// Path: /ms/{cluster_id}/tso
func TSOSvcRootPath() string {
	return fmt.Sprintf(tsoSvcRootPathFormat, global.ClusterID())
}

// LegacyRootPath returns the root path of legacy pd service.
// Path: /pd/{cluster_id}
func LegacyRootPath() string {
	return fmt.Sprintf(rootPathFormat, global.ClusterID())
}

// KeyspaceGroupPrimaryPath returns the path of keyspace group primary.
// default keyspace group: "/ms/{cluster_id}/tso/00000/primary".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}/primary".
func KeyspaceGroupPrimaryPath(keyspaceGroupID uint32) string {
	if keyspaceGroupID == constant.DefaultKeyspaceGroupID {
		return fmt.Sprintf(defaultKeyspaceGroupPrimaryFormat, global.ClusterID())
	}
	return fmt.Sprintf(keyspaceGroupPrimaryFormat, global.ClusterID(), keyspaceGroupID)
}

// SchedulingPrimaryPath returns the path of scheduling primary.
// Path: /ms/{cluster_id}/scheduling/primary
func SchedulingPrimaryPath() string {
	return fmt.Sprintf(schedulingPrimaryPathFormat, global.ClusterID())
}

// KeyspaceGroupsElectionPath returns the path of keyspace groups election.
// default keyspace group: "/ms/{cluster_id}/tso/00000".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}".
func KeyspaceGroupsElectionPath(keyspaceGroupID uint32) string {
	if keyspaceGroupID == constant.DefaultKeyspaceGroupID {
		return fmt.Sprintf(defaultKeyspaceGroupsElectionPath, global.ClusterID())
	}
	return fmt.Sprintf(keyspaceGroupsElectionKeyFormat, global.ClusterID(), keyspaceGroupID)
}

// GetCompiledNonDefaultIDRegexp returns the compiled regular expression for matching non-default keyspace group id.
func GetCompiledNonDefaultIDRegexp() *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(compiledNonDefaultIDRegexpFormat, global.ClusterID()))
}

// encodeKeyspaceGroupID from uint32 to string.
func encodeKeyspaceGroupID(groupID uint32) string {
	return fmt.Sprintf("%05d", groupID)
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

// KeyspaceGroupLocalTSPath constructs the timestampOracle path prefix for Local TSO, which is:
//  1. for the default keyspace group:
//     lta/{dc-location} in /pd/{cluster_id}/lta/{dc-location}/timestamp
//  2. for the non-default keyspace groups:
//     {group}/lta/{dc-location} in /ms/{cluster_id}/tso/{group}/lta/{dc-location}/timestamp
func KeyspaceGroupLocalTSPath(keyPrefix string, groupID uint32, dcLocation string) string {
	if groupID == constant.DefaultKeyspaceGroupID {
		return path.Join(keyPrefix, dcLocation)
	}
	return path.Join(fmt.Sprintf("%05d", groupID), keyPrefix, dcLocation)
}

// TimestampPath returns the timestamp path for the given timestamp oracle path prefix.
func TimestampPath(tsPath string) string {
	return path.Join(tsPath, TimestampKey)
}

// FullTimestampPath returns the full timestamp path.
//  1. for the default keyspace group:
//     /pd/{cluster_id}/timestamp
//  2. for the non-default keyspace groups:
//     /ms/{cluster_id}/tso/{group}/gta/timestamp
func FullTimestampPath(groupID uint32) string {
	if groupID == constant.DefaultKeyspaceGroupID {
		return fmt.Sprintf(defaultKeyspaceGroupGlobalTSPathFormat, global.ClusterID())
	}
	return fmt.Sprintf(keyspaceGroupGlobalTSPathFormat, global.ClusterID(), groupID)
}
