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
	"regexp"
	"strconv"
	"strings"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

const (
	pdRootPath = "/pd"
	// ClusterPath is the path to save the cluster meta information.
	ClusterPath = "raft"
	// Config is the path to save the PD config.
	Config = "config"
	// ServiceMiddlewarePath is the path to save the service middleware config.
	ServiceMiddlewarePath = "service_middleware"
	schedulePath          = "schedule"
	gcPath                = "gc"
	ruleCommonPath        = "rule"
	// RulesPath is the path to save the placement rules.
	RulesPath = "rules"
	// RuleGroupPath is the path to save the placement rule groups.
	RuleGroupPath = "rule_group"
	// RegionLabelPath is the path to save the region label.
	RegionLabelPath = "region_label"
	replicationPath = "replication_mode"
	// CustomSchedulerConfigPath is the path to save the scheduler config.
	CustomSchedulerConfigPath = "scheduler_config"
	// GCWorkerServiceSafePointID is the service id of GC worker.
	GCWorkerServiceSafePointID = "gc_worker"
	minResolvedTS              = "min_resolved_ts"
	externalTimeStamp          = "external_timestamp"
	keyspaceSafePointPrefix    = "keyspaces/gc_safepoint"
	keyspaceGCSafePointSuffix  = "gc"
	keyspacePrefix             = "keyspaces"
	keyspaceMetaInfix          = "meta"
	keyspaceIDInfix            = "id"
	gcSafePointInfix           = "gc_safe_point"
	serviceSafePointInfix      = "service_safe_point"
	regionPathPrefix           = "raft/r"
	// resource group storage endpoint has prefix `resource_group`
	// ResourceGroupSettingsPath is the path to save the resource group settings.
	ResourceGroupSettingsPath = "settings"
	// ResourceGroupStatesPath is the path to save the resource group states.
	ResourceGroupStatesPath = "states"
	// ControllerConfigPath is the path to save the controller config.
	ControllerConfigPath = "controller"
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

	// ClusterIDPath is the path to store cluster id
	ClusterIDPath = "/pd/cluster_id"
)

// PDRootPath returns the PD root path.
func PDRootPath() string {
	return path.Join(pdRootPath, strconv.FormatUint(ClusterID(), 10))
}

// AppendToRootPath appends the given key to the rootPath.
func AppendToRootPath(rootPath string, key string) string {
	return path.Join(rootPath, key)
}

// ClusterRootPath appends the `ClusterPath` to the rootPath.
func ClusterRootPath(rootPath string) string {
	return AppendToRootPath(rootPath, ClusterPath)
}

// ClusterBootstrapTimeKey returns the path to save the cluster bootstrap timestamp.
func ClusterBootstrapTimeKey() string {
	return path.Join(ClusterPath, "status", "raft_bootstrap_time")
}

// ConfigPath returns the path to save the PD config.
func ConfigPath() string {
	return path.Join(PDRootPath(), Config)
}

// SchedulerConfigPathPrefix returns the path prefix to save the scheduler config.
func SchedulerConfigPathPrefix() string {
	return path.Join(PDRootPath(), CustomSchedulerConfigPath)
}

// RulesPathPrefix returns the path prefix to save the placement rules.
func RulesPathPrefix() string {
	return path.Join(PDRootPath(), RulesPath)
}

// RuleCommonPathPrefix returns the path prefix to save the placement rule common config.
func RuleCommonPathPrefix() string {
	return path.Join(PDRootPath(), ruleCommonPath)
}

// RuleGroupPathPrefix returns the path prefix to save the placement rule groups.
func RuleGroupPathPrefix() string {
	return path.Join(PDRootPath(), RuleGroupPath)
}

// RegionLabelPathPrefix returns the path prefix to save the region label.
func RegionLabelPathPrefix() string {
	return path.Join(PDRootPath(), RegionLabelPath)
}

// SchedulerConfigPath returns the path to save the scheduler config.
func SchedulerConfigPath(schedulerName string) string {
	return path.Join(CustomSchedulerConfigPath, schedulerName)
}

// StorePath returns the store meta info key path with the given store ID.
func StorePath(storeID uint64) string {
	return path.Join(ClusterPath, "s", fmt.Sprintf("%020d", storeID))
}

// StorePathPrefix returns the store meta info key path prefix.
func StorePathPrefix() string {
	return path.Join(PDRootPath(), ClusterPath, "s") + "/"
}

// ExtractStoreIDFromPath extracts the store ID from the given path.
func ExtractStoreIDFromPath(path string) (uint64, error) {
	idStr := strings.TrimLeft(strings.TrimPrefix(path, StorePathPrefix()), "0")
	return strconv.ParseUint(idStr, 10, 64)
}

// StoreLeaderWeightPath returns the store leader weight key path with the given store ID.
func StoreLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

// StoreRegionWeightPath returns the store region weight key path with the given store ID.
func StoreRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// RegionPath returns the region meta info key path with the given region ID.
func RegionPath(regionID uint64) string {
	var buf strings.Builder
	buf.Grow(len(regionPathPrefix) + 1 + keyLen) // Preallocate memory

	buf.WriteString(regionPathPrefix)
	buf.WriteString("/")
	s := strconv.FormatUint(regionID, 10)
	b := make([]byte, keyLen)
	copy(b, s)
	if len(s) < keyLen {
		diff := keyLen - len(s)
		copy(b[diff:], s)
		for i := range diff {
			b[i] = '0'
		}
	} else if len(s) > keyLen {
		copy(b, s[len(s)-keyLen:])
	}
	buf.Write(b)

	return buf.String()
}

// ResourceGroupSettingKeyPath returns the path to save the resource group settings.
func ResourceGroupSettingKeyPath(groupName string) string {
	return path.Join(ResourceGroupSettingsPath, groupName)
}

// ResourceGroupStateKeyPath returns the path to save the resource group states.
func ResourceGroupStateKeyPath(groupName string) string {
	return path.Join(ResourceGroupStatesPath, groupName)
}

// RuleKeyPath returns the path to save the placement rule with the given rule key.
func RuleKeyPath(ruleKey string) string {
	return path.Join(RulesPath, ruleKey)
}

// RuleGroupIDPath returns the path to save the placement rule group with the given group ID.
func RuleGroupIDPath(groupID string) string {
	return path.Join(RuleGroupPath, groupID)
}

// RegionLabelKeyPath returns the path to save the region label with the given rule key.
func RegionLabelKeyPath(ruleKey string) string {
	return path.Join(RegionLabelPath, ruleKey)
}

// ReplicationModePath returns the path to save the replication mode with the given mode.
func ReplicationModePath(mode string) string {
	return path.Join(replicationPath, mode)
}

// GCSafePointPath returns the GC safe point key path.
func GCSafePointPath() string {
	return path.Join(gcPath, "safe_point")
}

// GCSafePointServicePrefixPath returns the GC safe point service key path prefix.
func GCSafePointServicePrefixPath() string {
	return path.Join(GCSafePointPath(), "service") + "/"
}

// GCSafePointServicePath returns the GC safe point service key path with the given service ID.
func GCSafePointServicePath(serviceID string) string {
	return path.Join(GCSafePointPath(), "service", serviceID)
}

// MinResolvedTSPath returns the min resolved ts path.
func MinResolvedTSPath() string {
	return path.Join(ClusterPath, minResolvedTS)
}

// ExternalTimestampPath returns the external timestamp path.
func ExternalTimestampPath() string {
	return path.Join(ClusterPath, externalTimeStamp)
}

// GCSafePointV2Path is the storage path of gc safe point v2.
// Path: keyspaces/gc_safe_point/{keyspaceID}
func GCSafePointV2Path(keyspaceID uint32) string {
	return buildPath(false, keyspacePrefix, gcSafePointInfix, EncodeKeyspaceID(keyspaceID))
}

// GCSafePointV2Prefix is the path prefix to all gc safe point v2.
// Prefix: keyspaces/gc_safe_point/
func GCSafePointV2Prefix() string {
	return buildPath(true, keyspacePrefix, gcSafePointInfix)
}

// ServiceSafePointV2Path is the storage path of service safe point v2.
// Path: keyspaces/service_safe_point/{spaceID}/{serviceID}
func ServiceSafePointV2Path(keyspaceID uint32, serviceID string) string {
	return buildPath(false, keyspacePrefix, serviceSafePointInfix, EncodeKeyspaceID(keyspaceID), serviceID)
}

// ServiceSafePointV2Prefix is the path prefix of all service safe point that belongs to a specific keyspace.
// Can be used to retrieve keyspace's service safe point at once.
// Path: keyspaces/service_safe_point/{spaceID}/
func ServiceSafePointV2Prefix(keyspaceID uint32) string {
	return buildPath(true, keyspacePrefix, serviceSafePointInfix, EncodeKeyspaceID(keyspaceID))
}

// KeyspaceMetaPrefix returns the prefix of keyspaces' metadata.
// Prefix: keyspaces/meta/
func KeyspaceMetaPrefix() string {
	return path.Join(keyspacePrefix, keyspaceMetaInfix) + "/"
}

// KeyspaceMetaPath returns the path to the given keyspace's metadata.
// Path: keyspaces/meta/{space_id}
func KeyspaceMetaPath(spaceID uint32) string {
	idStr := EncodeKeyspaceID(spaceID)
	return path.Join(KeyspaceMetaPrefix(), idStr)
}

// KeyspaceIDPath returns the path to keyspace id from the given name.
// Path: keyspaces/id/{name}
func KeyspaceIDPath(name string) string {
	return path.Join(keyspacePrefix, keyspaceIDInfix, name)
}

// EncodeKeyspaceID from uint32 to string.
// It adds extra padding to make encoded ID ordered.
// Encoded ID can be decoded directly with strconv.ParseUint.
// Width of the padded keyspaceID is 8 (decimal representation of uint24max is 16777215).
func EncodeKeyspaceID(spaceID uint32) string {
	return fmt.Sprintf("%08d", spaceID)
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

// SchedulingSvcRootPath returns the root path of scheduling service.
// Path: /ms/{cluster_id}/scheduling
func SchedulingSvcRootPath() string {
	return svcRootPath(constant.SchedulingServiceName)
}

// TSOSvcRootPath returns the root path of tso service.
// Path: /ms/{cluster_id}/tso
func TSOSvcRootPath() string {
	return svcRootPath(constant.TSOServiceName)
}

func svcRootPath(svcName string) string {
	c := strconv.FormatUint(ClusterID(), 10)
	return path.Join(constant.MicroserviceRootPath, c, svcName)
}

// LegacyRootPath returns the root path of legacy pd service.
// Path: /pd/{cluster_id}
func LegacyRootPath() string {
	return path.Join(pdRootPath, strconv.FormatUint(ClusterID(), 10))
}

// KeyspaceGroupPrimaryPath returns the path of keyspace group primary.
// default keyspace group: "/ms/{cluster_id}/tso/00000/primary".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}/primary".
func KeyspaceGroupPrimaryPath(rootPath string, keyspaceGroupID uint32) string {
	electionPath := KeyspaceGroupsElectionPath(rootPath, keyspaceGroupID)
	return path.Join(electionPath, constant.PrimaryKey)
}

// SchedulingPrimaryPath returns the path of scheduling primary.
// Path: /ms/{cluster_id}/scheduling/primary
func SchedulingPrimaryPath() string {
	return path.Join(SchedulingSvcRootPath(), constant.PrimaryKey)
}

// KeyspaceGroupsElectionPath returns the path of keyspace groups election.
// default keyspace group: "/ms/{cluster_id}/tso/00000".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}".
func KeyspaceGroupsElectionPath(rootPath string, keyspaceGroupID uint32) string {
	if keyspaceGroupID == constant.DefaultKeyspaceGroupID {
		return path.Join(rootPath, "00000")
	}
	return path.Join(rootPath, constant.KeyspaceGroupsKey, keyspaceGroupsElectionKey, fmt.Sprintf("%05d", keyspaceGroupID))
}

// GetCompiledNonDefaultIDRegexp returns the compiled regular expression for matching non-default keyspace group id.
func GetCompiledNonDefaultIDRegexp() *regexp.Regexp {
	rootPath := TSOSvcRootPath()
	pattern := strings.Join([]string{rootPath, constant.KeyspaceGroupsKey, keyspaceGroupsElectionKey, `(\d{5})`, constant.PrimaryKey + `$`}, "/")
	return regexp.MustCompile(pattern)
}

// encodeKeyspaceGroupID from uint32 to string.
func encodeKeyspaceGroupID(groupID uint32) string {
	return fmt.Sprintf("%05d", groupID)
}

func buildPath(withSuffix bool, str ...string) string {
	var sb strings.Builder
	for i := range str {
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
	rootPath := TSOSvcRootPath()
	tsPath := TimestampPath(KeyspaceGroupGlobalTSPath(groupID))
	if groupID == constant.DefaultKeyspaceGroupID {
		rootPath = LegacyRootPath()
	}
	return path.Join(rootPath, tsPath)
}

const (
	registryKey = "registry"
)

// RegistryPath returns the full path to store microservice addresses.
func RegistryPath(serviceName, serviceAddr string) string {
	return strings.Join([]string{constant.MicroserviceRootPath,
		strconv.FormatUint(ClusterID(), 10), serviceName, registryKey, serviceAddr}, "/")
}

// ServicePath returns the path to store microservice addresses.
func ServicePath(serviceName string) string {
	return strings.Join([]string{constant.MicroserviceRootPath,
		strconv.FormatUint(ClusterID(), 10), serviceName, registryKey, ""}, "/")
}

// TSOPath returns the path to store TSO addresses.
func TSOPath() string {
	return ServicePath("tso")
}
