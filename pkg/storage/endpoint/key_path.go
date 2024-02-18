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

	"github.com/tikv/pd/pkg/mcs/utils"
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
	tsoServiceKey                = utils.TSOServiceName
	globalTSOAllocatorEtcdPrefix = "gta"
	// TimestampKey is the key of timestamp oracle used for the suffix.
	TimestampKey = "timestamp"

	tsoKeyspaceGroupPrefix      = tsoServiceKey + "/" + utils.KeyspaceGroupsKey
	keyspaceGroupsMembershipKey = "membership"
	keyspaceGroupsElectionKey   = "election"

	// we use uint64 to represent ID, the max length of uint64 is 20.
	keyLen = 20
)

// PDRootPath returns the PD root path.
func PDRootPath(clusterID uint64) string {
	return path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
}

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

// ConfigPath returns the path to save the PD config.
func ConfigPath(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), configPath)
}

// SchedulerConfigPathPrefix returns the path prefix to save the scheduler config.
func SchedulerConfigPathPrefix(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), customSchedulerConfigPath)
}

// RulesPathPrefix returns the path prefix to save the placement rules.
func RulesPathPrefix(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), rulesPath)
}

// RuleCommonPathPrefix returns the path prefix to save the placement rule common config.
func RuleCommonPathPrefix(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), ruleCommonPath)
}

// RuleGroupPathPrefix returns the path prefix to save the placement rule groups.
func RuleGroupPathPrefix(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), ruleGroupPath)
}

// RegionLabelPathPrefix returns the path prefix to save the region label.
func RegionLabelPathPrefix(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), regionLabelPath)
}

func schedulerConfigPath(schedulerName string) string {
	return path.Join(customSchedulerConfigPath, schedulerName)
}

// StorePath returns the store meta info key path with the given store ID.
func StorePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

// StorePathPrefix returns the store meta info key path prefix.
func StorePathPrefix(clusterID uint64) string {
	return path.Join(PDRootPath(clusterID), clusterPath, "s") + "/"
}

// ExtractStoreIDFromPath extracts the store ID from the given path.
func ExtractStoreIDFromPath(clusterID uint64, path string) (uint64, error) {
	idStr := strings.TrimLeft(strings.TrimPrefix(path, StorePathPrefix(clusterID)), "0")
	return strconv.ParseUint(idStr, 10, 64)
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

// KeyspaceIDAlloc returns the path of the keyspace id's persistent window boundary.
// Path: keyspaces/alloc_id
func KeyspaceIDAlloc() string {
	return path.Join(keyspacePrefix, keyspaceAllocID)
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

// ResourceManagerSvcRootPath returns the root path of resource manager service.
// Path: /ms/{cluster_id}/resource_manager
func ResourceManagerSvcRootPath(clusterID uint64) string {
	return svcRootPath(clusterID, utils.ResourceManagerServiceName)
}

// SchedulingSvcRootPath returns the root path of scheduling service.
// Path: /ms/{cluster_id}/scheduling
func SchedulingSvcRootPath(clusterID uint64) string {
	return svcRootPath(clusterID, utils.SchedulingServiceName)
}

// TSOSvcRootPath returns the root path of tso service.
// Path: /ms/{cluster_id}/tso
func TSOSvcRootPath(clusterID uint64) string {
	return svcRootPath(clusterID, utils.TSOServiceName)
}

func svcRootPath(clusterID uint64, svcName string) string {
	c := strconv.FormatUint(clusterID, 10)
	return path.Join(utils.MicroserviceRootPath, c, svcName)
}

// LegacyRootPath returns the root path of legacy pd service.
// Path: /pd/{cluster_id}
func LegacyRootPath(clusterID uint64) string {
	return path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
}

// KeyspaceGroupPrimaryPath returns the path of keyspace group primary.
// default keyspace group: "/ms/{cluster_id}/tso/00000/primary".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}/primary".
func KeyspaceGroupPrimaryPath(rootPath string, keyspaceGroupID uint32) string {
	electionPath := KeyspaceGroupsElectionPath(rootPath, keyspaceGroupID)
	return path.Join(electionPath, utils.PrimaryKey)
}

// SchedulingPrimaryPath returns the path of scheduling primary.
// Path: /ms/{cluster_id}/scheduling/primary
func SchedulingPrimaryPath(clusterID uint64) string {
	return path.Join(SchedulingSvcRootPath(clusterID), utils.PrimaryKey)
}

// KeyspaceGroupsElectionPath returns the path of keyspace groups election.
// default keyspace group: "/ms/{cluster_id}/tso/00000".
// non-default keyspace group: "/ms/{cluster_id}/tso/keyspace_groups/election/{group}".
func KeyspaceGroupsElectionPath(rootPath string, keyspaceGroupID uint32) string {
	if keyspaceGroupID == utils.DefaultKeyspaceGroupID {
		return path.Join(rootPath, "00000")
	}
	return path.Join(rootPath, utils.KeyspaceGroupsKey, keyspaceGroupsElectionKey, fmt.Sprintf("%05d", keyspaceGroupID))
}

// GetCompiledNonDefaultIDRegexp returns the compiled regular expression for matching non-default keyspace group id.
func GetCompiledNonDefaultIDRegexp(clusterID uint64) *regexp.Regexp {
	rootPath := TSOSvcRootPath(clusterID)
	pattern := strings.Join([]string{rootPath, utils.KeyspaceGroupsKey, keyspaceGroupsElectionKey, `(\d{5})`, utils.PrimaryKey + `$`}, "/")
	return regexp.MustCompile(pattern)
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
	if groupID == utils.DefaultKeyspaceGroupID {
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
	if groupID == utils.DefaultKeyspaceGroupID {
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
func FullTimestampPath(clusterID uint64, groupID uint32) string {
	rootPath := TSOSvcRootPath(clusterID)
	tsPath := TimestampPath(KeyspaceGroupGlobalTSPath(groupID))
	if groupID == utils.DefaultKeyspaceGroupID {
		rootPath = LegacyRootPath(clusterID)
	}
	return path.Join(rootPath, tsPath)
}
