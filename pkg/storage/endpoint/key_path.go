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
	clusterPath              = "raft"
	configPath               = "config"
	serviceMiddlewarePath    = "service_middleware"
	schedulePath             = "schedule"
	gcPath                   = "gc"
	rulesPath                = "rules"
	ruleGroupPath            = "rule_group"
	regionLabelPath          = "region_label"
	replicationPath          = "replication_mode"
	customScheduleConfigPath = "scheduler_config"
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
	microserviceKey = "ms"
	tsoServiceKey   = utils.TSOServiceName
	timestampKey    = "timestamp"

	tsoKeyspaceGroupPrefix     = "tso/keyspace_groups"
	keyspaceGroupMembershipKey = "membership"

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
	return path.Join(tsoKeyspaceGroupPrefix, keyspaceGroupMembershipKey)
}

// KeyspaceGroupIDPath returns the path to keyspace id from the given name.
// Path: tso/keyspace_groups/membership/{id}
func KeyspaceGroupIDPath(id uint32) string {
	return path.Join(tsoKeyspaceGroupPrefix, keyspaceGroupMembershipKey, encodeKeyspaceGroupID(id))
}

// ExtractKeyspaceGroupIDFromPath extracts keyspace group id from the given path, which contains
// the pattern of `tso/keyspace_groups/membership/(\d{5})$`.
func ExtractKeyspaceGroupIDFromPath(path string) (uint32, error) {
	pattern := strings.Join([]string{KeyspaceGroupIDPrefix(), `(\d{5})$`}, "/")
	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(path)
	if match == nil {
		return 0, fmt.Errorf("invalid keyspace group id path: %s", path)
	}
	id, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse keyspace group ID: %v", err)
	}
	return uint32(id), nil
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
