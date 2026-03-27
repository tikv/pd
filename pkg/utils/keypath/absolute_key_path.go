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
	"regexp"
	"strconv"
	"strings"

	"github.com/tikv/pd/pkg/keyspace/constant"
)

const (
	// GCWorkerServiceSafePointID is the service id of GC worker.
	GCWorkerServiceSafePointID = "gc_worker"
	// NativeBRServiceSafePointID is the service id of native BR.
	NativeBRServiceSafePointID = "native_br"
)

// Leader and primary are the same thing in this context.
const (
	// ClusterIDPath is the path to store cluster id
	ClusterIDPath = "/pd/cluster_id" // "/pd/cluster_id"

	leaderPathFormat            = "/pd/%d/leader"                             // "/pd/{cluster_id}/leader"
	allocIDPathFormat           = "/pd/%d/alloc_id"                           // "/pd/{cluster_id}/alloc_id"
	keyspaceAllocIDPathFormat   = "/pd/%d/keyspaces/alloc_id"                 // "/pd/{cluster_id}/keyspaces/alloc_id"
	configPathFormat            = "/pd/%d/config"                             // "/pd/{cluster_id}/config"
	schedulerConfigPathFormat   = "/pd/%d/scheduler_config/%s"                // "/pd/{cluster_id}/scheduler_config/{scheduler_name}"
	storeLeaderWeightPathFormat = "/pd/%d/schedule/store_weight/%020d/leader" // "/pd/{cluster_id}/schedule/store_weight/{store_id}/leader"
	storeRegionWeightPathFormat = "/pd/%d/schedule/store_weight/%020d/region" // "/pd/{cluster_id}/schedule/store_weight/{store_id}/region"

	serviceMiddlewarePathFormat   = "/pd/%d/service_middleware"                  // "/pd/{cluster_id}/service_middleware"
	replicationModePathFormat     = "/pd/%d/replication_mode/%s"                 // "/pd/{cluster_id}/replication_mode/{mode}"
	recoveringMarkPathFormat      = "/pd/%d/cluster/markers/snapshot-recovering" // "/pd/{cluster_id}/cluster/markers/snapshot-recovering"
	pitrRestoreModeMarkPathFormat = "/pd/%d/cluster/markers/pitr-restore-mode"   // "/pd/{cluster_id}/cluster/markers/pitr-restore-mode"

	memberBinaryDeployPathFormat   = "/pd/%d/member/%d/deploy_path"     // "/pd/{cluster_id}/member/{member_id}/deploy_path"
	memberGitHashPath              = "/pd/%d/member/%d/git_hash"        // "/pd/{cluster_id}/member/{member_id}/git_hash"
	memberBinaryVersionPathFormat  = "/pd/%d/member/%d/binary_version"  // "/pd/{cluster_id}/member/{member_id}/binary_version"
	memberLeaderPriorityPathFormat = "/pd/%d/member/%d/leader_priority" // "/pd/{cluster_id}/member/{member_id}/leader_priority"

	rulePathFormat = "/pd/%d/rules/%s" // "/pd/{cluster_id}/rules/{rule_id}"
	// ruleConfigPrefixFormat is used to watch rulePathFormat and ruleGroupPathFormat, so it should be the parent directory of them.
	ruleCommonPrefixFormat  = "/pd/%d/rule"            // "/pd/{cluster_id}/rule"
	ruleGroupPathFormat     = "/pd/%d/rule_group/%s"   // "/pd/{cluster_id}/rule_group/{group_id}"
	regionLabelPathFormat   = "/pd/%d/region_label/%s" // "/pd/{cluster_id}/region_label/{label_id}"
	regionLabelPrefixFormat = "/pd/%d/region_label/"   // "/pd/{cluster_id}/region_label/"

	// Maintenance task path format
	maintenanceTaskPathFormat = "/pd/%d/maintenance/%s" // "/pd/{cluster_id}/maintenance/{task_type}"

	// "%08d" adds extra padding to make encoded ID ordered.
	// Encoded ID can be decoded directly with strconv.ParseUint. Width of the
	// padded keyspaceID is 8 (decimal representation of uint24max is 16777215).
	gcStateRevisionPathFormat            = "/pd/%d/gc/gc_state_revision"         // "/pd/{cluster_id}/gc/gc_state_revision"
	unifiedGCSafePointPathFormat         = "/pd/%d/gc/safe_point"                // "/pd/{cluster_id}/gc/safe_point"
	keyspaceLevelGCSafePointPrefixFormat = "/pd/%d/keyspaces/gc_safe_point/"     // "/pd/{cluster_id}/keyspaces/gc_safe_point/"
	keyspaceLevelGCSafePointPathFormat   = "/pd/%d/keyspaces/gc_safe_point/%08d" // "/pd/{cluster_id}/keyspaces/gc_safe_point/{keyspace_id}"
	// Compatible with old data that was directly written to etcd by TiDB.
	unifiedTxnSafePointPath = "/tidb/store/gcworker/saved_safe_point"
	// Note that keyspace-level keys written directly from TiDB doesn't pad the keyspace ID with zeroes.
	keyspaceLevelTxnSafePointPath     = "/keyspaces/tidb/%d/tidb/store/gcworker/saved_safe_point" // "/keyspaces/tidb/{keyspace_id}/tidb/store/gcworker/saved_safe_point"
	unifiedGCBarrierPathFormat        = "/pd/%d/gc/safe_point/service/%s"                         // "/pd/{cluster_id}/gc/safe_point/service/{barrier_id}"
	keyspaceLevelGCBarrierPathFormat  = "/pd/%d/keyspaces/service_safe_point/%08d/%s"             // "/pd/{cluster_id}/keyspaces/service_safe_point/{keyspace_id}/{barrier_id}"
	globalGCBarrierPathFormat         = "/pd/%d/gc/global_gc_barriers/%s"                         // "/pd/{cluster_id}/gc/global_gc_barriers/{barrier_id}"
	globalGCBarrierPathPrefix         = "/pd/%d/gc/global_gc_barriers/"
	unifiedTiDBMinStartTSPrefix       = "/tidb/server/minstartts/"
	keyspaceLevelTiDBMinStartTSPrefix = "/keyspaces/tidb/%d/tidb/server/minstartts/" // "/keyspaces/tidb/{keyspace_id}/tidb/server/minstartts"
	gcSafePointV2PrefixFormat         = keyspaceLevelGCSafePointPrefixFormat
	gcSafePointV2PathFormat           = keyspaceLevelGCSafePointPathFormat
	serviceSafePointV2PathFormat      = keyspaceLevelGCBarrierPathFormat

	clusterPathFormat              = "/pd/%d/raft"                            // "/pd/{cluster_id}/raft"
	clusterBootstrapTimePathFormat = "/pd/%d/raft/status/raft_bootstrap_time" // "/pd/{cluster_id}/raft/status/raft_bootstrap_time"
	storePathPrefixFormat          = "/pd/%d/raft/s/"                         // "/pd/{cluster_id}/raft/s/"
	storePathFormat                = "/pd/%d/raft/s/%020d"                    // "/pd/{cluster_id}/raft/s/{store_id}"
	minResolvedTSPathFormat        = "/pd/%d/raft/min_resolved_ts"            // "/pd/{cluster_id}/raft/min_resolved_ts"
	externalTimestampPathFormat    = "/pd/%d/raft/external_timestamp"         // "/pd/{cluster_id}/raft/external_timestamp"

	keyspaceMetaPrefixFormat    = "/pd/%d/keyspaces/meta/"                     // "/pd/{cluster_id}/keyspaces/meta/"
	keyspaceMetaPathFormat      = "/pd/%d/keyspaces/meta/%08d"                 // "/pd/{cluster_id}/keyspaces/meta/{keyspace_id}"
	keyspaceIDPathFormat        = "/pd/%d/keyspaces/id/%s"                     // "/pd/{cluster_id}/keyspaces/id/{keyspace_name}"
	keyspaceGroupIDPrefixFormat = "/pd/%d/tso/keyspace_groups/membership/"     // "/pd/{cluster_id}/tso/keyspace_groups/membership/"
	keyspaceGroupIDPathFormat   = "/pd/%d/tso/keyspace_groups/membership/%05d" // "/pd/{cluster_id}/tso/keyspace_groups/membership/{group_id}"
	keyspaceGroupIDPattern      = `tso/keyspace_groups/membership/(\d{5})$`

	servicePathFormat  = "/ms/%d/%s/registry/"   // "/ms/{cluster_id}/{service_name}/registry/"
	registryPathFormat = "/ms/%d/%s/registry/%s" // "/ms/{cluster_id}/{service_name}/registry/{service_addr}"

	msPrimaryPathFormat           = "/ms/%d/%s/primary"                                // "/ms/{cluster_id}/{service_name}/primary"
	msTsoDefaultPrimaryPathFormat = "/ms/%d/tso/00000/primary"                         // "/ms/{cluster_id}/tso/00000/primary"
	msTsoKespacePrimaryPathFormat = "/ms/%d/tso/keyspace_groups/election/%05d/primary" // "/ms/{cluster_id}/tso/keyspace_groups/election/{group_id}/primary"

	// `expected_primary` is the flag to indicate the expected primary.
	// 1. When the primary was campaigned successfully, it will set the `expected_primary` flag.
	// 2. Using `{service}/primary/transfer` API will revoke the previous lease and set a new `expected_primary` flag.
	// This flag used to help new primary to campaign successfully while other secondaries can skip the campaign.
	msExpectedPrimaryPathFormat            = "/ms/%d/%s/primary/expected_primary"                                // "/ms/{cluster_id}/{service_name}/primary/expected_primary"
	msTsoDefaultExpectedPrimaryPathFormat  = "/ms/%d/tso/00000/primary/expected_primary"                         // "/ms/{cluster_id}/tso/00000/primary"
	msTsoKeyspaceExpectedPrimaryPathFormat = "/ms/%d/tso/keyspace_groups/election/%05d/primary/expected_primary" // "/ms/{cluster_id}/tso/keyspace_groups/election/{group_id}/primary"

	// resource group path
	keyspaceResourceGroupSettingsPathPrefixFormat = "resource_group/keyspace/settings/"      // "resource_group/keyspace/settings/"
	keyspaceResourceGroupSettingsPathFormat       = "resource_group/keyspace/settings/%d/%s" // "resource_group/keyspace/settings/{keyspace_id}/{group_name}"
	keyspaceResourceGroupStatesPathPrefixFormat   = "resource_group/keyspace/states/"        // "resource_group/keyspace/states/"
	keyspaceResourceGroupStatesPathFormat         = "resource_group/keyspace/states/%d/%s"   // "resource_group/keyspace/states/{keyspace_id}/{group_name}"
	// service limit path
	keyspaceServiceLimitsPathPrefixFormat = "resource_group/keyspace/service_limits/"   // "resource_group/keyspace/service_limits/"
	keyspaceServiceLimitsPathFormat       = "resource_group/keyspace/service_limits/%d" // "resource_group/keyspace/service_limits/{keyspace_id}"
	// legacy resource group path without introducing keyspace, to keep compatibility,
	// resource groups loaded from the legacy path will be assigned to the default keyspace ID.
	resourceGroupSettingsPathFormat = "resource_group/settings/%s" // "resource_group/settings/{group_name}"
	resourceGroupStatesPathFormat   = "resource_group/states/%s"   // "resource_group/states/{group_name}"
	controllerConfigPath            = "resource_group/controller"  // "resource_group/controller"

	timestampPathFormat   = "/pd/%d/timestamp"              // "/pd/{cluster_id}/timestamp"
	msTimestampPathFormat = "/ms/%d/tso/%05d/gta/timestamp" // "/ms/{cluster_id}/tso/{group_id}/gta/timestamp"

	affinityGroupPathFormat = "/pd/%d/affinity_groups/%s" // "/pd/{cluster_id}/affinity_groups/{group_id}"
)

// MsParam is the parameter of microservice.
type MsParam struct {
	ServiceName string
	GroupID     uint32 // only used for tso keyspace group
}

// Prefix returns the parent directory of the given path.
func Prefix(str string) string {
	return path.Dir(str)
}

// AllocIDPath returns the alloc id path.
func AllocIDPath() string {
	return fmt.Sprintf(allocIDPathFormat, ClusterID())
}

// KeyspaceAllocIDPath returns the keyspace alloc id path.
func KeyspaceAllocIDPath() string {
	return fmt.Sprintf(keyspaceAllocIDPathFormat, ClusterID())
}

// RegistryPath returns the full path to store microservice addresses.
func RegistryPath(serviceName, serviceAddr string) string {
	return fmt.Sprintf(registryPathFormat, ClusterID(), serviceName, serviceAddr)
}

// ServicePath returns the path to store microservice addresses.
func ServicePath(serviceName string) string {
	return fmt.Sprintf(servicePathFormat, ClusterID(), serviceName)
}

// ClusterPath is the path to save the cluster meta information.
func ClusterPath() string {
	return fmt.Sprintf(clusterPathFormat, ClusterID())
}

// ClusterBootstrapTimePath returns the path to save the cluster bootstrap time.
func ClusterBootstrapTimePath() string {
	return fmt.Sprintf(clusterBootstrapTimePathFormat, ClusterID())
}

// StorePath returns the store meta info key path with the given store ID.
func StorePath(storeID uint64) string {
	return fmt.Sprintf(storePathFormat, ClusterID(), storeID)
}

// StorePathPrefix returns the store meta info key path prefix.
func StorePathPrefix() string {
	return fmt.Sprintf(storePathPrefixFormat, ClusterID())
}

// ExtractStoreIDFromPath extracts the store ID from the given path.
func ExtractStoreIDFromPath(path string) (uint64, error) {
	idStr := strings.TrimLeft(strings.TrimPrefix(path, StorePathPrefix()), "0")
	return strconv.ParseUint(idStr, 10, 64)
}

// MinResolvedTSPath returns the min resolved ts path.
func MinResolvedTSPath() string {
	return fmt.Sprintf(minResolvedTSPathFormat, ClusterID())
}

// ExternalTimestampPath returns the external timestamp path.
func ExternalTimestampPath() string {
	return fmt.Sprintf(externalTimestampPathFormat, ClusterID())
}

// RecoveringMarkPath returns the path to save the recovering mark.
func RecoveringMarkPath() string {
	return fmt.Sprintf(recoveringMarkPathFormat, ClusterID())
}

// PitrRestoreModeMarkPath returns the path to save the pitr restore mode mark.
func PitrRestoreModeMarkPath() string {
	return fmt.Sprintf(pitrRestoreModeMarkPathFormat, ClusterID())
}

// KeyspaceMetaPrefix returns the prefix of keyspaces' metadata.
func KeyspaceMetaPrefix() string {
	return fmt.Sprintf(keyspaceMetaPrefixFormat, ClusterID())
}

// KeyspaceMetaPath returns the path to the given keyspace's metadata.
func KeyspaceMetaPath(spaceID uint32) string {
	return fmt.Sprintf(keyspaceMetaPathFormat, ClusterID(), spaceID)
}

// KeyspaceIDPath returns the path to keyspace id from the given keyspace name.
func KeyspaceIDPath(name string) string {
	return fmt.Sprintf(keyspaceIDPathFormat, ClusterID(), name)
}

// KeyspaceGroupIDPrefix returns the prefix of keyspace group id.
func KeyspaceGroupIDPrefix() string {
	return fmt.Sprintf(keyspaceGroupIDPrefixFormat, ClusterID())
}

// KeyspaceGroupIDPath returns the path to keyspace id from the given name.
func KeyspaceGroupIDPath(id uint32) string {
	return fmt.Sprintf(keyspaceGroupIDPathFormat, ClusterID(), id)
}

// GetCompiledKeyspaceGroupIDRegexp returns the compiled regular expression for matching keyspace group id.
func GetCompiledKeyspaceGroupIDRegexp() *regexp.Regexp {
	return regexp.MustCompile(keyspaceGroupIDPattern)
}

// ServiceMiddlewarePath is the path to save the service middleware config.
func ServiceMiddlewarePath() string {
	return fmt.Sprintf(serviceMiddlewarePathFormat, ClusterID())
}

// StoreLeaderWeightPath returns the store leader weight key path with the given store ID.
func StoreLeaderWeightPath(storeID uint64) string {
	return fmt.Sprintf(storeLeaderWeightPathFormat, ClusterID(), storeID)
}

// StoreRegionWeightPath returns the store region weight key path with the given store ID.
func StoreRegionWeightPath(storeID uint64) string {
	return fmt.Sprintf(storeRegionWeightPathFormat, ClusterID(), storeID)
}

// ReplicationModePath returns the path to save the replication mode with the given mode.
func ReplicationModePath(mode string) string {
	return fmt.Sprintf(replicationModePathFormat, ClusterID(), mode)
}

// TimestampPath returns the timestamp path for the given group id.
func TimestampPath(groupID uint32) string {
	if groupID == constant.DefaultKeyspaceGroupID {
		return fmt.Sprintf(timestampPathFormat, ClusterID())
	}
	return fmt.Sprintf(msTimestampPathFormat, ClusterID(), groupID)
}

// RegionPath returns the region meta info key path with the given region ID.
func RegionPath(regionID uint64) string {
	// we use uint64 to represent ID, the max length of uint64 is 20.
	const (
		keyLen           = 20
		regionPathPrefix = "raft/r"
	)

	var buf strings.Builder

	clusterID := strconv.FormatUint(ClusterID(), 10)
	buf.Grow(4 + len(clusterID) + len(regionPathPrefix) + 1 + keyLen) // Preallocate memory

	buf.WriteString("/pd/")
	buf.WriteString(clusterID)
	buf.WriteString("/")
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
