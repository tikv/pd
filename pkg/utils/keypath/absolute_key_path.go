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

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

// Leader and primary are the same thing in this context.
const (
	leaderPathFormat               = "/pd/%d/leader"                    // "/pd/{cluster_id}/leader"
	memberBinaryDeployPathFormat   = "/pd/%d/member/%d/deploy_path"     // "/pd/{cluster_id}/member/{member_id}/deploy_path"
	memberGitHashPath              = "/pd/%d/member/%d/git_hash"        // "/pd/{cluster_id}/member/{member_id}/git_hash"
	memberBinaryVersionPathFormat  = "/pd/%d/member/%d/binary_version"  // "/pd/{cluster_id}/member/{member_id}/binary_version"
	allocIDPathFormat              = "/pd/%d/alloc_id"                  // "/pd/{cluster_id}/alloc_id"
	keyspaceAllocIDPathFormat      = "/pd/%d/keyspaces/alloc_id"        // "/pd/{cluster_id}/keyspaces/alloc_id"
	kemberLeaderPriorityPathFormat = "/pd/%d/member/%d/leader_priority" // "/pd/{cluster_id}/member/{member_id}/leader_priority"
	gcSafePointV2PrefixFormat      = "/pd/%d/keyspaces/gc_safe_point/"  // "/pd/{cluster_id}/keyspaces/gc_safe_point/"

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

	recoveringMarkPathFormat = "/pd/%d/cluster/markers/snapshot-recovering" // "/pd/{cluster_id}/cluster/markers/snapshot-recovering"

	servicePathFormat  = "/ms/%d/%s/registry"    // "/ms/{cluster_id}/{service_name}/registry"
	registryPathFormat = "/ms/%d/%s/registry/%s" // "/ms/{cluster_id}/{service_name}/registry/{service_addr}"

	msLeaderPathFormat           = "/ms/%d/%s/primary"                                // "/ms/{cluster_id}/{service_name}/primary"
	msTsoDefaultLeaderPathFormat = "/ms/%d/tso/00000/primary"                         // "/ms/{cluster_id}/tso/00000/primary"
	msTsoKespaceLeaderPathFormat = "/ms/%d/tso/keyspace_groups/election/%05d/primary" // "/ms/{cluster_id}/tso/keyspace_groups/election/{group_id}/primary"

	// `expected_primary` is the flag to indicate the expected primary/leader.
	// 1. When the leader was campaigned successfully, it will set the `expected_primary` flag.
	// 2. Using `{service}/primary/transfer` API will revoke the previous lease and set a new `expected_primary` flag.
	// This flag used to help new primary to campaign successfully while other secondaries can skip the campaign.
	msExpectedLeaderPathFormat           = "/ms/%d/%s/primary/expected_primary"                                // "/ms/{cluster_id}/{service_name}/primary/expected_primary"
	msTsoDefaultExpectedLeaderPathFormat = "/ms/%d/tso/00000/primary/expected_primary"                         // "/ms/{cluster_id}/tso/00000/primary"
	msTsoKespaceExpectedLeaderPathFormat = "/ms/%d/tso/keyspace_groups/election/%05d/primary/expected_primary" // "/ms/{cluster_id}/tso/keyspace_groups/election/{group_id}/primary"

	// resource group path
	resourceGroupSettingsPathFormat = "/resource_group/settings/%s" // "/resource_group/settings/{group_name}"
	resourceGroupStatesPathFormat   = "/resource_group/states/%s"   // "/resource_group/states/{group_name}"
	controllerConfigPath            = "/resource_group/controller"  // "/resource_group/controller"
)

// MsParam is the parameter of micro service.
type MsParam struct {
	ServiceName string
	GroupID     uint32 // only used for tso keyspace group
}

// Prefix returns the parent directory of the given path.
func Prefix(str string) string {
	return path.Dir(str)
}

// LeaderPath returns the leader path.
func LeaderPath(p *MsParam) string {
	if p == nil || p.ServiceName == "" {
		return fmt.Sprintf(leaderPathFormat, ClusterID())
	}
	if p.ServiceName == constant.TSOServiceName {
		if p.GroupID == 0 {
			return fmt.Sprintf(msTsoDefaultLeaderPathFormat, ClusterID())
		}
		return fmt.Sprintf(msTsoKespaceLeaderPathFormat, ClusterID(), p.GroupID)
	}
	return fmt.Sprintf(msLeaderPathFormat, ClusterID(), p.ServiceName)
}

// ExpectedPrimaryPath returns the expected_primary path.
func ExpectedPrimaryPath(p *MsParam) string {
	if p.ServiceName == constant.TSOServiceName {
		if p.GroupID == 0 {
			return fmt.Sprintf(msTsoDefaultExpectedLeaderPathFormat, ClusterID())
		}
		return fmt.Sprintf(msTsoKespaceExpectedLeaderPathFormat, ClusterID(), p.GroupID)
	}
	return fmt.Sprintf(msExpectedLeaderPathFormat, ClusterID(), p.ServiceName)
}

// MemberBinaryDeployPath returns the member binary deploy path.
func MemberBinaryDeployPath(id uint64) string {
	return fmt.Sprintf(memberBinaryDeployPathFormat, ClusterID(), id)
}

// MemberGitHashPath returns the member git hash path.
func MemberGitHashPath(id uint64) string {
	return fmt.Sprintf(memberGitHashPath, ClusterID(), id)
}

// MemberBinaryVersionPath returns the member binary version path.
func MemberBinaryVersionPath(id uint64) string {
	return fmt.Sprintf(memberBinaryVersionPathFormat, ClusterID(), id)
}

// AllocIDPath returns the alloc id path.
func AllocIDPath() string {
	return fmt.Sprintf(allocIDPathFormat, ClusterID())
}

// KeyspaceAllocIDPath returns the keyspace alloc id path.
func KeyspaceAllocIDPath() string {
	return fmt.Sprintf(keyspaceAllocIDPathFormat, ClusterID())
}

// MemberLeaderPriorityPath returns the member leader priority path.
func MemberLeaderPriorityPath(id uint64) string {
	return fmt.Sprintf(kemberLeaderPriorityPathFormat, ClusterID(), id)
}

// RegistryPath returns the full path to store microservice addresses.
func RegistryPath(serviceName, serviceAddr string) string {
	return fmt.Sprintf(registryPathFormat, ClusterID(), serviceName, serviceAddr)
}

// ServicePath returns the path to store microservice addresses.
func ServicePath(serviceName string) string {
	return fmt.Sprintf(servicePathFormat, ClusterID(), serviceName)
}

// GCSafePointV2Prefix is the path prefix to all gc safe point v2.
func GCSafePointV2Prefix() string {
	return fmt.Sprintf(gcSafePointV2PrefixFormat, ClusterID())
}

// ClusterPath is the path to save the cluster meta information.
func ClusterPath() string {
	return fmt.Sprintf(clusterPathFormat, ClusterID())
}

// ClusterBootstrapTimeKey returns the path to save the cluster bootstrap timestamp.
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

func RecoveringMarkPath() string {
	return fmt.Sprintf(recoveringMarkPathFormat, ClusterID())
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
	pattern := strings.Join([]string{KeyspaceGroupIDPrefix(), `(\d{5})$`}, "/")
	return regexp.MustCompile(pattern)
}

// RegionPath returns the region meta info key path with the given region ID.
func RegionPath(regionID uint64) string {
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
