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
)

const (
	leaderPathFormat              = "/pd/%d/leader"                   // "/pd/{cluster_id}/leader"
	memberBinaryDeployPathFormat  = "/pd/%d/member/%d/deploy_path"    // "/pd/{cluster_id}/member/{member_id}/deploy_path"
	memberGitHashPath             = "/pd/%d/member/%d/git_hash"       // "/pd/{cluster_id}/member/{member_id}/git_hash"
	memberBinaryVersionPathFormat = "/pd/%d/member/%d/binary_version" // "/pd/{cluster_id}/member/{member_id}/binary_version"
	allocIDPathFormat             = "/pd/%d/alloc_id"                 // "/pd/{cluster_id}/alloc_id"
	keyspaceAllocIDPathFormat     = "/pd/%d/keyspaces/alloc_id"       // "/pd/{cluster_id}/keyspaces/alloc_id"

	msLeaderPathFormat           = "/ms/%d/%s/primary"                                // "/ms/{cluster_id}/{service_name}/primary"
	msTsoDefaultLeaderPathFormat = "/ms/%d/tso/00000/primary"                         // "/ms/{cluster_id}/tso/00000/primary"
	msTsoKespaceLeaderPathFormat = "/ms/%d/tso/keyspace_groups/election/%05d/primary" // "/ms/{cluster_id}/tso/keyspace_groups/election/{group_id}/primary"
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
	if p.ServiceName == "tso" {
		if p.GroupID == 0 {
			return fmt.Sprintf(msTsoDefaultLeaderPathFormat, ClusterID())
		}
		return fmt.Sprintf(msTsoKespaceLeaderPathFormat, ClusterID(), p.GroupID)
	}
	return fmt.Sprintf(msLeaderPathFormat, ClusterID(), p.ServiceName)
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
