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

package global

import (
	"fmt"
	"sync/atomic"
)

// clusterID is the unique ID for the cluster.
// TODO: remove it to a proper place.
var clusterID atomic.Value

const (
	dcLocationConfigEtcdPrefixFormat = "/pd/%d/dc-location"
	dcLocationPathFormat             = "/pd/%d/dc-location/%d"
	memberLeaderPriorityPathFormat   = "/pd/%d/member/%d/leader_priority"
	memberBinaryDeployPathFormat     = "/pd/%d/member/%d/deploy_path"
	memberGitHashPath                = "/pd/%d/member/%d/git_hash"
	memberBinaryVersionPathFormat    = "/pd/%d/member/%d/binary_version"
	leaderPathFormat                 = "/pd/%d/leader"
)

// ClusterID returns the cluster ID.
func ClusterID() uint64 {
	return clusterID.Load().(uint64)
}

// SetClusterID sets the cluster ID.
func SetClusterID(id uint64) {
	clusterID.Store(id)
}

func GetLeaderPath() string {
	return fmt.Sprintf(leaderPathFormat, ClusterID())
}
