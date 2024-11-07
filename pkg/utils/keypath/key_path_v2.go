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
	leaderPathFormat     = "/pd/%d/leader"         // "/pd/{cluster_id}/leader"
	dcLocationPathFormat = "/pd/%d/dc-location/%d" // "/pd/{cluster_id}/dc-location/{member_id}"

	msLeaderPathFormat     = "/ms/%d/%s/primary"        // "/ms/{cluster_id}/{service_name}/primary"
	msDCLocationPathFormat = "/ms/%d/%s/dc-location/%d" // "/ms/{cluster_id}/{service_name}/dc-location/{member_id}"
)

func Prefix(str string) string {
	return path.Dir(str)
}

func LeaderPath(serviceName string) string {
	if serviceName == "" {
		return fmt.Sprintf(leaderPathFormat, ClusterID())
	}
	return fmt.Sprintf(msLeaderPathFormat, ClusterID(), serviceName)
}

func DCLocationPath(serviceName string, id uint64) string {
	if serviceName == "" {
		return fmt.Sprintf(dcLocationPathFormat, ClusterID(), id)
	}
	return fmt.Sprintf(msDCLocationPathFormat, ClusterID(), serviceName, id)
}
