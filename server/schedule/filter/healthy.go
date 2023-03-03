// Copyright 2019 TiKV Project Authors.
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

package filter

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/server/schedule/config"
	"github.com/tikv/pd/server/schedule/placement"
)

// IsRegionHealthy checks if a region is healthy for scheduling. It requires the
// region does not have any down or pending peers.
func IsRegionHealthy(region *core.RegionInfo) bool {
	return IsRegionHealthyAllowPending(region) && !hasPendingPeers(region)
}

// IsRegionHealthyAllowPending checks if a region is healthy for scheduling.
// Differs from IsRegionHealthy, it allows the region to have pending peers.
func IsRegionHealthyAllowPending(region *core.RegionInfo) bool {
	return !hasDownPeers(region)
}

func hasPendingPeers(region *core.RegionInfo) bool {
	return len(region.GetPendingPeers()) > 0
}

func hasDownPeers(region *core.RegionInfo) bool {
	return len(region.GetDownPeers()) > 0
}

// IsRegionReplicated checks if a region is fully replicated. When placement
// rules is enabled, its peers should fit corresponding rules. When placement
// rules is disabled, it should have enough replicas and no any learner peer.
func IsRegionReplicated(cluster regionHealthCluster, region *core.RegionInfo) bool {
	if cluster.GetOpts().IsPlacementRulesEnabled() {
		return isRegionPlacementRuleSatisfied(cluster, region)
	}
	return isRegionReplicasSatisfied(cluster, region)
}

func isRegionPlacementRuleSatisfied(cluster regionHealthCluster, region *core.RegionInfo) bool {
	return cluster.GetRuleManager().FitRegion(cluster, region).IsSatisfied()
}

func isRegionReplicasSatisfied(cluster regionHealthCluster, region *core.RegionInfo) bool {
	return len(region.GetLearners()) == 0 && len(region.GetPeers()) == cluster.GetOpts().GetMaxReplicas()
}

// ReplicatedRegion returns a function that checks if a region is fully replicated.
func ReplicatedRegion(cluster regionHealthCluster) func(*core.RegionInfo) bool {
	return func(region *core.RegionInfo) bool { return IsRegionReplicated(cluster, region) }
}

// cluster provides an overview of a cluster's regions distribution.
type regionHealthCluster interface {
	core.StoreSetInformer
	core.RegionSetInformer
	GetOpts() config.Config
	GetRuleManager() *placement.RuleManager
}
