// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"encoding/json"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
)

// Group defines an affinity group. Regions belonging to it will tend to have the same distribution.
// NOTE: This type is exported by HTTP API and persisted in storage. Please pay more attention when modifying it.
type Group struct {
	// ID is a unique identifier for Group.
	ID string `json:"id"`
	// CreateTimestamp is the time when the Group was created.
	CreateTimestamp uint64 `json:"create_timestamp"`

	// The following parameters are all determined automatically.

	// LeaderStoreID indicates which store the leader should be on.
	LeaderStoreID uint64 `json:"leader_store_id,omitempty"`
	// VoterStoreIDs indicates which stores Voters should be on.
	VoterStoreIDs []uint64 `json:"voter_store_ids,omitempty"`
	// TODO: LearnerStoreIDs
}

func (g *Group) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}

// GroupState defines the runtime state of an affinity group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type GroupState struct {
	Group
	// Effect parameter indicates whether the current constraint is in effect.
	Effect bool `json:"effect"`
	// RangeCount indicates how many key ranges are associated with this group.
	RangeCount int `json:"range_count"`
	// RegionCount indicates how many Regions are currently in the affinity state.
	RegionCount int `json:"region_count"`
	// AffinityRegionCount indicates how many Regions have all Voter and Leader peers in the correct stores.
	AffinityRegionCount int `json:"affinity_region_count"`

	// affinityVer is used to mark the version of the cache.
	affinityVer uint64
	// groupInfoPtr is a pointer to the original information.
	// It is used only for pointer comparison and should not access any internal data.
	groupInfoPtr *runtimeGroupInfo
}

// IsRegionAffinity checks whether the Region is in an affinity state.
func (g *GroupState) isRegionAffinity(region *core.RegionInfo, cache *regionCache) bool {
	if region == nil || !g.Effect {
		return false
	}

	// Use the result in the cache when both the Region pointer and the Group’s affinityVer remain unchanged.
	if region == cache.region && g.affinityVer == cache.affinityVer {
		return cache.isAffinity
	}

	// Compare the Leader
	if region.GetLeader().GetStoreId() != g.LeaderStoreID {
		return false
	}
	// Compare the Voters
	voters := region.GetVoters()
	if len(voters) != len(g.VoterStoreIDs) {
		return false
	}
	expected := make(map[uint64]struct{}, len(voters))
	for _, voter := range g.VoterStoreIDs {
		expected[voter] = struct{}{}
	}
	for _, voter := range voters {
		if _, ok := expected[voter.GetStoreId()]; !ok {
			return false
		}
	}
	// TODO: Compare the Learners.
	return true
}

// runtimeGroupInfo contains meta information and runtime statistics for the Group.
type runtimeGroupInfo struct {
	Group

	// Effect parameter indicates whether the current constraint is in effect.
	// Constraints are typically released when the store is in an abnormal state.
	Effect bool
	// AffinityVer initializes at 1 and increments by 1 each time the Group changes.
	AffinityVer uint64
	// AffinityRegionCount indicates how many Regions have all Voter and Leader peers in the correct stores. (AffinityVer equals).
	AffinityRegionCount int

	// Regions represents the cache of Regions.
	Regions map[uint64]regionCache
	// TODO: Consider separate modification support in the future (read-modify keyrange-write)
	// Currently using label's internal multiple keyrange mechanism
	LabelRule *labeler.LabelRule
	// RangeCount counts how many KeyRanges exist in the Label.
	RangeCount int
}

// newGroupState creates a GroupState from the given runtimeGroupInfo.
// runtimeGroupInfo may need to be accessed under a Lock.
func newGroupState(g *runtimeGroupInfo) *GroupState {
	return &GroupState{
		Group: Group{
			ID:              g.ID,
			CreateTimestamp: g.CreateTimestamp,
			LeaderStoreID:   g.LeaderStoreID,
			VoterStoreIDs:   append([]uint64(nil), g.VoterStoreIDs...),
		},
		Effect:              g.Effect,
		RangeCount:          g.RangeCount,
		RegionCount:         len(g.Regions),
		AffinityRegionCount: g.AffinityRegionCount,
		affinityVer:         g.AffinityVer,
		groupInfoPtr:        g,
	}
}

// AdjustGroup validates the group and sets default values.
func (m *Manager) AdjustGroup(g *Group) error {
	if g.ID == "" {
		return errs.ErrAffinityGroupContent.FastGenByArgs("group ID should not be empty")
	}
	// TODO: Add more validation logic here if needed.
	// If no distribution is provided, we will use the default distribution.
	if g.LeaderStoreID == 0 && len(g.VoterStoreIDs) == 0 {
		return nil
	}
	// Once provided, leader store ID and voter store IDs must be provided together.
	if g.LeaderStoreID == 0 || len(g.VoterStoreIDs) == 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader store ID and voter store IDs must be provided together")
	}

	if m.storeSetInformer.GetStore(g.LeaderStoreID) == nil {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader store does not exist")
	}

	leaderInVoters := false
	storeSet := make(map[uint64]struct{})
	for _, storeID := range g.VoterStoreIDs {
		if storeID == g.LeaderStoreID {
			leaderInVoters = true
		}
		if _, exists := storeSet[storeID]; exists {
			return errs.ErrAffinityGroupContent.FastGenByArgs("duplicate voter store ID")
		}
		storeSet[storeID] = struct{}{}

		if m.storeSetInformer.GetStore(storeID) == nil {
			return errs.ErrAffinityGroupContent.FastGenByArgs("voter store does not exist")
		}
	}
	if !leaderInVoters {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader must be in voter stores")
	}
	return nil
}

// IsRegionAffinity checks if a region conforms to its affinity group distribution requirements.
// Returns true if the region:
// Belongs to an affinity group and satisfies all distribution constraints:
//   - Leader is on the expected store
//   - All voters are on the expected stores
func (m *Manager) IsRegionAffinity(region *core.RegionInfo) bool {
	_, isAffinity := m.GetRegionAffinityGroupState(region)
	return isAffinity
}
