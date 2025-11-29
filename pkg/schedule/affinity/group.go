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
	"slices"
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// condition is an enumeration that includes both Store state and Group state.
type condition int

const (
	// groupAvailable indicates that the current Group allows affinity scheduling and disallows other balancing scheduling.
	groupAvailable condition = iota

	// groupDegraded indicates that the current Group does not generate affinity scheduling but still disallows other balancing scheduling.
	// All values greater than groupAvailable and less than or equal to groupDegraded represent groupDegraded states.
	// The groupDegraded state should have an expiration time. After it expires, it should be treated as groupExpired.
	storeDisconnected
	storeLowSpace
	storePreparing
	storeEvictLeader
	groupDegraded

	// groupExpired indicates that the current Group does not generate affinity scheduling and allows other balancing scheduling.
	// All values greater than groupDegraded and less than or equal to groupExpired represent groupExpired states.
	storeDown
	storeRemovingOrRemoved
	groupExpired

	// groupAvailable, groupDegraded, and groupExpired define the Group’s availability lifecycle.
	// Roughly:
	//   groupAvailable ──degraded (e.g. store evict-leader)───────────────> groupDegraded
	//   groupDegraded  ──recovered────────────────────────────────────────> groupAvailable // expected to be temporary
	//   groupDegraded  ──expired──────────────────────────────────────────> groupExpired
	//   groupAvailable ──directly failed (e.g. store removed)─────────────> groupExpired
	//   groupExpired   ──reconfigured (e.g. peers moved to healthy stores)→ groupAvailable
	// groupDegraded is intended to be a temporary state that may return to groupAvailable,
	// while groupExpired usually represents a terminal state under the current topology,
	// but can become groupAvailable again after the Group’s stores/peers are reconfigured.
	// groupDegraded has an expiration time (degradedExpiredAt); once it expires, the Group is
	// automatically treated as groupExpired.
)

// toGroupState converts the condition into the corresponding Group state.
func (s condition) toGroupState() condition {
	if s == groupAvailable {
		return groupAvailable
	} else if s <= groupDegraded {
		return groupDegraded
	}
	return groupExpired
}

func (s condition) affectsLeaderOnly() bool {
	switch s {
	case storeEvictLeader:
		return true
	default:
		return false
	}
}

func (s condition) String() string {
	switch s.toGroupState() {
	case groupDegraded:
		return "degraded"
	case groupExpired:
		return "expired"
	default:
		return "available"
	}
}

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
	// RegularSchedulingEnabled indicates whether balance scheduling is allowed.
	RegularSchedulingEnabled bool `json:"regular_scheduling_enabled"`
	// AffinitySchedulingEnabled indicates whether affinity scheduling is allowed.
	AffinitySchedulingEnabled bool `json:"affinity_scheduling_enabled"`
	// RangeCount indicates how many key ranges are associated with this group.
	RangeCount int `json:"range_count"`
	// RegionCount indicates how many Regions are currently in the group.
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
func (g *GroupState) isRegionAffinity(region *core.RegionInfo) bool {
	if region == nil || !g.AffinitySchedulingEnabled {
		return false
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

	// State should use the condition enum values whose names start with group.
	State condition
	// DegradedExpiredAt indicates the expiration time of groupDegraded. After this time, it should be treated as groupExpired.
	DegradedExpiredAt uint64
	// AffinityVer initializes at 1 and increments by 1 each time the Group changes.
	AffinityVer uint64
	// AffinityRegionCount indicates how many Regions have all Voter and Leader peers in the correct stores. (AffinityVer equals).
	AffinityRegionCount int

	// Regions represents the cache of Regions.
	Regions map[uint64]regionCache
	// LabelRule using label's internal multiple keyrange mechanism.
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
			VoterStoreIDs:   slices.Clone(g.VoterStoreIDs),
		},
		RegularSchedulingEnabled:  g.IsRegularSchedulingEnabled(),
		AffinitySchedulingEnabled: g.IsAffinitySchedulingEnabled(),
		RangeCount:                g.RangeCount,
		RegionCount:               len(g.Regions),
		AffinityRegionCount:       g.AffinityRegionCount,
		affinityVer:               g.AffinityVer,
		groupInfoPtr:              g,
	}
}

// IsAvailable indicates that the Group is currently in the groupAvailable state,
// which allows affinity scheduling and disallows other balancing scheduling.
func (g *runtimeGroupInfo) IsAvailable() bool {
	return g.State.toGroupState() == groupAvailable
}

// IsExpired indicates that the Group is currently in the groupExpired state,
// which disallows affinity scheduling and allows other balancing scheduling.
func (g *runtimeGroupInfo) IsExpired() bool {
	switch g.State.toGroupState() {
	case groupExpired:
		return true
	case groupDegraded:
		return uint64(time.Now().Unix()) > g.DegradedExpiredAt
	default:
		return false
	}
}

func (g *runtimeGroupInfo) getState() condition {
	state := g.State.toGroupState()
	if state == groupAvailable {
		return groupAvailable
	} else if g.IsExpired() {
		return groupExpired
	}
	return groupDegraded
}

// IsAffinitySchedulingEnabled indicates whether affinity scheduling is allowed.
func (g *runtimeGroupInfo) IsAffinitySchedulingEnabled() bool {
	return g.IsAvailable() && g.LeaderStoreID != 0 && len(g.VoterStoreIDs) != 0
}

// IsRegularSchedulingEnabled indicates whether balance scheduling is allowed.
func (g *runtimeGroupInfo) IsRegularSchedulingEnabled() bool {
	return g.IsExpired() || g.LeaderStoreID == 0 || len(g.VoterStoreIDs) == 0
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

// GroupKeyRanges represents key ranges with group id.
type GroupKeyRanges struct {
	KeyRanges []keyutil.KeyRange
	GroupID   string
}
