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
	"regexp"
	"slices"
	"time"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/slice"
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
	storeEvictLeader
	storeDisconnected
	storePreparing
	storeLowSpace
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

// Phase is a status intended for API display
type Phase string

const (
	// PhasePending indicates that the Group is still determining the StoreIDs.
	// If the Group has no KeyRanges, it remains in PhasePending forever.
	PhasePending = Phase("pending")
	// PhasePreparing indicates that the Group is scheduling Regions according to the required Peers.
	PhasePreparing = Phase("preparing")
	// PhaseStable indicates that the Group has completed the required scheduling and is currently in a stable state.
	PhaseStable = Phase("stable")
)

// idPattern is a regex that specifies acceptable characters of the id.
// Valid id must be non-empty and 64 characters or fewer and consist only of letters (a-z, A-Z),
// numbers (0-9), hyphens (-), and underscores (_).
const idPattern = "^[-A-Za-z0-9_]{1,64}$"

var idRegexp = regexp.MustCompile(idPattern)

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

func (s condition) storeStateString() string {
	switch s {
	case storeEvictLeader:
		return "evicted"
	case storeDisconnected:
		return "disconnected"
	case storePreparing:
		return "preparing"
	case storeLowSpace:
		return "low-space"
	case storeDown:
		return "down"
	case storeRemovingOrRemoved:
		return "removing-or-removed"
	default:
		return "unknown"
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
	RegularSchedulingEnabled bool `json:"-"`
	// AffinitySchedulingEnabled indicates whether affinity scheduling is allowed.
	AffinitySchedulingEnabled bool `json:"-"`
	// Phase is a status intended for API display. See the definition of Phase for details.
	Phase Phase `json:"phase"`
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
	expected := make([]uint64, len(voters))
	for i, voter := range voters {
		expected[i] = voter.GetStoreId()
	}
	slices.Sort(expected)
	return slices.Equal(expected, g.VoterStoreIDs)
}

// runtimeGroupInfo contains meta information and runtime statistics for the Group.
// Note: runtimeGroupInfo must be used within Manager’s RWMutex. Otherwise, use the GroupState generated by newGroupState.
type runtimeGroupInfo struct {
	Group

	// state should use the condition enum values whose names start with group.
	// state must be used together with degradedExpiredAt. Therefore, GetState and SetState must always be used to
	// read or modify them, and state must not be accessed directly.
	state condition
	// degradedExpiredAt indicates the expiration time of groupDegraded. After this time, it should be treated as groupExpired.
	degradedExpiredAt uint64
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
func newGroupState(g *runtimeGroupInfo) *GroupState {
	var phase Phase
	affinitySchedulingEnabled := g.IsAffinitySchedulingEnabled()
	if g.RangeCount != 0 && affinitySchedulingEnabled {
		if g.AffinityRegionCount > 0 && len(g.Regions) == g.AffinityRegionCount {
			phase = PhaseStable
		} else {
			phase = PhasePreparing
		}
	} else {
		phase = PhasePending
	}

	return &GroupState{
		Group: Group{
			ID:              g.ID,
			CreateTimestamp: g.CreateTimestamp,
			LeaderStoreID:   g.LeaderStoreID,
			VoterStoreIDs:   slices.Clone(g.VoterStoreIDs),
		},
		RegularSchedulingEnabled:  g.IsRegularSchedulingEnabled(),
		AffinitySchedulingEnabled: affinitySchedulingEnabled,
		Phase:                     phase,
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
	return g.state.toGroupState() == groupAvailable
}

// IsExpired indicates that the Group is currently in the groupExpired state,
// which disallows affinity scheduling and allows other balancing scheduling.
func (g *runtimeGroupInfo) IsExpired() bool {
	switch g.state.toGroupState() {
	case groupExpired:
		return true
	case groupDegraded:
		return uint64(time.Now().Unix()) > g.degradedExpiredAt
	default:
		return false
	}
}

// GetState returns a condition whose value is always group-prefixed. It handles degradedExpiredAt internally.
func (g *runtimeGroupInfo) GetState() condition {
	state := g.state.toGroupState()
	if state == groupAvailable {
		return groupAvailable
	} else if g.IsExpired() {
		return groupExpired
	}
	return groupDegraded
}

// SetState updates state and degradedExpiredAt with the following behavior:
// - When the current state is groupAvailable
//   - newState = groupAvailable: do nothing
//   - newState = groupDegraded: change the state to groupDegraded and set degradedExpiredAt based on the current time
//   - newState = groupExpired: change the state to groupExpired
//
// - When the current state is groupDegraded
//   - newState = groupAvailable: change the state to groupAvailable
//   - newState = groupDegraded: do nothing and do not refresh degradedExpiredAt
//   - newState = groupExpired: change the state to groupExpired
//
// - When the current state is groupExpired
//   - newState = groupAvailable: change the state to groupAvailable
//   - newState = groupDegraded: do nothing and do not refresh degradedExpiredAt
//   - newState = groupExpired: do nothing
func (g *runtimeGroupInfo) SetState(newState condition) {
	// If the expiration time has been reached, change groupDegraded to groupExpired.
	if g.state == groupDegraded && g.IsExpired() {
		g.state = groupExpired
	}
	// Update state
	newState = newState.toGroupState()
	if newState == groupDegraded {
		// Only set the expiration time when transitioning from groupAvailable to groupDegraded.
		// Do nothing if the original state is already groupDegraded or groupExpired.
		if g.state == groupAvailable {
			g.state = groupDegraded
			g.degradedExpiredAt = newDegradedExpiredAtFromNow()
		}
	} else {
		g.state = newState
	}
}

func newDegradedExpiredAtFromNow() uint64 {
	return uint64(time.Now().Unix()) + defaultDegradedExpirationSeconds
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
	if err := ValidateGroupID(g.ID); err != nil {
		return err
	}
	// If no distribution is provided, we will use the default distribution.
	if g.LeaderStoreID == 0 && len(g.VoterStoreIDs) == 0 {
		return nil
	}
	// Once provided, leader store ID and voter store IDs must be provided together.
	if g.LeaderStoreID == 0 || len(g.VoterStoreIDs) == 0 {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader store ID and voter store IDs must be provided together")
	}

	voterStoreIDs := slices.Clone(g.VoterStoreIDs)
	slices.Sort(voterStoreIDs)
	if slice.HasDupInSorted(voterStoreIDs) {
		return errs.ErrAffinityGroupContent.FastGenByArgs("duplicate voter store ID")
	}
	if !slices.Contains(voterStoreIDs, g.LeaderStoreID) {
		return errs.ErrAffinityGroupContent.FastGenByArgs("leader must be in voter stores")
	}
	for _, storeID := range voterStoreIDs {
		if m.storeSetInformer.GetStore(storeID) == nil {
			return errs.ErrAffinityGroupContent.FastGenByArgs("store does not exist")
		}
	}

	return nil
}

// GroupKeyRanges represents key ranges with group id.
type GroupKeyRanges struct {
	KeyRanges []keyutil.KeyRange
	GroupID   string
}

// ValidateGroupID checks the ID format.
func ValidateGroupID(id string) error {
	if idRegexp.MatchString(id) {
		return nil
	}
	return errs.ErrInvalidGroupID.GenWithStackByArgs(id)
}
