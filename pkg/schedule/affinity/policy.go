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
	"fmt"
	"maps"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	// defaultAvailabilityCheckInterval is the default interval for checking store availability.
	defaultAvailabilityCheckInterval = 10 * time.Second
)

// storeCondition is an enum for store conditions. Valid values are the store-prefixed enum constants,
// which are split into three groups separated by degradedBoundary.
// Roughly, larger values indicate a more severe degree of unavailability.
type storeCondition int

const (
	storeAvailable storeCondition = iota

	// All values greater than storeAvailable and less than degradedBoundary will trigger groupDegraded.
	storeEvictLeader
	storeDisconnected
	storePreparing
	storeLowSpace
	degradedBoundary

	// All values greater than degradedBoundary will trigger groupExpired.
	storeDown
	storeRemoving
	storeRemoved
)

func (c storeCondition) String() string {
	switch c {
	case storeAvailable:
		return "available"
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
	case storeRemoving:
		return "removing"
	case storeRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

func (c storeCondition) groupAvailability() groupAvailability {
	switch {
	case c == storeAvailable:
		return groupAvailable
	case c <= degradedBoundary:
		return groupDegraded
	default:
		return groupExpired
	}
}

func (c storeCondition) affectsLeaderOnly() bool {
	switch c {
	case storeEvictLeader:
		return true
	default:
		return false
	}
}

func calcGroupAvailability(
	unavailableStores map[uint64]storeCondition,
	leaderStoreID uint64,
	voterStoreIDs []uint64,
) groupAvailability {
	worstCondition := storeAvailable
	for _, storeID := range voterStoreIDs {
		if condition, ok := unavailableStores[storeID]; ok && (!condition.affectsLeaderOnly() || storeID == leaderStoreID) {
			if worstCondition == storeAvailable || condition > worstCondition {
				worstCondition = condition
			}
		}
	}
	return worstCondition.groupAvailability()
}

// ObserveAvailableRegion observes available Region and collects information to update the Peer distribution within the Group.
func (m *Manager) ObserveAvailableRegion(region *core.RegionInfo, group *GroupState) {
	// Use the peer distribution of the first observed available Region as the result.
	// In the future, we may want to use a more sophisticated strategy rather than first-win.
	if group == nil || group.AffinitySchedulingAllowed {
		return
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	voterStoreIDs := make([]uint64, len(region.GetVoters()))
	for i, voter := range region.GetVoters() {
		voterStoreIDs[i] = voter.GetStoreId()
	}
	_, _ = m.updateAffinityGroupPeersWithAffinityVer(group.ID, group.affinityVer, leaderStoreID, voterStoreIDs)
}

// startAvailabilityCheckLoop starts a goroutine to periodically check store availability and invalidate groups with unavailable stores.
// TODO: If critical operations are added, a graceful shutdown is required.
func (m *Manager) startAvailabilityCheckLoop() {
	interval := defaultAvailabilityCheckInterval
	ticker := time.NewTicker(interval)
	failpoint.Inject("changeAvailabilityCheckInterval", func() {
		ticker.Reset(100 * time.Millisecond)
	})
	go func() {
		defer logutil.LogPanic()
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				log.Info("affinity manager availability check loop stopped")
				return
			case <-ticker.C:
				m.checkGroupsAvailability()
			}
		}
	}()
	log.Info("affinity manager availability check loop started", zap.Duration("interval", interval))
}

// checkGroupsAvailability checks the condition of stores and invalidates groups with unavailable stores.
func (m *Manager) checkGroupsAvailability() {
	if !m.IsAvailable() {
		return
	}
	unavailableStores := m.collectUnavailableStores()
	isUnavailableStoresChanged, groupAvailabilityChanges := m.getGroupAvailabilityChanges(unavailableStores)
	if isUnavailableStoresChanged {
		m.setGroupAvailabilityChanges(unavailableStores, groupAvailabilityChanges)
	}
	m.collectMetrics()
}

// collectMetrics collects the global metrics of the affinity manager.
func (m *Manager) collectMetrics() {
	m.RLock()
	defer m.RUnlock()

	// Collect global metrics
	groupCount.Set(float64(len(m.groups)))
	regionCount.Set(float64(len(m.regions)))
	affinityRegionCount.Set(float64(m.affinityRegionCount))
}

func (m *Manager) collectUnavailableStores() map[uint64]storeCondition {
	unavailableStores := make(map[uint64]storeCondition)
	stores := m.storeSetInformer.GetStores()
	lowSpaceRatio := m.conf.GetLowSpaceRatio()
	for _, store := range stores {
		switch {
		// First the conditions that will mark the group as expired
		case store.IsRemoved() || store.IsPhysicallyDestroyed():
			unavailableStores[store.GetID()] = storeRemoved
		case store.IsRemoving():
			unavailableStores[store.GetID()] = storeRemoving
		case store.IsUnhealthy():
			unavailableStores[store.GetID()] = storeDown

		// Then the conditions that will mark the group as degraded
		case !store.AllowLeaderTransferIn() || m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels()):
			unavailableStores[store.GetID()] = storeEvictLeader
		case store.IsDisconnected():
			unavailableStores[store.GetID()] = storeDisconnected
		case store.IsLowSpace(lowSpaceRatio):
			unavailableStores[store.GetID()] = storeLowSpace
		case store.IsPreparing():
			unavailableStores[store.GetID()] = storePreparing
		}
		// Note: We intentionally do NOT check:
		// - IsSlow(): Performance issue, not availability issue
	}
	return unavailableStores
}

func (m *Manager) getGroupAvailabilityChanges(unavailableStores map[uint64]storeCondition) (isUnavailableStoresChanged bool, groupAvailabilityChanges map[string]groupAvailability) {
	groupAvailabilityChanges = make(map[string]groupAvailability)
	availableGroupCount := 0
	unavailableGroupCount := 0

	// Validate whether unavailableStores has changed.
	m.RLock()
	isUnavailableStoresChanged = !maps.Equal(unavailableStores, m.unavailableStores)
	if !isUnavailableStoresChanged {
		m.RUnlock()
		return false, nil
	}

	// Analyze which Groups have changed availability
	// Collect log messages to print after releasing lock
	for _, groupInfo := range m.groups {
		availability := groupInfo.GetAvailability()

		// A Group in the expired status cannot be restored.
		if availability == groupExpired {
			unavailableGroupCount++
			continue
		}

		// Only Groups in the available or degraded status can be changed automatically.
		newAvailability := calcGroupAvailability(unavailableStores, groupInfo.LeaderStoreID, groupInfo.VoterStoreIDs)
		if availability != newAvailability {
			groupAvailabilityChanges[groupInfo.ID] = newAvailability
		}
		if newAvailability == groupAvailable {
			availableGroupCount++
		} else {
			unavailableGroupCount++
		}
	}
	m.RUnlock()

	if len(unavailableStores) > 0 {
		log.Warn("affinity groups invalidated due to unavailable stores",
			zap.Int("unavailable-store-count", len(unavailableStores)),
			zap.Int("unavailable-group-count", unavailableGroupCount),
			zap.Int("available-group-count", availableGroupCount))
	}

	return
}

func (m *Manager) setGroupAvailabilityChanges(unavailableStores map[uint64]storeCondition, groupAvailabilityChanges map[string]groupAvailability) {
	m.Lock()
	defer m.Unlock()
	m.unavailableStores = unavailableStores
	for groupID, availability := range groupAvailabilityChanges {
		m.updateGroupAvailabilityLocked(groupID, availability)
	}
}

func (m *Manager) checkHasUnavailableStore(leaderStoreID uint64, voterStoreIDs []uint64) error {
	m.RLock()
	defer m.RUnlock()
	for _, storeID := range voterStoreIDs {
		condition, ok := m.unavailableStores[storeID]
		if ok && (!condition.affectsLeaderOnly() || storeID == leaderStoreID) {
			return errs.ErrAffinityGroupContent.GenWithStackByArgs(fmt.Sprintf("store %d is %s", storeID, condition.String()))
		}
	}
	return nil
}
