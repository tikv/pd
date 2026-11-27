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
// This follows the leaderTarget and regionTarget logic in StoreStateFilter.anyConditionMatch.
type storeCondition int

const (
	storeAvailable storeCondition = iota

	// All values greater than storeAvailable and less than degradedBoundary will trigger groupDegraded.
	storeLeaderEvicted
	storeBusy
	storeDisconnected
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
	case storeLeaderEvicted:
		return "leader-evicted"
	case storeBusy:
		return "busy"
	case storeDisconnected:
		return "disconnected"
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
	case storeLeaderEvicted, storeBusy:
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

// ObserveAvailableRegion uses a healthy region's peer distribution to update an
// affinity target if the observed group instance, version, and revision are still current.
func (m *Manager) ObserveAvailableRegion(region *core.RegionInfo, group *GroupState) {
	// Use the peer distribution of the first observed available Region as the result.
	// In the future, we may want to use a more sophisticated strategy rather than first-win.
	if group == nil {
		return
	}
	if group.AffinitySchedulingAllowed && (!group.targetUnpersisted || !group.isRegionAffinity(region)) {
		return
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	voterStoreIDs := make([]uint64, len(region.GetVoters()))
	for i, voter := range region.GetVoters() {
		voterStoreIDs[i] = voter.GetStoreId()
	}
	_, _ = m.updateAffinityGroupPeersWithState(group.ID, group, leaderStoreID, voterStoreIDs)
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
	m.RLock()
	hasGroups := len(m.groups) > 0
	generation := m.storeConditionsGeneration
	m.RUnlock()
	if hasGroups {
		m.applyStoreScan(m.collectUnavailableStores(), generation)
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
		if condition := m.checkStoreCondition(store, lowSpaceRatio); condition != storeAvailable {
			unavailableStores[store.GetID()] = condition
		}
	}
	return unavailableStores
}

func (m *Manager) checkStoreCondition(store *core.StoreInfo, lowSpaceRatio float64) storeCondition {
	switch {
	// First the conditions that will mark the group as expired.
	case store == nil || store.IsRemoved() || store.IsPhysicallyDestroyed():
		return storeRemoved
	case store.IsRemoving():
		return storeRemoving
	case store.IsUnhealthy():
		return storeDown

	// Check replica-wide conditions before conditions that only affect leaders.
	case store.IsLowSpace(lowSpaceRatio):
		return storeLowSpace
	case store.IsDisconnected():
		return storeDisconnected
	case store.IsBusy():
		return storeBusy
	case !store.AllowLeaderTransferIn() || m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels()) ||
		store.EvictedAsSlowStore() || store.EvictedAsStoppingStore() || store.IsEvictedAsSlowTrend():
		return storeLeaderEvicted
	}
	// IsSlow and IsPreparing do not make a store unavailable.
	return storeAvailable
}

func (m *Manager) applyStoreScan(unavailableStores map[uint64]storeCondition, generation uint64) bool {
	m.RLock()
	needUpdate := generation == m.storeConditionsGeneration &&
		(!m.storeConditionsKnown || !maps.Equal(unavailableStores, m.unavailableStores))
	m.RUnlock()
	if !needUpdate {
		return false
	}

	m.Lock()
	wasKnown := m.storeConditionsKnown
	isUnavailableStoresChanged := !maps.Equal(unavailableStores, m.unavailableStores)
	if generation != m.storeConditionsGeneration || (wasKnown && !isUnavailableStoresChanged) {
		m.Unlock()
		return false
	}
	m.unavailableStores = unavailableStores
	m.storeConditionsKnown = true
	groupChanged := false
	availableGroupCount := 0
	unavailableGroupCount := 0
	for _, groupInfo := range m.groups {
		availability := groupInfo.GetAvailability()
		if availability != groupExpired {
			newAvailability := calcGroupAvailability(unavailableStores, groupInfo.LeaderStoreID, groupInfo.VoterStoreIDs)
			if availability != newAvailability && (!groupInfo.lastSentWritten || newAvailability != groupAvailable) {
				m.updateGroupAvailabilityLocked(groupInfo.ID, newAvailability)
				availability = newAvailability
				groupChanged = true
			}
		}
		if availability == groupAvailable {
			availableGroupCount++
		} else {
			unavailableGroupCount++
		}
	}
	m.Unlock()

	applied := isUnavailableStoresChanged || groupChanged
	if len(unavailableStores) > 0 && availableGroupCount+unavailableGroupCount > 0 {
		log.Warn("affinity groups invalidated due to unavailable stores",
			zap.Int("unavailable-store-count", len(unavailableStores)),
			zap.Int("unavailable-group-count", unavailableGroupCount),
			zap.Int("available-group-count", availableGroupCount))
	}
	return applied
}

func (m *Manager) checkHasUnavailableStore(leaderStoreID uint64, voterStoreIDs []uint64) error {
	m.RLock()
	defer m.RUnlock()
	return m.checkHasUnavailableStoreLocked(leaderStoreID, voterStoreIDs)
}

// Caller holds at least the read lock of RWMutex.
func (m *Manager) checkHasUnavailableStoreLocked(leaderStoreID uint64, voterStoreIDs []uint64) error {
	var lowSpaceRatio float64
	if !m.storeConditionsKnown {
		lowSpaceRatio = m.conf.GetLowSpaceRatio()
	}
	for _, storeID := range voterStoreIDs {
		store := m.storeSetInformer.GetStore(storeID)
		condition := m.unavailableStores[storeID]
		if store == nil {
			condition = storeRemoved
		} else if !m.storeConditionsKnown {
			condition = m.checkStoreCondition(store, lowSpaceRatio)
		}
		if condition != storeAvailable && (!condition.affectsLeaderOnly() || storeID == leaderStoreID) {
			return errs.ErrAffinityGroupContent.GenWithStackByArgs(fmt.Sprintf("store %d is %s", storeID, condition.String()))
		}
	}
	return nil
}
