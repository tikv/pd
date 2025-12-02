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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
)

const (
	// defaultAvailabilityCheckInterval is the default interval for checking store availability.
	defaultAvailabilityCheckInterval = 10 * time.Second
)

var (
	// availabilityCheckIntervalForTest can be set in tests to speed up availability checks.
	// Default is 0, which means use defaultAvailabilityCheckInterval.
	availabilityCheckIntervalForTest time.Duration
)

// logEntry is used to collect log messages to print after releasing lock
type logEntry struct {
	level            string // "info", "warn"
	groupID          string
	unavailableStore uint64
	state            condition
}

// ObserveAvailableRegion observes available Region and collects information to update the Peer distribution within the Group.
func (m *Manager) ObserveAvailableRegion(region *core.RegionInfo, group *GroupState) {
	// Use the peer distribution of the first observed available Region as the result.
	// In the future, we may want to use a more sophisticated strategy rather than first-win.
	if group == nil || !group.RegularSchedulingEnabled {
		return
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	voterStoreIDs := make([]uint64, len(region.GetVoters()))
	for i, voter := range region.GetVoters() {
		voterStoreIDs[i] = voter.GetStoreId()
	}
	_, _ = m.updateAffinityGroupPeersWithAffinityVer(group.ID, group.affinityVer, leaderStoreID, voterStoreIDs)
}

// getAvailabilityCheckInterval returns the availability check interval, which can be overridden for testing.
func getAvailabilityCheckInterval() time.Duration {
	if availabilityCheckIntervalForTest > 0 {
		return availabilityCheckIntervalForTest
	}
	return defaultAvailabilityCheckInterval
}

// SetAvailabilityCheckIntervalForTest sets the availability check interval for testing. Only use this in tests.
func SetAvailabilityCheckIntervalForTest(interval time.Duration) {
	availabilityCheckIntervalForTest = interval
}

// startAvailabilityCheckLoop starts a goroutine to periodically check store availability and invalidate groups with unavailable stores.
func (m *Manager) startAvailabilityCheckLoop() {
	interval := getAvailabilityCheckInterval()
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				log.Info("affinity manager availability check loop stopped")
				return
			case <-ticker.C:
				m.checkStoresAvailability()
			}
		}
	}()
	log.Info("affinity manager availability check loop started", zap.Duration("interval", interval))
}

// checkStoresAvailability checks the availability status of stores and invalidates groups with unavailable stores.
func (m *Manager) checkStoresAvailability() {
	if !m.IsAvailable() {
		return
	}
	unavailableStores := m.generateUnavailableStores()
	isUnavailableStoresChanged, groupStateChanges := m.getGroupStateChanges(unavailableStores)
	if isUnavailableStoresChanged {
		m.setGroupStateChanges(unavailableStores, groupStateChanges)
	}
}

func (m *Manager) generateUnavailableStores() map[uint64]condition {
	unavailableStores := make(map[uint64]condition)
	stores := m.storeSetInformer.GetStores()
	lowSpaceRatio := m.conf.GetLowSpaceRatio()
	for _, store := range stores {
		switch {
		// Check the groupExpired-related store state first
		case store.IsRemoved() || store.IsPhysicallyDestroyed() || store.IsRemoving():
			unavailableStores[store.GetID()] = storeRemovingOrRemoved
		case store.IsUnhealthy():
			unavailableStores[store.GetID()] = storeDown

		// Then check the groupDegraded-related store state
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

func (m *Manager) getGroupStateChanges(unavailableStores map[uint64]condition) (isUnavailableStoresChanged bool, groupStateChanges map[string]condition) {
	m.RLock()
	// Validate whether unavailableStores has changed.
	isUnavailableStoresChanged = len(m.unavailableStores) != len(unavailableStores)
	if !isUnavailableStoresChanged {
		for storeID, state := range m.unavailableStores {
			if state != unavailableStores[storeID] {
				isUnavailableStoresChanged = true
				break
			}
		}
		if !isUnavailableStoresChanged {
			m.RUnlock()
			return false, nil
		}
	}
	// Analyze which Groups have changed state
	// Collect log messages to print after releasing lock
	var logEntries []logEntry

	groupStateChanges = make(map[string]condition)
	for _, groupInfo := range m.groups {
		var unavailableStore uint64
		var maxState condition
		for _, storeID := range groupInfo.VoterStoreIDs {
			if state, ok := unavailableStores[storeID]; ok && (!state.affectsLeaderOnly() || storeID == groupInfo.LeaderStoreID) {
				if unavailableStore == 0 || state > maxState {
					unavailableStore = storeID
					maxState = state
				}
			}
		}
		newState := maxState.toGroupState()
		if newState != groupInfo.getState() {
			groupStateChanges[groupInfo.ID] = newState
			if unavailableStore != 0 {
				logEntries = append(logEntries, logEntry{
					level:            "warn",
					groupID:          groupInfo.ID,
					unavailableStore: unavailableStore,
					state:            newState,
				})
			} else {
				logEntries = append(logEntries, logEntry{
					level:   "info",
					groupID: groupInfo.ID,
				})
			}
		}
	}
	m.RUnlock()

	// Log after releasing lock
	for _, entry := range logEntries {
		switch entry.level {
		case "warn":
			log.Warn("affinity group invalidated due to unavailable stores",
				zap.String("group-id", entry.groupID),
				zap.Uint64("unavailable-store", entry.unavailableStore),
				zap.String("state", entry.state.String()))
		case "info":
			log.Info("affinity group become available", zap.String("group-id", entry.groupID))
		}
	}

	return
}

func (m *Manager) setGroupStateChanges(unavailableStores map[uint64]condition, groupStateChanges map[string]condition) {
	m.Lock()
	defer m.Unlock()
	m.unavailableStores = unavailableStores
	for groupID, state := range groupStateChanges {
		m.updateGroupStateLocked(groupID, state)
	}
}

func (m *Manager) hasUnavailableStore(leaderStoreID uint64, voterStoreIDs []uint64) error {
	m.RLock()
	defer m.RUnlock()
	for _, storeID := range voterStoreIDs {
		state, ok := m.unavailableStores[storeID]
		if ok && (!state.affectsLeaderOnly() || storeID == leaderStoreID) {
			return errs.ErrAffinityGroupContent.GenWithStackByArgs(fmt.Sprintf("store %d is %s", storeID, state.storeStateString()))
		}
	}
	return nil
}
