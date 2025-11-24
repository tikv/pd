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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
)

type storeState int

const (
	available storeState = iota
	down
	lowSpace
	preparing
	degraded // storeState less than degraded and non-zero indicate the store is not unusable.
	removingOrRemoved
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

// nolint
func (state storeState) isAvailable() bool {
	return state == available
}

// nolint
func (state storeState) isUnavailable() bool {
	return state != available
}

// nolint
func (state storeState) isDegraded() bool {
	return state > 0 && state <= degraded
}

// nolint
func (state storeState) isUnusable() bool {
	return state > degraded
}

// ObserveAvailableRegion observes available Region and collects information to update the Peer distribution within the Group.
func (m *Manager) ObserveAvailableRegion(region *core.RegionInfo, group *GroupState) {
	// Use the peer distribution of the first observed available Region as the result.
	// TODO: Improve the strategy.
	if group == nil || group.Effect || !m.IsAvailable() {
		return
	}
	leaderStoreID := region.GetLeader().GetStoreId()
	voterStoreIDs := make([]uint64, 0, len(region.GetVoters()))
	for _, voter := range region.GetVoters() {
		voterStoreIDs = append(voterStoreIDs, voter.GetStoreId())
	}
	if m.hasUnavailableStore(voterStoreIDs) {
		return
	}
	m.Lock()
	defer m.Unlock()
	m.updateGroupEffectLocked(group.ID, group.affinityVer, leaderStoreID, voterStoreIDs)
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
	if m.isUnavailableStoresChanged(unavailableStores) {
		m.setUnavailableStores(unavailableStores)
	}
}

func (m *Manager) generateUnavailableStores() map[uint64]storeState {
	unavailableStores := make(map[uint64]storeState)
	stores := m.storeSetInformer.GetStores()
	lowSpaceRatio := m.conf.GetLowSpaceRatio()
	for _, store := range stores {
		if !store.AllowLeaderTransferIn() || m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels()) {
			unavailableStores[store.GetID()] = degraded
			continue
		}
		if store.IsRemoved() || store.IsPhysicallyDestroyed() || store.IsRemoving() {
			unavailableStores[store.GetID()] = removingOrRemoved
		} else if store.IsUnhealthy() {
			// Use IsUnavailable (10min) to avoid frequent state flapping
			// IsUnavailable: DownTime > 10min (storeUnavailableDuration)
			// IsDisconnected: DownTime > 20s (storeDisconnectDuration) - too sensitive
			unavailableStores[store.GetID()] = down
		} else if store.IsLowSpace(lowSpaceRatio) {
			unavailableStores[store.GetID()] = lowSpace
		} else if store.IsPreparing() {
			unavailableStores[store.GetID()] = preparing
		}
		// Note: We intentionally do NOT check:
		// - IsDisconnected(): Too sensitive (20s), would cause frequent flapping
		// - IsSlow(): Performance issue, not availability issue
	}
	return unavailableStores
}

func (m *Manager) isUnavailableStoresChanged(unavailableStores map[uint64]storeState) bool {
	m.RLock()
	defer m.RUnlock()
	if len(m.unavailableStores) != len(unavailableStores) {
		return true
	}
	for storeID, state := range m.unavailableStores {
		if state != unavailableStores[storeID] {
			return true
		}
	}
	return false
}

func (m *Manager) setUnavailableStores(unavailableStores map[uint64]storeState) {
	m.Lock()
	defer m.Unlock()
	// Set unavailableStores
	m.unavailableStores = unavailableStores
	if len(m.unavailableStores) == 0 {
		return
	}
	// Update groupInfo
	for _, groupInfo := range m.groups {
		if !groupInfo.Effect {
			continue
		}
		unavailableStore := uint64(0)
		_, hasUnavailableStore := unavailableStores[groupInfo.LeaderStoreID]
		for _, storeID := range groupInfo.VoterStoreIDs {
			if !hasUnavailableStore {
				_, hasUnavailableStore = unavailableStores[storeID]
				if hasUnavailableStore {
					unavailableStore = storeID
				}
			}
		}
		if hasUnavailableStore {
			m.updateGroupEffectLocked(groupInfo.ID, 0, 0, nil)
			log.Warn("affinity group invalidated due to unavailable stores",
				zap.String("group-id", groupInfo.ID),
				zap.Uint64("unavailable-store", unavailableStore))
		}
	}
}

func (m *Manager) hasUnavailableStore(storeIDs []uint64) bool {
	m.RLock()
	defer m.RUnlock()
	for _, storeID := range storeIDs {
		_, ok := m.unavailableStores[storeID]
		if ok {
			return true
		}
	}
	return false
}
