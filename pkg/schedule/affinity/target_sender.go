// Copyright 2026 TiKV Project Authors.
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
	"errors"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	maxTargetBatchSize        = 128
	maxPendingTargetsPerGroup = 16
)

type observedTarget struct {
	target       GroupTarget
	groupInfoPtr *runtimeGroupInfo
	affinityVer  uint64
	persistOnly  bool
}

// matches checks the observation against the current group under Manager.RWMutex.
func (obs *observedTarget) matches(group *runtimeGroupInfo) bool {
	// A recreated group can reuse the same creation timestamp and affinity version.
	// A same-peers write can advance the revision without changing the affinity version.
	return group == obs.groupInfoPtr && group.AffinityVer == obs.affinityVer &&
		group.modRevision == obs.target.ExpectedModRevision && !group.rangeConflict
}

func (obs *observedTarget) sameCandidate(other observedTarget) bool {
	return obs.groupInfoPtr == other.groupInfoPtr && obs.affinityVer == other.affinityVer &&
		obs.target.ExpectedModRevision == other.target.ExpectedModRevision &&
		obs.target.LeaderStoreID == other.target.LeaderStoreID &&
		slices.Equal(obs.target.VoterStoreIDs, other.target.VoterStoreIDs)
}

// sendObservedTarget queues distinct current candidates in observation order.
// A full queue keeps its existing candidates.
// A 404 pauses HTTP attempts but lets groups use observations locally.
// Caller holds metaMutex.
func (m *Manager) sendObservedTarget(obs observedTarget) {
	id := obs.target.Expected.ID
	m.targetWriteMutex.Lock()
	queued := m.pendingTargets[id]
	m.RLock()
	group := m.groups[id]
	current := obs.matches(group)
	// A group's queued candidates all share the same instance, version, and revision.
	staleQueue := len(queued) > 0 && !queued[0].matches(group)
	m.RUnlock()
	now := time.Now()
	retrying := now.Before(m.targetWriteRetryAt)
	if retrying && m.targetEndpointUnsupported {
		m.targetWriteMutex.Unlock()
		if current && !obs.persistOnly {
			m.applyLocalObservedTarget(&obs)
		}
		return
	}
	if !current || m.ctx.Err() != nil || retrying {
		m.targetWriteMutex.Unlock()
		return
	}
	defer m.targetWriteMutex.Unlock()

	if staleQueue {
		// Keep the ID's position while replacing the obsolete queue.
		m.pendingTargets[id] = nil
		queued = nil
	}
	if sending, ok := m.sendingTargets[id]; ok && obs.sameCandidate(sending) {
		return
	}
	for _, pending := range queued {
		if obs.sameCandidate(pending) {
			return
		}
	}
	if lastSent := group.lastSentTarget; now.Before(group.targetSendAfter) && lastSent != nil && obs.sameCandidate(*lastSent) {
		return
	}
	if len(queued) == maxPendingTargetsPerGroup {
		return
	}
	if _, indexed := m.pendingTargets[id]; !indexed {
		m.pendingTargetIDs = append(m.pendingTargetIDs, id)
	}
	if m.pendingTargets == nil {
		m.pendingTargets = make(map[string][]observedTarget)
	}
	m.pendingTargets[id] = append(queued, obs)
	if !m.targetDrainRunning {
		m.targetDrainRunning = true
		go m.drainObservedTargets()
	} else if m.targetDrainWake != nil {
		close(m.targetDrainWake)
		m.targetDrainWake = nil
	}
}

// popObservedTargets takes at most one current candidate per group in FIFO order
// and returns the earliest cooldown deadline it encounters.
// Caller holds targetWriteMutex.
func (m *Manager) popObservedTargets(now time.Time) ([]observedTarget, time.Time) {
	batch := make([]observedTarget, 0, min(maxTargetBatchSize, len(m.pendingTargetIDs)))
	var retryAt time.Time
	m.RLock()
	defer m.RUnlock()
	for remaining := len(m.pendingTargetIDs); remaining > 0 && len(batch) < maxTargetBatchSize; remaining-- {
		id := m.pendingTargetIDs[0]
		m.pendingTargetIDs[0] = ""
		m.pendingTargetIDs = m.pendingTargetIDs[1:]
		queued := m.pendingTargets[id]
		if len(queued) == 0 || !queued[0].matches(m.groups[id]) {
			delete(m.pendingTargets, id)
			continue
		}
		obs := queued[0]
		if sendAfter := obs.groupInfoPtr.targetSendAfter; now.Before(sendAfter) {
			m.pendingTargetIDs = append(m.pendingTargetIDs, id)
			if retryAt.IsZero() || sendAfter.Before(retryAt) {
				retryAt = sendAfter
			}
			continue
		}
		batch = append(batch, obs)
		if len(queued) == 1 {
			delete(m.pendingTargets, id)
		} else {
			m.pendingTargets[id] = slices.Delete(queued, 0, 1)
			m.pendingTargetIDs = append(m.pendingTargetIDs, id)
		}
	}
	if len(m.pendingTargetIDs) == 0 {
		m.pendingTargetIDs = nil
	}
	return batch, retryAt
}

func (m *Manager) drainObservedTargets() {
	defer logutil.LogPanic()
	for {
		m.targetWriteMutex.Lock()
		m.targetDrainWake = nil
		now := time.Now()
		if m.ctx.Err() != nil || now.Before(m.targetWriteRetryAt) {
			clear(m.pendingTargets)
			m.pendingTargetIDs = nil
			m.targetDrainRunning = false
			m.targetWriteMutex.Unlock()
			return
		}
		batch, retryAt := m.popObservedTargets(now)
		if len(batch) == 0 {
			if retryAt.IsZero() {
				m.targetDrainRunning = false
				m.targetWriteMutex.Unlock()
				return
			}
			wake := make(chan struct{})
			m.targetDrainWake = wake
			m.targetWriteMutex.Unlock()
			timer := time.NewTimer(time.Until(retryAt))
			select {
			case <-m.ctx.Done():
			case <-wake:
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		if m.sendingTargets == nil {
			m.sendingTargets = make(map[string]observedTarget, maxTargetBatchSize)
		}
		targets := make([]GroupTarget, len(batch))
		for i, obs := range batch {
			targets[i] = obs.target
			m.sendingTargets[obs.target.Expected.ID] = obs
			obs.groupInfoPtr.lastSentTarget = &obs
		}
		m.targetWriteMutex.Unlock()

		written, err := m.targetWriter.WriteGroupTargets(m.ctx, targets)
		unsupported := errors.Is(err, ErrTargetWriterUnsupported)

		m.targetWriteMutex.Lock()
		clear(m.sendingTargets)
		finish := time.Now()
		m.targetEndpointUnsupported = unsupported
		if err == nil {
			// PD can skip individual targets in a successful response. Keep any
			// remaining candidates queued for after the cooldown.
			for _, obs := range batch {
				obs.groupInfoPtr.targetSendAfter = finish.Add(time.Second)
			}
			shouldReport := m.ctx.Err() == nil
			m.targetWriteMutex.Unlock()
			if len(written) < len(targets) {
				skipped := len(targets) - len(written)
				var firstSkippedID string
				for _, obs := range batch {
					id := obs.target.Expected.ID
					if slices.Contains(written, id) {
						continue
					}
					if firstSkippedID == "" {
						firstSkippedID = id
					}
					if obs.persistOnly {
						m.Lock()
						group := m.groups[id]
						if group == obs.groupInfoPtr && group.AffinityVer == obs.affinityVer &&
							group.modRevision == obs.target.ExpectedModRevision && group.persistedGroup != nil {
							m.applyGroupTargetLocked(group, group.persistedGroup)
						}
						m.Unlock()
					}
				}
				if !shouldReport {
					continue
				}
				targetWriteFailureCounter.Add(float64(skipped))
				log.Warn("skipped affinity group target writes",
					zap.Int("skipped-count", skipped),
					zap.Int("group-count", len(targets)),
					zap.String("sample-group-id", firstSkippedID))
			}
			continue
		}

		m.targetWriteRetryAt = finish.Add(time.Second)
		if unsupported {
			for _, id := range m.pendingTargetIDs {
				batch = append(batch, m.pendingTargets[id]...)
			}
		}
		clear(m.pendingTargets)
		m.pendingTargetIDs = nil
		m.targetDrainRunning = false
		m.targetWriteMutex.Unlock()

		if unsupported {
			m.metaMutex.Lock()
			for _, obs := range batch {
				if !obs.persistOnly {
					m.applyLocalObservedTarget(&obs)
				}
			}
			m.metaMutex.Unlock()
		} else if !errors.Is(err, ErrTargetWriterNotReady) && m.ctx.Err() == nil {
			targetWriteFailureCounter.Add(float64(len(targets)))
			log.Warn("failed to write affinity group targets", zap.Int("group-count", len(batch)), zap.Error(err))
		}
		return
	}
}

// applyLocalObservedTarget applies a healthy observation without a write when it
// matches the current or stored target, or PD does not implement the target
// endpoint. When it changes local state, it updates the observation so the
// caller can queue it for persistence and roll it back if PD skips it.
// Caller holds metaMutex.
func (m *Manager) applyLocalObservedTarget(obs *observedTarget) {
	target := &obs.target
	m.Lock()
	defer m.Unlock()

	groupInfo := m.groups[target.Expected.ID]
	if !obs.matches(groupInfo) {
		return
	}
	stored := groupInfo.storedGroup()
	if !sameStoredGroup(&stored, &target.Expected) {
		return
	}
	if groupInfo.IsAffinitySchedulingAllowed() {
		return
	}
	if !groupInfo.IsRegularSchedulingAllowed() && !slices.Equal(target.VoterStoreIDs, groupInfo.VoterStoreIDs) {
		return
	}
	// Store conditions can change during the request without affecting an unset group.
	if calcGroupAvailability(m.unavailableStores, target.LeaderStoreID, target.VoterStoreIDs) != groupAvailable {
		return
	}
	groupInfo.LeaderStoreID = target.LeaderStoreID
	groupInfo.VoterStoreIDs = slices.Clone(target.VoterStoreIDs)
	groupInfo.persistedGroup = nil
	if !sameStoredGroup(&stored, &groupInfo.Group) {
		groupInfo.persistedGroup = &stored
	}
	groupInfo.SetAvailability(groupAvailable)
	m.resetCountLocked(groupInfo)
	obs.affinityVer = groupInfo.AffinityVer
	obs.persistOnly = groupInfo.persistedGroup != nil
}
