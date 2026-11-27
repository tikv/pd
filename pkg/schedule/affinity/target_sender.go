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
	localOnly    bool // recover the stored target without sending a write
}

type inFlightTarget struct {
	observedTarget
	deleteRevision int64
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

// recoverLocalObservedTarget gives earlier candidates a chance to win before
// restoring the stored target. Caller holds metaMutex.
func (m *Manager) recoverLocalObservedTarget(obs *observedTarget, matchesStored bool) {
	m.targetWriteMutex.Lock()
	defer m.targetWriteMutex.Unlock()

	id := obs.target.Expected.ID
	queued := m.pendingTargets[id]
	sending, inFlight := m.sendingTargets[id]
	m.RLock()
	group := m.groups[id]
	if !obs.matches(group) || group.lastSentWritten {
		m.RUnlock()
		return
	}
	if len(queued) > 0 && !queued[0].matches(group) {
		queued = nil
	}
	active := len(queued) > 0 || (inFlight && sending.matches(group))
	m.RUnlock()
	if active {
		if matchesStored && !slices.ContainsFunc(queued, obs.sameCandidate) {
			local := *obs
			local.localOnly = true
			// Reserve room for this observation even if the HTTP queue is full.
			m.enqueueObservedTarget(queued, local)
		}
		return
	}
	m.applyLocalObservedTarget(obs)
}

// sendObservedTarget queues distinct current candidates in observation order.
// A full queue keeps its existing candidates.
// A 404 pauses HTTP attempts but lets groups use observations locally.
// Caller holds metaMutex.
func (m *Manager) sendObservedTarget(obs observedTarget) {
	id := obs.target.Expected.ID
	m.targetWriteMutex.Lock()
	defer m.targetWriteMutex.Unlock()
	queued := m.pendingTargets[id]
	m.RLock()
	group := m.groups[id]
	current := obs.matches(group) && !group.lastSentWritten
	// A group's queued candidates all share the same instance, version, and revision.
	staleQueue := len(queued) > 0 && !queued[0].matches(group)
	m.RUnlock()
	now := time.Now()
	retrying := now.Before(m.targetWriteRetryAt)
	coolingDown := now.Before(m.targetEndpointUnsupportedUntil)
	if !current || m.ctx.Err() != nil || (retrying && !coolingDown) || group.lastSentPolicyMismatch {
		return
	}

	if staleQueue {
		// Keep the ID's position while replacing the obsolete queue.
		m.pendingTargets[id] = nil
		queued = nil
	}
	if sending, ok := m.sendingTargets[id]; ok && obs.sameCandidate(sending.observedTarget) {
		return
	}
	if lastSent := group.lastSentTarget; now.Before(group.targetSendAfter) && lastSent != nil && obs.sameCandidate(*lastSent) {
		return
	}
	if slices.ContainsFunc(queued, obs.sameCandidate) || len(queued) >= maxPendingTargetsPerGroup {
		return
	}
	m.enqueueObservedTarget(queued, obs)
}

// enqueueObservedTarget appends to the current queue and wakes the sender.
// Caller holds targetWriteMutex.
func (m *Manager) enqueueObservedTarget(queued []observedTarget, obs observedTarget) {
	id := obs.target.Expected.ID
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

// popObservedTargets considers candidates in observation order, recovering
// stored targets locally as soon as they reach the front of their queue. It
// takes at most one HTTP candidate per group and returns the next cooldown deadline.
// Caller holds metaMutex and targetWriteMutex.
func (m *Manager) popObservedTargets(now time.Time, coolingDown bool) ([]observedTarget, time.Time) {
	batch := make([]observedTarget, 0, min(maxTargetBatchSize, len(m.pendingTargetIDs)))
	var retryAt time.Time
	m.Lock()
	defer m.Unlock()
	for remaining := len(m.pendingTargetIDs); remaining > 0 && len(batch) < maxTargetBatchSize; remaining-- {
		id := m.pendingTargetIDs[0]
		m.pendingTargetIDs[0] = ""
		m.pendingTargetIDs = m.pendingTargetIDs[1:]
		queued := m.pendingTargets[id]
		group := m.groups[id]
		for len(queued) > 0 {
			obs := queued[0]
			switch {
			case !obs.matches(group) || group.lastSentWritten:
				queued = nil
			case obs.localOnly || coolingDown:
				if !obs.persistOnly {
					m.applyLocalObservedTargetLocked(&obs)
				}
				queued = slices.Delete(queued, 0, 1)
				continue
			case !obs.persistOnly && m.checkHasUnavailableStoreLocked(obs.target.LeaderStoreID, obs.target.VoterStoreIDs) != nil:
				queued = slices.Delete(queued, 0, 1)
				continue
			case now.Before(group.targetSendAfter):
				if retryAt.IsZero() || group.targetSendAfter.Before(retryAt) {
					retryAt = group.targetSendAfter
				}
			default:
				batch = append(batch, obs)
				queued = slices.Delete(queued, 0, 1)
			}
			break
		}
		if len(queued) == 0 {
			delete(m.pendingTargets, id)
		} else {
			m.pendingTargets[id] = queued
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
		m.metaMutex.Lock()
		m.targetWriteMutex.Lock()
		m.targetDrainWake = nil
		now := time.Now()
		coolingDown := now.Before(m.targetEndpointUnsupportedUntil)
		if m.ctx.Err() != nil || (now.Before(m.targetWriteRetryAt) && !coolingDown) {
			clear(m.pendingTargets)
			m.pendingTargetIDs = nil
			m.targetDrainRunning = false
			m.targetWriteMutex.Unlock()
			m.metaMutex.Unlock()
			return
		}
		batch, retryAt := m.popObservedTargets(now, coolingDown)
		if len(batch) == 0 {
			if retryAt.IsZero() {
				m.targetDrainRunning = false
				m.targetWriteMutex.Unlock()
				m.metaMutex.Unlock()
				return
			}
			wake := make(chan struct{})
			m.targetDrainWake = wake
			m.targetWriteMutex.Unlock()
			m.metaMutex.Unlock()
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
			m.sendingTargets = make(map[string]inFlightTarget, maxTargetBatchSize)
		}
		targets := make([]GroupTarget, len(batch))
		for i, obs := range batch {
			targets[i] = obs.target
			m.sendingTargets[obs.target.Expected.ID] = inFlightTarget{observedTarget: obs}
		}
		m.targetWriteMutex.Unlock()
		m.metaMutex.Unlock()

		response, err := m.targetWriter.WriteGroupTargets(m.ctx, targets)
		unsupported := errors.Is(err, ErrTargetWriterUnsupported)
		notReady := errors.Is(err, ErrTargetWriterNotReady)

		m.metaMutex.Lock()
		m.targetWriteMutex.Lock()
		finish := time.Now()
		if err == nil || len(response.WrittenGroupIDs) != 0 || len(response.PolicyMismatchGroupIDs) != 0 {
			// PD can skip individual targets in a successful response. Keep any
			// remaining candidates queued for after the cooldown.
			m.Lock()
			written := make(map[string]struct{}, len(response.WrittenGroupIDs))
			for _, id := range response.WrittenGroupIDs {
				written[id] = struct{}{}
			}
			for _, obs := range batch {
				id := obs.target.Expected.ID
				sending, ok := m.sendingTargets[id]
				if !ok {
					continue // a local update superseded the in-flight request
				}
				_, confirmed := written[id]
				revision := response.WrittenRevisions[id]
				group := m.groups[id]
				if confirmed && revision > 0 {
					if !m.protectConfirmedTarget(&obs.target, revision, sending.deleteRevision) {
						continue
					}
				} else if group != obs.groupInfoPtr || group.modRevision != obs.target.ExpectedModRevision || group.confirmedRevision > 0 {
					continue
				}
				group.lastSentTarget = &obs
				group.lastSentWritten = confirmed
				group.lastSentPolicyMismatch = slices.Contains(response.PolicyMismatchGroupIDs, id)
				if group.lastSentPolicyMismatch {
					if queued := m.pendingTargets[id]; len(queued) > 0 && queued[0].matches(group) {
						m.pendingTargets[id] = nil
					}
				} else {
					group.targetSendAfter = finish.Add(time.Second)
				}
			}
			m.Unlock()
			if err == nil {
				clear(m.sendingTargets)
				var firstSkippedID string
				skipped := 0
				if len(written) < len(targets) {
					for _, obs := range batch {
						id := obs.target.Expected.ID
						if _, ok := written[id]; ok || slices.Contains(response.PolicyMismatchGroupIDs, id) {
							continue
						}
						skipped++
						if firstSkippedID == "" {
							firstSkippedID = id
						}
						if obs.persistOnly {
							m.Lock()
							group := m.groups[id]
							if group == obs.groupInfoPtr && group.AffinityVer == obs.affinityVer &&
								group.modRevision == obs.target.ExpectedModRevision && group.persistedGroup != nil &&
								!group.lastSentWritten && group.confirmedRevision == 0 {
								m.applyGroupTargetLocked(group, group.persistedGroup)
							}
							m.Unlock()
						}
					}
				}
				m.targetWriteMutex.Unlock()
				m.metaMutex.Unlock()
				if m.ctx.Err() == nil && firstSkippedID != "" {
					targetWriteFailureCounter.Add(float64(skipped))
					log.Warn("skipped affinity group target writes",
						zap.Int("skipped-count", skipped),
						zap.Int("group-count", len(targets)),
						zap.String("sample-group-id", firstSkippedID))
				}
				continue
			}
		}

		clear(m.sendingTargets)
		m.targetWriteRetryAt = finish.Add(time.Second)
		if unsupported {
			m.recordTargetEndpointUnsupportedLocked(finish)
			for _, obs := range batch {
				if !obs.persistOnly {
					m.applyLocalObservedTarget(&obs)
				}
			}
			m.targetWriteMutex.Unlock()
			m.metaMutex.Unlock()
			continue
		}
		var fallback []observedTarget
		if notReady {
			for _, id := range m.pendingTargetIDs {
				for _, obs := range m.pendingTargets[id] {
					if obs.localOnly {
						fallback = append(fallback, obs)
					}
				}
			}
		}
		clear(m.pendingTargets)
		m.pendingTargetIDs = nil
		m.targetDrainRunning = false

		for _, obs := range fallback {
			if !obs.persistOnly {
				m.applyLocalObservedTarget(&obs)
			}
		}
		m.targetWriteMutex.Unlock()
		m.metaMutex.Unlock()
		if !unsupported && !notReady && m.ctx.Err() == nil {
			if unconfirmed := len(targets) - len(response.WrittenGroupIDs) - len(response.PolicyMismatchGroupIDs); unconfirmed > 0 {
				targetWriteFailureCounter.Add(float64(unconfirmed))
			}
			log.Warn("failed to write affinity group targets", zap.Int("group-count", len(targets)), zap.Error(err))
		}
		return
	}
}

// recordTargetEndpointUnsupportedLocked defers both writers after a 404 and
// wakes the sender to consider queued candidates locally.
func (m *Manager) recordTargetEndpointUnsupportedLocked(now time.Time) {
	m.targetEndpointUnsupportedUntil = now.Add(time.Second)
	if m.targetDrainWake != nil {
		close(m.targetDrainWake)
		m.targetDrainWake = nil
	}
}

// applyLocalObservedTarget applies a healthy observation without a write when it
// matches the current or stored target, or PD does not implement the target
// endpoint. When it changes local state, it updates the observation so the
// caller can queue it for persistence and roll it back if PD skips it.
// Caller holds metaMutex and targetWriteMutex.
func (m *Manager) applyLocalObservedTarget(obs *observedTarget) {
	m.Lock()
	defer m.Unlock()
	m.applyLocalObservedTargetLocked(obs)
}

// applyLocalObservedTargetLocked also requires the Manager write lock.
func (m *Manager) applyLocalObservedTargetLocked(obs *observedTarget) {
	target := &obs.target
	groupInfo := m.groups[target.Expected.ID]
	if !obs.matches(groupInfo) || groupInfo.lastSentWritten {
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
	if m.checkHasUnavailableStoreLocked(target.LeaderStoreID, target.VoterStoreIDs) != nil {
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

// protectConfirmedTarget fences stale watch events and pauses scheduling while
// the group's peers differ from the confirmed target. The caller holds all
// three Manager locks.
func (m *Manager) protectConfirmedTarget(target *GroupTarget, revision, deletedAt int64) bool {
	id := target.Expected.ID
	group := m.groups[id]
	if group == nil {
		if revision > max(deletedAt, m.deletedTargetConfirmations[id]) {
			if m.deletedTargetConfirmations == nil {
				m.deletedTargetConfirmations = make(map[string]int64)
			}
			m.deletedTargetConfirmations[id] = revision
		}
		return false
	}
	if revision <= max(group.modRevision, group.confirmedRevision) {
		return false
	}
	group.confirmedRevision = revision
	group.lastSentWritten = true
	if !sameStoredGroup(&group.Group, target.result()) {
		group.awaitingTarget = true
		m.resetCountLocked(group)
	}
	return true
}
