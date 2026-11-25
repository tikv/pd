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

const maxTargetBatchSize = 128

type observedTarget struct {
	target       GroupTarget
	groupInfoPtr *runtimeGroupInfo
	affinityVer  uint64
	persistOnly  bool
}

// sendObservedTarget queues the latest observation per group for a single writer.
// A 404 pauses HTTP attempts but lets groups use observations locally.
// Caller holds metaMutex.
func (m *Manager) sendObservedTarget(target GroupTarget, groupInfoPtr *runtimeGroupInfo, affinityVer uint64, persistOnly bool) {
	m.targetWriteMutex.Lock()
	now := time.Now()
	retrying := now.Before(m.targetWriteRetryAt)
	if retrying && m.targetEndpointUnsupported {
		m.targetWriteMutex.Unlock()
		if !persistOnly {
			m.applyLocalObservedTarget(target, groupInfoPtr, affinityVer)
		}
		return
	}
	id := target.Expected.ID
	if m.ctx.Err() != nil || retrying ||
		(groupInfoPtr != nil && now.Before(groupInfoPtr.targetSendAfter)) {
		m.targetWriteMutex.Unlock()
		return
	}
	if _, queued := m.pendingTargets[id]; !queued {
		m.pendingTargetIDs = append(m.pendingTargetIDs, id)
	}
	if m.pendingTargets == nil {
		m.pendingTargets = make(map[string]observedTarget)
	}
	m.pendingTargets[id] = observedTarget{target, groupInfoPtr, affinityVer, persistOnly}
	if !m.targetWriteInFlight {
		m.targetWriteInFlight = true
		go m.drainObservedTargets()
	}
	m.targetWriteMutex.Unlock()
}

func (m *Manager) drainObservedTargets() {
	defer logutil.LogPanic()
	for {
		m.targetWriteMutex.Lock()
		now := time.Now()
		if m.ctx.Err() != nil || now.Before(m.targetWriteRetryAt) {
			clear(m.pendingTargets)
			m.pendingTargetIDs = nil
			m.targetWriteInFlight = false
			m.targetWriteMutex.Unlock()
			return
		}
		batch := make([]observedTarget, 0, min(maxTargetBatchSize, len(m.pendingTargetIDs)))
		for len(batch) < maxTargetBatchSize && len(m.pendingTargetIDs) > 0 {
			id := m.pendingTargetIDs[0]
			m.pendingTargetIDs[0] = ""
			m.pendingTargetIDs = m.pendingTargetIDs[1:]
			obs := m.pendingTargets[id]
			if obs.groupInfoPtr == nil || !now.Before(obs.groupInfoPtr.targetSendAfter) {
				batch = append(batch, obs)
			}
			delete(m.pendingTargets, id)
		}
		if len(m.pendingTargetIDs) == 0 {
			m.pendingTargetIDs = nil
		}
		if len(batch) == 0 {
			m.targetWriteInFlight = false
			m.targetWriteMutex.Unlock()
			return
		}
		m.targetWriteMutex.Unlock()

		targets := make([]GroupTarget, len(batch))
		for i, obs := range batch {
			targets[i] = obs.target
		}
		written, err := m.targetWriter.WriteGroupTargets(m.ctx, targets)
		unsupported := errors.Is(err, ErrTargetWriterUnsupported)

		m.targetWriteMutex.Lock()
		finish := time.Now()
		m.targetEndpointUnsupported = unsupported
		if err == nil {
			// PD can skip individual targets in a successful response.
			for _, obs := range batch {
				if obs.groupInfoPtr != nil {
					obs.groupInfoPtr.targetSendAfter = finish.Add(time.Second)
				}
			}
			shouldReport := m.ctx.Err() == nil
			m.targetWriteMutex.Unlock()
			if shouldReport && len(written) < len(targets) {
				skipped := len(targets) - len(written)
				var firstSkippedID string
				for _, target := range targets {
					if !slices.Contains(written, target.Expected.ID) {
						firstSkippedID = target.Expected.ID
						break
					}
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
				batch = append(batch, m.pendingTargets[id])
			}
		}
		clear(m.pendingTargets)
		m.pendingTargetIDs = nil
		m.targetWriteInFlight = false
		m.targetWriteMutex.Unlock()

		if unsupported {
			m.metaMutex.Lock()
			for _, obs := range batch {
				if !obs.persistOnly {
					m.applyLocalObservedTarget(obs.target, obs.groupInfoPtr, obs.affinityVer)
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
// endpoint. Caller holds metaMutex.
func (m *Manager) applyLocalObservedTarget(target GroupTarget, groupInfoPtr *runtimeGroupInfo, affinityVer uint64) {
	voterStoreIDs := sortedStoreIDs(target.VoterStoreIDs)

	m.Lock()
	defer m.Unlock()

	groupInfo, ok := m.groups[target.Expected.ID]
	// A recreated group can have the same creation timestamp and affinity version.
	if !ok || groupInfo != groupInfoPtr || groupInfo.AffinityVer != affinityVer {
		return
	}
	// A same-peers write can advance the revision without changing the affinity version.
	if groupInfo.modRevision != target.ExpectedModRevision {
		return
	}
	stored := groupInfo.storedGroup()
	if !sameStoredGroup(&stored, &target.Expected) {
		return
	}
	if groupInfo.IsAffinitySchedulingAllowed() {
		return
	}
	if !groupInfo.IsRegularSchedulingAllowed() && !slices.Equal(voterStoreIDs, groupInfo.VoterStoreIDs) {
		return
	}
	// Store conditions can change during the request without affecting an unset group.
	if calcGroupAvailability(m.unavailableStores, target.LeaderStoreID, voterStoreIDs) != groupAvailable {
		return
	}
	groupInfo.LeaderStoreID = target.LeaderStoreID
	groupInfo.VoterStoreIDs = voterStoreIDs
	groupInfo.persistedGroup = nil
	if !sameStoredGroup(&stored, &groupInfo.Group) {
		groupInfo.persistedGroup = &stored
	}
	groupInfo.SetAvailability(groupAvailable)
	m.resetCountLocked(groupInfo)
}
