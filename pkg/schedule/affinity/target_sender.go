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

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	maxPendingTargets   = 1024
	targetBatchInterval = 50 * time.Millisecond
	targetCooldown      = time.Second
)

type observedTarget struct {
	target       GroupTarget
	groupInfoPtr *runtimeGroupInfo
	affinityVer  uint64
	persistOnly  bool
}

type observedTargetState struct {
	observation observedTarget
	pending     bool
	inFlight    bool
	nextSend    time.Time
}

// sendObservedTarget coalesces observations without waiting for persistence. Caller holds metaMutex.
func (m *Manager) sendObservedTarget(obs observedTarget) {
	m.targetWriteMutex.Lock()
	if m.ctx.Err() != nil {
		m.targetWriteMutex.Unlock()
		return
	}
	if time.Now().Before(m.targetEndpointUnsupportedUntil) {
		m.targetWriteMutex.Unlock()
		m.applyLocalObservedTarget(obs)
		return
	}
	defer m.targetWriteMutex.Unlock()
	if m.pendingTargets == nil {
		m.pendingTargets = make(map[string]*observedTargetState)
	}
	id := obs.target.Expected.ID
	state := m.pendingTargets[id]
	if state == nil {
		if len(m.pendingTargets) >= maxPendingTargets {
			return
		}
		state = &observedTargetState{}
		m.pendingTargets[id] = state
	}
	if !state.pending {
		m.pendingTargetIDs = append(m.pendingTargetIDs, id)
		state.pending = true
	}
	state.observation = obs
	if !m.targetSenderRunning {
		m.targetSenderRunning = true
		go m.drainObservedTargets()
	}
}

func (m *Manager) drainObservedTargets() {
	defer logutil.LogPanic()
	delay := targetBatchInterval
	for {
		timer := time.NewTimer(delay)
		select {
		case <-m.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		m.targetWriteMutex.Lock()
		if m.ctx.Err() != nil {
			clear(m.pendingTargets)
			m.pendingTargetIDs = nil
			m.targetSenderRunning = false
			m.targetWriteMutex.Unlock()
			return
		}
		now := time.Now()
		if now.Before(m.targetEndpointUnsupportedUntil) {
			m.targetWriteMutex.Unlock()
			m.fallbackObservedTargets(nil)
			return
		}
		for id, state := range m.pendingTargets {
			if !state.pending && !state.inFlight && !now.Before(state.nextSend) {
				delete(m.pendingTargets, id)
			}
		}
		if len(m.pendingTargets) == 0 {
			m.targetSenderRunning = false
			m.targetWriteMutex.Unlock()
			return
		}
		batch := make([]observedTarget, 0, min(len(m.pendingTargetIDs), etcdutil.MaxEtcdTxnOps))
		remaining := m.pendingTargetIDs[:0]
		for _, id := range m.pendingTargetIDs {
			state := m.pendingTargets[id]
			if len(batch) == etcdutil.MaxEtcdTxnOps || state.inFlight || now.Before(state.nextSend) {
				remaining = append(remaining, id)
				continue
			}
			batch = append(batch, state.observation)
			state.pending, state.inFlight = false, true
		}
		m.pendingTargetIDs = remaining
		m.targetWriteMutex.Unlock()
		delay = targetBatchInterval
		if len(batch) == 0 {
			continue
		}
		targets := make([]GroupTarget, len(batch))
		for i, obs := range batch {
			targets[i] = obs.target
		}
		_, err := m.targetWriter.WriteGroupTargets(m.ctx, targets)
		if errors.Is(err, ErrTargetWriterUnsupported) {
			m.recordTargetEndpointUnsupported()
			m.fallbackObservedTargets(batch)
			return
		}
		m.targetWriteMutex.Lock()
		for _, target := range targets {
			id := target.Expected.ID
			state := m.pendingTargets[id]
			state.inFlight = false
			if err == nil {
				// HTTP 200 includes benign CAS skips, which also cool down.
				state.nextSend = time.Now().Add(targetCooldown)
			} else if !state.pending {
				delete(m.pendingTargets, id)
			}
		}
		if err != nil {
			m.targetWriteRetryAt = time.Now().Add(targetCooldown)
			delay = time.Until(m.targetWriteRetryAt)
		}
		m.targetWriteMutex.Unlock()
		if err != nil && !errors.Is(err, ErrTargetWriterNotReady) && m.ctx.Err() == nil {
			targetWriteFailureCounter.Inc()
			log.Warn("failed to write affinity group targets", zap.Int("group-count", len(batch)), zap.Error(err))
		}
	}
}

// recordTargetEndpointUnsupported defers both remote writers after a 404.
func (m *Manager) recordTargetEndpointUnsupported() {
	m.targetWriteMutex.Lock()
	m.targetEndpointUnsupportedUntil = time.Now().Add(targetCooldown)
	m.targetWriteMutex.Unlock()
}

// fallbackObservedTargets handles the failed batch and any queued observations.
// Local fallback can change the target, so the next probe uses fresh observations.
func (m *Manager) fallbackObservedTargets(batch []observedTarget) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()

	m.targetWriteMutex.Lock()
	for _, id := range m.pendingTargetIDs {
		batch = append(batch, m.pendingTargets[id].observation)
	}
	clear(m.pendingTargets)
	m.pendingTargetIDs = nil
	m.targetSenderRunning = false
	m.targetWriteMutex.Unlock()

	for _, obs := range batch {
		if m.ctx.Err() != nil {
			return
		}
		m.applyLocalObservedTarget(obs)
	}
}

// applyLocalObservedTarget initializes or updates a group's target in memory
// when PD does not yet implement the conditional target endpoint. Once PD
// upgrades, later patrol observations persist the target against the empty
// stored group in Expected. Caller holds metaMutex.
func (m *Manager) applyLocalObservedTarget(obs observedTarget) {
	target := obs.target
	if obs.persistOnly || target.Expected.LeaderStoreID != 0 || len(target.Expected.VoterStoreIDs) != 0 {
		return
	}
	voterStoreIDs := sortedStoreIDs(target.VoterStoreIDs)
	if target.LeaderStoreID == 0 || len(voterStoreIDs) == 0 {
		return
	}

	m.Lock()
	defer m.Unlock()

	groupInfo, ok := m.groups[target.Expected.ID]
	// A recreated group can have the same creation timestamp and affinity version.
	if !ok || groupInfo != obs.groupInfoPtr || groupInfo.AffinityVer != obs.affinityVer {
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
	if groupInfo.LeaderStoreID == target.LeaderStoreID && slices.Equal(groupInfo.VoterStoreIDs, voterStoreIDs) {
		if !groupInfo.IsAvailable() {
			m.setTargetAvailabilityLocked(groupInfo)
			m.resetCountLocked(groupInfo)
		}
		return
	}
	groupInfo.LeaderStoreID = target.LeaderStoreID
	groupInfo.VoterStoreIDs = voterStoreIDs
	groupInfo.targetUnpersisted = true
	groupInfo.availability = groupDegraded
	groupInfo.degradedExpiredAt = newDegradedExpiredAtFromNow()
	m.setTargetAvailabilityLocked(groupInfo)
	m.resetCountLocked(groupInfo)
}
