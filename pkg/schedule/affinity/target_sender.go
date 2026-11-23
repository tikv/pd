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

// sendObservedTarget permits one outstanding first-win write per Manager. Busy
// observations are discarded. A 404 pauses HTTP attempts but lets groups
// without an active target use a fresh observation locally. Caller holds metaMutex.
func (m *Manager) sendObservedTarget(target GroupTarget, groupInfoPtr *runtimeGroupInfo, affinityVer uint64, persistOnly bool) {
	m.targetWriteMutex.Lock()
	now := time.Now()
	if now.Before(m.targetEndpointUnsupportedUntil) {
		m.targetWriteMutex.Unlock()
		if !persistOnly && m.ctx.Err() == nil {
			m.applyLocalObservedTarget(target, groupInfoPtr, affinityVer)
		}
		return
	}
	if m.ctx.Err() != nil || m.targetWriteInFlight || now.Before(m.targetWriteRetryAt) {
		m.targetWriteMutex.Unlock()
		return
	}
	m.targetWriteInFlight = true
	m.targetWriteMutex.Unlock()
	go func() {
		defer logutil.LogPanic()
		written, err := m.targetWriter.WriteGroupTargets(m.ctx, []GroupTarget{target})
		if err == nil && !slices.Contains(written, target.Expected.ID) {
			err = errors.New("affinity group target was not written")
		}
		unsupported := errors.Is(err, ErrTargetWriterUnsupported)
		if unsupported && !persistOnly {
			m.metaMutex.Lock()
			if m.ctx.Err() == nil {
				m.applyLocalObservedTarget(target, groupInfoPtr, affinityVer)
			}
			m.metaMutex.Unlock()
		}
		m.targetWriteMutex.Lock()
		m.targetWriteInFlight = false
		if err != nil {
			m.targetWriteRetryAt = time.Now().Add(time.Second)
		}
		if unsupported {
			m.targetEndpointUnsupportedUntil = m.targetWriteRetryAt
		} else {
			m.targetEndpointUnsupportedUntil = time.Time{}
		}
		m.targetWriteMutex.Unlock()
		if err != nil && !unsupported && !errors.Is(err, ErrTargetWriterNotReady) && m.ctx.Err() == nil {
			targetWriteFailureCounter.Inc()
			log.Warn("failed to write affinity group target", zap.String("group-id", target.Expected.ID), zap.Error(err))
		}
	}()
}

// applyLocalObservedTarget initializes or updates a group's target in memory
// when PD does not yet implement the conditional target endpoint. Once PD
// upgrades, later patrol observations persist the target against the empty
// stored group in Expected. Caller holds metaMutex.
func (m *Manager) applyLocalObservedTarget(target GroupTarget, groupInfoPtr *runtimeGroupInfo, affinityVer uint64) {
	if target.Expected.LeaderStoreID != 0 || len(target.Expected.VoterStoreIDs) != 0 {
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
	if !ok || groupInfo != groupInfoPtr || groupInfo.AffinityVer != affinityVer {
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
	groupInfo.targetUnpersisted = true
	groupInfo.SetAvailability(groupAvailable)
	m.resetCountLocked(groupInfo)
}
