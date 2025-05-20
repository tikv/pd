// Copyright 2022 TiKV Project Authors.
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

package progress

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	gcInterval  = 2 * time.Minute
	expiredTime = 30 * time.Second

	removingAction  Action = "removing"
	preparingAction Action = "preparing"
)

// Manager is used to maintain the progresses we care about.
type Manager struct {
	syncutil.RWMutex
	progresses map[uint64]*progressIndicator
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		progresses: make(map[uint64]*progressIndicator),
	}
}

// Action is the action of the progress.
type Action string

// Progress is the progress of the online/offline store.
type Progress struct {
	Action
	ProgressPercent float64
	LeftSecond      float64
	CurrentSpeed    float64
}

// Reset resets the progress manager.
func (m *Manager) Reset() {
	storesProgressGauge.Reset()
	storesSpeedGauge.Reset()
	storesETAGauge.Reset()

	m.Lock()
	defer m.Unlock()

	m.progresses = make(map[uint64]*progressIndicator)
}

func (m *Manager) addProgress(
	storeID uint64,
	action Action,
	current, total float64,
	updateInterval time.Duration,
) {
	m.progresses[storeID] = newProgressIndicator(
		action,
		current, total,
		updateInterval,
	)
}

// GC starts a goroutine to clean up the completed progress.
func (m *Manager) GC(ctx context.Context) {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.gcCompletedProgress()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) gcCompletedProgress() {
	m.Lock()
	defer m.Unlock()
	for storeID, p := range m.progresses {
		exactExpiredTime := expiredTime
		failpoint.Inject("gcExpiredTime", func(val failpoint.Value) {
			if s, ok := val.(string); ok {
				var err error
				exactExpiredTime, err = time.ParseDuration(s)
				if err != nil {
					panic(err)
				}
			}
		})
		if p.completeAt.Before(time.Now().Add(-exactExpiredTime)) {
			storeIDStr := strconv.FormatUint(storeID, 10)
			delete(m.progresses, storeID)
			storesProgressGauge.DeleteLabelValues("", storeIDStr, string(p.Action))
			storesSpeedGauge.DeleteLabelValues("", storeIDStr, string(p.Action))
			storesETAGauge.DeleteLabelValues("", storeIDStr, string(p.Action))
		}
	}
}

func (m *Manager) markProgressAsFinished(storeID uint64) {
	m.Lock()
	defer m.Unlock()
	if _, exist := m.progresses[storeID]; !exist {
		return
	}

	m.progresses[storeID].completeAt = time.Now()
	m.progresses[storeID].push(m.progresses[storeID].targetRegionSize)
}

// UpdateProgress updates the progress of the store.
func (m *Manager) UpdateProgress(
	store *core.StoreInfo,
	currentRegionSize, threadhold float64,
) {
	p := m.GetProgressByStoreID(store.GetID())
	if p != nil && ((p.Action == preparingAction && !store.IsPreparing()) ||
		(p.Action == removingAction && !store.IsRemoving())) {
		m.markProgressAsFinished(store.GetID())
		return
	}
	var (
		storeID = store.GetID()
		action  Action
		//  targetRegionSize is the total region size that need to be added/deleted.
		//  - If the store is in preparing state, it represents the total region size
		//    that need to be added, and it may be updated during the process.
		//  - If the store is in removing state, it represents the total region size
		//    that need to be deleted, and it is set in the first time.
		targetRegionSize float64
	)
	switch store.GetNodeState() {
	case metapb.NodeState_Preparing:
		action = preparingAction
		targetRegionSize = threadhold
	case metapb.NodeState_Removing:
		action = removingAction
		m.Lock()
		p, exist := m.progresses[storeID]
		if exist && p.Action == removingAction {
			// currentRegionSize represents the current deleted region size.
			currentRegionSize = p.targetRegionSize - currentRegionSize
		} else {
			// targetRegionSize represents the total region size that need to be
			// deleted. It is set in the first time when the store is in removing state.
			targetRegionSize = currentRegionSize
			currentRegionSize = 0
		}
		m.Unlock()
	default:
		return
	}

	m.updateProgress(storeID, action, currentRegionSize, targetRegionSize)
}

// nolint:confusing-naming
func (m *Manager) updateProgress(storeID uint64, action Action, currentRegionSize, targetRegionSize float64) {
	m.Lock()
	defer m.Unlock()

	p, exist := m.progresses[storeID]
	if !exist || p.Action != action {
		m.addProgress(storeID, action, currentRegionSize, targetRegionSize, updateInterval)
		return
	}

	// The targetRegionSize of the preparing progress may be updated.
	if p.targetRegionSize < targetRegionSize {
		p.targetRegionSize = targetRegionSize
	}

	p.push(currentRegionSize)

	storeLabel := strconv.FormatUint(storeID, 10)
	// For now, we don't need to record the address of the store. We can record
	// them when we need it.
	storesProgressGauge.WithLabelValues("", storeLabel, string(action)).Set(p.ProgressPercent)
	storesSpeedGauge.WithLabelValues("", storeLabel, string(action)).Set(p.CurrentSpeed)
	storesETAGauge.WithLabelValues("", storeLabel, string(action)).Set(p.LeftSecond)
}

// GetProgresses gets progresses according to the filter.
func (m *Manager) GetProgressByStoreID(storeID uint64) *Progress {
	m.RLock()
	defer m.RUnlock()

	p, exist := m.progresses[storeID]
	if !exist {
		return nil
	}
	return p.Progress
}

// GetAverageProgressByAction gets the average progress of all stores
func (m *Manager) GetAverageProgressByAction(action Action) *Progress {
	m.RLock()
	defer m.RUnlock()

	var (
		totalProgressPercent, totalLeftSeconds, totalCurrentSpeed float64
		count                                                     int
	)
	for _, p := range m.progresses {
		if p.Action == action {
			totalProgressPercent += p.Progress.ProgressPercent
			totalLeftSeconds += p.LeftSecond
			totalCurrentSpeed += p.CurrentSpeed
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return &Progress{
		Action:          action,
		ProgressPercent: totalProgressPercent / float64(count),
		LeftSecond:      totalLeftSeconds / float64(count),
		CurrentSpeed:    totalCurrentSpeed / float64(count),
	}
}
