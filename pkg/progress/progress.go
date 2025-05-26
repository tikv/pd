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
	"math"
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

type patrolRegionsDurationGetter interface {
	GetPatrolRegionsDuration() time.Duration
}

// Manager is used to maintain the progresses we care about.
type Manager struct {
	syncutil.RWMutex
	progresses                  map[uint64]*progressIndicator
	completedProgress           map[uint64]*progressIndicator
	patrolRegionsDurationGetter patrolRegionsDurationGetter

	updateInterval time.Duration
}

// NewManager creates a new Manager.
func NewManager(patrolRegionsDurationGetter patrolRegionsDurationGetter,
	updateInterval time.Duration) *Manager {
	return &Manager{
		progresses:                  make(map[uint64]*progressIndicator),
		completedProgress:           make(map[uint64]*progressIndicator),
		patrolRegionsDurationGetter: patrolRegionsDurationGetter,
		updateInterval:              updateInterval,
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

// SetPatrolRegionsDurationGetter sets the patrol regions duration getter.
func (m *Manager) SetPatrolRegionsDurationGetter(getter patrolRegionsDurationGetter) {
	m.Lock()
	defer m.Unlock()
	m.patrolRegionsDurationGetter = getter
}

func (m *Manager) addProgress(
	storeID uint64,
	action Action,
	current, total float64,
) {
	m.progresses[storeID] = newProgressIndicator(
		action,
		current, total,
		m.updateInterval,
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
	for storeID, p := range m.completedProgress {
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
			delete(m.completedProgress, storeID)
			storesProgressGauge.DeleteLabelValues("", storeIDStr, string(p.Action))
			storesSpeedGauge.DeleteLabelValues("", storeIDStr, string(p.Action))
			storesETAGauge.DeleteLabelValues("", storeIDStr, string(p.Action))
		}
	}
}

func (m *Manager) markProgressAsFinished(storeID uint64) {
	m.Lock()
	defer m.Unlock()
	p, exist := m.progresses[storeID]
	if !exist {
		return
	}

	p.completeAt = time.Now()
	p.push(p.targetRegionSize)
	m.completedProgress[storeID] = p
	delete(m.progresses, storeID)
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

	m.updateStoreProgress(storeID, action, currentRegionSize, targetRegionSize)
}

func (m *Manager) updateStoreProgress(
	storeID uint64,
	action Action,
	currentRegionSize, targetRegionSize float64,
) {
	m.Lock()
	defer m.Unlock()

	p, exist := m.progresses[storeID]
	if !exist || p.Action != action {
		m.addProgress(storeID, action, currentRegionSize, targetRegionSize)
		return
	}

	// The targetRegionSize of the preparing progress may be updated.
	if p.targetRegionSize < targetRegionSize {
		p.targetRegionSize = targetRegionSize
	}
	if action == removingAction {
		// If the number of regions is large, each round of PatrolRegion takes a
		// long time. The regions of the offline store may have not been scanned
		// during some updateInterval. In this case, if the window is not large
		// enough, the offline speed will vary greatly, which is not in line
		// with expectations. Therefore, we adjust the window size based on the
		// time consumed by PatrolRegion to avoid excessive fluctuations in the
		// offline speed, which may cause misleading results.
		p.adjustWindowLength(m.patrolRegionsDurationGetter.GetPatrolRegionsDuration())
	}

	p.push(currentRegionSize)

	storeLabel := strconv.FormatUint(storeID, 10)
	// For now, we don't need to record the address of the store. We can record
	// them when we need it.
	storesProgressGauge.WithLabelValues("", storeLabel, string(action)).Set(p.ProgressPercent)
	storesSpeedGauge.WithLabelValues("", storeLabel, string(action)).Set(p.CurrentSpeed)
	storesETAGauge.WithLabelValues("", storeLabel, string(action)).Set(p.LeftSecond)
}

// GetProgressByStoreID gets progresses by the store id.
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
			totalProgressPercent += p.ProgressPercent
			totalLeftSeconds += p.LeftSecond
			totalCurrentSpeed += p.CurrentSpeed
			count++
		}
	}
	if count == 0 {
		return nil
	}
	if math.IsInf(totalLeftSeconds, 1) {
		totalLeftSeconds = math.MaxFloat64
	} else {
		totalLeftSeconds /= float64(count)
	}

	return &Progress{
		Action:          action,
		ProgressPercent: totalProgressPercent / float64(count),
		LeftSecond:      totalLeftSeconds,
		CurrentSpeed:    totalCurrentSpeed / float64(count),
	}
}
