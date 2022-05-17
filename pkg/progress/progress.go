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
	"container/list"
	"fmt"
	"math"
	"time"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
)

// speedStatisticalWindow is the speed calculation window
const speedStatisticalWindow = 10 * time.Minute

// Manager is used to maintain the progresses we care about.
type Manager struct {
	syncutil.RWMutex
	progesses map[string]*progressIndicator
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		progesses: make(map[string]*progressIndicator),
	}
}

// progressIndicator reflects a specified progress.
type progressIndicator struct {
	total     float64
	remaining float64
	// We use a fixed interval's history to calculate the latest average speed.
	history *list.List
	// We use speedStatisticalWindow / updateInterval to get the windowLengthLimit.
	// Assume that the windowLengthLimit is 3, the init value is 1. after update 3 times with 2, 3, 4 separately. The window will become [1, 2, 3, 4].
	// Then we update it again with 5, the window will become [2, 3, 4, 5].
	windowLengthLimit int
	updateInterval    time.Duration
	lastSpeed         float64
}

// Reset resets the progress manager.
func (m *Manager) Reset() {
	m.Lock()
	defer m.Unlock()

	m.progesses = make(map[string]*progressIndicator)
}

// AddProgress adds a progress into manager if it doesn't exist.
func (m *Manager) AddProgress(progress string, current, total float64, updateInterval time.Duration) (exist bool) {
	m.Lock()
	defer m.Unlock()

	history := list.New()
	history.PushBack(current)
	if _, exist = m.progesses[progress]; !exist {
		m.progesses[progress] = &progressIndicator{
			total:             total,
			remaining:         total,
			history:           history,
			windowLengthLimit: int(speedStatisticalWindow / updateInterval),
			updateInterval:    updateInterval,
		}
	}
	return
}

// UpdateProgress updates the progress if it exists.
func (m *Manager) UpdateProgress(progress string, current, remaining float64, isInc bool) {
	m.Lock()
	defer m.Unlock()

	if p, exist := m.progesses[progress]; exist {
		p.remaining = remaining
		if p.total < remaining {
			p.total = remaining
		}

		if p.history.Len() > p.windowLengthLimit {
			p.history.Remove(p.history.Front())
		}
		p.history.PushBack(current)

		// It means it just init and we haven't update the progress
		if p.history.Len() <= 1 {
			p.lastSpeed = 0
		} else if isInc {
			// the value increases, e.g., [1, 2, 3]
			p.lastSpeed = (p.history.Back().Value.(float64) - p.history.Front().Value.(float64)) /
				(float64(p.history.Len()-1) * p.updateInterval.Seconds())
		} else {
			// the value decreases, e.g., [3, 2, 1]
			p.lastSpeed = (p.history.Front().Value.(float64) - p.history.Back().Value.(float64)) /
				(float64(p.history.Len()-1) * p.updateInterval.Seconds())
		}
		if p.lastSpeed < 0 {
			p.lastSpeed = 0
		}
	}
}

// UpdateProgressTotal updates the total value of a progress if it exists.
func (m *Manager) UpdateProgressTotal(progress string, total float64) {
	m.Lock()
	defer m.Unlock()

	if p, exist := m.progesses[progress]; exist {
		p.total = total
	}
}

// RemoveProgress removes a progress from manager.
func (m *Manager) RemoveProgress(progress string) (exist bool) {
	m.Lock()
	defer m.Unlock()

	if _, exist = m.progesses[progress]; exist {
		delete(m.progesses, progress)
		return
	}
	return
}

// GetProgresses gets progresses according to the filter.
func (m *Manager) GetProgresses(filter func(p string) bool) []string {
	m.RLock()
	defer m.RUnlock()

	processes := []string{}
	for p := range m.progesses {
		if filter(p) {
			processes = append(processes, p)
		}
	}
	return processes
}

// Status returns the current progress status of a give name.
func (m *Manager) Status(progress string) (process, leftSeconds, currentSpeed float64, err error) {
	m.RLock()
	defer m.RUnlock()

	if p, exist := m.progesses[progress]; exist {
		process = 1 - p.remaining/p.total
		if process < 0 {
			process = 0
			err = errs.ErrProgressWrongStatus.FastGenByArgs(fmt.Sprintf("the remaining: %v is larger than the total: %v", p.remaining, p.total))
			return
		}
		currentSpeed = p.lastSpeed
		// When the progress is newly added, there is no last speed.
		if p.lastSpeed == 0 && p.history.Len() <= 1 {
			currentSpeed = 0
		}

		leftSeconds = p.remaining / currentSpeed
		if math.IsNaN(leftSeconds) || math.IsInf(leftSeconds, 0) {
			leftSeconds = math.MaxFloat64
		}
		return
	}
	err = errs.ErrProgressNotFound.FastGenByArgs(fmt.Sprintf("the progress: %s", progress))
	return
}
