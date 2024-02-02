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
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// speedStatisticalWindowCapacity is the size of the time window used to calculate the speed,
	// but it does not mean that all data in it will be used to calculate the speed,
	// which data is used depends on the patrol region duration
	speedStatisticalWindowCapacity = 2 * time.Hour
	// minSpeedCalculationWindow is the minimum speed calculation window
	minSpeedCalculationWindow = 10 * time.Minute
)

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
	// We use (speedStatisticalWindowCapacity / updateInterval + 1) to get the windowCapacity.
	// Assume that the windowCapacity is 4, the init value is 1. After update 3 times with 2, 3, 4 separately. The window will become [1, 2, 3, 4].
	// Then we update it again with 5, the window will become [2, 3, 4, 5].
	windowCapacity int
	// windowLength is used to determine what data will be computed.
	// Assume that the windowLength is 2, the init value is 1. The value that will be calculated are [1]. After update 3 times with 2, 3, 4 separately. The value that will be calculated are [3,4] and the values in queue are [(1,2),3,4].
	// It helps us avoid calculation results jumping change when patrol-region-duration changes.
	windowLength int
	// front is the first element which should be used.
	// position indicates where the front is currently in the queue
	// Assume that the windowLength is 2, the init value is 1. The front is [1] and position is 1. After update 3 times with 2, 3, 4 separately. The front is [3], and the position is 2.
	front    *list.Element
	position int

	updateInterval time.Duration
	lastSpeed      float64
}

// Reset resets the progress manager.
func (m *Manager) Reset() {
	m.Lock()
	defer m.Unlock()

	m.progesses = make(map[string]*progressIndicator)
}

// Option is used to do some action for progressIndicator.
type Option func(*progressIndicator)

// WindowDurationOption changes the time window size.
func WindowDurationOption(dur time.Duration) func(*progressIndicator) {
	return func(pi *progressIndicator) {
		if dur < minSpeedCalculationWindow {
			dur = minSpeedCalculationWindow
		} else if dur > speedStatisticalWindowCapacity {
			dur = speedStatisticalWindowCapacity
		}
		pi.windowLength = int(dur/pi.updateInterval) + 1
	}
}

// AddProgress adds a progress into manager if it doesn't exist.
func (m *Manager) AddProgress(progress string, current, total float64, updateInterval time.Duration, opts ...Option) (exist bool) {
	m.Lock()
	defer m.Unlock()

	history := list.New()
	history.PushBack(current)
	if _, exist = m.progesses[progress]; !exist {
		pi := &progressIndicator{
			total:          total,
			remaining:      total,
			history:        history,
			windowCapacity: int(speedStatisticalWindowCapacity/updateInterval) + 1,
			windowLength:   int(minSpeedCalculationWindow / updateInterval),
			updateInterval: updateInterval,
		}
		for _, op := range opts {
			op(pi)
		}
		m.progesses[progress] = pi
		pi.front = history.Front()
		pi.position = 1
	}
	return
}

// UpdateProgress updates the progress if it exists.
func (m *Manager) UpdateProgress(progress string, current, remaining float64, isInc bool, opts ...Option) {
	m.Lock()
	defer m.Unlock()

	if p, exist := m.progesses[progress]; exist {
		for _, op := range opts {
			op(p)
		}
		p.remaining = remaining
		if p.total < remaining {
			p.total = remaining
		}

		p.history.PushBack(current)
		p.position++

		// try to move `front` into correct place.
		for p.position > p.windowLength {
			p.front = p.front.Next()
			p.position--
		}
		for p.position < p.windowLength && p.front.Prev() != nil {
			p.front = p.front.Prev()
			p.position++
		}

		for p.history.Len() > p.windowCapacity {
			p.history.Remove(p.history.Front())
		}

		// It means it just init and we haven't update the progress
		if p.history.Len() <= 1 {
			p.lastSpeed = 0
		} else if isInc {
			// the value increases, e.g., [1, 2, 3]
			p.lastSpeed = (p.history.Back().Value.(float64) - p.front.Value.(float64)) /
				(float64(p.position-1) * p.updateInterval.Seconds())
		} else {
			// the value decreases, e.g., [3, 2, 1]
			p.lastSpeed = (p.front.Value.(float64) - p.history.Back().Value.(float64)) /
				(float64(p.position-1) * p.updateInterval.Seconds())
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
