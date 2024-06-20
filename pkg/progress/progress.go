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
	// maxSpeedCalculationWindow is the maximum size of the time window used to calculate the speed,
	// but it does not mean that all data in it will be used to calculate the speed,
	// which data is used depends on the patrol region duration
	maxSpeedCalculationWindow = 2 * time.Hour
	// minSpeedCalculationWindow is the minimum speed calculation window
	minSpeedCalculationWindow = 10 * time.Minute
)

// Manager is used to maintain the progresses we care about.
type Manager struct {
	syncutil.RWMutex
	progresses map[string]*progressIndicator
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		progresses: make(map[string]*progressIndicator),
	}
}

// progressIndicator reflects a specified progress.
type progressIndicator struct {
	total     float64
	remaining float64
	// We use a fixed interval's history to calculate the latest average speed.
	history *list.List
	// We use (maxSpeedCalculationWindow / updateInterval + 1) to get the windowCapacity.
	// Assume that the windowCapacity is 4, the init value is 1. After update 3 times with 2, 3, 4 separately. The window will become [1, 2, 3, 4].
	// Then we update it again with 5, the window will become [2, 3, 4, 5].
	windowCapacity int
	// windowLength is used to determine what data will be computed.
	// Assume that the windowLength is 2, the init value is 1. The value that will be calculated are [1].
	// After update 3 times with 2, 3, 4 separately. The value that will be calculated are [3,4] and the values in queue are [(1,2),3,4].
	// It helps us avoid calculation results jumping change when patrol-region-interval changes.
	windowLength int
	// front is the first element which should be used.
	// currentWindowLength indicates where the front is currently in the queue.
	// Assume that the windowLength is 2, the init value is 1. The front is [1] and currentWindowLength is 1.
	// After update 3 times with 2, 3, 4 separately.
	// The front is [3], the currentWindowLength is 2, and values in queue are [(1,2),3,4]
	//                                                                                ^ front
	//                                                                                - - currentWindowLength = len([3,4]) = 2
	// We will always keep the currentWindowLength equal to windowLength if the actual size is enough.
	front               *list.Element
	currentWindowLength int

	updateInterval time.Duration
	lastSpeed      float64
}

// Reset resets the progress manager.
func (m *Manager) Reset() {
	m.Lock()
	defer m.Unlock()

	m.progresses = make(map[string]*progressIndicator)
}

// Option is used to do some action for progressIndicator.
type Option func(*progressIndicator)

// WindowDurationOption changes the time window size.
func WindowDurationOption(dur time.Duration) func(*progressIndicator) {
	return func(pi *progressIndicator) {
		if dur < minSpeedCalculationWindow {
			dur = minSpeedCalculationWindow
		} else if dur > maxSpeedCalculationWindow {
			dur = maxSpeedCalculationWindow
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
	if _, exist = m.progresses[progress]; !exist {
		pi := &progressIndicator{
			total:          total,
			remaining:      total,
			history:        history,
			windowCapacity: int(maxSpeedCalculationWindow/updateInterval) + 1,
			windowLength:   int(minSpeedCalculationWindow / updateInterval),
			updateInterval: updateInterval,
		}
		for _, op := range opts {
			op(pi)
		}
		m.progresses[progress] = pi
		pi.front = history.Front()
		pi.currentWindowLength = 1
	}
	return
}

// UpdateProgress updates the progress if it exists.
func (m *Manager) UpdateProgress(progress string, current, remaining float64, isInc bool, opts ...Option) {
	m.Lock()
	defer m.Unlock()

	p, exist := m.progresses[progress]
	if !exist {
		return
	}

	for _, op := range opts {
		op(p)
	}
	p.remaining = remaining
	if p.total < remaining {
		p.total = remaining
	}

	p.history.PushBack(current)
	p.currentWindowLength++

	// try to move `front` into correct place.
	for p.currentWindowLength > p.windowLength {
		p.front = p.front.Next()
		p.currentWindowLength--
	}
	for p.currentWindowLength < p.windowLength && p.front.Prev() != nil {
		p.front = p.front.Prev()
		p.currentWindowLength++
	}

	for p.history.Len() > p.windowCapacity {
		p.history.Remove(p.history.Front())
	}

	// It means it just init and we haven't update the progress
	if p.history.Len() <= 1 {
		p.lastSpeed = 0
	} else if isInc {
		// the value increases, e.g., [1, 2, 3]
		p.lastSpeed = (current - p.front.Value.(float64)) /
			(float64(p.currentWindowLength-1) * p.updateInterval.Seconds())
	} else {
		// the value decreases, e.g., [3, 2, 1]
		p.lastSpeed = (p.front.Value.(float64) - current) /
			(float64(p.currentWindowLength-1) * p.updateInterval.Seconds())
	}
	if p.lastSpeed < 0 {
		p.lastSpeed = 0
	}
}

// UpdateProgressTotal updates the total value of a progress if it exists.
func (m *Manager) UpdateProgressTotal(progress string, total float64) {
	m.Lock()
	defer m.Unlock()

	if p, exist := m.progresses[progress]; exist {
		p.total = total
	}
}

// RemoveProgress removes a progress from manager.
func (m *Manager) RemoveProgress(progress string) (exist bool) {
	m.Lock()
	defer m.Unlock()

	if _, exist = m.progresses[progress]; exist {
		delete(m.progresses, progress)
		return
	}
	return
}

// GetProgresses gets progresses according to the filter.
func (m *Manager) GetProgresses(filter func(p string) bool) []string {
	m.RLock()
	defer m.RUnlock()

	progresses := make([]string, 0, len(m.progresses))
	for p := range m.progresses {
		if filter(p) {
			progresses = append(progresses, p)
		}
	}
	return progresses
}

// Status returns the current progress status of a give name.
func (m *Manager) Status(progressName string) (progress, leftSeconds, currentSpeed float64, err error) {
	m.RLock()
	defer m.RUnlock()

	p, exist := m.progresses[progressName]
	if !exist {
		err = errs.ErrProgressNotFound.FastGenByArgs(fmt.Sprintf("the progress: %s", progressName))
		return
	}
	progress = 1 - p.remaining/p.total
	if progress < 0 {
		progress = 0
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
