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
	"fmt"
	"math"
	"time"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
)

// speedStatisticalInterval is the speed calculation interval
var speedStatisticalInterval = 5 * time.Minute

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
	// we use a fixed interval to calculate the latest average speed.
	lastTimeRemaining float64
	lastSpeed         float64
	lastTime          time.Time
}

// Reset resets the progress manager.
func (m *Manager) Reset() {
	m.Lock()
	defer m.Unlock()

	m.progesses = make(map[string]*progressIndicator)
}

// AddProgress adds a progress into manager if it doesn't exist.
func (m *Manager) AddProgress(progress string, total float64) (exist bool) {
	m.Lock()
	defer m.Unlock()

	if _, exist = m.progesses[progress]; !exist {
		m.progesses[progress] = &progressIndicator{
			total:             total,
			remaining:         total,
			lastTimeRemaining: total,
			lastTime:          time.Now(),
		}
	}
	return
}

// UpdateProgressRemaining updates the remaining value of a progress if it exists.
func (m *Manager) UpdateProgressRemaining(progress string, remaining float64) {
	m.Lock()
	defer m.Unlock()

	if p, exist := m.progesses[progress]; exist {
		p.remaining = remaining
		if p.total < remaining {
			p.total = remaining
		}
		if p.lastTimeRemaining < remaining {
			p.lastTimeRemaining = remaining
		}
		// calculate the average speed for every `speedStatisticalInterval`
		if time.Since(p.lastTime) >= speedStatisticalInterval {
			if (p.lastTimeRemaining - remaining) <= 0 {
				p.lastSpeed = 0
			} else {
				p.lastSpeed = (p.lastTimeRemaining - remaining) / time.Since(p.lastTime).Seconds()
			}
			p.lastTime = time.Now()
			p.lastTimeRemaining = remaining
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
		currentSpeed = 0
		// when the progress is newly added
		if p.lastSpeed == 0 && time.Since(p.lastTime) < speedStatisticalInterval {
			currentSpeed = (p.lastTimeRemaining - p.remaining) / time.Since(p.lastTime).Seconds()
		} else {
			currentSpeed = p.lastSpeed
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
