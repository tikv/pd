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
	"math"
	"sync"
	"time"
)

// Manager is used to maintain the progresses we care about.
type Manager struct {
	sync.RWMutex
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
	total       float64
	left        float64
	startTime   time.Time
	speedPerSec float64
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
			total:     total,
			left:      total,
			startTime: time.Now(),
		}
	}
	return
}

// UpdateProgress updates a progress into manager if it doesn't exist.
func (m *Manager) UpdateProgress(progress string, left float64) {
	m.Lock()
	defer m.Unlock()

	if p, exist := m.progesses[progress]; exist {
		p.left = left
		if p.total < left {
			p.total = left
		}
		p.speedPerSec = (p.total - p.left) / time.Since(p.startTime).Seconds()
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
	m.Lock()
	defer m.Unlock()

	processes := []string{}
	for p := range m.progesses {
		if filter(p) {
			processes = append(processes, p)
		}
	}
	return processes
}

// Status returns the current progress status of a give name.
func (m *Manager) Status(progress string) (process, leftSeconds, currentSpeed float64) {
	m.RLock()
	defer m.RUnlock()
	if p, exist := m.progesses[progress]; exist {
		process = 1 - p.left/p.total
		speedPerSec := (p.total - p.left) / time.Since(p.startTime).Seconds()
		leftSeconds = p.left / speedPerSec
		if math.IsNaN(leftSeconds) || math.IsInf(leftSeconds, 0) {
			leftSeconds = math.MaxFloat64
		}
		currentSpeed = speedPerSec
		return
	}
	return 0, 0, 0
}
