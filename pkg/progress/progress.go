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
	"sync"
	"time"
)

// Manager is used to maintain the progresses we are care about.
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
	current     float64
	startTime   time.Time
	speedPerSec float64
}

// Reset resets the progress manager.
func (m *Manager) Reset() {
	m.Lock()
	defer m.Unlock()

	m.progesses = make(map[string]*progressIndicator)
}

// AddOrUpdateProgress adds a progress into manager if it doesn't exist.
func (m *Manager) AddOrUpdateProgress(progress string, total, current float64) (exist bool) {
	m.Lock()
	defer m.Unlock()

	var p *progressIndicator
	if p, exist = m.progesses[progress]; exist {
		p.current = current
		if p.total < total {
			p.total = total
		}
		p.speedPerSec = (p.total - p.current) / time.Since(p.startTime).Seconds()
		return
	}
	m.progesses[progress] = &progressIndicator{
		total:     total,
		startTime: time.Now(),
	}
	return
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
		process = 1 - p.current/p.total
		leftSeconds = p.current / ((p.total - p.current) / time.Since(p.startTime).Seconds())
		currentSpeed = p.speedPerSec
		return
	}
	return 0, 0, 0
}
