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
	total          float64
	current        float64
	startTime      time.Time
	lastUpdateTime time.Time
	speedPerSec    float64
}

// AddProgressIndicator adds a progress into manager.
func (m *Manager) AddProgressIndicator(progress string, total float64) {
	m.Lock()
	defer m.Unlock()

	m.progesses[progress] = &progressIndicator{
		total:     total,
		startTime: time.Now(),
	}
}

// RemoveProgressIndicator removes a progress from manager.
func (m *Manager) RemoveProgressIndicator(progress string) {
	m.Lock()
	defer m.Unlock()

	delete(m.progesses, progress)
}

// UpdateProgressIndicator updates the progress of a given name.
func (m *Manager) UpdateProgressIndicator(progress string, count float64) {
	m.Lock()
	defer m.Unlock()

	m.progesses[progress].current += count
	m.progesses[progress].speedPerSec = count / time.Since(m.progesses[progress].lastUpdateTime).Seconds()
	m.progesses[progress].lastUpdateTime = time.Now()
}

// LeftSeconds returns the left seconds until finishing.
func (m *Manager) LeftSeconds(progress string) float64 {
	m.RLock()
	defer m.RUnlock()

	return time.Since(m.progesses[progress].startTime).Seconds() /
		m.progesses[progress].current * (m.progesses[progress].total - m.progesses[progress].current)
}

// Process returns the current progress of a give name.
func (m *Manager) Process(progress string) float64 {
	m.RLock()
	defer m.RUnlock()

	return m.progesses[progress].current / m.progesses[progress].total
}

// CurrentSpeed returns the current speed of a given name.
func (m *Manager) CurrentSpeed(progress string) float64 {
	m.RLock()
	defer m.RUnlock()

	return m.progesses[progress].speedPerSec
}
