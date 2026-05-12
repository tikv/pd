// Copyright 2023 TiKV Project Authors.
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

package storelimit

import (
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// minSnapSize is the min value to check the windows has enough size.
	minSnapSize = 10
	// defaultWindowSize is the default window size.
	defaultWindowSize = 100

	defaultProportion = 20
	defaultIntegral   = 10
)

var _ StoreLimit = &SlidingWindows{}

// SlidingWindows is a multi sliding windows
type SlidingWindows struct {
	mu      syncutil.RWMutex
	windows []*window
	lastSum float64
}

// NewSlidingWindows is the construct of SlidingWindows.
func NewSlidingWindows() *SlidingWindows {
	windows := make([]*window, constant.PriorityLevelLen)
	for i := range constant.PriorityLevelLen {
		windows[i] = newWindow(int64(defaultWindowSize) >> i)
	}
	return &SlidingWindows{
		windows: windows,
	}
}

// Version returns v2
func (*SlidingWindows) Version() string {
	return VersionV2
}

// Feedback is used to update the capacity of the sliding windows.
func (s *SlidingWindows) Feedback(e float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// If the limiter is available, we don't need to update the capacity.
	if s.windows[constant.Low].available() {
		return
	}
	s.lastSum += e
	// There are two constants to control the proportion of the sum and the current error.
	// The sum of the error is used to ensure the capacity is more stable even if the error is zero.
	// In the final scene, the sum of the error should be stable and the current error should be zero.
	cap := defaultProportion*e + defaultIntegral*s.lastSum
	// The capacity should be at least the default window size.
	if cap < defaultWindowSize {
		cap = defaultWindowSize
	}
	s.set(cap, SendSnapshot)
}

// Reset does nothing because the capacity depends on the feedback.
func (*SlidingWindows) Reset(_ float64, _ Type) {}

func (s *SlidingWindows) set(cap float64, typ Type) {
	if typ != SendSnapshot {
		return
	}
	if cap < 0 {
		cap = minSnapSize
	}
	for i, v := range s.windows {
		v.reset(int64(cap) >> i)
	}
}

// GetCap returns the capacity of the sliding windows.
func (s *SlidingWindows) GetCap() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.windows[0].capacity
}

// GetUsed returns the used size in the sliding windows.
func (s *SlidingWindows) GetUsed() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	used := make([]int64, len(s.windows))
	for i, v := range s.windows {
		used[i] = v.getUsed()
	}
	return used
}

// Available returns whether the token can be taken.
// The order of checking windows is from low to high.
// It checks the given window finally if the lower window has no free size.
func (s *SlidingWindows) Available(_ int64, typ Type, level constant.PriorityLevel) bool {
	if typ != SendSnapshot {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := 0; i <= int(level); i++ {
		if s.windows[i].available() {
			return true
		}
	}
	return false
}

// Take tries to take the token.
// It will consume the given window finally if the lower window has no free size.
func (s *SlidingWindows) Take(token int64, typ Type, level constant.PriorityLevel) bool {
	if typ != SendSnapshot {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i <= int(level); i++ {
		if s.windows[i].take(token) {
			return true
		}
	}
	return false
}

// Ack indicates that some executing operator has been finished.
// The order of refilling windows is from high to low.
// It will refill the highest window first.
func (s *SlidingWindows) Ack(token int64, typ Type) {
	if typ != SendSnapshot {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := constant.PriorityLevelLen - 1; i >= 0; i-- {
		if token = s.windows[i].ack(token); token <= 0 {
			break
		}
	}
}

// window is a sliding window.
// |---used---|----available---|
// |-------- capacity  --------|
type window struct {
	// capacity is the max size of the window.
	capacity int64
	// used is the size of the operators that are executing.
	// the used can be larger than the capacity.but allow
	// one operator to exceed the capacity at most.
	used int64
	// the count of operators in the window.
	count uint
}

func newWindow(capacity int64) *window {
	// the min capacity is minSnapSize to allow one operator at least.
	if capacity < minSnapSize {
		capacity = minSnapSize
	}
	return &window{capacity: capacity, used: 0, count: 0}
}

func (s *window) reset(capacity int64) {
	// the min capacity is minSnapSize to allow one operator at least.
	if capacity < minSnapSize {
		capacity = minSnapSize
	}
	s.capacity = capacity
}

// Ack indicates that some executing operator has been finished.
func (s *window) ack(token int64) int64 {
	if s.used == 0 {
		return token
	}
	available := int64(0)
	if s.used > token {
		s.used -= token
	} else {
		available = token - s.used
		s.used = 0
	}
	s.count--
	return available
}

// getUsed returns the used size in the sliding windows.
func (s *window) getUsed() int64 {
	return s.used
}

func (s *window) available() bool {
	return s.used+minSnapSize <= s.capacity
}

func (s *window) take(token int64) bool {
	if !s.available() {
		return false
	}
	s.used += token
	s.count++
	return true
}
