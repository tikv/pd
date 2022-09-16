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

package storelimit

import "github.com/tikv/pd/pkg/syncutil"

const (
	snapSize = 10
)

// SlidingWindows is a multi sliding windows
type SlidingWindows struct {
	mu      syncutil.RWMutex
	windows []*window
}

// NewSlidingWindows is the construct of SlidingWindows.
func NewSlidingWindows(capacity int64) *SlidingWindows {
	windows := make([]*window, MaxPriority)
	for i := 0; i < int(MaxPriority); i++ {
		windows[i] = newWindow(capacity >> i)
	}

	return &SlidingWindows{
		windows: windows,
	}
}

// Reset resets the capacity of the sliding windows.
func (s *SlidingWindows) Reset(capacity int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, v := range s.windows {
		v.reset(capacity >> i)
	}
}

// GetUsed returns the used size in the sliding windows.
func (s *SlidingWindows) GetUsed() int64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	used := int64(0)
	for _, v := range s.windows {
		used += v.getUsed()
	}
	return used
}

// Available returns whether the token can be taken.
// It will check the given window finally if the lower window has no free size.
func (s *SlidingWindows) Available(_ int64, level Level) bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := 0; i < int(level); i++ {
		if s.windows[i].available() {
			return true
		}
	}
	return false
}

// Take tries to take the token.
// It will consume the given window finally if the lower window has no free size.
func (s *SlidingWindows) Take(token int64, level Level) bool {
	if s == nil {
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
// It will refill the highest window first.
func (s *SlidingWindows) Ack(token int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := MaxPriority - 1; i >= 0; i-- {
		if token = s.windows[i].ack(token); token <= 0 {
			break
		}
	}
}

// window is a sliding window.
type window struct {
	capacity int64
	used     int64
	count    uint
}

func newWindow(capacity int64) *window {
	// the min capacity is snapSize to allow one operator at least.
	if capacity < snapSize {
		capacity = snapSize
	}
	return &window{capacity: capacity, used: 0, count: 0}
}

func (s *window) reset(capacity int64) {
	if capacity < snapSize {
		capacity = snapSize
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
	return s.used+snapSize <= s.capacity
}

func (s *window) take(token int64) bool {
	if !s.available() {
		return false
	}
	s.used += token
	s.count++
	return true
}
