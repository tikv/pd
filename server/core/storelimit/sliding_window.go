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

import (
	"github.com/tikv/pd/pkg/syncutil"
)

// SlidingWindows limits the operators of a store
type SlidingWindows struct {
	mu       syncutil.Mutex
	capacity int64
	used     int64
}

// NewSlidingWindows is the construct of sliding windows.
func NewSlidingWindows(capacity int64) *SlidingWindows {
	return &SlidingWindows{capacity: capacity, used: 0}
}

// Adjust the sliding window capacity.
func (s *SlidingWindows) Adjust(capacity int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capacity = capacity
}

// Ack indicates that some executing operator has been finished.
func (s *SlidingWindows) Ack(token int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used > token {
		s.used -= token
	}
	s.used = 0
}

// Available returns false if there is no free size for the token.
func (s *SlidingWindows) Available(token int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used+token <= s.capacity
}

// GetUsed returns the used size in the sliding windows.
func (s *SlidingWindows) GetUsed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// Take some size if there are some free size more than token.
func (s *SlidingWindows) Take(token int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used+token <= s.capacity {
		s.used += token
		return true
	}
	return false
}
