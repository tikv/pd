// Copyright 2025 TiKV Project Authors.
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

package keyrange

import (
	"sync"

	"github.com/tikv/pd/pkg/utils/keyutil"
)

// Manager is a manager for key ranges.
type Manager struct {
	sync.Mutex
	sortedKeyRanges *keyutil.KeyRanges
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		sortedKeyRanges: &keyutil.KeyRanges{},
	}
}

// GetNonOverlappingKeyRanges returns the non-overlapping key ranges of the given base key range.
func (s *Manager) GetNonOverlappingKeyRanges(base *keyutil.KeyRange) []keyutil.KeyRange {
	s.Lock()
	defer s.Unlock()
	return s.sortedKeyRanges.SubtractKeyRanges(base)
}

// IsEmpty returns true if the key ranges is empty
func (s *Manager) IsEmpty() bool {
	s.Lock()
	defer s.Unlock()
	return s.sortedKeyRanges.IsEmpty()
}

// Append appends the key ranges to the manager.
func (s *Manager) Append(rs []keyutil.KeyRange) {
	s.Lock()
	defer s.Unlock()
	for _, r := range rs {
		s.sortedKeyRanges.Append(r.StartKey, r.EndKey)
	}
	s.sortedKeyRanges.SortAndDeduce()
}

// Delete deletes the overlapping key ranges from the manager.
func (s *Manager) Delete(rs []keyutil.KeyRange) {
	s.Lock()
	defer s.Unlock()
	for _, r := range rs {
		s.sortedKeyRanges.Delete(&r)
	}
}
