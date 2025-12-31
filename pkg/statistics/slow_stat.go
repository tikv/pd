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

package statistics

import (
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// SlowStat contains cluster's slow nodes' statistics.
type SlowStat struct {
	*SlowStoresStats
}

// NewSlowStat creates the container to hold slow nodes' statistics.
func NewSlowStat() *SlowStat {
	return &SlowStat{
		SlowStoresStats: NewSlowStoresStats(),
	}
}

// SlowStoresStats is a cached statistics for the slow store.
type SlowStoresStats struct {
	syncutil.RWMutex
	slowStores map[uint64]struct{}
}

// NewSlowStoresStats creates a new slowStoresStats cache.
func NewSlowStoresStats() *SlowStoresStats {
	return &SlowStoresStats{
		slowStores: make(map[uint64]struct{}),
	}
}

// RemoveSlowStoreStatus removes SlowStoreStats with a given store ID.
func (s *SlowStoresStats) RemoveSlowStoreStatus(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.slowStores, storeID)
}

// ObserveSlowStoreStatus updates SlowStoreStats with a given store ID.
func (s *SlowStoresStats) ObserveSlowStoreStatus(storeID uint64, isSlow bool) {
	s.Lock()
	defer s.Unlock()
	// If the given store was slow, this store should be recorded. Otherwise,
	// this store should be removed from the recording list.
	if _, ok := s.slowStores[storeID]; !ok && isSlow {
		s.slowStores[storeID] = struct{}{}
	} else {
		delete(s.slowStores, storeID)
	}
}

// ExistsSlowStores returns whether there exists slow stores in this cluster.
func (s *SlowStoresStats) ExistsSlowStores() bool {
	s.RLock()
	defer s.RUnlock()
	return len(s.slowStores) > 0
}
