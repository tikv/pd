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

package gc

import (
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/storage/endpoint"
)

// SafePointManager is the manager for safePoint of GC and services
type SafePointManager struct {
	*gcSafePointManager
	// TODO add ServiceSafepointManager
}

// NewSafepointManager creates a SafePointManager of GC and services
func NewSafepointManager(store endpoint.GCSafePointStorage) *SafePointManager {
	return &SafePointManager{
		newGCSafePointManager(store),
	}
}

type gcSafePointManager struct {
	syncutil.RWMutex
	store endpoint.GCSafePointStorage
}

func newGCSafePointManager(store endpoint.GCSafePointStorage) *gcSafePointManager {
	return &gcSafePointManager{store: store}
}

// LoadGCSafePoint loads current GC safe point from storage.
func (manager *gcSafePointManager) LoadGCSafePoint() (uint64, error) {
	return manager.store.LoadGCSafePoint()
}

// UpdateGCSafePoint updates the safepoint if it is greater than the previous one
// it returns the old safepoint in the storage.
func (manager *gcSafePointManager) UpdateGCSafePoint(newSafePoint uint64) (oldSafePoint uint64, err error) {
	manager.Lock()
	defer manager.Unlock()
	// TODO: cache the safepoint in the storage.
	oldSafePoint, err = manager.store.LoadGCSafePoint()
	if err != nil {
		return
	}
	if oldSafePoint >= newSafePoint {
		return
	}
	err = manager.store.SaveGCSafePoint(newSafePoint)
	return
}
