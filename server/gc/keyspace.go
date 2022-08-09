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
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/keyspace"
	"github.com/tikv/pd/server/storage/endpoint"
)

// KeyspaceSafePointManager manages safe points for all keyspaces.
type KeyspaceSafePointManager struct {
	updateGCLock      *syncutil.LockGroup
	updateServiceLock *syncutil.LockGroup
	keyspaceManager   *keyspace.Manager
	store             endpoint.KeyspaceGCSafePointStorage
}

// NewKeyspaceSafePointManager creates a KeyspaceSafePointManager of GC and services.
func NewKeyspaceSafePointManager(store endpoint.KeyspaceGCSafePointStorage, keyspaceManager *keyspace.Manager) *KeyspaceSafePointManager {
	return &KeyspaceSafePointManager{
		updateGCLock:      syncutil.NewLockGroup(syncutil.WithHash(keyspace.SpaceIDHash)),
		updateServiceLock: syncutil.NewLockGroup(syncutil.WithHash(keyspace.SpaceIDHash)),
		keyspaceManager:   keyspaceManager,
		store:             store,
	}
}

// ListGCSafePoints returns a slice containing all keyspaces' gc safe point.
func (manager *KeyspaceSafePointManager) ListGCSafePoints() (gcSafePoints []*gcpb.GCSafePoint, err error) {
	return manager.store.LoadAllKeyspaceGCSafePoints()
}

// UpdateGCSafePoint updates keyspace's safepoint if it is greater than the old value.
// It returns the old safepoint in the storage.
// This update is only allowed when keyspace is ENABLED.
func (manager *KeyspaceSafePointManager) UpdateGCSafePoint(spaceID uint32, newSafePoint uint64) (oldSafePoint uint64, err error) {
	state, err := manager.keyspaceManager.LoadState(spaceID)
	if err != nil { // This should capture cases where keyspace does not exist.
		return 0, err
	}
	if state != keyspacepb.KeyspaceState_ENABLED {
		return 0, errors.Errorf("failed to update GC safe point for keyspace %d, keyspace not enabled", spaceID)
	}
	manager.updateGCLock.Lock(spaceID)
	defer manager.updateGCLock.Unlock(spaceID)
	oldSafePoint, err = manager.store.LoadKeyspaceGCSafePoint(spaceID)
	if err != nil || oldSafePoint >= newSafePoint {
		return oldSafePoint, err
	}
	return oldSafePoint, manager.store.SaveKeyspaceGCSafePoint(spaceID, newSafePoint)
}

// UpdateServiceSafePoint update keyspace's service safepoint if it's greater than the minimum service safe point for that keyspace.
// It returns the min service safe point for that keyspace after the update, and weather an update took place.
// This update is only allowed when keyspace is ENABLED or DISABLED,
// as ome service (e.g., CDC) may need to update their safe point after keyspace is disabled.
func (manager *KeyspaceSafePointManager) UpdateServiceSafePoint(spaceID uint32, serviceID string, newSafePoint uint64, ttl int64, now time.Time) (minServiceSafePoint *endpoint.ServiceSafePoint, updated bool, err error) {
	state, err := manager.keyspaceManager.LoadState(spaceID)
	if err != nil { // This should capture cases where keyspace does not exist.
		return nil, false, err
	}
	if state != keyspacepb.KeyspaceState_ENABLED {
		return nil, false, errors.Errorf("failed to update service safe point for keyspace %d, keyspace archived", spaceID)
	}
	manager.updateServiceLock.Lock(spaceID)
	defer manager.updateServiceLock.Unlock(spaceID)
	minServiceSafePoint, err = manager.store.LoadMinServiceSafePoint(spaceID, now)
	if err != nil || ttl <= 0 || newSafePoint < minServiceSafePoint.SafePoint {
		return minServiceSafePoint, false, err
	}
	ssp := &endpoint.ServiceSafePoint{
		ServiceID: serviceID,
		ExpiredAt: now.Unix() + ttl,
		SafePoint: newSafePoint,
	}
	if math.MaxInt64-now.Unix() <= ttl {
		ssp.ExpiredAt = math.MaxInt64
	}
	if err = manager.store.SaveServiceSafePoint(spaceID, ssp); err != nil {
		return nil, false, err
	}

	// If the min safePoint is updated, reload min.
	if serviceID == minServiceSafePoint.ServiceID {
		minServiceSafePoint, err = manager.store.LoadMinServiceSafePoint(spaceID, now)
	}
	return minServiceSafePoint, true, err
}

// LoadGCSafePoint loads given keyspace's gc safe point.
func (manager *KeyspaceSafePointManager) LoadGCSafePoint(spaceID uint32) (uint64, error) {
	return manager.store.LoadKeyspaceGCSafePoint(spaceID)
}

// RemoveServiceSafePoint removes given keyspace's target service safe point.
func (manager *KeyspaceSafePointManager) RemoveServiceSafePoint(spaceID uint32, serviceID string) error {
	return manager.store.RemoveServiceSafePoint(spaceID, serviceID)
}
