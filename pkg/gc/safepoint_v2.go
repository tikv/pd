// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

var (
	// allowUpdateSafePoint specifies under which states is a keyspace allowed to update it's gc & service safe points.
	allowUpdateSafePoint = []keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED,
		keyspacepb.KeyspaceState_DISABLED,
	}
)

// SafePointV2Manager is the manager for GCSafePointV2 and ServiceSafePointV2.
type SafePointV2Manager struct {
	*syncutil.LockGroup
	ctx context.Context
	// keyspaceStorage stores keyspace meta.
	keyspaceStorage endpoint.KeyspaceStorage
	// v2Storage is the storage GCSafePointV2 and ServiceSafePointV2.
	v2Storage endpoint.SafePointV2Storage
	// v1Storage is the storage for v1 format GCSafePoint and ServiceGCSafePoint, it's used during pd update.
	v1Storage endpoint.GCSafePointStorage
}

// NewSafePointManagerV2 returns a new SafePointV2Manager.
func NewSafePointManagerV2(
	ctx context.Context,
	keyspaceStore endpoint.KeyspaceStorage,
	v2Storage endpoint.SafePointV2Storage,
	v1Storage endpoint.GCSafePointStorage,
) *SafePointV2Manager {
	return &SafePointV2Manager{
		ctx:             ctx,
		LockGroup:       syncutil.NewLockGroup(syncutil.WithHash(keyspace.MaskKeyspaceID)),
		keyspaceStorage: keyspaceStore,
		v2Storage:       v2Storage,
		v1Storage:       v1Storage,
	}
}

// LoadGCSafePoint returns GCSafePointV2 of keyspaceID.
func (manager *SafePointV2Manager) LoadGCSafePoint(keyspaceID uint32) (*endpoint.GCSafePointV2, error) {
	manager.Lock(keyspaceID)
	defer manager.Unlock(keyspaceID)
	// Check if keyspace is valid to load.
	if err := manager.checkKeyspace(keyspaceID, false); err != nil {
		return nil, err
	}
	gcSafePoint, err := manager.getGCSafePoint(keyspaceID)
	if err != nil {
		log.Warn("failed to load gc safe point",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.Error(err),
		)
		return nil, err
	}
	return gcSafePoint, nil
}

// checkKeyspace check if target keyspace exists, and if request is a update request,
// also check if keyspace state allows for update.
func (manager *SafePointV2Manager) checkKeyspace(keyspaceID uint32, updateRequest bool) error {
	failpoint.Inject("checkKeyspace", func() {
		failpoint.Return(nil)
	})

	err := manager.keyspaceStorage.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err := manager.keyspaceStorage.LoadKeyspaceMeta(txn, keyspaceID)
		if err != nil {
			return err
		}
		// If a keyspace does not exist, then loading its gc safe point is prohibited.
		if meta == nil {
			return keyspace.ErrKeyspaceNotFound
		}
		// If keyspace's state does not permit updating safe point, we return error.
		if updateRequest && !slice.Contains(allowUpdateSafePoint, meta.GetState()) {
			return errors.Errorf("cannot update keyspace that's %s", meta.GetState().String())
		}
		return nil
	})
	if err != nil {
		log.Warn("check keyspace failed",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.Error(err),
		)
	}
	return err
}

// getGCSafePoint first try to load gc safepoint from v2 storage, if failed, load from v1 storage instead.
func (manager *SafePointV2Manager) getGCSafePoint(keyspaceID uint32) (*endpoint.GCSafePointV2, error) {
	v2SafePoint, err := manager.v2Storage.LoadGCSafePointV2(keyspaceID)
	if err != nil {
		return nil, err
	}
	// If failed to find a valid safe point, check if a safe point exist in v1 storage, and use it.
	if v2SafePoint.SafePoint == 0 {
		v1SafePoint, err := manager.v1Storage.LoadGCSafePoint()
		if err != nil {
			return nil, err
		}
		log.Info("keyspace does not have a gc safe point, using v1 gc safe point instead",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.Uint64("gc-safe-point-v1", v1SafePoint))
		v2SafePoint.SafePoint = v1SafePoint
	}
	return v2SafePoint, nil
}

// UpdateGCSafePoint is used to update gc safe point for given keyspace.
func (manager *SafePointV2Manager) UpdateGCSafePoint(gcSafePoint *endpoint.GCSafePointV2) (oldGCSafePoint *endpoint.GCSafePointV2, err error) {
	manager.Lock(gcSafePoint.KeyspaceID)
	defer manager.Unlock(gcSafePoint.KeyspaceID)
	// Check if keyspace is valid to load.
	if err = manager.checkKeyspace(gcSafePoint.KeyspaceID, true); err != nil {
		return
	}
	oldGCSafePoint, err = manager.getGCSafePoint(gcSafePoint.KeyspaceID)
	if err != nil {
		return
	}
	if oldGCSafePoint.SafePoint >= gcSafePoint.SafePoint {
		return
	}
	err = manager.v2Storage.SaveGCSafePointV2(gcSafePoint)
	return
}

// UpdateServiceSafePoint update keyspace service safe point with the given serviceSafePoint.
func (manager *SafePointV2Manager) UpdateServiceSafePoint(serviceSafePoint *endpoint.ServiceSafePointV2, now time.Time) (*endpoint.ServiceSafePointV2, error) {
	manager.Lock(serviceSafePoint.KeyspaceID)
	defer manager.Unlock(serviceSafePoint.KeyspaceID)
	// Check if keyspace is valid to update.
	if err := manager.checkKeyspace(serviceSafePoint.KeyspaceID, true); err != nil {
		return nil, err
	}
	minServiceSafePoint, err := manager.v2Storage.LoadMinServiceSafePointV2(serviceSafePoint.KeyspaceID, now)
	if err != nil {
		return nil, err
	}
	if serviceSafePoint.SafePoint < minServiceSafePoint.SafePoint {
		log.Warn("failed to update service safe point, proposed safe point smaller than current min",
			zap.Error(err),
			zap.Uint32("keyspace-id", serviceSafePoint.KeyspaceID),
			zap.Uint64("request-service-safe-point", serviceSafePoint.SafePoint),
			zap.Uint64("min-service-safe-point", minServiceSafePoint.SafePoint),
		)
		return minServiceSafePoint, nil
	}
	if err = manager.v2Storage.SaveServiceSafePointV2(serviceSafePoint); err != nil {
		return nil, err
	}
	// If the updated safe point is the original min safe point, reload min safe point.
	if serviceSafePoint.ServiceID == minServiceSafePoint.ServiceID {
		minServiceSafePoint, err = manager.v2Storage.LoadMinServiceSafePointV2(serviceSafePoint.KeyspaceID, now)
	}
	if err != nil {
		log.Info("update service safe point",
			zap.String("service-id", serviceSafePoint.ServiceID),
			zap.Int64("expire-at", serviceSafePoint.ExpiredAt),
			zap.Uint64("safepoint", serviceSafePoint.SafePoint),
		)
	}
	return minServiceSafePoint, err
}

// RemoveServiceSafePoint remove keyspace service safe point with the given keyspaceID and serviceID.
func (manager *SafePointV2Manager) RemoveServiceSafePoint(keyspaceID uint32, serviceID string, now time.Time) (*endpoint.ServiceSafePointV2, error) {
	manager.Lock(keyspaceID)
	defer manager.Unlock(keyspaceID)
	// Check if keyspace is valid to update.
	if err := manager.checkKeyspace(keyspaceID, true); err != nil {
		return nil, err
	}
	// Remove target safe point.
	if err := manager.v2Storage.RemoveServiceSafePointV2(keyspaceID, serviceID); err != nil {
		return nil, err
	}
	// Load min safe point.
	minServiceSafePoint, err := manager.v2Storage.LoadMinServiceSafePointV2(keyspaceID, now)
	if err != nil {
		return nil, err
	}
	return minServiceSafePoint, nil
}
