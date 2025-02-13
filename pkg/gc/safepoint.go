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
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

var blockGCSafePointErrmsg = "don't allow update gc safe point v1."
var blockServiceSafepointErrmsg = "don't allow update service safe point v1."

// GCStateManager is the manager for safePoint of GC and services.
type GCStateManager struct {
	lock            syncutil.RWMutex
	gcMetaStorage   endpoint.GCStateStorage
	cfg             config.PDServerConfig
	keyspaceManager keyspace.Manager
}

// NewGCStateManager creates a GCStateManager of GC and services.
func NewGCStateManager(store endpoint.GCStateStorage, cfg config.PDServerConfig) *GCStateManager {
	return &GCStateManager{gcMetaStorage: store, cfg: cfg}
}

func (m *GCStateManager) redirectKeyspace(keyspaceID uint32, isUserAPI bool) (uint32, error) {
	// Regard it as NullKeyspaceID if the given one is invalid (exceeds the valid range of keyspace id), no matter
	// whether it exactly matches the NullKeyspaceID.
	if keyspaceID & ^constant.ValidKeyspaceIDMask != 0 {
		return constant.NullKeyspaceID, nil
	}

	keyspaceMeta, err := m.keyspaceManager.LoadKeyspaceByID(keyspaceID)
	if err != nil {
		return 0, err
	}
	if keyspaceMeta.Config[keyspace.GCManagementType] != keyspace.KeyspaceLevelGC {
		if isUserAPI {
			// The user API is expected to always work. Operate on the state of global GC instead.
			return constant.NullKeyspaceID, nil
		}
		// Internal API should never be called on keyspaces without keyspace level GC. They won't perform any active
		// GC operation and will be managed by the global GC.
		return 0, errs.ErrGCOnInvalidKeyspace.GenWithStackByArgs(keyspaceID)
	}

	return keyspaceID, nil
}

// CompatibleLoadGCSafePoint loads current GC safe point from storage.
func (m *GCStateManager) CompatibleLoadGCSafePoint(keyspaceID uint32) (uint64, error) {
	keyspaceID, err := m.redirectKeyspace(keyspaceID, false)
	if err != nil {
		return 0, err
	}

	// No need to acquire the lock as a single-key read operation is atomic.
	return m.gcMetaStorage.LoadGCSafePoint(keyspaceID)
}

// AdvanceGCSafePoint tries to advance the GC safe point to the given target. If the target is less than the current
// value or greater than the txn safe point, it returns an error.
func (m *GCStateManager) AdvanceGCSafePoint(keyspaceID uint32, target uint64) (oldGCSafePoint uint64, newGCSafePoint uint64, err error) {
	return m.advanceGCSafePointImpl(keyspaceID, target, false)
}

// CompatibleUpdateGCSafePoint tries to advance the GC safe point to the given target. If the target is less than the
// current value, it returns the current value without updating it.
// This is provided for compatibility purpose, making the existing uses of the deprecated API `UpdateGCSafePoint`
// still work.
func (m *GCStateManager) CompatibleUpdateGCSafePoint(target uint64) (oldGCSafePoint uint64, newGCSafePoint uint64, err error) {
	return m.advanceGCSafePointImpl(constant.NullKeyspaceID, target, true)
}

func (m *GCStateManager) advanceGCSafePointImpl(keyspaceID uint32, target uint64, compatible bool) (oldGCSafePoint uint64, newGCSafePoint uint64, err error) {
	keyspaceID, err = m.redirectKeyspace(keyspaceID, false)
	if err != nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	newGCSafePoint = target

	err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		var err1 error
		oldGCSafePoint, err1 = m.gcMetaStorage.LoadGCSafePoint(keyspaceID)
		if err1 != nil {
			return err1
		}
		if target < oldGCSafePoint {
			if compatible {
				// When in compatible mode, trying to update the safe point to a smaller value fails silently, returning
				// the actual value. There exist some use cases that fetches the current value by passing zero.
				log.Warn("deprecated API `UpdateGCSafePoint` is called with invalid argument",
					zap.Uint64("currentGCSafePoint", oldGCSafePoint), zap.Uint64("attemptedGCSafePoint", target))
				newGCSafePoint = oldGCSafePoint
				return nil
			}
			// Otherwise, return error to reject the operation explicitly.
			return errs.ErrDecreasingGCSafePoint.GenWithStackByArgs(oldGCSafePoint, target)
		}
		txnSafePoint, err1 := m.gcMetaStorage.LoadTxnSafePoint(keyspaceID)
		if err1 != nil {
			return err1
		}
		if target > txnSafePoint {
			return errs.ErrGCSafePointExceedsTxnSafePoint.GenWithStackByArgs(oldGCSafePoint, target, txnSafePoint)
		}

		return wb.SetGCSafePoint(keyspaceID, target)
	})
	if err != nil {
		return 0, 0, err
	}

	if keyspaceID == constant.NullKeyspaceID {
		gcSafePointGauge.WithLabelValues("gc_safepoint").Set(float64(target))
	}

	return
}

func (m *GCStateManager) AdvanceTxnSafePoint(keyspaceID uint32, target uint64) (AdvanceTxnSafePointResult, error) {
	keyspaceID, err := m.redirectKeyspace(keyspaceID, false)
	if err != nil {
		return AdvanceTxnSafePointResult{}, err
	}

	m.lock.Lock()
	m.lock.Unlock()

	var oldTxnSafePoint uint64
	newTxnSafePoint := target
	var blockingBarrier *endpoint.GCBarrier
	var blockingMinStartTSOwner string

	err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		var err1 error
		oldTxnSafePoint, err1 = m.gcMetaStorage.LoadTxnSafePoint(keyspaceID)
		if err1 != nil {
			return err1
		}

		if target < oldTxnSafePoint {
			return errs.ErrDecreasingTxnSafePoint.GenWithStackByArgs(oldTxnSafePoint, target)
		}

		barriers, err1 := m.gcMetaStorage.LoadAllGCBarriers(keyspaceID)
		if err1 != nil {
			return err1
		}

		now := time.Now()
		for _, barrier := range barriers {
			if barrier.IsExpired(now) {
				// Perform lazy delete to the expired GC barriers.
				// WARNING: It might look like a reasonable optimization idea to perform the lazy-deletion in a lower
				// frequency (instead of everytime checking it). However, it's UNSAFE considering the possibility of
				// system clock drifts and PD leader changes, in which case an expired GC barrier may be back to
				// not-expired state again. Once we regard a GC barrier as expired, it must be expired *strictly*,
				// otherwise it may break the constraint that GC barriers must block the txn safe point from being
				// advanced over them.
				err1 = wb.DeleteGCBarrier(keyspaceID, barrier.BarrierID)
				if err1 != nil {
					return err1
				}
				// Do not block GC with expired barriers.
				continue
			}

			if barrier.BarrierTS < newTxnSafePoint {
				newTxnSafePoint = barrier.BarrierTS
				blockingBarrier = barrier
			}
		}

		// Compatible with old TiDB nodes that use TiDBMinStartTS to block GC.
		ownerKey, minStartTS, err1 := m.gcMetaStorage.LoadTiDBMinStartTS(keyspaceID)
		if err1 != nil {
			return err1
		}

		if minStartTS < newTxnSafePoint {
			// Note that txn safe point is defined inclusive: snapshots that exactly equals to the txn safe point are
			// considered valid.
			newTxnSafePoint = minStartTS
			blockingBarrier = nil
			blockingMinStartTSOwner = ownerKey
		}

		// TODO: Consider compatibility of the special service safe point "gc_worker".

		return wb.SetTxnSafePoint(keyspaceID, newTxnSafePoint)
	})
	if err != nil {
		return AdvanceTxnSafePointResult{}, err
	}

	blockerDesc := ""
	if blockingBarrier != nil {
		blockerDesc = blockingBarrier.String()
	} else if len(blockingMinStartTSOwner) > 0 {
		blockerDesc = fmt.Sprintf("TiDBMinStartTS { Key: %s, MinStartTS: %d }", blockingMinStartTSOwner, newTxnSafePoint)
	}

	if len(blockerDesc) > 0 {
		log.Info("GC advancing txn safe point is blocked",
			zap.Uint64("oldTxnSafePoint", oldTxnSafePoint), zap.Uint64("target", target),
			zap.Uint64("newTxnSafePoint", newTxnSafePoint), zap.String("blocker", blockerDesc))
	}

	return AdvanceTxnSafePointResult{
		OldTxnSafePoint:    oldTxnSafePoint,
		Target:             target,
		NewTxnSafePoint:    newTxnSafePoint,
		BlockerDescription: blockerDesc,
	}, nil
}

func (m *GCStateManager) SetGCBarrier(keyspaceID uint32, barrierID string, barrierTS uint64, ttl time.Duration, now time.Time) (*endpoint.GCBarrier, error) {
	if ttl <= 0 {
		return nil, errs.ErrInvalidArgument.GenWithStackByArgs("ttl", ttl)
	}

	keyspaceID, err := m.redirectKeyspace(keyspaceID, true)
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	newBarrier := endpoint.GCBarrier{
		BarrierID:      barrierID,
		BarrierTS:      barrierTS,
		ExpirationTime: nil,
		KeyspaceID:     keyspaceID,
	}
	if ttl < time.Duration(math.MaxInt64) {
		expirationTime := now.Add(ttl)
		newBarrier.ExpirationTime = &expirationTime
	}

	err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		txnSafePoint, err1 := m.gcMetaStorage.LoadTxnSafePoint(keyspaceID)
		if err1 != nil {
			return err1
		}
		if barrierTS < txnSafePoint {
			return errs.ErrInvalidGCBarrier.GenWithStackByArgs(barrierTS, txnSafePoint)
		}
		err1 = wb.SetGCBarrier(keyspaceID, newBarrier)
		return err1
	})
	if err != nil {
		return nil, err
	}

	return &newBarrier, nil
}

func (m *GCStateManager) DeleteGCBarrier(keyspaceID uint32, barrierID string) (*endpoint.GCBarrier, error) {
	keyspaceID, err := m.redirectKeyspace(keyspaceID, true)
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	var deletedBarrier *endpoint.GCBarrier
	err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		var err1 error
		deletedBarrier, err1 = m.gcMetaStorage.LoadGCBarrier(keyspaceID, barrierID)
		if err1 != nil {
			return err1
		}
		return wb.DeleteGCBarrier(keyspaceID, barrierID)
	})
	return deletedBarrier, err
}

func (m *GCStateManager) getGCStateInTransaction(keyspaceID uint32, _ *endpoint.GCStateWriteBatch) (GCState, error) {
	result := GCState{
		KeyspaceID: keyspaceID,
	}
	if keyspaceID != constant.NullKeyspaceID {
		result.IsKeyspaceLevel = true
	}

	var err error
	result.TxnSafePoint, err = m.gcMetaStorage.LoadTxnSafePoint(keyspaceID)
	if err != nil {
		return GCState{}, err
	}

	result.GCSafePoint, err = m.gcMetaStorage.LoadGCSafePoint(keyspaceID)
	if err != nil {
		return GCState{}, err
	}

	result.GCBarriers, err = m.gcMetaStorage.LoadAllGCBarriers(keyspaceID)
	if err != nil {
		return GCState{}, err
	}

	return result, nil
}

func (m *GCStateManager) GetGCState(keyspaceID uint32) (GCState, error) {
	keyspaceID, err := m.redirectKeyspace(keyspaceID, true)
	if err != nil {
		return GCState{}, err
	}

	var result GCState
	err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		var err1 error
		result, err1 = m.getGCStateInTransaction(keyspaceID, wb)
		return err1
	})

	if err != nil {
		return GCState{}, err
	}

	return result, nil
}

func (m *GCStateManager) GetGlobalGCState() (map[uint32]GCState, error) {
	// TODO: Handle the case that there are too many keyspaces and loading them at once is not suitable.
	allKeyspaces, err := m.keyspaceManager.LoadRangeKeyspace(0, 0)
	if err != nil {
		return nil, err
	}

	// Do not guarantee atomicity among different keyspaces here.
	results := make(map[uint32]GCState)
	err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		nullKeyspaceState, err1 := m.getGCStateInTransaction(constant.NullKeyspaceID, wb)
		if err1 != nil {
			return err1
		}
		results[constant.NullKeyspaceID] = nullKeyspaceState
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, keyspaceMeta := range allKeyspaces {
		if keyspaceMeta.Config[keyspace.GCManagementType] != keyspace.KeyspaceLevelGC {
			results[keyspaceMeta.Id] = GCState{
				KeyspaceID:      keyspaceMeta.Id,
				IsKeyspaceLevel: false,
			}
			continue
		}

		err = m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
			state, err1 := m.getGCStateInTransaction(keyspaceMeta.Id, wb)
			if err1 != nil {
				return err1
			}
			results[keyspaceMeta.Id] = state
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (m *GCStateManager) CompatibleUpdateServiceGCSafePoint(serviceID string, newServiceSafePoint uint64, ttl int64, now time.Time) (minServiceSafePoint *endpoint.ServiceSafePoint, updated bool, err error) {
	keyspaceID := constant.NullKeyspaceID
	m.lock.Lock()
	defer m.lock.Unlock()

	err := m.gcMetaStorage.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {

	})
	if err != nil {
		return nil, false, err
	}
}

// _UpdateServiceGCSafePoint update the safepoint for a specific service.
func (m *GCStateManager) _UpdateServiceGCSafePoint(serviceID string, newSafePoint uint64, ttl int64, now time.Time) (minServiceSafePoint *endpoint.ServiceSafePoint, updated bool, err error) {
	if m.cfg.BlockSafePointV1 {
		return nil, false, errors.New(blockServiceSafepointErrmsg)
	}
	// This function won't support keyspace as it's being deprecated.
	m.lock.Lock(constant.NullKeyspaceID)
	defer m.lock.Unlock(constant.NullKeyspaceID)
	minServiceSafePoint, err = m.gcMetaStorage.LoadMinServiceGCSafePoint(now)
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
	if err := m.gcMetaStorage.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, false, err
	}

	// If the min safePoint is updated, load the next one.
	if serviceID == minServiceSafePoint.ServiceID {
		minServiceSafePoint, err = m.gcMetaStorage.LoadMinServiceGCSafePoint(now)
	}
	return minServiceSafePoint, true, err
}

type AdvanceTxnSafePointResult struct {
	OldTxnSafePoint    uint64
	Target             uint64
	NewTxnSafePoint    uint64
	BlockerDescription string
}

type GCState struct {
	KeyspaceID      uint32
	IsKeyspaceLevel bool
	TxnSafePoint    uint64
	GCSafePoint     uint64
	GCBarriers      []*endpoint.GCBarrier
}
