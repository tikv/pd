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
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

var blockGCSafePointErrmsg = "don't allow update gc safe point v1."
var blockServiceSafepointErrmsg = "don't allow update service safe point v1."

// GCStateManager is the manager for safePoint of GC and services.
type GCStateManager struct {
	lock            syncutil.RWMutex
	gcMetaStorage   endpoint.GCStateProvider
	cfg             config.PDServerConfig
	keyspaceManager *keyspace.Manager
}

// NewGCStateManager creates a GCStateManager of GC and services.
func NewGCStateManager(store endpoint.GCStateProvider, cfg config.PDServerConfig, keyspaceManager *keyspace.Manager) *GCStateManager {
	return &GCStateManager{gcMetaStorage: store, cfg: cfg, keyspaceManager: keyspaceManager}
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
func (m *GCStateManager) CompatibleLoadGCSafePoint() (uint64, error) {
	keyspaceID, err := m.redirectKeyspace(constant.NullKeyspaceID, false)
	if err != nil {
		return 0, err
	}

	// No need to acquire the lock as a single-key read operation is atomic.
	return m.gcMetaStorage.LoadGCSafePoint(keyspaceID)
}

// AdvanceGCSafePoint tries to advance the GC safe point to the given target. If the target is less than the current
// value or greater than the txn safe point, it returns an error.
func (m *GCStateManager) AdvanceGCSafePoint(keyspaceID uint32, target uint64) (oldGCSafePoint uint64, newGCSafePoint uint64, err error) {
	keyspaceID, err = m.redirectKeyspace(keyspaceID, false)
	if err != nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	return m.advanceGCSafePointImpl(keyspaceID, target, false)
}

// CompatibleUpdateGCSafePoint tries to advance the GC safe point to the given target. If the target is less than the
// current value, it returns the current value without updating it.
// This is provided for compatibility purpose, making the existing uses of the deprecated API `UpdateGCSafePoint`
// still work.
func (m *GCStateManager) CompatibleUpdateGCSafePoint(target uint64) (oldGCSafePoint uint64, newGCSafePoint uint64, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.advanceGCSafePointImpl(constant.NullKeyspaceID, target, true)
}

func (m *GCStateManager) advanceGCSafePointImpl(keyspaceID uint32, target uint64, compatible bool) (oldGCSafePoint uint64, newGCSafePoint uint64, err error) {
	newGCSafePoint = target

	err = m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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

	return m.advanceTxnSafePointImpl(keyspaceID, target)
}

// advanceTxnSafePointImpl is the internal implementation of AdvanceTxnSafePoint, assuming keyspaceID has been checked
// and the mutex has been acquired.
func (m *GCStateManager) advanceTxnSafePointImpl(keyspaceID uint32, target uint64) (AdvanceTxnSafePointResult, error) {
	isCompatibleMode := false

	// A helper function for handling the compatibility of the service safe point of "gc_worker", which is needed
	// for making it able to downgrade to previous versions where service safe points are still in use.
	keepGCWorkerServiceSafePointCompatible := func(wb *endpoint.GCStateWriteBatch, sspAsGCBarrier *endpoint.GCBarrier) error {
		// In old versions, every time TiDB performs GC, it updates the service safe point of "gc_worker" to the GC
		// safe point it attempts to advance to, and is allowed to be greater than the minimum service safe point (in
		// which case the minimum one will be the actual GC safe point to use).
		// Note that in old versions, there wasn't the concept of txn safe point. The step to update the service safe
		// point of "gc_worker" is somewhat just like the current procedure of advancing the txn safe point, the most
		// important purpose of which is to find the actual GC safe point that's safe to use.
		isCompatibleMode = true
		sspAsGCBarrier.BarrierTS = target
		// Ensure service safe point of "gc_worker" should never expire.
		sspAsGCBarrier.ExpirationTime = nil
		return wb.SetGCBarrier(keyspaceID, sspAsGCBarrier)
	}

	var oldTxnSafePoint uint64
	newTxnSafePoint := target
	var blockingBarrier *endpoint.GCBarrier
	var blockingMinStartTSOwner *string

	err := m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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
			if barrier.BarrierID == keypath.GCWorkerServiceSafePointID {
				err1 = keepGCWorkerServiceSafePointCompatible(wb, barrier)
				if err1 != nil {
					return err1
				}
				continue
			}

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
		ownerKey, minStartTS, err1 := m.gcMetaStorage.CompatibleLoadTiDBMinStartTS(keyspaceID)
		if err1 != nil {
			return err1
		}

		if minStartTS != 0 && len(ownerKey) != 0 && minStartTS < newTxnSafePoint {
			// Note that txn safe point is defined inclusive: snapshots that exactly equals to the txn safe point are
			// considered valid.
			newTxnSafePoint = minStartTS
			blockingBarrier = nil
			blockingMinStartTSOwner = &ownerKey
		}

		return wb.SetTxnSafePoint(keyspaceID, newTxnSafePoint)
	})
	if err != nil {
		return AdvanceTxnSafePointResult{}, err
	}

	blockerDesc := ""
	if blockingBarrier != nil {
		blockerDesc = blockingBarrier.String()
	} else if blockingMinStartTSOwner != nil {
		blockerDesc = fmt.Sprintf("TiDBMinStartTS { Key: %+q, MinStartTS: %d }", *blockingMinStartTSOwner, newTxnSafePoint)
	}

	if newTxnSafePoint != target {
		if blockingBarrier == nil && blockingMinStartTSOwner == nil {
			panic("unreachable")
		}
		log.Info("txn safe point advancement is being blocked",
			zap.Uint64("oldTxnSafePoint", oldTxnSafePoint), zap.Uint64("target", target),
			zap.Uint64("newTxnSafePoint", newTxnSafePoint), zap.String("blocker", blockerDesc),
			zap.Bool("isCompatibleMode", isCompatibleMode))
	} else {
		log.Info("txn safe point advanced",
			zap.Uint64("oldTxnSafePoint", oldTxnSafePoint), zap.Uint64("newTxnSafePoint", newTxnSafePoint),
			zap.Bool("isCompatibleMode", isCompatibleMode))
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

	return m.setGCBarrierImpl(keyspaceID, barrierID, barrierTS, ttl, now)
}

func (m *GCStateManager) setGCBarrierImpl(keyspaceID uint32, barrierID string, barrierTS uint64, ttl time.Duration, now time.Time) (*endpoint.GCBarrier, error) {
	// The barrier ID (or service ID or the service safe points) is reserved for keeping backward compatibility.
	if keyspaceID == constant.NullKeyspaceID && barrierID == keypath.GCWorkerServiceSafePointID {
		return nil, errs.ErrReservedGCBarrierID.GenWithStackByArgs(barrierID)
	}

	var expirationTime *time.Time = nil
	if ttl < time.Duration(math.MaxInt64) {
		t := now.Add(ttl)
		expirationTime = &t
	}
	newBarrier := endpoint.NewGCBarrier(barrierID, barrierTS, expirationTime)

	err := m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		txnSafePoint, err1 := m.gcMetaStorage.LoadTxnSafePoint(keyspaceID)
		if err1 != nil {
			return err1
		}
		if barrierTS < txnSafePoint {
			return errs.ErrGCBarrierTSBehindTxnSafePoint.GenWithStackByArgs(barrierTS, txnSafePoint)
		}
		err1 = wb.SetGCBarrier(keyspaceID, newBarrier)
		return err1
	})
	if err != nil {
		return nil, err
	}

	return newBarrier, nil
}

func (m *GCStateManager) DeleteGCBarrier(keyspaceID uint32, barrierID string) (*endpoint.GCBarrier, error) {
	keyspaceID, err := m.redirectKeyspace(keyspaceID, true)
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	return m.deleteGCBarrierImpl(keyspaceID, barrierID)
}

func (m *GCStateManager) deleteGCBarrierImpl(keyspaceID uint32, barrierID string) (*endpoint.GCBarrier, error) {
	var deletedBarrier *endpoint.GCBarrier
	err := m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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
	err = m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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
	err = m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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

		err = m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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

func saturatingDuration(ratio int64, base time.Duration) time.Duration {
	if ratio < 0 && base < 0 {
		ratio, base = -ratio, -base
	}
	if ratio < 0 || base < 0 {
		return 0
	}
	h, l := bits.Mul64(uint64(ratio), uint64(base))
	if h != 0 || l > uint64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(l)
}

// CompatibleUpdateServiceGCSafePoint updates the service safe point of the given serviceID. Service safe points are
// being deprecated, and this method provides compatibility for components that are still using service safe point API.
// This method simulates the behavior of the service safe points in old versions, by internally using txn safe points
// and GC barriers. The behaviors are mapped as follows:
//
//   - The service safe point with service ID "gc_worker" is mapped to the txn safe point.
//   - The service safe point with other service IDs are mapped to GC barriers with barrier IDs equal to the given
//     service IDs.
//
// Note that the behavior of the service safe point of "gc_worker" is not perfectly the same as before: it can no longer
// be advanced over other service safe points, but will be blocked by the minimal one; and if the cluster was running
// with TiDB node that haven't migrated to the new GC APIs, it can also be blocked by the *TiDB min start ts* written by
// those TiDB nodes.
//
// Therefore, the method's behavior is as follows:
//   - If the given serviceID is "gc_worker", it internally calls AdvanceTxnSafePoint.
//   - If the advancing result is the same as newServiceSafePoint, it's the case that the updated service safe
//     point of "gc_worker" is exactly the minimal one. Returns a simulated service safe point with the service ID
//     equals to "gc_worker".
//   - Otherwise, it's the case that the service safe point of "gc_worker" is successfully updated, but it's not
//     the minimal service safe point. Returns a simulated service safe point whose serviceID starts with
//     "__pseudo_service:" to simulate the minimal service safe point. It may actually be either a GC barrier or
//     a *TiDB min start ts*.
//   - If the given serviceID is anything else, it internally calls SetGCBarrier or DeleteGCBarrier, depending on
//     whether the `ttl` is positive or not. As the txn safe point is always less or equal to any GC barriers, we
//     simulate the case that the service safe point of "gc_worker" is the minimal one, and return a service safe point
//     with the service ID equals to "gc_worker".
//
// This function only works on the NullKeyspace.
func (m *GCStateManager) CompatibleUpdateServiceGCSafePoint(serviceID string, newServiceSafePoint uint64, ttl int64, now time.Time) (minServiceSafePoint *endpoint.ServiceSafePoint, updated bool, err error) {
	keyspaceID := constant.NullKeyspaceID
	m.lock.Lock()
	defer m.lock.Unlock()

	// TODO: After implementing the global GC barrier, redirect the invocation on "native_br" to `SetGlobalGCBarrier`.
	if serviceID == keypath.GCWorkerServiceSafePointID {
		if ttl != math.MaxInt64 {
			return nil, false, errors.New("TTL of gc_worker's service safe point must be infinity")
		}

		res, err := m.advanceTxnSafePointImpl(keyspaceID, newServiceSafePoint)
		if err != nil {
			return nil, false, err
		}
		if res.NewTxnSafePoint != newServiceSafePoint {
			minServiceSafePoint = &endpoint.ServiceSafePoint{
				ServiceID: "__pseudo_service:" + res.BlockerDescription,
				ExpiredAt: math.MaxInt64,
				SafePoint: res.NewTxnSafePoint,
			}
		} else {
			minServiceSafePoint = &endpoint.ServiceSafePoint{
				ServiceID: keypath.GCWorkerServiceSafePointID,
				ExpiredAt: math.MaxInt64,
				SafePoint: newServiceSafePoint,
			}
		}
		updated = res.OldTxnSafePoint != res.NewTxnSafePoint
	} else {
		if ttl > 0 {
			_, err = m.setGCBarrierImpl(keyspaceID, serviceID, newServiceSafePoint, saturatingDuration(ttl, time.Second), now)
		} else {
			_, err = m.deleteGCBarrierImpl(keyspaceID, serviceID)
		}

		if err != nil && !errors.Is(err, errs.ErrGCBarrierTSBehindTxnSafePoint) {
			return nil, false, err
		}
		// The atomicity between setting/deleting GC barrier and loading the txn safe point is not guaranteed here.
		// It doesn't matter much whether it's atomic, but it's important to ensure LoadTxnSafePoint happens *AFTER*
		// setting/deleting GC barrier.
		txnSafePoint, err := m.gcMetaStorage.LoadTxnSafePoint(keyspaceID)
		if err != nil {
			return nil, false, err
		}
		minServiceSafePoint = &endpoint.ServiceSafePoint{
			ServiceID: keypath.GCWorkerServiceSafePointID,
			ExpiredAt: math.MaxInt64,
			SafePoint: txnSafePoint,
		}
		updated = ttl > 0 && txnSafePoint <= newServiceSafePoint
	}
	return minServiceSafePoint, updated, nil
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
