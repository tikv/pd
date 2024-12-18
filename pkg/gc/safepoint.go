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

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
)

var blockGCSafePointErrmsg = "don't allow update gc safe point v1."
var blockServiceSafepointErrmsg = "don't allow update service safe point v1."

// SafePointManager is the manager for safePoint of GC and services.
type SafePointManager struct {
	gcLock        syncutil.Mutex
	serviceGCLock syncutil.Mutex
	store         endpoint.GCSafePointStorage
	cfg           config.PDServerConfig
}

// NewSafePointManager creates a SafePointManager of GC and services.
func NewSafePointManager(store endpoint.GCSafePointStorage, cfg config.PDServerConfig) *SafePointManager {
	return &SafePointManager{store: store, cfg: cfg}
}

// LoadGCSafePoint loads current GC safe point from storage.
func (manager *SafePointManager) LoadGCSafePoint() (uint64, error) {
	return manager.store.LoadGCSafePoint()
}

// UpdateGCSafePoint updates the safepoint if it is greater than the previous one
// it returns the old safepoint in the storage.
func (manager *SafePointManager) UpdateGCSafePoint(newSafePoint uint64) (oldSafePoint uint64, err error) {
	manager.gcLock.Lock()
	defer manager.gcLock.Unlock()
	// TODO: cache the safepoint in the storage.
	oldSafePoint, err = manager.store.LoadGCSafePoint()
	if err != nil {
		return
	}
	if manager.cfg.BlockSafePointV1 {
		err = errors.New(blockGCSafePointErrmsg)
		return
	}

	if oldSafePoint >= newSafePoint {
		return
	}
	err = manager.store.SaveGCSafePoint(newSafePoint)
	if err == nil {
		gcSafePointGauge.WithLabelValues("gc_safepoint").Set(float64(newSafePoint))
	}
	return
}

// UpdateServiceGCSafePoint update the safepoint for a specific service.
func (manager *SafePointManager) UpdateServiceGCSafePoint(serviceID string, newSafePoint uint64, ttl int64, now time.Time) (minServiceSafePoint *endpoint.ServiceSafePoint, updated bool, err error) {
	if manager.cfg.BlockSafePointV1 {
		return nil, false, errors.New(blockServiceSafepointErrmsg)
	}
	manager.serviceGCLock.Lock()
	defer manager.serviceGCLock.Unlock()
	minServiceSafePoint, err = manager.store.LoadMinServiceGCSafePoint(now)
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
	if err := manager.store.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, false, err
	}

	// If the min safePoint is updated, load the next one.
	if serviceID == minServiceSafePoint.ServiceID {
		minServiceSafePoint, err = manager.store.LoadMinServiceGCSafePoint(now)
	}
	return minServiceSafePoint, true, err
}
