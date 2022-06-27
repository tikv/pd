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

package endpoint

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// KeyspaceGCSafePoint is gcWorker's safepoint for specific key-space
type KeyspaceGCSafePoint struct {
	SpaceID   uint32 `json:"space_id"`
	SafePoint uint64 `json:"safe_point,omitempty"`
}

// KeyspaceGCSafePointStorage defines the storage operations on Keyspaces' safe points
type KeyspaceGCSafePointStorage interface {
	SaveServiceSafePoint(spaceID uint32, ssp *ServiceSafePoint) error
	LoadServiceSafePoint(spaceID uint32, serviceID string, now time.Time) (*ServiceSafePoint, error)
	LoadMinServiceSafePoint(spaceID uint32, now time.Time) (*ServiceSafePoint, error)
	RemoveServiceSafePoint(spaceID uint32, serviceID string) error

	SaveKeyspaceGCSafePoint(spaceID uint32, safePoint uint64) error
	LoadKeyspaceGCSafePoint(spaceID uint32) (uint64, error)
	LoadAllKeyspaceGCSafePoints() ([]*KeyspaceGCSafePoint, error)
}

var _ KeyspaceGCSafePointStorage = (*StorageEndpoint)(nil)

// SaveServiceSafePoint saves service safe point under given key-space.
func (se *StorageEndpoint) SaveServiceSafePoint(spaceID uint32, ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}

	if ssp.ServiceID == gcWorkerServiceSafePointID && ssp.ExpiredAt != math.MaxInt64 {
		return errors.New("TTL of gc_worker's service safe point must be infinity")
	}

	key := KeyspaceServiceSafePointPath(spaceID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}
	return se.Save(key, string(value))
}

// LoadServiceSafePoint reads ServiceSafePoint for the given keyspace ID and service name.
// Return nil if no safepoint exist for given service or just expired.
func (se *StorageEndpoint) LoadServiceSafePoint(spaceID uint32, serviceID string, now time.Time) (*ServiceSafePoint, error) {
	key := KeyspaceServiceSafePointPath(spaceID, serviceID)
	value, err := se.Load(key)
	if err != nil || value == "" {
		return nil, err
	}
	ssp := &ServiceSafePoint{}
	if err := json.Unmarshal([]byte(value), ssp); err != nil {
		return nil, err
	}
	if ssp.ExpiredAt < now.Unix() {
		go func() {
			if err = se.Remove(key); err != nil {
				log.Error("remove expired key meet error", zap.String("key", key), errs.ZapError(err))
			}
		}()
		return nil, nil
	}
	return ssp, nil
}

// LoadMinServiceSafePoint returns the minimum safepoint for the given keyspace.
// Note that this will also init gc-worker's safe point if it's missing.
func (se *StorageEndpoint) LoadMinServiceSafePoint(spaceID uint32, now time.Time) (*ServiceSafePoint, error) {
	prefix := KeyspaceServiceSafePointPrefix(spaceID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		// Create GC worker's service safe point for new keyspace
		return se.initGCWorkerServiceSafePoint(spaceID, 0)
	}

	hasGCWorker := false
	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	expiredKeys := make([]string, 0)
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err = json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		if ssp.ServiceID == gcWorkerServiceSafePointID {
			hasGCWorker = true
			// If gc_worker's expire time is incorrectly set, fix it.
			if ssp.ExpiredAt != math.MaxInt64 {
				ssp.ExpiredAt = math.MaxInt64
				err = se.SaveServiceSafePoint(spaceID, ssp)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
		// gather expired keys
		if ssp.ExpiredAt < now.Unix() {
			expiredKeys = append(expiredKeys, key)
			continue
		}

		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}

	// remove expired keys asynchronously
	go func() {
		for _, key := range expiredKeys {
			if err = se.Remove(key); err != nil {
				log.Error("remove expired key meet error", zap.String("key", key), errs.ZapError(err))
			}
		}
	}()
	if min.SafePoint == math.MaxUint64 {
		// There's no valid safe points. Just set gc_worker to 0.
		log.Info("there are no valid service safe points. init gc_worker's service safepoint to 0")
		return se.initGCWorkerServiceSafePoint(spaceID, 0)
	}

	if !hasGCWorker {
		// If there exists some service safe points but gc_worker is missing, init it with the min value among all
		// safe points
		return se.initGCWorkerServiceSafePoint(spaceID, min.SafePoint)
	}

	// successfully found a valid min safe point.
	return min, nil
}

func (se *StorageEndpoint) initGCWorkerServiceSafePoint(spaceID uint32, initialValue uint64) (*ServiceSafePoint, error) {
	ssp := &ServiceSafePoint{
		ServiceID: gcWorkerServiceSafePointID,
		SafePoint: initialValue,
		ExpiredAt: math.MaxInt64,
	}
	if err := se.SaveServiceSafePoint(spaceID, ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// RemoveServiceSafePoint removes target ServiceSafePoint
func (se *StorageEndpoint) RemoveServiceSafePoint(spaceID uint32, serviceID string) error {
	key := KeyspaceServiceSafePointPath(spaceID, serviceID)
	return se.Remove(key)
}

// SaveKeyspaceGCSafePoint saves GCSafePoint to the given keyspace.
func (se *StorageEndpoint) SaveKeyspaceGCSafePoint(spaceID uint32, safePoint uint64) error {
	value := strconv.FormatUint(safePoint, 16)
	return se.Save(KeyspaceGCSafePointPath(spaceID), value)
}

// LoadKeyspaceGCSafePoint reads GCSafePoint for the given keyspace.
// Returns 0 if target safe point does not exist.
func (se *StorageEndpoint) LoadKeyspaceGCSafePoint(spaceID uint32) (uint64, error) {
	value, err := se.Load(KeyspaceGCSafePointPath(spaceID))
	if err != nil || value == "" {
		return 0, err
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, err
	}
	return safePoint, nil
}

// LoadAllKeyspaceGCSafePoints returns slice of KeySpaceGCSafePoint.
func (se *StorageEndpoint) LoadAllKeyspaceGCSafePoints() ([]*KeyspaceGCSafePoint, error) {
	prefix := KeyspaceSafePointPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	suffix := KeySpaceGCSafePointSuffix()
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	safePoints := make([]*KeyspaceGCSafePoint, 0, len(values))
	for i := range keys {
		// skip non gc safe points
		if !strings.HasSuffix(keys[i], suffix) {
			continue
		}
		safePoint := &KeyspaceGCSafePoint{}
		spaceIDStr := strings.TrimPrefix(keys[i], prefix)
		spaceIDStr = strings.TrimSuffix(spaceIDStr, suffix)
		spaceID, err := strconv.ParseUint(spaceIDStr, 10, 32)
		if err != nil {
			return nil, err
		}
		safePoint.SpaceID = uint32(spaceID)
		value, err := strconv.ParseUint(values[i], 16, 64)
		if err != nil {
			return nil, err
		}
		safePoint.SafePoint = value
		safePoints = append(safePoints, safePoint)
	}
	return safePoints, nil
}
