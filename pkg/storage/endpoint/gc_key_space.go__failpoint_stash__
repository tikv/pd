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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// KeyspaceGCSafePoint is gcWorker's safepoint for specific key-space
type KeyspaceGCSafePoint struct {
	SpaceID   string `json:"space_id"`
	SafePoint uint64 `json:"safe_point,omitempty"`
}

// KeyspaceGCSafePointStorage defines the storage operations on Keyspaces' safe points
type KeyspaceGCSafePointStorage interface {
	// Service safe point interfaces.
	SaveServiceSafePoint(spaceID string, ssp *ServiceSafePoint) error
	LoadServiceSafePoint(spaceID, serviceID string) (*ServiceSafePoint, error)
	LoadMinServiceSafePoint(spaceID string, now time.Time) (*ServiceSafePoint, error)
	RemoveServiceSafePoint(spaceID, serviceID string) error
	// GC safe point interfaces.
	SaveKeyspaceGCSafePoint(spaceID string, safePoint uint64) error
	LoadKeyspaceGCSafePoint(spaceID string) (uint64, error)
	LoadAllKeyspaceGCSafePoints(withGCSafePoint bool) ([]*KeyspaceGCSafePoint, error)
}

var _ KeyspaceGCSafePointStorage = (*StorageEndpoint)(nil)

// SaveServiceSafePoint saves service safe point under given key-space.
func (se *StorageEndpoint) SaveServiceSafePoint(spaceID string, ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}
	key := KeyspaceServiceSafePointPath(spaceID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}
	return se.Save(key, string(value))
}

// LoadServiceSafePoint reads ServiceSafePoint for the given key-space ID and service name.
// Return nil if no safepoint exist for given service or just expired.
func (se *StorageEndpoint) LoadServiceSafePoint(spaceID, serviceID string) (*ServiceSafePoint, error) {
	key := KeyspaceServiceSafePointPath(spaceID, serviceID)
	value, err := se.Load(key)
	if err != nil || value == "" {
		return nil, err
	}
	ssp := &ServiceSafePoint{}
	if err := json.Unmarshal([]byte(value), ssp); err != nil {
		return nil, err
	}
	if ssp.ExpiredAt < time.Now().Unix() {
		go func() {
			defer logutil.LogPanic()

			if err = se.Remove(key); err != nil {
				log.Error("remove expired key meet error", zap.String("key", key), errs.ZapError(err))
			}
		}()
		return nil, nil
	}
	return ssp, nil
}

// LoadMinServiceSafePoint returns the minimum safepoint for the given key-space.
// Note that gc worker safe point are store separately.
// If no service safe point exist for the given key-space or all the service safe points just expired, return nil.
func (se *StorageEndpoint) LoadMinServiceSafePoint(spaceID string, now time.Time) (*ServiceSafePoint, error) {
	prefix := KeyspaceServiceSafePointPrefix(spaceID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	expiredKeys := make([]string, 0)
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err = json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
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
	// failpoint for immediate removal
	failpoint.Inject("removeExpiredKeys", func() {
		for _, key := range expiredKeys {
			if err = se.Remove(key); err != nil {
				log.Error("remove expired key meet error", zap.String("key", key), errs.ZapError(err))
			}
		}
		expiredKeys = []string{}
	})
	// remove expired keys asynchronously
	go func() {
		defer logutil.LogPanic()

		for _, key := range expiredKeys {
			if err = se.Remove(key); err != nil {
				log.Error("remove expired key meet error", zap.String("key", key), errs.ZapError(err))
			}
		}
	}()
	if min.SafePoint == math.MaxUint64 {
		// no service safe point or all of them are expired.
		return nil, nil
	}

	// successfully found a valid min safe point.
	return min, nil
}

// RemoveServiceSafePoint removes target ServiceSafePoint
func (se *StorageEndpoint) RemoveServiceSafePoint(spaceID, serviceID string) error {
	key := KeyspaceServiceSafePointPath(spaceID, serviceID)
	return se.Remove(key)
}

// SaveKeyspaceGCSafePoint saves GCSafePoint to the given key-space.
func (se *StorageEndpoint) SaveKeyspaceGCSafePoint(spaceID string, safePoint uint64) error {
	value := strconv.FormatUint(safePoint, 16)
	return se.Save(KeyspaceGCSafePointPath(spaceID), value)
}

// LoadKeyspaceGCSafePoint reads GCSafePoint for the given key-space.
// Returns 0 if target safepoint not exist.
func (se *StorageEndpoint) LoadKeyspaceGCSafePoint(spaceID string) (uint64, error) {
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

// LoadAllKeyspaceGCSafePoints returns slice of KeyspaceGCSafePoint.
// If withGCSafePoint set to false, returned safePoints will be 0.
func (se *StorageEndpoint) LoadAllKeyspaceGCSafePoints(withGCSafePoint bool) ([]*KeyspaceGCSafePoint, error) {
	prefix := KeyspaceSafePointPrefix()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	suffix := KeyspaceGCSafePointSuffix()
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
		spaceID := strings.TrimPrefix(keys[i], prefix)
		spaceID = strings.TrimSuffix(spaceID, suffix)
		safePoint.SpaceID = spaceID
		if withGCSafePoint {
			value, err := strconv.ParseUint(values[i], 16, 64)
			if err != nil {
				return nil, err
			}
			safePoint.SafePoint = value
		}
		safePoints = append(safePoints, safePoint)
	}
	return safePoints, nil
}
