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

	"github.com/pingcap/errors"
	"github.com/tikv/pd/server/storage/kv"
	"go.etcd.io/etcd/clientv3"
)

const (
	spaceIDBase   = 16
	safePointBase = 16
)

// KeySpaceGCSafePoint is gcWorker's safepoint for specific key-space
type KeySpaceGCSafePoint struct {
	SpaceID   uint32 `json:"space_id"`
	SafePoint uint64 `json:"safe_point,omitempty"`
}

// KeySpaceGCSafePointStorage defines the storage operations on KeySpaces' safe points
type KeySpaceGCSafePointStorage interface {
	// Service safe point interfaces.
	SaveServiceSafePoint(spaceID uint32, ssp *ServiceSafePoint, ttl int64) error
	LoadServiceSafePoint(spaceID uint32, serviceID string) (*ServiceSafePoint, error)
	LoadMinServiceSafePoint(spaceID uint32) (*ServiceSafePoint, error)
	RemoveServiceSafePoint(spaceID uint32, serviceID string) error
	// GC safe point interfaces.
	SaveKeySpaceGCSafePoint(spaceID uint32, safePoint uint64) error
	LoadKeySpaceGCSafePoint(spaceID uint32) (uint64, error)
	LoadAllKeySpaceGCSafePoints(withGCSafePoint bool) ([]*KeySpaceGCSafePoint, error)
	// Revision interfaces.
	TouchKeySpaceRevision(spaceID uint32) error
	LoadKeySpaceRevision(spaceID uint32) (int64, error)
}

var _ KeySpaceGCSafePointStorage = (*StorageEndpoint)(nil)

// SaveServiceSafePoint saves service safe point under given key-space.
func (se *StorageEndpoint) SaveServiceSafePoint(spaceID uint32, ssp *ServiceSafePoint, ttl int64) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}
	etcdEndpoint, err := se.getEtcdBase()
	if err != nil {
		return err
	}
	key := KeySpaceServiceSafePointPath(spaceID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}
	// A MaxInt64 ttl means safe point never expire.
	if ttl == math.MaxInt64 {
		return etcdEndpoint.Save(key, string(value))
	}
	return etcdEndpoint.SaveWithTTL(key, string(value), ttl)
}

// LoadServiceSafePoint reads ServiceSafePoint for the given key-space ID and service name.
// Return nil if no safepoint exist for given service.
func (se *StorageEndpoint) LoadServiceSafePoint(spaceID uint32, serviceID string) (*ServiceSafePoint, error) {
	key := KeySpaceServiceSafePointPath(spaceID, serviceID)
	value, err := se.Load(key)
	if err != nil || value == "" {
		return nil, err
	}
	ssp := &ServiceSafePoint{}
	if err := json.Unmarshal([]byte(value), ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadMinServiceSafePoint returns the minimum safepoint for the given key-space.
// Note that gc worker safe point are store separately.
// If no service safe point exist for the given key-space or all the service safe points just expired, return nil.
func (se *StorageEndpoint) LoadMinServiceSafePoint(spaceID uint32) (*ServiceSafePoint, error) {
	prefix := KeySpaceServiceSafePointPrefix(spaceID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	_, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	for i := range values {
		ssp := &ServiceSafePoint{}
		if err = json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}
	if min.SafePoint == math.MaxUint64 {
		// no service safe point or all of them are expired.
		return nil, nil
	}

	// successfully found a valid min safe point.
	return min, nil
}

// RemoveServiceSafePoint removes target ServiceSafePoint
func (se *StorageEndpoint) RemoveServiceSafePoint(spaceID uint32, serviceID string) error {
	key := KeySpaceServiceSafePointPath(spaceID, serviceID)
	return se.Remove(key)
}

// SaveKeySpaceGCSafePoint saves GCSafePoint to the given key-space.
func (se *StorageEndpoint) SaveKeySpaceGCSafePoint(spaceID uint32, safePoint uint64) error {
	value := strconv.FormatUint(safePoint, safePointBase)
	return se.Save(KeySpaceGCSafePointPath(spaceID), value)
}

// LoadKeySpaceGCSafePoint reads GCSafePoint for the given key-space.
// Returns 0 if target safepoint not exist.
func (se *StorageEndpoint) LoadKeySpaceGCSafePoint(spaceID uint32) (uint64, error) {
	value, err := se.Load(KeySpaceGCSafePointPath(spaceID))
	if err != nil || value == "" {
		return 0, err
	}
	safePoint, err := strconv.ParseUint(value, safePointBase, 64)
	if err != nil {
		return 0, err
	}
	return safePoint, nil
}

// LoadAllKeySpaceGCSafePoints returns slice of KeySpaceGCSafePoint.
// If withGCSafePoint set to false, returned safePoints will be 0.
func (se *StorageEndpoint) LoadAllKeySpaceGCSafePoints(withGCSafePoint bool) ([]*KeySpaceGCSafePoint, error) {
	prefix := KeySpaceSafePointPrefix()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	suffix := KeySpaceGCSafePointSuffix()
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	safePoints := make([]*KeySpaceGCSafePoint, 0, len(values))
	for i := range keys {
		// skip non gc safe points
		if !strings.HasSuffix(keys[i], suffix) {
			continue
		}
		safePoint := &KeySpaceGCSafePoint{}
		spaceIDStr := strings.TrimPrefix(keys[i], prefix)
		spaceIDStr = strings.TrimSuffix(spaceIDStr, suffix)
		spaceID, err := strconv.ParseUint(spaceIDStr, spaceIDBase, 32)
		if err != nil {
			return nil, err
		}
		safePoint.SpaceID = uint32(spaceID)
		if withGCSafePoint {
			value, err := strconv.ParseUint(values[i], safePointBase, 64)
			if err != nil {
				return nil, err
			}
			safePoint.SafePoint = value
		}
		safePoints = append(safePoints, safePoint)
	}
	return safePoints, nil
}

// TouchKeySpaceRevision advances revision of the given key space.
// It's used when new service safe point is saved.
func (se *StorageEndpoint) TouchKeySpaceRevision(spaceID uint32) error {
	path := KeySpacePath(spaceID)
	return se.Save(path, "")
}

// LoadKeySpaceRevision loads the revision of the given key space.
func (se *StorageEndpoint) LoadKeySpaceRevision(spaceID uint32) (int64, error) {
	etcdEndpoint, err := se.getEtcdBase()
	if err != nil {
		return 0, err
	}
	keySpacePath := KeySpacePath(spaceID)
	_, revision, err := etcdEndpoint.LoadRevision(keySpacePath)
	return revision, err
}

// getEtcdBase retrieves etcd base from storage endpoint.
// It's used by operations that needs etcd endpoint specifically.
func (se *StorageEndpoint) getEtcdBase() (*kv.EtcdKVBase, error) {
	etcdBase, ok := interface{}(se.Base).(*kv.EtcdKVBase)
	if !ok {
		return nil, errors.New("safepoint storage only supports etcd backend")
	}
	return etcdBase, nil
}
