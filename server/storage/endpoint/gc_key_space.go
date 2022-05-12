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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

// Predefined key spaces. More key spaces would come from "Multi-tenant".
const (
	// KeySpaceRawKVDefault is key space ID for RawKV.
	KeySpaceRawKVDefault = "default_rawkv"
)

// KeySpaceGCSafePoint is gcWorker's safepoint for specific key-space
type KeySpaceGCSafePoint struct {
	SpaceID   string `json:"space_id"`
	SafePoint uint64 `json:"safe_point"`
}

// KeySpaceGCSafePointStorage defines the storage operations on KeySpaces' safe points
type KeySpaceGCSafePointStorage interface {
	// Service safe point interfaces.
	SaveServiceSafePointByKeySpace(spaceID string, ssp *ServiceSafePoint) error
	LoadServiceSafePointByKeySpace(spaceID, serviceID string) (*ServiceSafePoint, error)
	LoadMinServiceSafePointByKeySpace(spaceID string, now time.Time) (*ServiceSafePoint, error)
	RemoveServiceSafePointByKeySpace(spaceID, serviceID string) error
	// GC safe point interfaces.
	SaveGCSafePointByKeySpace(gcSafePoint *KeySpaceGCSafePoint) error
	LoadGCSafePointByKeySpace(spaceID string) (*KeySpaceGCSafePoint, error)
	LoadAllKeySpaceGCSafePoints() ([]*KeySpaceGCSafePoint, error)
}

var _ KeySpaceGCSafePointStorage = (*StorageEndpoint)(nil)

// SaveServiceSafePointByKeySpace saves service safe point under given key-space.
func (se *StorageEndpoint) SaveServiceSafePointByKeySpace(spaceID string, ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}
	key := KeySpaceServiceSafePointPath(spaceID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}
	return se.Save(key, string(value))
}

// LoadServiceSafePointByKeySpace reads ServiceSafePoint for the given key-space ID and service name.
// Return nil if no safepoint not exist.
func (se *StorageEndpoint) LoadServiceSafePointByKeySpace(spaceID, serviceID string) (*ServiceSafePoint, error) {
	value, err := se.Load(KeySpaceServiceSafePointPath(spaceID, serviceID))
	if err != nil || value == "" {
		return nil, err
	}
	ssp := &ServiceSafePoint{}
	if err := json.Unmarshal([]byte(value), ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadMinServiceSafePointByKeySpace returns the minimum safepoint for the given key-space.
// Note that gc worker safe point are store separately.
// If no service safe point exist for the given key-space or all the service safe points just expired, return nil.
func (se *StorageEndpoint) LoadMinServiceSafePointByKeySpace(spaceID string, now time.Time) (*ServiceSafePoint, error) {
	prefix := KeySpaceServiceSafePointPrefix(spaceID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}

	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err = json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}

		// remove expired safe points.
		if ssp.ExpiredAt < now.Unix() {
			err = se.Remove(key)
			if err != nil {
				return nil, err
			}
			continue
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

// RemoveServiceSafePointByKeySpace removes GCSafePoint for the given key-space.
func (se *StorageEndpoint) RemoveServiceSafePointByKeySpace(spaceID, serviceID string) error {
	key := KeySpaceServiceSafePointPath(spaceID, serviceID)
	return se.Remove(key)
}

// SaveGCSafePointByKeySpace saves GCSafePoint to the given key-space.
func (se *StorageEndpoint) SaveGCSafePointByKeySpace(gcSafePoint *KeySpaceGCSafePoint) error {
	safePoint, err := json.Marshal(gcSafePoint)
	if err != nil {
		return err
	}
	return se.Save(KeySpaceGCSafePointPath(gcSafePoint.SpaceID), string(safePoint))
}

// LoadGCSafePointByKeySpace reads GCSafePoint for the given key-space.
// return nil if safepoint not exist.
func (se *StorageEndpoint) LoadGCSafePointByKeySpace(spaceID string) (*KeySpaceGCSafePoint, error) {
	value, err := se.Load(KeySpaceGCSafePointPath(spaceID))
	if err != nil || value == "" {
		return nil, err
	}
	gcSafePoint := &KeySpaceGCSafePoint{}
	if err := json.Unmarshal([]byte(value), gcSafePoint); err != nil {
		return nil, err
	}
	return gcSafePoint, nil
}

// LoadAllKeySpaceGCSafePoints returns slice of key-spaces and their corresponding gc safe points.
// It also returns spaceID of any default key spaces that do not have a gc safepoint.
func (se *StorageEndpoint) LoadAllKeySpaceGCSafePoints() ([]*KeySpaceGCSafePoint, error) {
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
		if err = json.Unmarshal([]byte(values[i]), safePoint); err != nil {
			return nil, err
		}
		safePoints = append(safePoints, safePoint)
	}

	// make sure all default key spaces are included in result
	defaultKeySpaces := []string{KeySpaceRawKVDefault}
	for _, defaultKeySpace := range defaultKeySpaces {
		value, err := se.Load(KeySpaceGCSafePointPath(defaultKeySpace))
		if err != nil {
			return nil, err
		}
		if value == "" {
			safePoints = append(safePoints, &KeySpaceGCSafePoint{SpaceID: defaultKeySpace})
		}
	}
	return safePoints, nil
}
