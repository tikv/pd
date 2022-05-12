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

// LoadAllKeySpaces returns a list of all key-space IDs.
// We have only predefined key-spaces by now.
// More key-spaces would come from "Multi-tenant".
func (se *StorageEndpoint) LoadAllKeySpaces() ([]*KeySpaceGCSafePoint, error) {
	keySpaces := []*KeySpaceGCSafePoint{
		{
			SpaceID: KeySpaceRawKVDefault,
		},
	}
	return keySpaces, nil
}

// SaveServiceSafePoint saves service safe point under given key-space.
func (se *StorageEndpoint) SaveServiceSafePoint(spaceID string, ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}
	key := ServiceSafePointPath(spaceID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}
	return se.Save(key, string(value))
}

// LoadServiceSafePoint reads ServiceSafePoint for the given key-space ID and service name.
// Return nil if no safepoint not exist.
func (se *StorageEndpoint) LoadServiceSafePoint(spaceID, serviceID string) (*ServiceSafePoint, error) {
	value, err := se.Load(ServiceSafePointPath(spaceID, serviceID))
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
func (se *StorageEndpoint) LoadMinServiceSafePoint(spaceID string, now time.Time) (*ServiceSafePoint, error) {
	prefix := ServiceSafePointPrefix(spaceID)
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

// RemoveServiceSafePoint removes GCSafePoint for the given key-space.
func (se *StorageEndpoint) RemoveServiceSafePoint(spaceID, serviceID string) error {
	key := ServiceSafePointPath(spaceID, serviceID)
	return se.Remove(key)
}

// SaveKeySpaceGCSafePoint saves GCSafePoint to the given key-space.
func (se *StorageEndpoint) SaveKeySpaceGCSafePoint(gcSafePoint *KeySpaceGCSafePoint) error {
	safePoint, err := json.Marshal(gcSafePoint)
	if err != nil {
		return err
	}
	return se.Save(KeySpaceGCSafePointPath(gcSafePoint.SpaceID), string(safePoint))
}

// LoadKeySpaceGCSafePoint reads GCSafePoint for the given key-space.
// return nil if safepoint not exist.
func (se *StorageEndpoint) LoadKeySpaceGCSafePoint(spaceID string) (*KeySpaceGCSafePoint, error) {
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
func (se *StorageEndpoint) LoadAllKeySpaceGCSafePoints() ([]*KeySpaceGCSafePoint, error) {
	prefix := SafePointPrefix()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	suffix := GCSafePointSuffix()
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
	return safePoints, nil
}
