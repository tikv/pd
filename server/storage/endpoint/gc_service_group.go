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
	"time"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

// Predefine service groups. More service groups would come from "Multi-tenant".
const (
	// ServiceGroupRawKVDefault is service group ID for RawKV.
	ServiceGroupRawKVDefault = "default_rawkv"
)

// LoadAllServiceGroups returns a list of all service group IDs.
// We have only predefine service groups by now.
// More service groups would come from "Multi-tenant".
func (se *StorageEndpoint) LoadAllServiceGroups() ([]string, error) {
	serviceGroupIDs := []string{
		ServiceGroupRawKVDefault,
	}

	return serviceGroupIDs, nil
}

// SaveServiceSafePointByServiceGroup saves service safe point under given service group.
func (se *StorageEndpoint) SaveServiceSafePointByServiceGroup(serviceGroupID string, ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}
	key := GCServiceSafePointPathByServiceGroup(serviceGroupID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}
	return se.Save(key, string(value))
}

// LoadServiceSafePointByServiceGroup reads ServiceSafePoint for the given service group and service name.
// Return nil if no safepoint not exist.
func (se *StorageEndpoint) LoadServiceSafePointByServiceGroup(serviceGroupID, serviceID string) (*ServiceSafePoint, error) {
	value, err := se.Load(GCServiceSafePointPathByServiceGroup(serviceGroupID, serviceID))
	if err != nil || value == "" {
		return nil, err
	}
	ssp := &ServiceSafePoint{}
	if err := json.Unmarshal([]byte(value), ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadMinServiceSafePointByServiceGroup returns the minimum safepoint for the given service group.
// Note that gc worker safe point are store separately.
// If no service safe point exist for the given service group or all the service safe points just expired, return nil.
func (se *StorageEndpoint) LoadMinServiceSafePointByServiceGroup(serviceGroupID string, now time.Time) (*ServiceSafePoint, error) {
	prefix := GCServiceSafePointPrefixPathByServiceGroup(serviceGroupID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}

	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}

		// remove expired safe points.
		if ssp.ExpiredAt < now.Unix() {
			se.Remove(key)
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

// RemoveServiceSafePointByServiceGroup removes a service safe point.
func (se *StorageEndpoint) RemoveServiceSafePointByServiceGroup(serviceGroupID, serviceID string) error {
	key := GCServiceSafePointPathByServiceGroup(serviceGroupID, serviceID)
	return se.Remove(key)
}

// SaveGCSafePointByServiceGroup saves GCSafePoint under given service group.
func (se *StorageEndpoint) SaveGCSafePointByServiceGroup(gcSafePoint *ServiceGroupGCSafePoint) error {
	safePoint, err := json.Marshal(gcSafePoint)
	if err != nil {
		return err
	}
	return se.Save(gcSafePointPathByServiceGroup(gcSafePoint.ServiceGroupID), string(safePoint))
}

// LoadGCSafePointByServiceGroup reads GCSafePoint for the given service group.
// return nil if no safepoint not exist.
func (se *StorageEndpoint) LoadGCSafePointByServiceGroup(serviceGroupID string) (*ServiceGroupGCSafePoint, error) {
	value, err := se.Load(gcSafePointPathByServiceGroup(serviceGroupID))
	if err != nil || value == "" {
		return nil, err
	}
	gcSafePoint := &ServiceGroupGCSafePoint{}
	if err := json.Unmarshal([]byte(value), gcSafePoint); err != nil {
		return nil, err
	}
	return gcSafePoint, nil
}

// LoadAllServiceGroupGCSafePoints returns two slices of ServiceGroupIDs and their corresponding safe points.
func (se *StorageEndpoint) LoadAllServiceGroupGCSafePoints() ([]*ServiceGroupGCSafePoint, error) {
	prefix := gcServiceGroupGCSafePointPrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	_, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	safePoints := make([]*ServiceGroupGCSafePoint, 0, len(values))
	for _, value := range values {
		gcSafePoint := &ServiceGroupGCSafePoint{}
		if err := json.Unmarshal([]byte(value), gcSafePoint); err != nil {
			return nil, err
		}
		safePoints = append(safePoints, gcSafePoint)
	}
	return safePoints, nil
}
