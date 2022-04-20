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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"go.etcd.io/etcd/clientv3"
)

// ServiceSafePoint is the safepoint for a specific service
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ServiceSafePoint struct {
	ServiceID string `json:"service_id"`
	ExpiredAt int64  `json:"expired_at"`
	SafePoint uint64 `json:"safe_point"`
}

// GCSafePoint is gcWorker's safepoint for specific service group
type GCSafePoint struct {
	ServiceGroupID string `json:"service_group_id"`
	SafePoint      uint64 `json:"safe_point"`
}

// GCSafePointStorage defines the storage operations on the GC safe point.
type GCSafePointStorage interface {
	LoadGCSafePoint() (uint64, error)
	SaveGCSafePoint(safePoint uint64) error
	LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error)
	LoadAllServiceGCSafePoints() ([]*ServiceSafePoint, error)
	SaveServiceGCSafePoint(ssp *ServiceSafePoint) error
	RemoveServiceGCSafePoint(serviceID string) error

	LoadGCWorkerSafePoint(serviceGroupID string) (*GCSafePoint, error)
	SaveGCWorkerSafePoint(serviceGroupID string, gcSafePoint *GCSafePoint) error
	LoadAllServiceGroupGCSafePoints() ([]*pdpb.ServiceGroupSafepoint, error)
	RemoveServiceSafePointByServiceGroup(serviceGroupID, serviceID string) error
	LoadMinServiceSafePointByServiceGroup(serviceGroupID string, now time.Time) (*ServiceSafePoint, error)
	SaveServiceSafePointByServiceGroup(serviceGroupID string, ssp *ServiceSafePoint) error
}

var _ GCSafePointStorage = (*StorageEndpoint)(nil)

// LoadGCSafePoint loads current GC safe point from storage.
func (se *StorageEndpoint) LoadGCSafePoint() (uint64, error) {
	value, err := se.Load(gcSafePointPath())
	if err != nil || value == "" {
		return 0, err
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return safePoint, nil
}

// SaveGCSafePoint saves new GC safe point to storage.
func (se *StorageEndpoint) SaveGCSafePoint(safePoint uint64) error {
	value := strconv.FormatUint(safePoint, 16)
	return se.Save(gcSafePointPath(), value)
}

// LoadMinServiceGCSafePoint returns the minimum safepoint across all services
func (se *StorageEndpoint) LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error) {
	prefix := GCSafePointServicePrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// There's no service safepoint. It may be a new cluster, or upgraded from an older version where all service
		// safepoints are missing. For the second case, we have no way to recover it. Store an initial value 0 for
		// gc_worker.
		return se.initServiceGCSafePointForGCWorker(0)
	}

	hasGCWorker := false
	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		if ssp.ServiceID == gcWorkerServiceSafePointID {
			hasGCWorker = true
			// If gc_worker's expire time is incorrectly set, fix it.
			if ssp.ExpiredAt != math.MaxInt64 {
				ssp.ExpiredAt = math.MaxInt64
				err = se.SaveServiceGCSafePoint(ssp)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}

		if ssp.ExpiredAt < now.Unix() {
			se.Remove(key)
			continue
		}
		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}

	if min.SafePoint == math.MaxUint64 {
		// There's no valid safepoints and we have no way to recover it. Just set gc_worker to 0.
		log.Info("there are no valid service safepoints. init gc_worker's service safepoint to 0")
		return se.initServiceGCSafePointForGCWorker(0)
	}

	if !hasGCWorker {
		// If there exists some service safepoints but gc_worker is missing, init it with the min value among all
		// safepoints (including expired ones)
		return se.initServiceGCSafePointForGCWorker(min.SafePoint)
	}

	return min, nil
}

func (se *StorageEndpoint) initServiceGCSafePointForGCWorker(initialValue uint64) (*ServiceSafePoint, error) {
	ssp := &ServiceSafePoint{
		ServiceID: gcWorkerServiceSafePointID,
		SafePoint: initialValue,
		ExpiredAt: math.MaxInt64,
	}
	if err := se.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadAllServiceGCSafePoints returns all services GC safepoints
func (se *StorageEndpoint) LoadAllServiceGCSafePoints() ([]*ServiceSafePoint, error) {
	prefix := GCSafePointServicePrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*ServiceSafePoint{}, nil
	}

	ssps := make([]*ServiceSafePoint, 0, len(keys))
	for i := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		ssps = append(ssps, ssp)
	}

	return ssps, nil
}

// SaveServiceGCSafePoint saves a GC safepoint for the service
func (se *StorageEndpoint) SaveServiceGCSafePoint(ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}

	if ssp.ServiceID == gcWorkerServiceSafePointID && ssp.ExpiredAt != math.MaxInt64 {
		return errors.New("TTL of gc_worker's service safe point must be infinity")
	}

	key := gcSafePointServicePath(ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}

	return se.Save(key, string(value))
}

// RemoveServiceGCSafePoint removes a GC safepoint for the service
func (se *StorageEndpoint) RemoveServiceGCSafePoint(serviceID string) error {
	if serviceID == gcWorkerServiceSafePointID {
		return errors.New("cannot remove service safe point of gc_worker")
	}
	key := gcSafePointServicePath(serviceID)
	return se.Remove(key)
}

// LoadGCWorkerSafePoint reads GCSafePoint for the given service group
func (se *StorageEndpoint) LoadGCWorkerSafePoint(serviceGroupID string) (*GCSafePoint, error) {
	value, err := se.Load(gcWorkerSafePointPath(serviceGroupID))
	if err != nil || value == "" {
		return nil, err
	}
	gcSafePoint := &GCSafePoint{}
	if err := json.Unmarshal([]byte(value), gcSafePoint); err != nil {
		return nil, err
	}

	return gcSafePoint, nil
}

// SaveGCWorkerSafePoint saves GCSafePoint under given service group
func (se *StorageEndpoint) SaveGCWorkerSafePoint(serviceGroupID string, gcSafePoint *GCSafePoint) error {
	safePoint, err := json.Marshal(gcSafePoint)
	if err != nil {
		return err
	}
	return se.Save(gcWorkerSafePointPath(serviceGroupID), string(safePoint))
}

// LoadAllServiceGroupGCSafePoints returns a slice contains GCSafePoint for every service group
func (se *StorageEndpoint) LoadAllServiceGroupGCSafePoints() ([]*pdpb.ServiceGroupSafepoint, error) {
	prefix := safePointPrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*pdpb.ServiceGroupSafepoint{}, nil
	}
	gcSafePoints := make([]*pdpb.ServiceGroupSafepoint, 0, 2) // there are probably only two service groups
	for i := range keys {
		// skip service safe point
		if !strings.HasSuffix(keys[i], gcWorkerSafePointSuffix()) {
			continue
		}

		gcSafePoint := &GCSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), gcSafePoint); err != nil {
			return nil, err
		}
		serviceGroupSafePoint := &pdpb.ServiceGroupSafepoint{
			ServiceGroupId: []byte(gcSafePoint.ServiceGroupID),
			SafePoint:      gcSafePoint.SafePoint,
		}
		gcSafePoints = append(gcSafePoints, serviceGroupSafePoint)
	}

	return gcSafePoints, nil
}

// RemoveServiceSafePointByServiceGroup removes a service safe point
func (se *StorageEndpoint) RemoveServiceSafePointByServiceGroup(serviceGroupID, serviceID string) error {
	key := serviceSafePointPath(serviceGroupID, serviceID)
	return se.Remove(key)
}

// SaveServiceSafePointByServiceGroup saves service safe point under given service group
func (se *StorageEndpoint) SaveServiceSafePointByServiceGroup(serviceGroupID string, ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}
	key := serviceSafePointPath(serviceGroupID, ssp.ServiceID)
	value, err := json.Marshal(ssp)
	if err != nil {
		return err
	}

	return se.Save(key, string(value))
}

// LoadMinServiceSafePointByServiceGroup returns the minimum safepoint for the given service group
// note that gc worker safe point are store separately
func (se *StorageEndpoint) LoadMinServiceSafePointByServiceGroup(serviceGroupID string, now time.Time) (*ServiceSafePoint, error) {
	prefix := serviceSafePointPrefixPath(serviceGroupID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		// the given service group does not have a service safe point yet
		return nil, nil
	}

	min := &ServiceSafePoint{SafePoint: math.MaxInt64}
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}

		// remove expired safe points
		if ssp.ExpiredAt < now.Unix() {
			se.Remove(key)
			continue
		}

		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}

	if min.SafePoint == math.MaxUint64 {
		// fail to find a valid service safe point under current service group
		// this can be normal behavior if the only safe point just expired
		return nil, nil
	}

	// successfully found a valid min safe point
	return min, nil
}
