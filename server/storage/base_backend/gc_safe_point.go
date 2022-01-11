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

package backend

import (
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.etcd.io/etcd/clientv3"
)

var _ endpoint.GCSafePointStorage = (*BaseBackend)(nil)

// LoadGCSafePoint loads current GC safe point from storage.
func (bb *BaseBackend) LoadGCSafePoint() (uint64, error) {
	value, err := bb.Load(gcSafePointPath())
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, err
	}
	return safePoint, nil
}

// SaveGCSafePoint saves new GC safe point to storage.
func (bb *BaseBackend) SaveGCSafePoint(safePoint uint64) error {
	value := strconv.FormatUint(safePoint, 16)
	return bb.Save(gcSafePointPath(), value)
}

// LoadMinServiceGCSafePoint returns the minimum safepoint across all services
func (bb *BaseBackend) LoadMinServiceGCSafePoint(now time.Time) (*endpoint.ServiceSafePoint, error) {
	prefix := GCSafePointServicePrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := bb.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// There's no service safepoint. It may be a new cluster, or upgraded from an older version where all service
		// safepoints are missing. For the second case, we have no way to recover it. Store an initial value 0 for
		// gc_worker.
		return bb.initServiceGCSafePointForGCWorker(0)
	}

	hasGCWorker := false
	min := &endpoint.ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &endpoint.ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		if ssp.ServiceID == gcWorkerServiceSafePointID {
			hasGCWorker = true
			// If gc_worker's expire time is incorrectly set, fix it.
			if ssp.ExpiredAt != math.MaxInt64 {
				ssp.ExpiredAt = math.MaxInt64
				err = bb.SaveServiceGCSafePoint(ssp)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}

		if ssp.ExpiredAt < now.Unix() {
			bb.Remove(key)
			continue
		}
		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}

	if min.SafePoint == math.MaxUint64 {
		// There's no valid safepoints and we have no way to recover it. Just set gc_worker to 0.
		log.Info("there are no valid service safepoints. init gc_worker's service safepoint to 0")
		return bb.initServiceGCSafePointForGCWorker(0)
	}

	if !hasGCWorker {
		// If there exists some service safepoints but gc_worker is missing, init it with the min value among all
		// safepoints (including expired ones)
		return bb.initServiceGCSafePointForGCWorker(min.SafePoint)
	}

	return min, nil
}

func (bb *BaseBackend) initServiceGCSafePointForGCWorker(initialValue uint64) (*endpoint.ServiceSafePoint, error) {
	ssp := &endpoint.ServiceSafePoint{
		ServiceID: gcWorkerServiceSafePointID,
		SafePoint: initialValue,
		ExpiredAt: math.MaxInt64,
	}
	if err := bb.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadAllServiceGCSafePoints returns all services GC safepoints
func (bb *BaseBackend) LoadAllServiceGCSafePoints() ([]*endpoint.ServiceSafePoint, error) {
	prefix := GCSafePointServicePrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := bb.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*endpoint.ServiceSafePoint{}, nil
	}

	ssps := make([]*endpoint.ServiceSafePoint, 0, len(keys))
	for i := range keys {
		ssp := &endpoint.ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		ssps = append(ssps, ssp)
	}

	return ssps, nil
}

// SaveServiceGCSafePoint saves a GC safepoint for the service
func (bb *BaseBackend) SaveServiceGCSafePoint(ssp *endpoint.ServiceSafePoint) error {
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

	return bb.Save(key, string(value))
}

// RemoveServiceGCSafePoint removes a GC safepoint for the service
func (bb *BaseBackend) RemoveServiceGCSafePoint(serviceID string) error {
	if serviceID == gcWorkerServiceSafePointID {
		return errors.New("cannot remove service safe point of gc_worker")
	}
	key := gcSafePointServicePath(serviceID)
	return bb.Remove(key)
}
