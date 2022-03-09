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
	"math"
	"path"
	"strconv"

	"go.etcd.io/etcd/clientv3"
)

// MinResolvedTSPoint is the min resolved ts for a store
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MinResolvedTSPoint struct {
	StoreID       uint64 `json:"store_id"`
	MinResolvedTS uint64 `json:"min_resolved_ts"`
}

// MinResolvedTSStorage defines the storage operations on the min resolved ts.
type MinResolvedTSStorage interface {
	LoadMinResolvedTS(storeID uint64) (uint64, error)
	SaveMinResolvedTS(storeID uint64, minResolvedTS uint64) error
	LoadClusterMinResolvedTS() (uint64, error)
	LoadAllMinResolvedTS() ([]*MinResolvedTSPoint, error)
	RemoveMinResolvedTS(storeID uint64) error
}

var _ MinResolvedTSStorage = (*StorageEndpoint)(nil)

// LoadMinResolvedTS loads the min resolved ts with the given store ID from storage.
func (se *StorageEndpoint) LoadMinResolvedTS(storeID uint64) (uint64, error) {
	value, err := se.Load(MinResolvedTSPath(storeID))
	if err != nil {
		return 0, err
	}
	if value == "" {
		return 0, nil
	}
	minResolvedTS, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, err
	}
	return minResolvedTS, nil
}

// SaveMinResolvedTS saves the min resolved ts with the given store ID to storage.
func (se *StorageEndpoint) SaveMinResolvedTS(storeID uint64, minResolvedTS uint64) error {
	value := strconv.FormatUint(minResolvedTS, 16)
	return se.Save(MinResolvedTSPath(storeID), value)
}

// LoadClusterMinResolvedTS returns the min resolved ts for the cluster
func (se *StorageEndpoint) LoadClusterMinResolvedTS() (uint64, error) {
	prefix := MinResolvedTSPrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return math.MaxUint64, err
	}
	if len(keys) == 0 {
		// There's no service safepoint. It may be a new cluster, or upgraded from an older version
		return 0, nil
	}

	min := uint64(math.MaxUint64)
	for i := range keys {
		var ts uint64
		if ts, err = strconv.ParseUint(values[i], 16, 64); err != nil {
			return min, err
		}
		if ts < min {
			min = ts
		}
	}
	return min, nil
}

// LoadAllMinResolvedTS returns min resolved ts of all stores.
func (se *StorageEndpoint) LoadAllMinResolvedTS() ([]*MinResolvedTSPoint, error) {
	prefix := MinResolvedTSPrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*MinResolvedTSPoint{}, nil
	}

	tss := make([]*MinResolvedTSPoint, 0, len(keys))
	for i, key := range keys {
		var minResolvedTS, storeID uint64
		if minResolvedTS, err = strconv.ParseUint(values[i], 16, 64); err != nil {
			return nil, err
		}
		if storeID, err = strconv.ParseUint(path.Base(key), 16, 64); err != nil {
			return nil, err
		}
		ts := &MinResolvedTSPoint{
			StoreID:       storeID,
			MinResolvedTS: minResolvedTS,
		}
		tss = append(tss, ts)
	}

	return tss, nil
}

// RemoveMinResolvedTS removes min resolved ts for the store
func (se *StorageEndpoint) RemoveMinResolvedTS(storeID uint64) error {
	key := MinResolvedTSPath(storeID)
	return se.Remove(key)
}
