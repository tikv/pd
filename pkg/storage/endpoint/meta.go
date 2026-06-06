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
	"context"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MetaStorage defines the storage operations on the PD cluster meta info.
type MetaStorage interface {
	LoadMeta(meta *metapb.Cluster) (bool, error)
	SaveMeta(meta *metapb.Cluster) error
	LoadStoreMeta(storeID uint64, store *metapb.Store) (bool, error)
	SaveStoreMeta(store *metapb.Store) error
	SaveStoreWeight(storeID uint64, leader, region float64) error
	LoadStores(f func(store *core.StoreInfo, opts ...core.StoreCreateOption)) error
	DeleteStoreMeta(store *metapb.Store) error
	RegionStorage
}

// RegionStorage defines the storage operations on the Region meta info.
type RegionStorage interface {
	LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error)
	LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error
	SaveRegion(region *metapb.Region) error
	DeleteRegion(region *metapb.Region) error
	Flush() error
	Close() error
}

var _ MetaStorage = (*StorageEndpoint)(nil)

const (
	// MaxKVRangeLimit is the max limit of the number of keys in a range.
	MaxKVRangeLimit = 10000
	// MaxLocalKVRangeLimit is the max limit of the number of keys in a local KV range.
	MaxLocalKVRangeLimit = 100000
	// MinKVRangeLimit is the min limit of the number of keys in a range.
	MinKVRangeLimit = 100
	// minParallelRegionDecodeBatch is the minimum batch size to decode storage
	// regions concurrently. Small batches stay serial to avoid worker overhead.
	minParallelRegionDecodeBatch = 1024
)

type regionBytesLoader interface {
	LoadRangeValues(startKey, endKey string, limit int) ([][]byte, error)
}

// LoadMeta loads cluster meta from the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (se *StorageEndpoint) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return se.loadProto(keypath.ClusterPath(), meta)
}

// SaveMeta save cluster meta to the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (se *StorageEndpoint) SaveMeta(meta *metapb.Cluster) error {
	return se.saveProto(keypath.ClusterPath(), meta)
}

// LoadStoreMeta loads one store from storage.
func (se *StorageEndpoint) LoadStoreMeta(storeID uint64, store *metapb.Store) (bool, error) {
	return se.loadProto(keypath.StorePath(storeID), store)
}

// SaveStoreMeta saves one store to storage.
func (se *StorageEndpoint) SaveStoreMeta(store *metapb.Store) error {
	return se.saveProto(keypath.StorePath(store.GetId()), store)
}

// SaveStoreWeight saves a store's leader and region weight to storage.
func (se *StorageEndpoint) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := se.Save(keypath.StoreLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return se.Save(keypath.StoreRegionWeightPath(storeID), regionValue)
}

// LoadStores loads all stores from storage to StoresInfo.
func (se *StorageEndpoint) LoadStores(putStore func(store *core.StoreInfo, opts ...core.StoreCreateOption)) error {
	nextID := uint64(0)
	endKey := keypath.StorePath(math.MaxUint64)
	for {
		key := keypath.StorePath(nextID)
		_, res, err := se.LoadRange(key, endKey, MinKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			if store.State == metapb.StoreState_Offline {
				store.NodeState = metapb.NodeState_Removing
			}
			if store.State == metapb.StoreState_Tombstone {
				store.NodeState = metapb.NodeState_Removed
			}
			leaderWeight, err := se.loadFloatWithDefaultValue(keypath.StoreLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := se.loadFloatWithDefaultValue(keypath.StoreRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}

			nextID = store.GetId() + 1
			putStore(core.NewStoreInfo(store,
				core.SetLeaderWeight(leaderWeight),
				core.SetRegionWeight(regionWeight),
			))
		}
		if len(res) < MinKVRangeLimit {
			return nil
		}
	}
}

func (se *StorageEndpoint) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := se.Load(path)
	if err != nil {
		return 0, err
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseFloat.Wrap(err).GenWithStackByArgs()
	}
	return val, nil
}

// DeleteStoreMeta deletes one store from storage.
func (se *StorageEndpoint) DeleteStoreMeta(store *metapb.Store) error {
	return se.Remove(keypath.StorePath(store.GetId()))
}

// LoadRegion loads one region from the backend storage.
func (se *StorageEndpoint) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	value, err := se.Load(keypath.RegionPath(regionID))
	if err != nil || value == "" {
		return false, err
	}
	err = proto.Unmarshal([]byte(value), region)
	if err != nil {
		return true, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	err = encryption.DecryptRegion(region, se.encryptionKeyManager)
	return true, err
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (se *StorageEndpoint) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	endKey := keypath.RegionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := se.regionLoadRangeLimit()
	for {
		failpoint.Inject("slowLoadRegion", func() {
			rangeLimit = 1
			time.Sleep(time.Second)
		})
		startKey := keypath.RegionPath(se.nextRegionID)
		res, err := se.loadRegionValues(startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= MinKVRangeLimit {
				continue
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		regions, err := se.decodeStorageRegions(res)
		if err != nil {
			return err
		}
		for _, region := range regions {
			se.nextRegionID = region.GetId() + 1
			overlaps := f(core.NewStorageRegionInfo(region))
			for _, item := range overlaps {
				if err := se.DeleteRegion(item.GetMeta()); err != nil {
					return err
				}
			}
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

func (se *StorageEndpoint) decodeStorageRegions(values [][]byte) ([]*metapb.Region, error) {
	if len(values) < minParallelRegionDecodeBatch {
		regions := make([]*metapb.Region, 0, len(values))
		for _, value := range values {
			region, err := se.decodeStorageRegion(value)
			if err != nil {
				return nil, err
			}
			regions = append(regions, region)
		}
		return regions, nil
	}

	regions := make([]*metapb.Region, len(values))
	workers := min(runtime.GOMAXPROCS(0), len(values))
	index := atomic.Int64{}
	hasErr := atomic.Bool{}
	var errOnce sync.Once
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				if hasErr.Load() {
					return
				}
				i := int(index.Add(1)) - 1
				if i >= len(values) {
					return
				}
				region, err := se.decodeStorageRegion(values[i])
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						hasErr.Store(true)
					})
					return
				}
				regions[i] = region
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return regions, nil
}

func (se *StorageEndpoint) decodeStorageRegion(value []byte) (*metapb.Region, error) {
	region := &metapb.Region{}
	if err := region.Unmarshal(value); err != nil {
		return nil, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	if err := encryption.DecryptRegion(region, se.encryptionKeyManager); err != nil {
		return nil, err
	}
	return region, nil
}

func (se *StorageEndpoint) regionLoadRangeLimit() int {
	if _, ok := se.Base.(regionBytesLoader); ok {
		return MaxLocalKVRangeLimit
	}
	return MaxKVRangeLimit
}

func (se *StorageEndpoint) loadRegionValues(startKey, endKey string, limit int) ([][]byte, error) {
	if loader, ok := se.Base.(regionBytesLoader); ok {
		return loader.LoadRangeValues(startKey, endKey, limit)
	}
	_, values, err := se.LoadRange(startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	res := make([][]byte, 0, len(values))
	for _, value := range values {
		res = append(res, []byte(value))
	}
	return res, nil
}

// SaveRegion saves one region to storage.
func (se *StorageEndpoint) SaveRegion(region *metapb.Region) error {
	region, err := encryption.EncryptRegion(region, se.encryptionKeyManager)
	if err != nil {
		return err
	}
	value, err := proto.Marshal(region)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByArgs()
	}
	return se.Save(keypath.RegionPath(region.GetId()), string(value))
}

// DeleteRegion deletes one region from storage.
func (se *StorageEndpoint) DeleteRegion(region *metapb.Region) error {
	return se.Remove(keypath.RegionPath(region.GetId()))
}

// Flush flushes the pending data to the underlying storage backend.
func (*StorageEndpoint) Flush() error { return nil }

// Close closes the underlying storage backend.
func (*StorageEndpoint) Close() error { return nil }
