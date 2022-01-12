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
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
)

// MetaStorage defines the storage operations on the PD cluster meta info.
type MetaStorage interface {
	LoadMeta(meta *metapb.Cluster) (bool, error)
	SaveMeta(meta *metapb.Cluster) error
	LoadStore(storeID uint64, store *metapb.Store) (bool, error)
	SaveStore(store *metapb.Store) error
	SaveStoreWeight(storeID uint64, leader, region float64) error
	LoadStores(f func(store *core.StoreInfo)) error
	DeleteStore(store *metapb.Store) error
	RegionStorage
}

// RegionStorage defines the storage operations on the Region meta info.
type RegionStorage interface {
	LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error)
	LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error
	LoadRegionsOnce(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error
	SaveRegion(region *metapb.Region) error
	DeleteRegion(region *metapb.Region) error
	Flush() error
	Close() error
}

var _ MetaStorage = (*StorageEndpoint)(nil)

const (
	// MaxKVRangeLimit is the max limit of the number of keys in a range.
	MaxKVRangeLimit = 10000
	// MinKVRangeLimit is the min limit of the number of keys in a range.
	MinKVRangeLimit = 100
)

// LoadMeta loads cluster meta from the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (se *StorageEndpoint) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return se.loadProto(clusterPath, meta)
}

// SaveMeta save cluster meta to the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (se *StorageEndpoint) SaveMeta(meta *metapb.Cluster) error {
	return se.saveProto(clusterPath, meta)
}

// LoadStore loads one store from storage.
func (se *StorageEndpoint) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return se.loadProto(StorePath(storeID), store)
}

// SaveStore saves one store to storage.
func (se *StorageEndpoint) SaveStore(store *metapb.Store) error {
	return se.saveProto(StorePath(store.GetId()), store)
}

// SaveStoreWeight saves a store's leader and region weight to storage.
func (se *StorageEndpoint) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := se.Save(storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return se.Save(storeRegionWeightPath(storeID), regionValue)
}

// LoadStores loads all stores from storage to StoresInfo.
func (se *StorageEndpoint) LoadStores(f func(store *core.StoreInfo)) error {
	nextID := uint64(0)
	endKey := StorePath(math.MaxUint64)
	for {
		key := StorePath(nextID)
		_, res, err := se.LoadRange(key, endKey, MinKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			leaderWeight, err := se.loadFloatWithDefaultValue(storeLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := se.loadFloatWithDefaultValue(storeRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			newStoreInfo := core.NewStoreInfo(store, core.SetLeaderWeight(leaderWeight), core.SetRegionWeight(regionWeight))

			nextID = store.GetId() + 1
			f(newStoreInfo)
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

// DeleteStore deletes one store from storage.
func (se *StorageEndpoint) DeleteStore(store *metapb.Store) error {
	return se.Remove(StorePath(store.GetId()))
}

// LoadRegion loads one region from the backend storage.
func (se *StorageEndpoint) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	value, err := se.Load(RegionPath(regionID))
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
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
	nextID := uint64(0)
	endKey := RegionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := MaxKVRangeLimit
	for {
		failpoint.Inject("slowLoadRegion", func() {
			rangeLimit = 1
			time.Sleep(time.Second)
		})
		startKey := RegionPath(nextID)
		_, res, err := se.LoadRange(startKey, endKey, rangeLimit)
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

		for _, r := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(r)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			if err = encryption.DecryptRegion(region, se.encryptionKeyManager); err != nil {
				return err
			}

			nextID = region.GetId() + 1
			overlaps := f(core.NewRegionInfo(region, nil))
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

// LoadRegionsOnce loads all regions from storage to RegionsInfo.
func (se *StorageEndpoint) LoadRegionsOnce(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	return se.LoadRegions(ctx, f)
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
	return se.Save(RegionPath(region.GetId()), string(value))
}

// DeleteRegion deletes one region from storage.
func (se *StorageEndpoint) DeleteRegion(region *metapb.Region) error {
	return se.Remove(RegionPath(region.GetId()))
}

// Flush flushes the pending data to the underlying storage backend.
func (se *StorageEndpoint) Flush() error { return nil }

// Close closes the underlying storage backend.
func (se *StorageEndpoint) Close() error { return nil }
