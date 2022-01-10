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
	storage "github.com/tikv/pd/server/storage/base_storage"
)

var _ storage.MetaStorage = (*BaseBackend)(nil)

const (
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
)

// LoadMeta loads cluster meta from the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (bb *BaseBackend) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return bb.loadProto(clusterPath, meta)
}

// SaveMeta save cluster meta to the storage. This method will only
// be used by the PD server, so we should only implement it for the etcd storage.
func (bb *BaseBackend) SaveMeta(meta *metapb.Cluster) error {
	return bb.saveProto(clusterPath, meta)
}

// LoadStore loads one store from storage.
func (bb *BaseBackend) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return bb.loadProto(StorePath(storeID), store)
}

// SaveStore saves one store to storage.
func (bb *BaseBackend) SaveStore(store *metapb.Store) error {
	return bb.saveProto(StorePath(store.GetId()), store)
}

// SaveStoreWeight saves a store's leader and region weight to storage.
func (bb *BaseBackend) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := bb.Save(storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return bb.Save(storeRegionWeightPath(storeID), regionValue)
}

// LoadStores loads all stores from storage to StoresInfo.
func (bb *BaseBackend) LoadStores(f func(store *core.StoreInfo)) error {
	nextID := uint64(0)
	endKey := StorePath(math.MaxUint64)
	for {
		key := StorePath(nextID)
		_, res, err := bb.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, str := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(str)); err != nil {
				return errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByArgs()
			}
			leaderWeight, err := bb.loadFloatWithDefaultValue(storeLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := bb.loadFloatWithDefaultValue(storeRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			newStoreInfo := core.NewStoreInfo(store, core.SetLeaderWeight(leaderWeight), core.SetRegionWeight(regionWeight))

			nextID = store.GetId() + 1
			f(newStoreInfo)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}

func (bb *BaseBackend) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := bb.Load(path)
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
func (bb *BaseBackend) DeleteStore(store *metapb.Store) error {
	return bb.Remove(StorePath(store.GetId()))
}

// LoadRegion loads one region from the backend storage.
func (bb *BaseBackend) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	value, err := bb.Load(RegionPath(regionID))
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
	err = encryption.DecryptRegion(region, bb.encryptionKeyManager)
	return true, err
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (bb *BaseBackend) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	nextID := uint64(0)
	endKey := RegionPath(math.MaxUint64)

	// Since the region key may be very long, using a larger rangeLimit will cause
	// the message packet to exceed the grpc message size limit (4MB). Here we use
	// a variable rangeLimit to work around.
	rangeLimit := maxKVRangeLimit
	for {
		failpoint.Inject("slowLoadRegion", func() {
			rangeLimit = 1
			time.Sleep(time.Second)
		})
		startKey := RegionPath(nextID)
		_, res, err := bb.LoadRange(startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= minKVRangeLimit {
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
			if err = encryption.DecryptRegion(region, bb.encryptionKeyManager); err != nil {
				return err
			}

			nextID = region.GetId() + 1
			overlaps := f(core.NewRegionInfo(region, nil))
			for _, item := range overlaps {
				if err := bb.DeleteRegion(item.GetMeta()); err != nil {
					return err
				}
			}
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

// SaveRegion saves one region to storage.
func (bb *BaseBackend) SaveRegion(region *metapb.Region) error {
	region, err := encryption.EncryptRegion(region, bb.encryptionKeyManager)
	if err != nil {
		return err
	}
	value, err := proto.Marshal(region)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByArgs()
	}
	return bb.Save(RegionPath(region.GetId()), string(value))
}

// DeleteRegion deletes one region from storage.
func (bb *BaseBackend) DeleteRegion(region *metapb.Region) error {
	return bb.Remove(RegionPath(region.GetId()))
}
