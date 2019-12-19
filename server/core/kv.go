// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
)

const (
	clusterPath  = "raft"
	configPath   = "config"
	schedulePath = "schedule"
	gcPath       = "gc"
)

const (
	maxKVRangeLimit = 10000
	minKVRangeLimit = 100
)

// KV wraps all kv operations, keep it stateless.
type KV struct {
	KVBase
	regionKV    *RegionKV
	useRegionKV int32
}

// NewKV creates KV instance with KVBase.
func NewKV(base KVBase) *KV {
	return &KV{
		KVBase: base,
	}
}

// SetRegionKV sets the region storage.
func (kv *KV) SetRegionKV(regionKV *RegionKV) *KV {
	kv.regionKV = regionKV
	return kv
}

// GetRegionKV gets the region storage.
func (kv *KV) GetRegionKV() *RegionKV {
	return kv.regionKV
}

// SwitchToRegionStorage switches to the region storage.
func (kv *KV) SwitchToRegionStorage() {
	atomic.StoreInt32(&kv.useRegionKV, 1)
}

// SwitchToDefaultStorage switches to the to default storage.
func (kv *KV) SwitchToDefaultStorage() {
	atomic.StoreInt32(&kv.useRegionKV, 0)
}

func (kv *KV) storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

// ClusterStatePath returns the path to save an option.
func (kv *KV) ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

func (kv *KV) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func (kv *KV) storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// LoadMeta loads cluster meta from KV store.
func (kv *KV) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return loadProto(kv.KVBase, clusterPath, meta)
}

// SaveMeta save cluster meta to KV store.
func (kv *KV) SaveMeta(meta *metapb.Cluster) error {
	return saveProto(kv.KVBase, clusterPath, meta)
}

// LoadStore loads one store from KV.
func (kv *KV) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return loadProto(kv.KVBase, kv.storePath(storeID), store)
}

// SaveStore saves one store to KV.
func (kv *KV) SaveStore(store *metapb.Store) error {
	return saveProto(kv.KVBase, kv.storePath(store.GetId()), store)
}

// DeleteStore deletes one store from KV.
func (kv *KV) DeleteStore(store *metapb.Store) error {
	return kv.Delete(kv.storePath(store.GetId()))
}

// LoadRegion loads one regoin from KV.
func (kv *KV) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	if atomic.LoadInt32(&kv.useRegionKV) > 0 {
		return loadProto(kv.regionKV, regionPath(regionID), region)
	}
	return loadProto(kv.KVBase, regionPath(regionID), region)
}

// LoadRegions loads all regions from KV to RegionsInfo.
func (kv *KV) LoadRegions(f func(region *RegionInfo) []*metapb.Region) error {
	if atomic.LoadInt32(&kv.useRegionKV) > 0 {
		return loadRegions(kv.regionKV, f)
	}
	return loadRegions(kv.KVBase, f)
}

// SaveRegion saves one region to KV.
func (kv *KV) SaveRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&kv.useRegionKV) > 0 {
		return kv.regionKV.SaveRegion(region)
	}
	return saveProto(kv.KVBase, regionPath(region.GetId()), region)
}

// DeleteRegion deletes one region from KV.
func (kv *KV) DeleteRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&kv.useRegionKV) > 0 {
		return deleteRegion(kv.regionKV, region)
	}
	return deleteRegion(kv.KVBase, region)
}

// SaveConfig stores marshalable cfg to the configPath.
func (kv *KV) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errors.WithStack(err)
	}
	return kv.Save(configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (kv *KV) LoadConfig(cfg interface{}) (bool, error) {
	value, err := kv.Load(configPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// LoadStores loads all stores from KV to StoresInfo.
func (kv *KV) LoadStores(stores *StoresInfo) error {
	nextID := uint64(0)
	endKey := kv.storePath(math.MaxUint64)
	for {
		key := kv.storePath(nextID)
		_, res, err := kv.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, s := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(s)); err != nil {
				return errors.WithStack(err)
			}
			leaderWeight, err := kv.loadFloatWithDefaultValue(kv.storeLeaderWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			regionWeight, err := kv.loadFloatWithDefaultValue(kv.storeRegionWeightPath(store.GetId()), 1.0)
			if err != nil {
				return err
			}
			newStoreInfo := NewStoreInfo(store, SetLeaderWeight(leaderWeight), SetRegionWeight(regionWeight))

			nextID = store.GetId() + 1
			stores.SetStore(newStoreInfo)
		}
		if len(res) < minKVRangeLimit {
			return nil
		}
	}
}

// SaveStoreWeight saves a store's leader and region weight to KV.
func (kv *KV) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := kv.Save(kv.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return err
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	return kv.Save(kv.storeRegionWeightPath(storeID), regionValue)
}

func (kv *KV) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := kv.Load(path)
	if err != nil {
		return 0, err
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return val, nil
}

// Flush flushes the dirty region to storage.
func (kv *KV) Flush() error {
	if kv.regionKV != nil {
		return kv.regionKV.FlushRegion()
	}
	return nil
}

// Close closes the kv.
func (kv *KV) Close() error {
	if kv.regionKV != nil {
		return kv.regionKV.Close()
	}
	return nil
}

// SaveGCSafePoint saves new GC safe point to KV.
func (kv *KV) SaveGCSafePoint(safePoint uint64) error {
	key := path.Join(gcPath, "safe_point")
	value := strconv.FormatUint(safePoint, 16)
	return kv.Save(key, value)
}

// LoadGCSafePoint loads current GC safe point from KV.
func (kv *KV) LoadGCSafePoint() (uint64, error) {
	key := path.Join(gcPath, "safe_point")
	value, err := kv.Load(key)
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

func loadProto(kv KVBase, key string, msg proto.Message) (bool, error) {
	value, err := kv.Load(key)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), msg)
	return true, errors.WithStack(err)
}

func saveProto(kv KVBase, key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	return kv.Save(key, string(value))
}
