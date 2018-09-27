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
	"time"

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
	dirtyFlushTick  = time.Second
)

// KVProxy is a proxy of KV storage.
type KVProxy struct {
	defaultKV KVBase
	regionKV  *RegionKV
	isDefault atomic.Value
}

// NewKVProxy return a proxy of KV storage.
func NewKVProxy(defaultKV KVBase) *KVProxy {
	kv := &KVProxy{defaultKV: defaultKV}
	kv.isDefault.Store(true)
	return kv
}

func (kv *KVProxy) getKVBase() KVBase {
	isDefault := kv.isDefault.Load().(bool)
	if isDefault {
		return kv.defaultKV
	}
	return kv.regionKV
}

// LoadRegion loads one regoin from KV.
func (kv *KVProxy) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	kvBase := kv.getKVBase()
	return loadProto(kvBase, regionPath(regionID), region)
}

// LoadRegions loads all regions from KV to RegionsInfo.
func (kv *KVProxy) LoadRegions(regions *RegionsInfo) error {
	kvBase := kv.getKVBase()
	return loadRegions(kvBase, regions)
}

// SaveRegion saves one region to KV.
func (kv *KVProxy) SaveRegion(region *metapb.Region) error {
	isDefault := kv.isDefault.Load().(bool)
	if isDefault {
		return saveProto(kv.defaultKV, regionPath(region.GetId()), region)
	}
	return kv.regionKV.SaveRegion(region)
}

// DeleteRegion deletes one region from KV.
func (kv *KVProxy) DeleteRegion(region *metapb.Region) error {
	kvBase := kv.getKVBase()
	return kvBase.Delete(regionPath(region.GetId()))
}

func (kv *KVProxy) switchRegionStorage(useDefault bool) {
	if kv.regionKV == nil {
		return
	}
	kv.isDefault.Store(useDefault)
}

// KV wraps all kv operations, keep it stateless.
type KV struct {
	KVBase
	proxy *KVProxy
}

// NewKV creates KV instance with KVBase.
func NewKV(base KVBase) *KV {
	return &KV{
		KVBase: base,
		proxy:  NewKVProxy(base),
	}
}

// SetProxyWithRegionKV sets the proxy with specified region storage.
func (kv *KV) SetProxyWithRegionKV(regionKV *RegionKV) *KV {
	kv.proxy.regionKV = regionKV
	return kv
}

// SwitchRegionStorage switch the region storage.
func (kv *KV) SwitchRegionStorage(useDefault bool) {
	kv.proxy.switchRegionStorage(useDefault)
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

// LoadRegion loads one regoin from KV.
func (kv *KV) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return kv.proxy.LoadRegion(regionID, region)
}

// LoadRegions loads all regions from KV to RegionsInfo.
func (kv *KV) LoadRegions(regions *RegionsInfo) error {
	return kv.proxy.LoadRegions(regions)
}

// SaveRegion saves one region to KV.
func (kv *KV) SaveRegion(region *metapb.Region) error {
	return kv.proxy.SaveRegion(region)
}

// DeleteRegion deletes one region from KV.
func (kv *KV) DeleteRegion(region *metapb.Region) error {
	return kv.proxy.DeleteRegion(region)
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
		res, err := kv.LoadRange(key, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for _, s := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(s)); err != nil {
				return errors.WithStack(err)
			}
			storeInfo := NewStoreInfo(store)
			leaderWeight, err := kv.loadFloatWithDefaultValue(kv.storeLeaderWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return err
			}
			storeInfo.LeaderWeight = leaderWeight
			regionWeight, err := kv.loadFloatWithDefaultValue(kv.storeRegionWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return err
			}
			storeInfo.RegionWeight = regionWeight

			nextID = store.GetId() + 1
			stores.SetStore(storeInfo)
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

// Flush flush the dirty region to storage.
func (kv *KV) Flush() error {
	if kv.proxy.regionKV != nil {
		return kv.proxy.regionKV.FlushRegion()
	}
	return nil
}

// Close close the kv.
func (kv *KV) Close() error {
	if kv.proxy.regionKV != nil {
		return kv.proxy.regionKV.Close()
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
