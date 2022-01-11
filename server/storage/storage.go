// Copyright 2017 TiKV Project Authors.
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

package storage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
	storage "github.com/tikv/pd/server/storage/base_storage"
	"go.etcd.io/etcd/clientv3"
)

// Storage is the interface for the backend storage of the PD.
type Storage interface {
	// Introducing the kv.Base here is to provide
	// the basic key-value read/write ability for the Storage.
	kv.Base
	storage.ConfigStorage
	storage.MetaStorage
	storage.RuleStorage
	storage.ComponentStorage
	storage.ReplicationStatusStorage
	storage.GCSafePointStorage
}

// NewStorageWithMemoryBackend creates a new storage with memory backend.
func NewStorageWithMemoryBackend() Storage {
	return newMemoryBackend()
}

// NewStorageWithEtcdBackend creates a new storage with etcd backend.
func NewStorageWithEtcdBackend(client *clientv3.Client, rootPath string) Storage {
	return newEtcdBackend(client, rootPath)
}

// NewStorageWithLevelDBBackend creates a new storage with LevelDB backend.
func NewStorageWithLevelDBBackend(
	ctx context.Context,
	rootPath string,
	ekm *encryptionkm.KeyManager,
) (*levelDBBackend, error) {
	return newLevelDBBackend(ctx, rootPath, ekm)
}

type pdStorage struct {
	Storage
	regionStorage storage.RegionStorage

	useRegionStorage int32
	regionLoaded     bool
	mu               sync.Mutex
}

// NewPDStorage creates a new PD storage with the given storage and region storage.
// Usually, the defaultStorage is etcd-backend, and the regionStorage is LevelDB-backend.
// TODO: maybe support other KV storages like BadgerDB in the future.
func NewPDStorage(defaultStorage Storage, regionStorage storage.RegionStorage) Storage {
	return &pdStorage{
		Storage:       defaultStorage,
		regionStorage: regionStorage,
	}
}

// GetRegionStorage gets the region storage.
func (ps *pdStorage) GetRegionStorage() storage.RegionStorage {
	return ps.regionStorage
}

// SwitchToRegionStorage switches to the region storage.
func (ps *pdStorage) SwitchToRegionStorage() {
	atomic.StoreInt32(&ps.useRegionStorage, 1)
}

// SwitchToDefaultStorage switches to the to default storage.
func (ps *pdStorage) SwitchToDefaultStorage() {
	atomic.StoreInt32(&ps.useRegionStorage, 0)
}

// LoadRegion loads one region from storage.
func (ps *pdStorage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.LoadRegion(regionID, region)
	}
	return ps.Storage.LoadRegion(regionID, region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (ps *pdStorage) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.LoadRegions(ctx, f)
	}
	return ps.Storage.LoadRegions(ctx, f)
}

// LoadRegionsOnce loads all regions from storage to RegionsInfo.Only load one time from regionStorage.
func (ps *pdStorage) LoadRegionsOnce(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	if atomic.LoadInt32(&ps.useRegionStorage) == 0 {
		return ps.Storage.LoadRegions(ctx, f)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if !ps.regionLoaded {
		if err := ps.regionStorage.LoadRegions(ctx, f); err != nil {
			return err
		}
		ps.regionLoaded = true
	}
	return nil
}

// SaveRegion saves one region to storage.
func (ps *pdStorage) SaveRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.SaveRegion(region)
	}
	return ps.Storage.SaveRegion(region)
}

// DeleteRegion deletes one region from storage.
func (ps *pdStorage) DeleteRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&ps.useRegionStorage) > 0 {
		return ps.regionStorage.DeleteRegion(region)
	}
	return ps.Storage.DeleteRegion(region)
}

// Flush flushes the dirty region to storage.
func (ps *pdStorage) Flush() error {
	if flushable, ok := ps.regionStorage.(interface{ FlushRegion() error }); ok {
		return flushable.FlushRegion()
	}
	return nil
}

// Close closes the rsm and its region storage.
func (ps *pdStorage) Close() error {
	if closableStorage, ok := ps.regionStorage.(interface{ Close() error }); ok {
		return closableStorage.Close()
	}
	return nil
}
