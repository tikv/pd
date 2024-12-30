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

package storage

import (
	"context"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// Storage is the interface for the backend storage of the PD.
type Storage interface {
	// Introducing the kv.Base here is to provide
	// the basic key-value read/write ability for the Storage.
	kv.Base
	endpoint.ServiceMiddlewareStorage
	endpoint.ConfigStorage
	endpoint.MetaStorage
	endpoint.RuleStorage
	endpoint.ReplicationStatusStorage
	endpoint.GCSafePointStorage
	endpoint.MinResolvedTSStorage
	endpoint.ExternalTSStorage
	endpoint.SafePointV2Storage
	endpoint.KeyspaceStorage
	endpoint.ResourceGroupStorage
	endpoint.TSOStorage
	endpoint.KeyspaceGroupStorage
}

// NewStorageWithMemoryBackend creates a new storage with memory backend.
func NewStorageWithMemoryBackend() Storage {
	return newMemoryBackend()
}

// NewStorageWithEtcdBackend creates a new storage with etcd backend.
func NewStorageWithEtcdBackend(client *clientv3.Client, rootPath string) Storage {
	return newEtcdBackend(client, rootPath)
}

// NewRegionStorageWithLevelDBBackend will create a specialized storage to
// store region meta information based on a LevelDB backend.
func NewRegionStorageWithLevelDBBackend(
	ctx context.Context,
	filePath string,
	ekm *encryption.Manager,
) (*RegionStorage, error) {
	levelDBBackend, err := newLevelDBBackend(ctx, filePath, ekm)
	if err != nil {
		return nil, err
	}
	return newRegionStorage(levelDBBackend), nil
}

// TODO: support other KV storage backends like BadgerDB in the future.

type regionSource int

const (
	unloaded regionSource = iota
	fromEtcd
	fromLeveldb
)

type coreStorage struct {
	Storage
	regionStorage endpoint.RegionStorage

	useRegionStorage atomic.Bool
	regionLoaded     regionSource
	mu               syncutil.RWMutex
}

// NewCoreStorage creates a new core storage with the given default and region storage.
// Usually, the defaultStorage is etcd-backend, and the regionStorage is LevelDB-backend.
// coreStorage can switch between the defaultStorage and regionStorage to read and write
// the region info, and all other storage interfaces will use the defaultStorage.
func NewCoreStorage(defaultStorage Storage, regionStorage endpoint.RegionStorage) Storage {
	return &coreStorage{
		Storage:       defaultStorage,
		regionStorage: regionStorage,
		regionLoaded:  unloaded,
	}
}

// RetrieveRegionStorage retrieve the region storage from the given storage.
// If it's a `coreStorage`, it will return the regionStorage inside, otherwise it will return the original storage.
func RetrieveRegionStorage(s Storage) endpoint.RegionStorage {
	switch ps := s.(type) {
	case *coreStorage:
		return ps.regionStorage
	default:
		return ps
	}
}

// TrySwitchRegionStorage try to switch whether the RegionStorage uses local or not,
// and returns the RegionStorage used after the switch.
// Returns nil if it cannot be switched.
func TrySwitchRegionStorage(s Storage, useLocalRegionStorage bool) endpoint.RegionStorage {
	ps, ok := s.(*coreStorage)
	if !ok {
		return nil
	}

	if useLocalRegionStorage {
		// Switch the region storage to regionStorage, all region info will be read/saved by the internal
		// regionStorage, and in most cases it's LevelDB-backend.
		ps.useRegionStorage.Store(true)
		return ps.regionStorage
	}
	// Switch the region storage to defaultStorage, all region info will be read/saved by the internal
	// defaultStorage, and in most cases it's etcd-backend.
	ps.useRegionStorage.Store(false)
	return ps.Storage
}

// TryLoadRegionsOnce loads all regions from storage to RegionsInfo.
// If the underlying storage is the local region storage, it will only load once.
func TryLoadRegionsOnce(ctx context.Context, s Storage, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	ps, ok := s.(*coreStorage)
	if !ok {
		return s.LoadRegions(ctx, f)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if !ps.useRegionStorage.Load() {
		err := ps.Storage.LoadRegions(ctx, f)
		if err == nil {
			ps.regionLoaded = fromEtcd
		}
		return err
	}

	if ps.regionLoaded == unloaded {
		if err := ps.regionStorage.LoadRegions(ctx, f); err != nil {
			return err
		}
		ps.regionLoaded = fromLeveldb
	}
	return nil
}

// LoadRegion loads one region from storage.
func (ps *coreStorage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	if ps.useRegionStorage.Load() {
		return ps.regionStorage.LoadRegion(regionID, region)
	}
	return ps.Storage.LoadRegion(regionID, region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (ps *coreStorage) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	if ps.useRegionStorage.Load() {
		return ps.regionStorage.LoadRegions(ctx, f)
	}
	return ps.Storage.LoadRegions(ctx, f)
}

// SaveRegion saves one region to storage.
func (ps *coreStorage) SaveRegion(region *metapb.Region) error {
	if ps.useRegionStorage.Load() {
		return ps.regionStorage.SaveRegion(region)
	}
	return ps.Storage.SaveRegion(region)
}

// DeleteRegion deletes one region from storage.
func (ps *coreStorage) DeleteRegion(region *metapb.Region) error {
	if ps.useRegionStorage.Load() {
		return ps.regionStorage.DeleteRegion(region)
	}
	return ps.Storage.DeleteRegion(region)
}

// Flush flushes the dirty region to storage.
// In coreStorage, only the regionStorage is flushed.
func (ps *coreStorage) Flush() error {
	if ps.regionStorage != nil {
		return ps.regionStorage.Flush()
	}
	return nil
}

// Close closes the region storage.
// In coreStorage, only the regionStorage is closable.
func (ps *coreStorage) Close() error {
	if ps.regionStorage != nil {
		return ps.regionStorage.Close()
	}
	return nil
}

// AreRegionsLoaded returns whether the regions are loaded.
func AreRegionsLoaded(s Storage) bool {
	ps := s.(*coreStorage)
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	failpoint.Inject("loadRegionSlow", func() {
		failpoint.Return(false)
	})
	if ps.useRegionStorage.Load() {
		return ps.regionLoaded == fromLeveldb
	}
	return ps.regionLoaded == fromEtcd
}
