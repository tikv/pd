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

package core

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
)

const clusterPath = "raft"

// Storage wraps all kv operations, keep it stateless.
// NOTICE: this struct will be replaced by server/storage later.
type Storage struct {
	kv.Base
	regionStorage        *RegionStorage
	encryptionKeyManager *encryptionkm.KeyManager
	useRegionStorage     int32
	regionLoaded         int32
	mu                   sync.Mutex
}

// StorageOpt represents available options to create Storage.
type StorageOpt struct {
	regionStorage        *RegionStorage
	encryptionKeyManager *encryptionkm.KeyManager
}

// StorageOption configures StorageOpt
type StorageOption func(*StorageOpt)

// WithRegionStorage sets RegionStorage to the Storage
func WithRegionStorage(regionStorage *RegionStorage) StorageOption {
	return func(opt *StorageOpt) {
		opt.regionStorage = regionStorage
	}
}

// WithEncryptionKeyManager sets EncryptionManager to the Storage
func WithEncryptionKeyManager(encryptionKeyManager *encryptionkm.KeyManager) StorageOption {
	return func(opt *StorageOpt) {
		opt.encryptionKeyManager = encryptionKeyManager
	}
}

// NewStorage creates Storage instance with Base.
func NewStorage(base kv.Base, opts ...StorageOption) *Storage {
	options := &StorageOpt{}
	for _, opt := range opts {
		opt(options)
	}
	return &Storage{
		Base:                 base,
		regionStorage:        options.regionStorage,
		encryptionKeyManager: options.encryptionKeyManager,
	}
}

// GetRegionStorage gets the region storage.
func (s *Storage) GetRegionStorage() *RegionStorage {
	return s.regionStorage
}

// SwitchToRegionStorage switches to the region storage.
func (s *Storage) SwitchToRegionStorage() {
	atomic.StoreInt32(&s.useRegionStorage, 1)
}

// SwitchToDefaultStorage switches to the to default storage.
func (s *Storage) SwitchToDefaultStorage() {
	atomic.StoreInt32(&s.useRegionStorage, 0)
}

func regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

// ClusterStatePath returns the path to save an option.
func (s *Storage) ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

// LoadRegion loads one region from storage.
func (s *Storage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return loadRegion(s.regionStorage, s.encryptionKeyManager, regionID, region)
	}
	return loadRegion(s.Base, s.encryptionKeyManager, regionID, region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (s *Storage) LoadRegions(ctx context.Context, f func(region *RegionInfo) []*RegionInfo) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return loadRegions(ctx, s.regionStorage, s.encryptionKeyManager, f)
	}
	return loadRegions(ctx, s.Base, s.encryptionKeyManager, f)
}

// LoadRegionsOnce loads all regions from storage to RegionsInfo.Only load one time from regionStorage.
func (s *Storage) LoadRegionsOnce(ctx context.Context, f func(region *RegionInfo) []*RegionInfo) error {
	if atomic.LoadInt32(&s.useRegionStorage) == 0 {
		return loadRegions(ctx, s.Base, s.encryptionKeyManager, f)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.regionLoaded == 0 {
		if err := loadRegions(ctx, s.regionStorage, s.encryptionKeyManager, f); err != nil {
			return err
		}
		s.regionLoaded = 1
	}
	return nil
}

// SaveRegion saves one region to storage.
func (s *Storage) SaveRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return s.regionStorage.SaveRegion(region)
	}
	return saveRegion(s.Base, s.encryptionKeyManager, region)
}

// DeleteRegion deletes one region from storage.
func (s *Storage) DeleteRegion(region *metapb.Region) error {
	if atomic.LoadInt32(&s.useRegionStorage) > 0 {
		return deleteRegion(s.regionStorage, region)
	}
	return deleteRegion(s.Base, region)
}

// Flush flushes the dirty region to storage.
func (s *Storage) Flush() error {
	if s.regionStorage != nil {
		return s.regionStorage.FlushRegion()
	}
	return nil
}

// Close closes the s.
func (s *Storage) Close() error {
	if s.regionStorage != nil {
		err := s.regionStorage.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func loadRegion(
	kv kv.Base,
	encryptionKeyManager *encryptionkm.KeyManager,
	regionID uint64,
	region *metapb.Region,
) (ok bool, err error) {
	value, err := kv.Load(regionPath(regionID))
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
	err = encryption.DecryptRegion(region, encryptionKeyManager)
	return true, err
}

func saveRegion(
	kv kv.Base,
	encryptionKeyManager *encryptionkm.KeyManager,
	region *metapb.Region,
) error {
	region, err := encryption.EncryptRegion(region, encryptionKeyManager)
	if err != nil {
		return err
	}
	value, err := proto.Marshal(region)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByArgs()
	}
	return kv.Save(regionPath(region.GetId()), string(value))
}
