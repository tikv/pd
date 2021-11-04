// Copyright 2018 TiKV Project Authors.
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
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/encryptionkm"
)

// RegionStorage is used to save regions.
type RegionStorage struct {
	storage        Storage
	levelDBStorage *LevelDBStorage

	useLevelDBStorage atomic.Value // Store as bool.
	regionLoaded      int32

	regionStorageCtx    context.Context
	regionStorageCancel context.CancelFunc
}

// NewRegionStorage returns a region storage that is used to save regions.
func NewRegionStorage(
	ctx context.Context,
	storage Storage,
	path string,
	encryptionKeyManager *encryptionkm.KeyManager,
) (*RegionStorage, error) {
	var (
		levelDBStorage *LevelDBStorage
		err            error
	)
	if len(path) > 0 {
		levelDBStorage, err = NewLevelDBStorage(path, encryptionKeyManager)
		if err != nil {
			return nil, err
		}
	}
	regionStorageCtx, regionStorageCancel := context.WithCancel(ctx)
	s := &RegionStorage{
		storage:             storage,
		levelDBStorage:      levelDBStorage,
		regionStorageCtx:    regionStorageCtx,
		regionStorageCancel: regionStorageCancel,
	}
	s.useLevelDBStorage.Store(false)
	if levelDBStorage != nil {
		go s.backgroundFlush()
	}
	return s, nil
}

func (s *RegionStorage) backgroundFlush() {
	var (
		isFlush bool
		err     error
	)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.levelDBStorage.mu.RLock()
			isFlush = s.levelDBStorage.flushTime.Before(time.Now())
			s.levelDBStorage.mu.RUnlock()
			if !isFlush {
				continue
			}
			if err = s.levelDBStorage.FlushRegion(); err != nil {
				log.Error("flush regions meet error", errs.ZapError(err))
			}
		case <-s.regionStorageCtx.Done():
			return
		}
	}
}

// SwitchToLevelDBStorage switches the region storage to LevelDB storage.
func (s *RegionStorage) SwitchToLevelDBStorage() {
	if s.levelDBStorage != nil {
		s.useLevelDBStorage.Store(true)
	}
}

// SwitchToDefaultStorage switches the region storage to the default storage.
func (s *RegionStorage) SwitchToDefaultStorage() {
	s.useLevelDBStorage.Store(false)
}

// GetLevelDBStorage returns the LevelDB storage inside the RegionStorage.
func (s *RegionStorage) GetLevelDBStorage() *LevelDBStorage {
	return s.levelDBStorage
}

// LoadRegion loads one region from storage.
func (s *RegionStorage) LoadRegion(regionID uint64, region *metapb.Region) (ok bool, err error) {
	if s.useLevelDBStorage.Load().(bool) {
		return s.levelDBStorage.LoadRegion(regionID, region)
	}
	return s.storage.LoadRegion(regionID, region)
}

// LoadRegions loads all regions from storage to RegionsInfo.
func (s *RegionStorage) LoadRegions(ctx context.Context, f func(region *RegionInfo) []*RegionInfo) error {
	if s.useLevelDBStorage.Load().(bool) {
		return s.levelDBStorage.LoadRegions(ctx, f)
	}
	return s.storage.LoadRegions(ctx, f)
}

// LoadRegionsOnce loads all regions from storage to RegionsInfo.Only load one time from regionStorage.
func (s *RegionStorage) LoadRegionsOnce(ctx context.Context, f func(region *RegionInfo) []*RegionInfo) error {
	if !s.useLevelDBStorage.Load().(bool) {
		return s.storage.LoadRegions(ctx, f)
	}
	s.levelDBStorage.mu.Lock()
	defer s.levelDBStorage.mu.Unlock()
	if s.regionLoaded == 0 {
		if err := s.levelDBStorage.LoadRegions(ctx, f); err != nil {
			return err
		}
		s.regionLoaded = 1
	}
	return nil
}

// SaveRegion saves one region to storage.
func (s *RegionStorage) SaveRegion(region *metapb.Region) error {
	if s.useLevelDBStorage.Load().(bool) {
		return s.levelDBStorage.SaveRegion(region)
	}
	return s.storage.SaveRegion(region)
}

// DeleteRegion deletes one region from storage.
func (s *RegionStorage) DeleteRegion(region *metapb.Region) error {
	if s.useLevelDBStorage.Load().(bool) {
		return s.levelDBStorage.DeleteRegion(region)
	}
	return s.storage.DeleteRegion(region)
}

// Flush flushes all regions to the LevelDB storage.
func (s *RegionStorage) Flush() error {
	return s.levelDBStorage.FlushRegion()
}

// Close closes the region storage.
func (s *RegionStorage) Close() error {
	err := s.levelDBStorage.FlushRegion()
	if err != nil {
		log.Error("meet error before close the region storage", errs.ZapError(err))
	}
	s.regionStorageCancel()
	err = s.levelDBStorage.Close()
	if err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}
