// Copyright 2021 TiKV Project Authors.
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
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/kv"
)

var _ Storage = (*LevelDBStorage)(nil)

const (
	// defaultBatchSize is the batch size to save the regions to region storage.
	defaultBatchSize = 100
	// defaultFlushRegionRate is the ttl to sync the regions to region storage.
	defaultFlushRegionRate = 3 * time.Second
)

// LevelDBStorage is a storage that stores data in LevelDB,
// which is used in the region-related storage.
type LevelDBStorage struct {
	defaultStorage
	mu           sync.RWMutex
	batchRegions map[string]*metapb.Region
	batchSize    int
	cacheSize    int
	flushRate    time.Duration
	flushTime    time.Time
}

// NewLevelDBStorage is used to create a new LevelDB storage.
func NewLevelDBStorage(path string, encryptionKeyManager *encryptionkm.KeyManager) (*LevelDBStorage, error) {
	levelDB, err := kv.NewLeveldbKV(path)
	if err != nil {
		return nil, err
	}
	return &LevelDBStorage{
		defaultStorage: defaultStorage{levelDB, encryptionKeyManager},
		batchRegions:   make(map[string]*metapb.Region, defaultBatchSize),
		batchSize:      defaultBatchSize,
		flushRate:      defaultFlushRegionRate,
	}, nil
}

// SaveRegion saves one region to storage.
func (s *LevelDBStorage) SaveRegion(region *metapb.Region) error {
	region, err := encryption.EncryptRegion(region, s.encryptionKeyManager)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cacheSize < s.batchSize-1 {
		s.batchRegions[regionPath(region.GetId())] = region
		s.cacheSize++

		s.flushTime = time.Now().Add(s.flushRate)
		return nil
	}
	s.batchRegions[regionPath(region.GetId())] = region
	err = s.flush()

	if err != nil {
		return err
	}
	return nil
}

func (s *LevelDBStorage) flush() error {
	if err := s.SaveRegions(s.batchRegions); err != nil {
		return err
	}
	s.cacheSize = 0
	s.batchRegions = make(map[string]*metapb.Region, s.batchSize)
	return nil
}

// FlushRegion saves the cache region to region storage.
func (s *LevelDBStorage) FlushRegion() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flush()
}

// SaveRegions stores some regions.
func (s *LevelDBStorage) SaveRegions(regions map[string]*metapb.Region) error {
	batch := new(leveldb.Batch)

	for key, r := range regions {
		value, err := proto.Marshal(r)
		if err != nil {
			return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
		}
		batch.Put([]byte(key), value)
	}

	if err := s.Base.(*kv.LeveldbKV).Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	return nil
}

// Close closes the LevelDB storage.
func (s *LevelDBStorage) Close() error {
	return s.Base.(*kv.LeveldbKV).Close()
}
