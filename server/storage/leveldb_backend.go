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
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/encryptionkm"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
)

const (
	// DefaultFlushRegionRate is the ttl to sync the regions to region storage.
	defaultFlushRegionRate = 3 * time.Second
	// DefaultBatchSize is the batch size to save the regions to region storage.
	defaultBatchSize = 100
)

// levelDBBackend is a storage backend that stores data in LevelDB,
// which is mainly used by the PD region storage.
type levelDBBackend struct {
	*endpoint.StorageEndpoint
	ekm                 *encryptionkm.KeyManager
	mu                  sync.RWMutex
	batchRegions        map[string]*metapb.Region
	batchSize           int
	cacheSize           int
	flushRate           time.Duration
	flushTime           time.Time
	regionStorageCtx    context.Context
	regionStorageCancel context.CancelFunc
}

// newLevelDBBackend is used to create a new LevelDB backend.
func newLevelDBBackend(ctx context.Context, rootPath string, ekm *encryptionkm.KeyManager) (*levelDBBackend, error) {
	levelDB, err := kv.NewLeveldbKV(rootPath)
	if err != nil {
		return nil, err
	}
	regionStorageCtx, regionStorageCancel := context.WithCancel(ctx)
	lb := &levelDBBackend{
		StorageEndpoint:     endpoint.NewStorageEndpoint(levelDB, ekm),
		ekm:                 ekm,
		batchSize:           defaultBatchSize,
		flushRate:           defaultFlushRegionRate,
		batchRegions:        make(map[string]*metapb.Region, defaultBatchSize),
		flushTime:           time.Now().Add(defaultFlushRegionRate),
		regionStorageCtx:    regionStorageCtx,
		regionStorageCancel: regionStorageCancel,
	}
	go lb.backgroundFlush()
	return lb, nil
}

var dirtyFlushTick = time.Second

func (lb *levelDBBackend) backgroundFlush() {
	var (
		isFlush bool
		err     error
	)
	ticker := time.NewTicker(dirtyFlushTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			lb.mu.RLock()
			isFlush = lb.flushTime.Before(time.Now())
			failpoint.Inject("regionStorageFastFlush", func() {
				isFlush = true
			})
			lb.mu.RUnlock()
			if !isFlush {
				continue
			}
			if err = lb.FlushRegion(); err != nil {
				log.Error("flush regions meet error", errs.ZapError(err))
			}
		case <-lb.regionStorageCtx.Done():
			return
		}
	}
}

func (lb *levelDBBackend) SaveRegion(region *metapb.Region) error {
	region, err := encryption.EncryptRegion(region, lb.ekm)
	if err != nil {
		return err
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.cacheSize < lb.batchSize-1 {
		lb.batchRegions[endpoint.RegionPath(region.GetId())] = region
		lb.cacheSize++

		lb.flushTime = time.Now().Add(lb.flushRate)
		return nil
	}
	lb.batchRegions[endpoint.RegionPath(region.GetId())] = region
	err = lb.flushLocked()

	if err != nil {
		return err
	}
	return nil
}

func (lb *levelDBBackend) SaveRegions(regions map[string]*metapb.Region) error {
	batch := new(leveldb.Batch)

	for key, r := range regions {
		value, err := proto.Marshal(r)
		if err != nil {
			return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
		}
		batch.Put([]byte(key), value)
	}

	if err := lb.Base.(*kv.LeveldbKV).Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	return nil
}

func (lb *levelDBBackend) DeleteRegion(region *metapb.Region) error {
	return lb.Remove(endpoint.RegionPath(region.GetId()))
}

// FlushRegion saves the cache region to region storage.
func (lb *levelDBBackend) FlushRegion() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.flushLocked()
}

func (lb *levelDBBackend) flushLocked() error {
	if err := lb.SaveRegions(lb.batchRegions); err != nil {
		return err
	}
	lb.cacheSize = 0
	lb.batchRegions = make(map[string]*metapb.Region, lb.batchSize)
	return nil
}

// Close closes the kv.
func (lb *levelDBBackend) Close() error {
	err := lb.FlushRegion()
	if err != nil {
		log.Error("meet error before close the region storage", errs.ZapError(err))
	}
	lb.regionStorageCancel()
	err = lb.Base.(*kv.LeveldbKV).Close()
	if err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}
