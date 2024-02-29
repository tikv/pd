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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// defaultFlushRate is the default interval to flush the data into the local storage.
	defaultFlushRate = 3 * time.Second
	// defaultBatchSize is the default batch size to save the data to the local storage.
	defaultBatchSize = 100
	// defaultDirtyFlushTick
	defaultDirtyFlushTick = time.Second
)

type levelDBKV interface {
	Key() string
	Value() ([]byte, error)
	Encrypt(*encryption.Manager) (levelDBKV, error)
}

// levelDBBackend is a storage backend that stores data in LevelDB,
// which is mainly used to store the PD Region meta information.
type levelDBBackend[T levelDBKV] struct {
	*endpoint.StorageEndpoint
	ekm       *encryption.Manager
	mu        syncutil.RWMutex
	batch     map[string]T
	batchSize int
	cacheSize int
	flushRate time.Duration
	flushTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
}

// newLevelDBBackend is used to create a new LevelDB backend.
func newLevelDBBackend[T levelDBKV](
	ctx context.Context,
	filePath string,
	ekm *encryption.Manager,
) (*levelDBBackend[T], error) {
	levelDB, err := kv.NewLevelDBKV(filePath)
	if err != nil {
		return nil, err
	}
	lb := &levelDBBackend[T]{
		StorageEndpoint: endpoint.NewStorageEndpoint(levelDB, ekm),
		ekm:             ekm,
		batchSize:       defaultBatchSize,
		flushRate:       defaultFlushRate,
		batch:           make(map[string]T, defaultBatchSize),
		flushTime:       time.Now().Add(defaultFlushRate),
	}
	lb.ctx, lb.cancel = context.WithCancel(ctx)
	go lb.backgroundFlush()
	return lb, nil
}

func (lb *levelDBBackend[T]) backgroundFlush() {
	defer logutil.LogPanic()

	var (
		isFlush bool
		err     error
	)
	ticker := time.NewTicker(defaultDirtyFlushTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			lb.mu.RLock()
			isFlush = lb.flushTime.Before(time.Now())
			failpoint.Inject("levelDBStorageFastFlush", func() {
				isFlush = true
			})
			lb.mu.RUnlock()
			if !isFlush {
				continue
			}
			if err = lb.Flush(); err != nil {
				log.Error("flush data meet error", errs.ZapError(err))
			}
		case <-lb.ctx.Done():
			return
		}
	}
}

func (lb *levelDBBackend[T]) SaveKV(kv T) error {
	key := kv.Key()
	value, err := kv.Encrypt(lb.ekm)
	if err != nil {
		return err
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.cacheSize < lb.batchSize-1 {
		lb.batch[key] = value.(T)
		lb.cacheSize++

		lb.flushTime = time.Now().Add(lb.flushRate)
		return nil
	}
	lb.batch[key] = value.(T)
	err = lb.flushLocked()

	if err != nil {
		return err
	}
	return nil
}

func (lb *levelDBBackend[T]) DeleteKV(kv T) error {
	return lb.Remove(kv.Key())
}

// Flush saves the batch cache to the underlying storage.
func (lb *levelDBBackend[T]) Flush() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.flushLocked()
}

func (lb *levelDBBackend[T]) flushLocked() error {
	if err := lb.saveBatchLocked(); err != nil {
		return err
	}
	lb.cacheSize = 0
	lb.batch = make(map[string]T, lb.batchSize)
	return nil
}

func (lb *levelDBBackend[T]) saveBatchLocked() error {
	batch := new(leveldb.Batch)

	for k, v := range lb.batch {
		value, err := v.Value()
		if err != nil {
			return err
		}
		batch.Put([]byte(k), value)
	}

	if err := lb.Base.(*kv.LevelDBKV).Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	return nil
}

// Close closes the LevelDB kv. It will call Flush() once before closing.
func (lb *levelDBBackend[T]) Close() error {
	err := lb.Flush()
	if err != nil {
		log.Error("meet error before close the leveldb storage", errs.ZapError(err))
	}
	lb.cancel()
	err = lb.Base.(*kv.LevelDBKV).Close()
	if err != nil {
		return errs.ErrLevelDBClose.Wrap(err).GenWithStackByArgs()
	}
	return nil
}
