// Copyright 2026 TiKV Project Authors.
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

package gc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const (
	enabledKeyspaceMinRangeSize   = 4096
	enabledKeyspaceTasksPerWorker = 32
)

// load pins the entire attempt to the initial transaction's revision. Its map
// stays private until every range succeeds; a failed attempt cancels and joins
// the producer and workers before discarding the partial result.
func (i *enabledKeyspaceIndex) load(ctx context.Context, onPage func([]enabledKeyspace)) (entries map[uint32]enabledKeyspace, revision int64, err error) {
	concurrency := i.getLoadConcurrency()
	started := time.Now()
	log.Info("start loading GC keyspace index", zap.String("prefix", i.prefix),
		zap.Int("concurrency", concurrency), zap.Int("page-size", enabledKeyspacePageSize))
	defer func() {
		message := "load enabled keyspace index completed"
		if ctx.Err() != nil {
			message = "loading GC keyspace index canceled"
		} else if err != nil {
			message = "loading GC keyspace index failed"
		}
		log.Info(message, zap.String("prefix", i.prefix), zap.Int("concurrency", concurrency),
			zap.Int64("revision", revision), zap.Int("enabled-keyspace-count", len(entries)),
			zap.Duration("cost", time.Since(started)), zap.Error(err))
	}()
	end := clientv3.GetPrefixRangeEnd(i.prefix)
	requestCtx, cancel := context.WithTimeout(ctx, enabledKeyspaceRequestTimeout)
	resp, err := i.client.Txn(requestCtx).Then(
		clientv3.OpGet(i.prefix, clientv3.WithRange(end), clientv3.WithLimit(enabledKeyspacePageSize)),
		clientv3.OpGet(keypath.KeyspaceAllocIDPath()),
	).Commit()
	cancel()
	if err != nil {
		return nil, 0, err
	}
	if resp.Header == nil || resp.Header.Revision <= 0 || len(resp.Responses) != 2 {
		return nil, 0, errors.New("invalid initial keyspace metadata transaction response")
	}
	first := resp.Responses[0].GetResponseRange()
	allocator := resp.Responses[1].GetResponseRange()
	if first == nil || allocator == nil {
		return nil, 0, errors.New("missing range response in initial keyspace metadata transaction")
	}
	revision = resp.Header.Revision
	page, lastID, err := i.decodePage(first.Kvs, first.More, revision)
	if err != nil {
		return nil, 0, err
	}
	entries = make(map[uint32]enabledKeyspace)
	var mu sync.Mutex
	consume := func(page []enabledKeyspace) {
		mu.Lock()
		for _, entry := range page {
			entries[entry.id] = entry
		}
		mu.Unlock()
		if onPage != nil {
			onPage(page)
		}
	}
	consume(page)
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if !first.More {
		return entries, revision, nil
	}

	// H is only a hint to stop subdividing. Missing, malformed, or out-of-range
	// hints retain the first page and scan the entire remaining prefix.
	var watermark uint64
	if len(allocator.Kvs) == 1 && len(allocator.Kvs[0].Value) == 8 {
		value := binary.BigEndian.Uint64(allocator.Kvs[0].Value)
		if value <= uint64(constant.MaxValidKeyspaceID) {
			watermark = value
		}
	}
	lo := uint64(lastID) + 1
	var span uint64
	if watermark > lo {
		span = watermark - lo
	}
	taskBudget := uint64(concurrency) * enabledKeyspaceTasksPerWorker
	// Bound range tasks, including empty scans, while allowing each task to
	// paginate. Starting boundaries at lo keeps the task count within budget.
	rangeSize := max(uint64(enabledKeyspaceMinRangeSize), (span+taskBudget-1)/taskBudget)
	rangeCount := max(uint64(1), (span+rangeSize-1)/rangeSize)
	log.Info("planned GC keyspace index ranges", zap.String("prefix", i.prefix),
		zap.Int("concurrency", concurrency), zap.Uint64("range-size", rangeSize),
		zap.Uint64("task-budget", taskBudget), zap.Uint64("range-count", rangeCount),
		zap.Uint64("next-id", lo), zap.Uint64("watermark", watermark), zap.Int64("revision", revision))
	cursor := string(first.Kvs[len(first.Kvs)-1].Key) + "\x00"
	type keyRange struct{ start, end string }
	tasks := make(chan keyRange, concurrency)
	group, attemptCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(tasks)
		for boundary := lo + rangeSize; ; boundary += rangeSize {
			taskEnd := end
			if boundary < watermark {
				taskEnd = fmt.Sprintf("%s%08d", i.prefix, boundary)
			}
			select {
			case <-attemptCtx.Done():
				return attemptCtx.Err()
			case tasks <- keyRange{start: cursor, end: taskEnd}:
			}
			if taskEnd == end {
				return nil
			}
			cursor = taskEnd
		}
	})
	for range concurrency {
		group.Go(func() error {
			for {
				select {
				case <-attemptCtx.Done():
					return attemptCtx.Err()
				case task, ok := <-tasks:
					if !ok {
						return nil
					}
					if err := i.loadRange(attemptCtx, task.start, task.end, revision, consume); err != nil {
						return err
					}
				}
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return entries, revision, nil
}

func (i *enabledKeyspaceIndex) loadRange(ctx context.Context, start, end string, revision int64, consume func([]enabledKeyspace)) error {
	for {
		requestCtx, cancel := context.WithTimeout(ctx, enabledKeyspaceRequestTimeout)
		resp, err := i.client.Get(requestCtx, start, clientv3.WithRange(end), clientv3.WithLimit(enabledKeyspacePageSize), clientv3.WithRev(revision))
		cancel()
		if err != nil {
			return err
		}
		page, _, err := i.decodePage(resp.Kvs, resp.More, revision)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		consume(page)
		if !resp.More {
			return nil
		}
		start = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

// decodePage completes validation before exposing any entries to the caller.
func (i *enabledKeyspaceIndex) decodePage(kvs []*mvccpb.KeyValue, more bool, revision int64) ([]enabledKeyspace, uint32, error) {
	if more && len(kvs) == 0 {
		return nil, 0, fmt.Errorf("empty keyspace metadata page at revision %d", revision)
	}
	page := make([]enabledKeyspace, 0, len(kvs))
	var lastID uint32
	for _, kv := range kvs {
		if kv == nil {
			return nil, 0, fmt.Errorf("missing keyspace metadata at revision %d", revision)
		}
		id, entry, enabled, err := i.decodeKeyspaceMeta(kv.Key, kv.Value)
		if err != nil {
			return nil, 0, err
		}
		lastID = id
		if enabled {
			page = append(page, entry)
		}
	}
	return page, lastID, nil
}
