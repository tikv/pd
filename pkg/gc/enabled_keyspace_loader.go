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
	"strconv"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const (
	enabledKeyspaceRangeSize   = 4096
	enabledKeyspaceLoadWorkers = 4
)

// load pins the entire attempt to the initial transaction's revision. Its map
// stays private until every range succeeds; a failed attempt cancels and joins
// the producer and workers before discarding the partial result.
func (c *enabledKeyspaceCache) load(ctx context.Context, onPage func([]enabledKeyspace)) (map[uint32]enabledKeyspace, int64, error) {
	end := clientv3.GetPrefixRangeEnd(c.prefix)
	requestCtx, cancel := context.WithTimeout(ctx, enabledKeyspaceRequestTimeout)
	resp, err := c.client.Txn(requestCtx).Then(
		clientv3.OpGet(c.prefix, clientv3.WithRange(end), clientv3.WithLimit(enabledKeyspacePageSize)),
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
	revision := resp.Header.Revision
	page, lastID, err := c.decodePage(first.Kvs, first.More, revision)
	if err != nil {
		return nil, 0, err
	}
	entries := make(map[uint32]enabledKeyspace)
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
	cursor := string(first.Kvs[len(first.Kvs)-1].Key) + "\x00"
	type keyRange struct{ start, end string }
	tasks := make(chan keyRange, enabledKeyspaceLoadWorkers)
	group, attemptCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(tasks)
		for boundary := (uint64(lastID)/enabledKeyspaceRangeSize + 1) * enabledKeyspaceRangeSize; ; boundary += enabledKeyspaceRangeSize {
			taskEnd := end
			if boundary < watermark {
				taskEnd = fmt.Sprintf("%s%08d", c.prefix, boundary)
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
	for range enabledKeyspaceLoadWorkers {
		group.Go(func() error {
			for {
				select {
				case <-attemptCtx.Done():
					return attemptCtx.Err()
				case task, ok := <-tasks:
					if !ok {
						return nil
					}
					if err := c.loadRange(attemptCtx, task.start, task.end, revision, consume); err != nil {
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

func (c *enabledKeyspaceCache) loadRange(ctx context.Context, start, end string, revision int64, consume func([]enabledKeyspace)) error {
	for {
		requestCtx, cancel := context.WithTimeout(ctx, enabledKeyspaceRequestTimeout)
		resp, err := c.client.Get(requestCtx, start, clientv3.WithRange(end), clientv3.WithLimit(enabledKeyspacePageSize), clientv3.WithRev(revision))
		cancel()
		if err != nil {
			return err
		}
		page, _, err := c.decodePage(resp.Kvs, resp.More, revision)
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
func (c *enabledKeyspaceCache) decodePage(kvs []*mvccpb.KeyValue, more bool, revision int64) ([]enabledKeyspace, uint32, error) {
	if more && len(kvs) == 0 {
		return nil, 0, fmt.Errorf("empty keyspace metadata page at revision %d", revision)
	}
	page := make([]enabledKeyspace, 0, len(kvs))
	var lastID uint32
	for _, kv := range kvs {
		if kv == nil {
			return nil, 0, fmt.Errorf("missing keyspace metadata at revision %d", revision)
		}
		id, entry, enabled, err := c.decode(kv.Key, kv.Value)
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

func (c *enabledKeyspaceCache) decode(rawKey, rawValue []byte) (uint32, enabledKeyspace, bool, error) {
	key := string(rawKey)
	if !strings.HasPrefix(key, c.prefix) {
		return 0, enabledKeyspace{}, false, fmt.Errorf("keyspace metadata key %q is outside prefix", key)
	}
	id64, err := strconv.ParseUint(strings.TrimPrefix(key, c.prefix), 10, 32)
	if err != nil {
		return 0, enabledKeyspace{}, false, fmt.Errorf("invalid keyspace metadata key %q: %w", key, err)
	}
	id := uint32(id64)
	meta := &keyspacepb.KeyspaceMeta{}
	if err := proto.Unmarshal(rawValue, meta); err != nil {
		return 0, enabledKeyspace{}, false, fmt.Errorf("decode keyspace metadata %q: %w", key, err)
	}
	if meta.GetId() != id {
		return 0, enabledKeyspace{}, false, fmt.Errorf("keyspace metadata %q contains ID %d", key, meta.GetId())
	}
	return id, enabledKeyspace{id: id, gcManagementType: meta.Config[keyspace.GCManagementType]}, meta.State == keyspacepb.KeyspaceState_ENABLED, nil
}
