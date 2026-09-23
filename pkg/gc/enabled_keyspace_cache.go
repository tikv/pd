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
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

const (
	enabledKeyspacePageSize       = 256
	enabledKeyspaceRequestTimeout = 5 * time.Second
	enabledKeyspaceRetryDelay     = time.Second
	enabledKeyspaceWatchTimeout   = 10 * time.Second
)

// enabledKeyspace is a value copy of the metadata needed by GC initialization.
type enabledKeyspace struct {
	id               uint32
	gcManagementType string
}

// enabledKeyspaceCache belongs to one leadership term. The published map and
// its revision always describe one complete, successfully applied snapshot.
type enabledKeyspaceCache struct {
	termCtx context.Context
	client  *clientv3.Client
	prefix  string

	mu       sync.Mutex
	entries  map[uint32]enabledKeyspace
	revision int64
	ready    bool
	changed  chan struct{}
}

func newEnabledKeyspaceCache(termCtx context.Context, client *clientv3.Client, prefix string) *enabledKeyspaceCache {
	return &enabledKeyspaceCache{
		termCtx: termCtx,
		client:  client,
		prefix:  prefix,
		changed: make(chan struct{}),
	}
}

// run blocks until the leadership term ends. A failed or compacted watch is
// followed by a complete reload, so no missing revision is silently skipped.
func (c *enabledKeyspaceCache) run() {
	for c.termCtx.Err() == nil {
		entries, revision, err := c.load()
		if err == nil {
			c.publish(entries, revision)
			_ = c.watch(revision + 1)
		}
		if c.termCtx.Err() != nil {
			return
		}
		select {
		case <-c.termCtx.Done():
			return
		case <-time.After(enabledKeyspaceRetryDelay):
		}
	}
}

func (c *enabledKeyspaceCache) waitReady(ctx context.Context) error {
	for {
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return err
		}
		if c.ready {
			c.mu.Unlock()
			return c.termCtx.Err()
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.termCtx.Done():
			return c.termCtx.Err()
		case <-changed:
		}
	}
}

func (c *enabledKeyspaceCache) snapshotAtLeast(ctx context.Context, revision int64) ([]enabledKeyspace, int64, error) {
	for {
		c.mu.Lock()
		if err := c.termCtx.Err(); err != nil {
			c.mu.Unlock()
			return nil, 0, err
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return nil, 0, err
		}
		if c.ready && c.revision >= revision {
			result := make([]enabledKeyspace, 0, len(c.entries))
			for _, entry := range c.entries {
				result = append(result, entry)
			}
			applied := c.revision
			c.mu.Unlock()
			slices.SortFunc(result, func(a, b enabledKeyspace) int {
				switch {
				case a.id < b.id:
					return -1
				case a.id > b.id:
					return 1
				default:
					return 0
				}
			})
			return result, applied, nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-c.termCtx.Done():
			return nil, 0, c.termCtx.Err()
		case <-changed:
		}
	}
}

func (c *enabledKeyspaceCache) publish(entries map[uint32]enabledKeyspace, revision int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.termCtx.Err() != nil {
		return
	}
	c.entries = entries
	c.revision = revision
	c.ready = true
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *enabledKeyspaceCache) publishProgress(changes map[uint32]*enabledKeyspace, revision int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.termCtx.Err() != nil || revision <= c.revision {
		return
	}
	for id, entry := range changes {
		if entry == nil {
			delete(c.entries, id)
		} else {
			c.entries[id] = *entry
		}
	}
	c.revision = revision
	close(c.changed)
	c.changed = make(chan struct{})
}

// load reads each page at the first page's revision and returns only after the
// entire prefix has been decoded. No partial page is ever published.
func (c *enabledKeyspaceCache) load() (map[uint32]enabledKeyspace, int64, error) {
	entries := make(map[uint32]enabledKeyspace)
	start := c.prefix
	end := clientv3.GetPrefixRangeEnd(c.prefix)
	var revision int64
	for {
		ctx, cancel := context.WithTimeout(c.termCtx, enabledKeyspaceRequestTimeout)
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(enabledKeyspacePageSize)}
		if revision != 0 {
			opts = append(opts, clientv3.WithRev(revision))
		}
		resp, err := c.client.Get(ctx, start, opts...)
		cancel()
		if err != nil {
			return nil, 0, err
		}
		if revision == 0 {
			revision = resp.Header.Revision
		}
		for _, kv := range resp.Kvs {
			id, entry, enabled, err := c.decode(kv.Key, kv.Value)
			if err != nil {
				return nil, 0, err
			}
			if enabled {
				entries[id] = entry
			}
		}
		if !resp.More {
			return entries, revision, nil
		}
		if len(resp.Kvs) == 0 {
			return nil, 0, fmt.Errorf("empty keyspace metadata page at revision %d", revision)
		}
		start = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
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

// watch updates a private working set. Progress notifications are ordered after
// prior events on the same stream, so they prove a complete applied revision.
// Event response headers alone may be ahead of the delivered prefix events.
func (c *enabledKeyspaceCache) watch(nextRevision int64) error {
	watcher := clientv3.NewWatcher(c.client)
	defer watcher.Close()
	watchCtx, cancel := context.WithCancel(clientv3.WithRequireLeader(c.termCtx))
	defer cancel()
	watchCh := watcher.Watch(watchCtx, c.prefix, clientv3.WithPrefix(), clientv3.WithRev(nextRevision), clientv3.WithProgressNotify())
	ticker := time.NewTicker(etcdutil.RequestProgressInterval)
	defer ticker.Stop()
	pending := make(map[uint32]*enabledKeyspace)
	publishedRevision := nextRevision - 1
	pendingRevision := publishedRevision
	lastProgress := time.Now()
	for {
		select {
		case <-c.termCtx.Done():
			return c.termCtx.Err()
		case <-ticker.C:
			if time.Since(lastProgress) >= enabledKeyspaceWatchTimeout {
				return fmt.Errorf("keyspace metadata watch made no progress for %s", enabledKeyspaceWatchTimeout)
			}
			ctx, cancel := context.WithTimeout(watchCtx, enabledKeyspaceRequestTimeout)
			err := watcher.RequestProgress(ctx)
			cancel()
			if err != nil {
				return err
			}
		case resp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("keyspace metadata watch closed")
			}
			if err := resp.Err(); err != nil {
				return err
			}
			if resp.IsProgressNotify() {
				if resp.Header.Revision < pendingRevision || resp.Header.Revision < publishedRevision ||
					(resp.Header.Revision == publishedRevision && len(pending) != 0) {
					return fmt.Errorf("keyspace metadata watch progress %d precedes applied events at %d", resp.Header.Revision, pendingRevision)
				}
				c.publishProgress(pending, resp.Header.Revision)
				publishedRevision = resp.Header.Revision
				pendingRevision = publishedRevision
				lastProgress = time.Now()
				clear(pending)
				continue
			}
			if len(resp.Events) == 0 {
				continue
			}
			// Decode the complete clientv3 response before changing the working
			// set. clientv3 merges fragmented watch responses before delivery.
			type change struct {
				id      uint32
				entry   enabledKeyspace
				enabled bool
			}
			changes := make([]change, 0, len(resp.Events))
			for _, event := range resp.Events {
				if event.Kv == nil || event.Kv.ModRevision <= publishedRevision {
					return fmt.Errorf("keyspace metadata watch received stale or missing event at revision %d", publishedRevision)
				}
				pendingRevision = max(pendingRevision, event.Kv.ModRevision)
				switch event.Type {
				case mvccpb.PUT:
					id, entry, enabled, err := c.decode(event.Kv.Key, event.Kv.Value)
					if err != nil {
						return err
					}
					changes = append(changes, change{id: id, entry: entry, enabled: enabled})
				case mvccpb.DELETE:
					id64, err := strconv.ParseUint(strings.TrimPrefix(string(event.Kv.Key), c.prefix), 10, 32)
					if err != nil {
						return fmt.Errorf("invalid deleted keyspace metadata key %q: %w", event.Kv.Key, err)
					}
					changes = append(changes, change{id: uint32(id64)})
				default:
					return fmt.Errorf("unexpected keyspace metadata event type %v", event.Type)
				}
			}
			for _, change := range changes {
				if change.enabled {
					entry := change.entry
					pending[change.id] = &entry
				} else {
					pending[change.id] = nil
				}
			}
		}
	}
}
