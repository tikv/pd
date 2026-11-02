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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/server/config"
)

const (
	enabledKeyspacePageSize       = 256
	enabledKeyspaceRequestTimeout = 5 * time.Second
	enabledKeyspaceRetryDelay     = time.Second
	enabledKeyspaceMaxRetryDelay  = 30 * time.Second
	enabledKeyspaceLogInterval    = 30 * time.Second
	enabledKeyspaceWatchTimeout   = 10 * time.Second
)

// enabledKeyspace is a value copy of the metadata needed by GC initialization.
type enabledKeyspace struct {
	id               uint32
	gcManagementType string
}

// enabledKeyspaceLoadHooks only reports pages before the first successful
// snapshot. Callbacks run outside the index mutex and own their value slices.
// onPage may run concurrently; callbacks must not wait for GC I/O.
type enabledKeyspaceLoadHooks struct {
	onPage            func([]enabledKeyspace)
	onInitialSnapshot func([]enabledKeyspace)
}

// enabledKeyspaceIndex belongs to one leadership term. The published map and
// its revision always describe one complete, successfully applied snapshot.
type enabledKeyspaceIndex struct {
	client             *clientv3.Client
	prefix             string
	watcherFactory     func(*clientv3.Client) clientv3.Watcher
	getLoadConcurrency func() int

	mu       sync.Mutex
	entries  map[uint32]enabledKeyspace
	revision int64
	stopped  error
	ready    bool
	changed  chan struct{}
}

func newEnabledKeyspaceIndex(client *clientv3.Client, prefix string) *enabledKeyspaceIndex {
	return &enabledKeyspaceIndex{
		client:             client,
		prefix:             prefix,
		watcherFactory:     clientv3.NewWatcher,
		getLoadConcurrency: func() int { return config.DefaultGCKeyspaceIndexLoadConcurrency },
		changed:            make(chan struct{}),
	}
}

// run blocks until the leadership term ends. A failed or compacted watch is
// followed by a complete reload, so no missing revision is silently skipped.
// The watch intentionally outlives initial GC warmup in preparation for upcoming
// WatchGCStates initialization with revision-consistent metadata snapshots
// (https://github.com/tikv/pd/pull/11264).
// Call run once per index. Returning retires the index and wakes its waiters.
func (i *enabledKeyspaceIndex) run(ctx context.Context, hooks enabledKeyspaceLoadHooks) {
	defer func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		i.stopped = ctx.Err()
		close(i.changed)
		i.changed = make(chan struct{})
	}()
	retryDelay := enabledKeyspaceRetryDelay
	var lastLog time.Time
	suppressedErrors := 0
	for ctx.Err() == nil {
		entries, revision, err := i.load(ctx, hooks.onPage)
		phase := "load"
		if err == nil {
			if i.publish(ctx, entries, revision) {
				if hooks.onInitialSnapshot != nil {
					hooks.onInitialSnapshot(sortedEnabledKeyspaces(entries))
				}
				hooks = enabledKeyspaceLoadHooks{}
			}
			watchStarted := time.Now()
			err = i.watch(ctx, revision+1)
			phase = "watch"
			if time.Since(watchStarted) >= enabledKeyspaceMaxRetryDelay {
				retryDelay = enabledKeyspaceRetryDelay
			}
		}
		if ctx.Err() != nil {
			return
		}
		if time.Since(lastLog) >= enabledKeyspaceLogInterval {
			log.Warn("failed to synchronize enabled keyspace index",
				zap.String("prefix", i.prefix),
				zap.String("phase", phase),
				zap.Int64("revision", i.appliedRevision()),
				zap.Duration("retry-delay", retryDelay),
				zap.Int("suppressed-errors", suppressedErrors),
				zap.Error(err))
			lastLog = time.Now()
			suppressedErrors = 0
		} else {
			suppressedErrors++
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		retryDelay = min(retryDelay*2, enabledKeyspaceMaxRetryDelay)
	}
}

func (i *enabledKeyspaceIndex) appliedRevision() int64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.revision
}

// canSkipGCStateWarmup reports whether the published metadata excludes a
// non-null keyspace from independent GC. Before the index is ready, a missing
// entry cannot establish that the scope is obsolete.
func (i *enabledKeyspaceIndex) canSkipGCStateWarmup(id uint32) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, exists := i.entries[id]
	return i.ready && (!exists || entry.gcManagementType != keyspace.KeyspaceLevelGC)
}

func (i *enabledKeyspaceIndex) waitReady(ctx context.Context) error {
	for {
		i.mu.Lock()
		if err := ctx.Err(); err != nil {
			i.mu.Unlock()
			return err
		}
		if i.stopped != nil {
			err := i.stopped
			i.mu.Unlock()
			return err
		}
		if i.ready {
			i.mu.Unlock()
			return nil
		}
		changed := i.changed
		i.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (i *enabledKeyspaceIndex) snapshotAtLeast(ctx context.Context, revision int64) ([]enabledKeyspace, int64, error) {
	for {
		i.mu.Lock()
		if err := i.stopped; err != nil {
			i.mu.Unlock()
			return nil, 0, err
		}
		if err := ctx.Err(); err != nil {
			i.mu.Unlock()
			return nil, 0, err
		}
		if i.ready && i.revision >= revision {
			result := make([]enabledKeyspace, 0, len(i.entries))
			for _, entry := range i.entries {
				result = append(result, entry)
			}
			applied := i.revision
			i.mu.Unlock()
			sortEnabledKeyspaces(result)
			return result, applied, nil
		}
		changed := i.changed
		i.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-changed:
		}
	}
}

func (i *enabledKeyspaceIndex) publish(ctx context.Context, entries map[uint32]enabledKeyspace, revision int64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	i.entries = entries
	i.revision = revision
	i.ready = true
	close(i.changed)
	i.changed = make(chan struct{})
	return true
}

func (i *enabledKeyspaceIndex) publishProgress(ctx context.Context, changes map[uint32]*enabledKeyspace, revision int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if ctx.Err() != nil || revision <= i.revision {
		return
	}
	for id, entry := range changes {
		if entry == nil {
			delete(i.entries, id)
		} else {
			i.entries[id] = *entry
		}
	}
	i.revision = revision
	close(i.changed)
	i.changed = make(chan struct{})
}

// watch updates a private working set. Progress notifications are ordered after
// prior events on the same stream, so they prove a complete applied revision.
// Event response headers alone may be ahead of the delivered prefix events.
func (i *enabledKeyspaceIndex) watch(ctx context.Context, nextRevision int64) error {
	watcher := i.watcherFactory(i.client)
	defer watcher.Close()
	watchCtx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	done := make(chan struct{})
	go grpcutil.CheckStream(watchCtx, cancel, done)
	watchCh := watcher.Watch(watchCtx, i.prefix, clientv3.WithPrefix(), clientv3.WithRev(nextRevision), clientv3.WithProgressNotify())
	done <- struct{}{}
	if err := watchCtx.Err(); err != nil {
		return fmt.Errorf("keyspace metadata watch creation failed: %w", err)
	}
	ticker := time.NewTicker(etcdutil.RequestProgressInterval)
	defer ticker.Stop()
	pending := make(map[uint32]*enabledKeyspace)
	publishedRevision := nextRevision - 1
	pendingRevision := publishedRevision
	lastProgress := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
				return errors.New("keyspace metadata watch closed")
			}
			if err := resp.Err(); err != nil {
				return err
			}
			if resp.IsProgressNotify() {
				if resp.Header.Revision < pendingRevision || resp.Header.Revision < publishedRevision ||
					(resp.Header.Revision == publishedRevision && len(pending) != 0) {
					return fmt.Errorf("keyspace metadata watch progress %d precedes applied events at %d", resp.Header.Revision, pendingRevision)
				}
				i.publishProgress(ctx, pending, resp.Header.Revision)
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
					id, entry, enabled, err := i.decodeKeyspaceMeta(event.Kv.Key, event.Kv.Value)
					if err != nil {
						return err
					}
					changes = append(changes, change{id: id, entry: entry, enabled: enabled})
				case mvccpb.DELETE:
					id, err := i.parseKeyspaceID(event.Kv.Key)
					if err != nil {
						return fmt.Errorf("invalid deleted keyspace metadata: %w", err)
					}
					changes = append(changes, change{id: id})
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

func (i *enabledKeyspaceIndex) parseKeyspaceID(rawKey []byte) (uint32, error) {
	key := string(rawKey)
	if !strings.HasPrefix(key, i.prefix) {
		return 0, fmt.Errorf("keyspace metadata key %q is outside prefix", key)
	}
	id64, err := strconv.ParseUint(strings.TrimPrefix(key, i.prefix), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid keyspace metadata key %q: %w", key, err)
	}
	return uint32(id64), nil
}

func (i *enabledKeyspaceIndex) decodeKeyspaceMeta(rawKey, rawValue []byte) (uint32, enabledKeyspace, bool, error) {
	id, err := i.parseKeyspaceID(rawKey)
	if err != nil {
		return 0, enabledKeyspace{}, false, err
	}
	meta := &keyspacepb.KeyspaceMeta{}
	if err := proto.Unmarshal(rawValue, meta); err != nil {
		return 0, enabledKeyspace{}, false, fmt.Errorf("decode keyspace metadata %q: %w", rawKey, err)
	}
	if meta.GetId() != id {
		return 0, enabledKeyspace{}, false, fmt.Errorf("keyspace metadata %q contains ID %d", rawKey, meta.GetId())
	}
	return id, enabledKeyspace{id: id, gcManagementType: meta.Config[keyspace.GCManagementType]}, meta.State == keyspacepb.KeyspaceState_ENABLED, nil
}

func sortedEnabledKeyspaces(entries map[uint32]enabledKeyspace) []enabledKeyspace {
	result := make([]enabledKeyspace, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}
	sortEnabledKeyspaces(result)
	return result
}

func sortEnabledKeyspaces(entries []enabledKeyspace) {
	slices.SortFunc(entries, func(a, b enabledKeyspace) int {
		switch {
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		default:
			return 0
		}
	})
}
