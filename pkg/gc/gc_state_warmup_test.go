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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func gcWarmupRead(req any) *etcdserverpb.TxnRequest {
	txn, ok := req.(*etcdserverpb.TxnRequest)
	if !ok || len(txn.Compare) != 0 || len(txn.Success) < 2 {
		return nil
	}
	first := txn.Success[0].GetRequestRange()
	if first == nil || !strings.HasSuffix(string(first.Key), "/gcworker/saved_safe_point") {
		return nil
	}
	return txn
}

func gcWarmupEntries(start, count int) []enabledKeyspace {
	entries := make([]enabledKeyspace, count)
	for i := range entries {
		entries[i] = enabledKeyspace{id: uint32(start + i), gcManagementType: keyspace.KeyspaceLevelGC}
	}
	return entries
}

func runGCWarmupTest(t *testing.T, w *gcStateWarmup) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go w.run(ctx)
	t.Cleanup(func() { cancel(); gcWarmupWait(t, w.done) })
}

func gcWarmupWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("warmup event timed out")
	}
}

// Splitting inputs before filtering would produce two transactions for mixed
// cache hits, and refusing partial batches would strand the final 16 scopes.
func TestGCStateWarmupBatches(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  int
		cached bool
		want   []int
	}{
		{"filtered", 120, true, []int{60}},
		{"page", 256, false, []int{16, 60, 60, 60, 60}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var sizes []int
			m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if txn := gcWarmupRead(req); txn != nil {
					mu.Lock()
					sizes = append(sizes, len(txn.Success)/2)
					mu.Unlock()
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			})
			m.gcStateCache.store(constant.NullKeyspaceID, gcStateCacheEntry{})
			entries := gcWarmupEntries(100, tc.count)
			if tc.cached {
				for i, e := range entries {
					if i%2 == 0 {
						m.gcStateCache.store(e.id, gcStateCacheEntry{})
					}
				}
			}
			w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
			w.onPage(entries)
			w.onInitialSnapshot(entries)
			runGCWarmupTest(t, w)
			gcWarmupWait(t, w.done)
			mu.Lock()
			require.ElementsMatch(t, tc.want, sizes)
			mu.Unlock()
			for _, e := range entries {
				_, ok := m.gcStateCache.load(e.id)
				require.True(t, ok)
			}
		})
	}
}

// Metadata completion cannot gate GC work from a page already delivered.
func TestGCStateWarmupStreamsPartialPage(t *testing.T) {
	read := make(chan struct{}, 1)
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) == keypath.TxnSafePointPath(100) {
			read <- struct{}{}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	runGCWarmupTest(t, w)
	w.onPage(gcWarmupEntries(100, 1))
	gcWarmupWait(t, read)
	select {
	case <-w.done:
		t.Fatal("campaign finished before complete snapshot")
	default:
	}
	w.onInitialSnapshot(gcWarmupEntries(100, 1))
	gcWarmupWait(t, w.done)
}

// Four blocked background reads include the prioritized null singleton; a
// foreground singleton must still execute without a background worker slot.
func TestGCStateWarmupConcurrencyAndForeground(t *testing.T) {
	entered := make(chan int, 8)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var active, maxActive atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) != keypath.TxnSafePointPath(2) {
			n := active.Add(1)
			for {
				old := maxActive.Load()
				if n <= old || maxActive.CompareAndSwap(old, n) {
					break
				}
			}
			defer active.Add(-1)
			entered <- len(txn.Success) / 2
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	w.onPage(gcWarmupEntries(100, 256))
	w.onInitialSnapshot(gcWarmupEntries(100, 256))
	runGCWarmupTest(t, w)
	sizes := make([]int, 0, 4)
	for range 4 {
		select {
		case n := <-entered:
			sizes = append(sizes, n)
		case <-time.After(5 * time.Second):
			t.Fatal("four background reads did not overlap")
		}
	}
	require.ElementsMatch(t, []int{1, 60, 60, 60}, sizes)
	foreground := make(chan error, 1)
	go func() { _, err := m.GetGCState(2, true); foreground <- err }()
	select {
	case err := <-foreground:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("foreground waited for background capacity")
	}
	require.Equal(t, int32(4), maxActive.Load())
	unblock()
	gcWarmupWait(t, w.done)
	require.Equal(t, int32(4), maxActive.Load())
}

// Dropped bounded page hints must be reconciled from the initial snapshot.
func TestGCStateWarmupQueueReconciliation(t *testing.T) {
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	entries := gcWarmupEntries(100, 1000)
	hintsDone := make(chan struct{})
	go func() {
		for _, e := range entries {
			w.onPage([]enabledKeyspace{e})
		}
		w.onInitialSnapshot(entries)
		close(hintsDone)
	}()
	gcWarmupWait(t, hintsDone)
	runGCWarmupTest(t, w)
	gcWarmupWait(t, w.done)
	for _, e := range entries {
		_, ok := m.gcStateCache.load(e.id)
		require.True(t, ok, "scope %d", e.id)
	}
}

// Transport failure retries the original batch; a malformed member does not
// prevent healthy members from completing or make them repeat on recovery.
func TestGCStateWarmupRetriesFailures(t *testing.T) {
	var reads atomic.Int32
	retryEntered := make(chan struct{}, 1)
	release := make(chan struct{})
	storage, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) != keypath.TxnSafePointPath(constant.NullKeyspaceID) {
					n := reads.Add(1)
					if n == 1 {
						return errors.New("injected transport failure")
					}
					if n == 3 {
						if len(txn.Success) != 2 {
							return errors.New("recovery repeated healthy scopes")
						}
						retryEntered <- struct{}{}
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			}))
		},
	})
	t.Cleanup(func() { m.stopGCStateGeneration(m.activeGeneration.Load()); cancel(); clean() })
	require.NoError(t, storage.Save(keypath.TxnSafePointPath(101), "broken"))
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	w.onPage(gcWarmupEntries(100, 60))
	w.onInitialSnapshot(gcWarmupEntries(100, 60))
	started := time.Now()
	runGCWarmupTest(t, w)
	gcWarmupWait(t, retryEntered)
	require.GreaterOrEqual(t, time.Since(started), time.Second, "failures must back off")
	for _, e := range gcWarmupEntries(100, 60) {
		_, ok := m.gcStateCache.load(e.id)
		require.Equal(t, e.id != 101, ok)
	}
	require.NoError(t, storage.Save(keypath.TxnSafePointPath(101), "123"))
	close(release)
	gcWarmupWait(t, w.done)
	require.Equal(t, int32(3), reads.Load())
	value, ok := m.gcStateCache.load(101)
	require.True(t, ok)
	require.Equal(t, uint64(123), value.TxnSafePoint)
}

func TestGCStateWarmupJoinedSuccessSurvivesInvalidation(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) == keypath.TxnSafePointPath(100) {
			reads.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	foreground := make(chan error, 1)
	go func() {
		_, err := m.loadGCState(context.Background(), m.activeGeneration.Load(), 100)
		foreground <- err
	}()
	gcWarmupWait(t, entered)
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	runGCWarmupTest(t, w)
	w.onPage(gcWarmupEntries(100, 1))
	require.Eventually(t, func() bool {
		w.assembly.Lock()
		defer w.assembly.Unlock()
		scope := w.scopes[100]
		return scope != nil && scope.joined != nil
	}, 3*time.Second, time.Millisecond)
	w.assembly.Lock()
	close(release)
	err := <-foreground
	m.mu.Lock()
	m.gcStateCache.remove(100)
	m.mu.Unlock()
	w.assembly.Unlock()
	require.NoError(t, err)
	w.onInitialSnapshot(gcWarmupEntries(100, 1))
	gcWarmupWait(t, w.done)
	require.Equal(t, int32(1), reads.Load())
	_, ok := m.gcStateCache.load(100)
	require.False(t, ok)
}

// A successful hint remains completed even if invalidated before the complete
// initial snapshot; later live membership cannot add work to this campaign.
func TestGCStateWarmupInitialTargetsOnly(t *testing.T) {
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	index := newEnabledKeyspaceCache(nil, "")
	w := newGCStateWarmup(m, m.activeGeneration.Load(), index)
	runGCWarmupTest(t, w)
	w.onPage(gcWarmupEntries(100, 1))
	require.Eventually(t, func() bool {
		w.assembly.Lock()
		defer w.assembly.Unlock()
		scope := w.scopes[100]
		return scope != nil && scope.completed
	}, 3*time.Second, time.Millisecond)
	m.gcStateCache.remove(100)
	entries := map[uint32]enabledKeyspace{100: gcWarmupEntries(100, 1)[0]}
	index.publish(context.Background(), entries, 1)
	w.onInitialSnapshot(gcWarmupEntries(100, 1))
	gcWarmupWait(t, w.done)
	index.publish(context.Background(), map[uint32]enabledKeyspace{101: gcWarmupEntries(101, 1)[0]}, 2)
	w.onPage(gcWarmupEntries(101, 1))
	for _, id := range []uint32{100, 101} {
		_, ok := m.gcStateCache.load(id)
		require.False(t, ok)
	}
	require.Nil(t, w.scopes, "temporary completion state must be released")
}

func newGCWarmupRuntimeTest(t *testing.T, intercept grpc.UnaryClientInterceptor) *GCStateManager {
	t.Helper()
	var clientCfg clientv3.Config
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{etcdClientCfgModifier: func(cfg *clientv3.Config) {
		cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(intercept))
		clientCfg = *cfg
	}})
	client, err := clientv3.New(clientCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		m.stopGCStateGeneration(m.activeGeneration.Load())
		require.NoError(t, client.Close())
		cancel()
		clean()
	})
	// The unconfigured manager remains passive in classic deployments.
	require.Nil(t, m.activeGeneration.Load().index)
	m.stopGCStateGeneration(m.activeGeneration.Load())
	m.SetEtcdClient(client)
	return m
}

func TestGCStateWarmupRuntime(t *testing.T) {
	metadataEntered := make(chan struct{}, 1)
	nullEntered := make(chan struct{}, 1)
	releaseMetadata := make(chan struct{})
	releaseNull := make(chan struct{})
	var enabled atomic.Bool
	m := newGCWarmupRuntimeTest(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if enabled.Load() {
			if txn, ok := req.(*etcdserverpb.TxnRequest); ok && len(txn.Success) > 0 {
				if first := txn.Success[0].GetRequestRange(); first != nil {
					switch string(first.Key) {
					case keypath.KeyspaceMetaPrefix():
						metadataEntered <- struct{}{}
						select {
						case <-releaseMetadata:
						case <-ctx.Done():
							return ctx.Err()
						}
					case keypath.TxnSafePointPath(constant.NullKeyspaceID):
						nullEntered <- struct{}{}
						select {
						case <-releaseNull:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	enabled.Store(true)
	stop := m.OnNodeBecomesLeader()
	generation := m.activeGeneration.Load()
	gcWarmupWait(t, metadataEntered)
	gcWarmupWait(t, nullEntered)
	close(releaseMetadata)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, generation.index.waitReady(ctx))
	require.Eventually(t, func() bool { _, ok := m.gcStateCache.load(2); return ok }, 3*time.Second, time.Millisecond)
	// Metadata and ordinary scopes progressed while the null read was held.
	close(releaseNull)
	gcWarmupWait(t, generation.warmup.done)
	select {
	case <-generation.workDone:
		t.Fatal("metadata watch exited with campaign")
	default:
	}
	stop()
	gcWarmupWait(t, generation.workDone)
	enabled.Store(false)
	newStop := m.OnNodeBecomesLeader()
	defer newStop()
	current := m.activeGeneration.Load()
	gcWarmupWait(t, current.warmup.done)
	stop()
	require.Same(t, current, m.activeGeneration.Load())
	_, ok := m.gcStateCache.load(constant.NullKeyspaceID)
	require.True(t, ok)
}

func TestGCStateWarmupCancellationWithWriterAndFullQueue(t *testing.T) {
	entered := make(chan struct{}, 4)
	var enabled atomic.Bool
	m := newGCWarmupRuntimeTest(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if enabled.Load() {
			if txn := gcWarmupRead(req); txn != nil {
				entered <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			if txn, ok := req.(*etcdserverpb.TxnRequest); ok && len(txn.Success) > 0 {
				if first := txn.Success[0].GetRequestRange(); first != nil && string(first.Key) == keypath.KeyspaceMetaPrefix() {
					<-ctx.Done()
					return ctx.Err()
				}
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	enabled.Store(true)
	stop := m.OnNodeBecomesLeader()
	generation := m.activeGeneration.Load()
	generation.warmup.onPage(gcWarmupEntries(100, 256))
	for range 4 {
		gcWarmupWait(t, entered)
	}
	offered := make(chan struct{})
	go func() {
		for range 100 {
			generation.warmup.onPage(gcWarmupEntries(1000, 256))
		}
		close(offered)
	}()
	gcWarmupWait(t, offered)
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		m.mu.Lock()
		m.gcStateCache.remove(100)
		m.mu.Unlock()
		close(writerDone)
	}()
	gcWarmupWait(t, writerStarted)
	require.Eventually(t, func() bool {
		if m.mu.TryRLock() {
			m.mu.RUnlock()
			return false
		}
		return true
	}, 3*time.Second, time.Millisecond, "writer must be queued behind the four readers")
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	gcWarmupWait(t, stopped)
	gcWarmupWait(t, writerDone)
	gcWarmupWait(t, generation.workDone)
	require.Nil(t, generation.warmup.scopes)
	require.Nil(t, generation.warmup.pages)
	require.Nil(t, m.activeGeneration.Load())
}

// A compacted watch rebuilds live membership while an original failed target
// is pending. New, newly-enabled, and newly-independent scopes stay cold.
func TestGCStateWarmupReloadDoesNotExpandTargets(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	loadStarted := make(chan struct{}, 1)
	releaseLoad := make(chan struct{})
	var loadCount atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) == keypath.TxnSafePointPath(100) && loadCount.Add(1) == 1 {
			loadStarted <- struct{}{}
			select {
			case <-releaseLoad:
				return errors.New("initial flight failed")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	foregroundDone := make(chan error, 1)
	go func() {
		_, err := m.loadGCState(context.Background(), m.activeGeneration.Load(), 100)
		foregroundDone <- err
	}()
	gcWarmupWait(t, loadStarted)
	putEnabledKeyspaceTestMeta(t, client, 100, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	putEnabledKeyspaceTestMeta(t, client, 101, keyspacepb.KeyspaceState_DISABLED, keyspace.KeyspaceLevelGC)
	putEnabledKeyspaceTestMeta(t, client, 102, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	index := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	first := true
	index.watcherFactory = func(c *clientv3.Client) clientv3.Watcher {
		watcher := clientv3.NewWatcher(c)
		if !first {
			return watcher
		}
		first = false
		return &pauseBeforeWatch{Watcher: watcher, started: started, release: release}
	}
	w := newGCStateWarmup(m, m.activeGeneration.Load(), index)
	runGCWarmupTest(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	indexDone := make(chan struct{})
	go func() {
		index.run(ctx, enabledKeyspaceLoadHooks{onPage: w.onPage, onInitialSnapshot: w.onInitialSnapshot})
		close(indexDone)
	}()
	t.Cleanup(func() { cancel(); gcWarmupWait(t, indexDone) })
	gcWarmupWait(t, started)
	require.Eventually(t, func() bool {
		w.assembly.Lock()
		defer w.assembly.Unlock()
		scope := w.scopes[100]
		return scope != nil && scope.joined != nil
	}, 3*time.Second, time.Millisecond)
	putEnabledKeyspaceTestMeta(t, client, 101, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	putEnabledKeyspaceTestMeta(t, client, 102, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	latest := putEnabledKeyspaceTestMeta(t, client, 103, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	_, err := client.Compact(ctx, latest, clientv3.WithCompactPhysical())
	require.NoError(t, err)
	close(release)
	entries, _, err := index.snapshotAtLeast(ctx, latest)
	require.NoError(t, err)
	require.Len(t, entries, 4)
	close(releaseLoad)
	require.Error(t, <-foregroundDone)
	gcWarmupWait(t, w.done)
	_, ok := m.gcStateCache.load(100)
	require.True(t, ok)
	for _, id := range []uint32{101, 102, 103} {
		_, ok := m.gcStateCache.load(id)
		require.False(t, ok)
	}
}

func TestGCStateWarmupBeforeMetadataFinalPage(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	putEnabledKeyspaceTestIDs(t, client, enabledKeyspaceTestIDs(300))
	laterPage := make(chan struct{}, 4)
	release := make(chan struct{})
	intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if request, ok := req.(*etcdserverpb.RangeRequest); ok && strings.HasPrefix(string(request.Key), enabledKeyspaceTestPrefix) {
			laterPage <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	index := newEnabledKeyspaceCache(intercepted, enabledKeyspaceTestPrefix)
	w := newGCStateWarmup(m, m.activeGeneration.Load(), index)
	runGCWarmupTest(t, w)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		index.run(ctx, enabledKeyspaceLoadHooks{onPage: w.onPage, onInitialSnapshot: w.onInitialSnapshot})
		close(done)
	}()
	t.Cleanup(func() { cancel(); gcWarmupWait(t, done) })
	gcWarmupWait(t, laterPage)
	require.Eventually(t, func() bool {
		_, first := m.gcStateCache.load(0)
		_, last := m.gcStateCache.load(255)
		return first && last
	}, 3*time.Second, time.Millisecond)
	index.mu.Lock()
	ready := index.ready
	index.mu.Unlock()
	require.False(t, ready)
	close(release)
	gcWarmupWait(t, w.done)
	_, ok := m.gcStateCache.load(299)
	require.True(t, ok)
}

func TestGCStateWarmupRetiresObsoleteTarget(t *testing.T) {
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if txn := gcWarmupRead(req); txn != nil && string(txn.Success[0].GetRequestRange().Key) == keypath.TxnSafePointPath(100) {
			reads.Add(1)
			return errors.New("unavailable original scope")
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	index := newEnabledKeyspaceCache(nil, "")
	index.publish(context.Background(), map[uint32]enabledKeyspace{100: gcWarmupEntries(100, 1)[0]}, 1)
	w := newGCStateWarmup(m, m.activeGeneration.Load(), index)
	w.onInitialSnapshot(gcWarmupEntries(100, 1))
	runGCWarmupTest(t, w)
	require.Eventually(t, func() bool { w.assembly.Lock(); defer w.assembly.Unlock(); return w.waiting[100] != nil }, 3*time.Second, time.Millisecond)
	index.publish(context.Background(), map[uint32]enabledKeyspace{}, 2)
	gcWarmupWait(t, w.done)
	require.Equal(t, int32(1), reads.Load())
	_, ok := m.gcStateCache.load(100)
	require.False(t, ok)
}
