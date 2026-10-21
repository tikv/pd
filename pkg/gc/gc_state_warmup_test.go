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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
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

func TestGCStateWarmupBookkeepingWhileLoaderWaitsForManager(t *testing.T) {
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	started := make(chan struct{})
	var startedOnce sync.Once
	const beforeLock = "github.com/tikv/pd/pkg/gc/beforeGCStateLoadManagerLock"
	require.NoError(t, failpoint.EnableCall(beforeLock, func() { startedOnce.Do(func() { close(started) }) }))
	defer func() { require.NoError(t, failpoint.Disable(beforeLock)) }()
	m.mu.Lock()
	var unlocked sync.Once
	unblock := func() { unlocked.Do(m.mu.Unlock) }
	defer unblock()
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	runGCWarmupTest(t, w)
	gcWarmupWait(t, started)

	// A worker waiting for a safe-point writer must leave scheduling available
	// so delivered metadata can be reconciled without waiting for that writer.
	offered := make(chan struct{})
	go func() {
		w.onPage(gcWarmupEntries(100, 1))
		w.onInitialSnapshot(gcWarmupEntries(100, 1))
		close(offered)
	}()
	gcWarmupWait(t, offered)
	reconciled := make(chan struct{})
	go func() {
		w.stateMu.Lock()
		w.ingestPages()
		w.reconcileInitialSnapshot()
		w.stateMu.Unlock()
		close(reconciled)
	}()
	progressed := false
	select {
	case <-reconciled:
		progressed = true
	case <-time.After(time.Second):
	}
	unblock()
	gcWarmupWait(t, reconciled)
	gcWarmupWait(t, w.done)
	require.True(t, progressed, "metadata bookkeeping blocked behind the manager lock")
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

// Available pages must replenish the preceding page's partial batch. Waiting
// to publish the initial snapshot keeps its reconciliation from hiding a split.
func TestGCStateWarmupAcrossPages(t *testing.T) {
	// Give competing workers time to consume candidates after each miss. A
	// per-candidate assembly lock would split the tail into multiple reads.
	const access = "github.com/tikv/pd/pkg/gc/getGCStateCacheAccess"
	require.NoError(t, failpoint.EnableCall(access, func(string) { runtime.Gosched() }))
	defer func() { require.NoError(t, failpoint.Disable(access)) }()
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
	entries := gcWarmupEntries(100, 512)
	published := make(map[uint32]*atomic.Bool, len(entries))
	for _, entry := range entries {
		published[entry.id] = new(atomic.Bool)
	}
	var remaining atomic.Int32
	remaining.Store(int32(len(entries)))
	allPublished := make(chan struct{})
	const onPublished = "github.com/tikv/pd/pkg/gc/onGCStateLoadPublished"
	require.NoError(t, failpoint.EnableCall(onPublished, func(manager *GCStateManager, id uint32) {
		if manager != m {
			return
		}
		if seen, expected := published[id]; expected && seen.CompareAndSwap(false, true) && remaining.Add(-1) == 0 {
			close(allPublished)
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(onPublished)) }()
	for start := 0; start < len(entries); start += 60 {
		require.NoError(t, m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
			for _, entry := range entries[start:min(start+60, len(entries))] {
				if err := wb.SetTxnSafePoint(entry.id, uint64(entry.id)+1000); err != nil {
					return err
				}
				if err := wb.SetGCSafePoint(entry.id, uint64(entry.id)+500); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	m.gcStateCache.store(constant.NullKeyspaceID, gcStateCacheEntry{})
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	w.onPage(entries[:256])
	w.onPage(entries[256:])
	runGCWarmupTest(t, w)
	gcWarmupWait(t, allPublished)
	w.onInitialSnapshot(entries)
	gcWarmupWait(t, w.done)
	mu.Lock()
	observed := slices.Clone(sizes)
	mu.Unlock()
	require.ElementsMatch(t, []int{60, 60, 60, 60, 60, 60, 60, 60, 32}, observed)
	for _, entry := range entries {
		cached, ok := m.gcStateCache.load(entry.id)
		require.True(t, ok)
		require.Equal(t, gcStateCacheEntry{TxnSafePoint: uint64(entry.id) + 1000, GCSafePoint: uint64(entry.id) + 500}, cached)
	}
}

// Batch assembly consumes delivered pages without reconciling the full snapshot
// under the generation lock. The worker loop later repairs missing page hints.
func TestGCStateWarmupDefersSnapshotReconciliation(t *testing.T) {
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	m.gcStateCache.store(constant.NullKeyspaceID, gcStateCacheEntry{})
	w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
	entries := gcWarmupEntries(100, 8)
	w.onPage(entries[:2])
	w.onPage(entries[2:4])
	w.onInitialSnapshot(entries)

	result, err := m.loadGCStateBatch(context.Background(), w.generation, w.next)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{100, 101, 102, 103}, result.completed)
	require.Empty(t, result.failed)
	require.Empty(t, result.joined)
	require.False(t, w.initialSnapshotReconciled)
	for _, entry := range entries[4:] {
		_, cached := m.gcStateCache.load(entry.id)
		require.False(t, cached)
	}
	w.stateMu.Lock()
	w.recordBatchResult(result, time.Now())
	w.stateMu.Unlock()

	runGCWarmupTest(t, w)
	gcWarmupWait(t, w.done)
	require.True(t, w.initialSnapshotReconciled)
	for _, entry := range entries {
		_, cached := m.gcStateCache.load(entry.id)
		require.True(t, cached)
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

// The configured number of blocked background reads includes the prioritized
// null singleton. Foreground reads remain independent of background capacity.
func TestGCStateWarmupConcurrencyAndForeground(t *testing.T) {
	for _, concurrency := range []int{1, 2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
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
			m.stopGCStateGeneration(m.activeGeneration.Load())
			options := config.NewPersistOptions(&config.Config{PDServerCfg: m.cfg})
			next := options.GetPDServerConfig().Clone()
			next.GCStateCacheWarmupConcurrency = concurrency
			options.SetPDServerConfig(next)
			m.SetConfigGetter(options.GetPDServerConfig)
			m.OnNodeBecomesLeader()
			core, logs := observer.New(zap.InfoLevel)
			restore := log.ReplaceGlobals(zap.New(core), nil)
			defer restore()
			w := newGCStateWarmup(m, m.activeGeneration.Load(), nil)
			// Updating the setting after creation must not resize this campaign.
			next = next.Clone()
			next.GCStateCacheWarmupConcurrency = 8
			options.SetPDServerConfig(next)
			w.onPage(gcWarmupEntries(100, 256))
			w.onInitialSnapshot(gcWarmupEntries(100, 256))
			runGCWarmupTest(t, w)
			sizes := make([]int, 0, concurrency)
			for range concurrency {
				select {
				case n := <-entered:
					sizes = append(sizes, n)
				case <-time.After(5 * time.Second):
					t.Fatal("configured background reads did not overlap")
				}
			}
			require.ElementsMatch(t, []int{1, 60, 60, 60}[:concurrency], sizes)
			started := logs.FilterMessage("start warming GC state cache").All()
			require.Len(t, started, 1)
			require.EqualValues(t, concurrency, started[0].ContextMap()["concurrency"])
			bookkeeping := make(chan struct{})
			go func() {
				w.stateMu.Lock()
				w.refresh(time.Now())
				w.stateMu.Unlock()
				close(bookkeeping)
			}()
			gcWarmupWait(t, bookkeeping)
			foreground := make(chan error, 1)
			go func() { _, err := m.GetGCState(2, true); foreground <- err }()
			select {
			case err := <-foreground:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("foreground waited for background capacity")
			}
			require.Equal(t, int32(concurrency), maxActive.Load())
			unblock()
			gcWarmupWait(t, w.done)
			require.Equal(t, int32(concurrency), maxActive.Load())
			completed := logs.FilterMessage("warming GC state cache completed").All()
			require.Len(t, completed, 1)
			require.EqualValues(t, concurrency, completed[0].ContextMap()["concurrency"])
		})
	}
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
	var releaseOnce sync.Once
	unblockLoad := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblockLoad()
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
		w.stateMu.Lock()
		defer w.stateMu.Unlock()
		scope := w.scopes[100]
		return scope != nil && scope.joined != nil
	}, 3*time.Second, time.Millisecond)
	// Pause scheduling outside every lock until the joined flight has completed
	// and its cache entry has been invalidated. Bookkeeping must then observe the
	// terminal success even though the cached value is gone.
	paused := make(chan struct{}, w.concurrency)
	resume := make(chan struct{})
	var resumeOnce sync.Once
	unblockWarmup := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblockWarmup()
	const schedule = "github.com/tikv/pd/pkg/gc/beforeGCStateWarmupSchedule"
	require.NoError(t, failpoint.EnableCall(schedule, func() {
		select {
		case <-resume:
			return
		default:
		}
		paused <- struct{}{}
		<-resume
	}))
	defer func() { require.NoError(t, failpoint.Disable(schedule)) }()
	w.notify()
	for range w.concurrency {
		gcWarmupWait(t, paused)
	}
	unblockLoad()
	err := <-foreground
	m.mu.Lock()
	m.gcStateCache.remove(100)
	m.mu.Unlock()
	unblockWarmup()
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
	index := newEnabledKeyspaceIndex(nil, "")
	w := newGCStateWarmup(m, m.activeGeneration.Load(), index)
	runGCWarmupTest(t, w)
	w.onPage(gcWarmupEntries(100, 1))
	require.Eventually(t, func() bool {
		w.stateMu.Lock()
		defer w.stateMu.Unlock()
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
	options := config.NewPersistOptions(&config.Config{PDServerCfg: m.cfg})
	next := options.GetPDServerConfig().Clone()
	next.GCStateCacheWarmupConcurrency = 2
	options.SetPDServerConfig(next)
	m.SetConfigGetter(options.GetPDServerConfig)
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
	require.Equal(t, 2, generation.warmup.concurrency)
	next = next.Clone()
	next.GCStateCacheWarmupConcurrency = 1
	options.SetPDServerConfig(next)
	require.Same(t, generation.warmup, m.activeGeneration.Load().warmup)
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
	require.Equal(t, 1, current.warmup.concurrency)
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
	index := newEnabledKeyspaceIndex(client, enabledKeyspaceTestPrefix)
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
		w.stateMu.Lock()
		defer w.stateMu.Unlock()
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
	index := newEnabledKeyspaceIndex(intercepted, enabledKeyspaceTestPrefix)
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
	index := newEnabledKeyspaceIndex(nil, "")
	index.publish(context.Background(), map[uint32]enabledKeyspace{100: gcWarmupEntries(100, 1)[0]}, 1)
	w := newGCStateWarmup(m, m.activeGeneration.Load(), index)
	w.onInitialSnapshot(gcWarmupEntries(100, 1))
	runGCWarmupTest(t, w)
	require.Eventually(t, func() bool {
		w.stateMu.Lock()
		defer w.stateMu.Unlock()
		return w.scopes[100] != nil && w.scopes[100].retryDelay > 0
	}, 3*time.Second, time.Millisecond)
	index.publish(context.Background(), map[uint32]enabledKeyspace{}, 2)
	gcWarmupWait(t, w.done)
	require.Equal(t, int32(1), reads.Load())
	_, ok := m.gcStateCache.load(100)
	require.False(t, ok)
}
