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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

func readStarterSafePoint(t *testing.T, m *GCStateManager, id uint32) (uint64, int64) {
	t.Helper()
	rows, err := m.gcMetaStorage.LoadGCSafePointPairs(t.Context(), []uint32{id})
	require.NoError(t, err)
	require.NoError(t, rows[0].Err)
	return rows[0].TxnSafePoint, rows[0].TxnSafePointRevision
}

func TestStarterGCMigrationNativeReadCosts(t *testing.T) {
	var cutoffReads, pairReads atomic.Int64
	opt := newGCStateManagerForTestOptions{
		useEnabledKeyspaceIndex: true,
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if r, ok := req.(*etcdserverpb.RangeRequest); ok && string(r.Key) == keypath.GCMigrationCutoffPath() {
					cutoffReads.Add(1)
				}
				if gcWarmupRead(req) != nil {
					pairReads.Add(1)
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			}))
		},
	}
	opt.generateKeyspacesByCount(1)
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, opt)
	t.Cleanup(clean)
	t.Cleanup(cancel)
	for range 2 {
		stop := manager.OnNodeBecomesLeader()
		t.Cleanup(stop)
		gcWarmupWait(t, manager.activeGeneration.Load().warmup.done)
		reads := pairReads.Load()
		// A cold load costs one batch read. Subsequent cache hits cost none.
		manager.gcStateCache.remove(1)
		for range 5 {
			_, err := manager.GetGCState(1, true)
			require.NoError(t, err)
		}
		require.Equal(t, reads+1, pairReads.Load())
		stop()
	}
	require.EqualValues(t, 1, cutoffReads.Load())
}

func TestStarterGCMigrationStartup(t *testing.T) {
	_, provider, native, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceIndex: true})
	defer clean()
	defer cancel()
	ctx, client := t.Context(), native.etcdClient
	native.stopGCStateGeneration(native.activeGeneration.Load())
	canceled, cancelRead := context.WithCancel(ctx)
	cancelRead()
	_, err := LoadMigrationCutoff(canceled, client)
	require.Error(t, err)
	key := keypath.GCMigrationCutoffPath()
	_, err = client.Put(ctx, key, "unknown")
	require.NoError(t, err)
	_, err = LoadMigrationCutoff(ctx, client)
	require.ErrorContains(t, err, "invalid starter GC migration cutoff")
	// Rewriting the cutoff key violates its create-once contract even with a valid value.
	_, err = client.Put(ctx, key, "1")
	require.NoError(t, err)
	_, err = LoadMigrationCutoff(ctx, client)
	require.ErrorContains(t, err, "invalid starter GC migration cutoff")
	_, err = client.Delete(ctx, key)
	require.NoError(t, err)
	lease, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = client.Put(ctx, key, "1", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	_, err = LoadMigrationCutoff(ctx, client)
	require.ErrorContains(t, err, "invalid starter GC migration cutoff")
	_, err = client.Delete(ctx, key)
	require.NoError(t, err)

	_, err = client.Put(ctx, keypath.TxnSafePointPath(2), "42")
	require.NoError(t, err)
	put, err := client.Put(ctx, key, "1")
	require.NoError(t, err)
	cutoff, err := LoadMigrationCutoff(ctx, client)
	require.NoError(t, err)
	require.Equal(t, put.Header.Revision, cutoff)
	upgraded := NewGCStateManager(provider, *native.getConfig(), native.keyspaceManager, native.metaServiceGroupManager, client, cutoff)
	defer upgraded.CloseBarrierMetrics()
	_, err = upgraded.GetGCState(2, true)
	require.ErrorIs(t, err, errs.ErrNotLeader)
	stop := upgraded.OnNodeBecomesLeader()
	defer stop()
	state, err := upgraded.GetGCState(2, true)
	require.NoError(t, err)
	require.EqualValues(t, 42, state.TxnSafePoint)
	_, revision := readStarterSafePoint(t, upgraded, 2)
	require.Greater(t, revision, put.Header.Revision)
	stop()
	_, err = upgraded.GetGCState(2, true)
	require.ErrorIs(t, err, errs.ErrNotLeader)
}

func TestStarterGCMigrationSourceValues(t *testing.T) {
	const missing = "(missing)"
	cases := []struct {
		name, destination, source string
		gc                        uint64
		want, wantError           string
	}{
		{"target behind G", "10", "150", 120, "150", ""},
		{"equal", "150", "150", 150, "150", ""},
		{"empty scope", missing, missing, 0, "0", ""},
		{"source missing with target zero", "0", missing, 0, "0", ""},
		{"explicit source zero", missing, "0", 0, "0", ""},
		{"max uint64", missing, "18446744073709551615", 0, "18446744073709551615", ""},
		{"target ahead", "151", "150", 0, "", "exceeds frozen"},
		{"G ahead", "100", "150", 151, "", "exceeds frozen"},
		{"target with missing source", "1", missing, 0, "", "exceeds frozen"},
		{"G with missing source", missing, missing, 1, "", "exceeds frozen"},
		{"empty target", "", "100", 0, "", "empty transaction safe point"},
		{"malformed target", "bad", "100", 0, "", "invalid syntax"},
		{"empty source", "0", "", 0, "", "invalid transaction safe point"},
		{"overflowing source", missing, "18446744073709551616", 0, "", "invalid transaction safe point"},
	}
	ctx := context.Background()
	manager, dest, _, cutoff := newStarterGCFixture(t, len(cases), func(dest, source *clientv3.Client) {
		for i, tc := range cases {
			id := uint32(i + 1)
			key := keypath.TxnSafePointPath(id)
			for client, value := range map[*clientv3.Client]string{source: tc.source, dest: tc.destination} {
				var err error
				if value == missing {
					_, err = client.Delete(ctx, key)
				} else {
					_, err = client.Put(ctx, key, value)
				}
				require.NoError(t, err)
			}
			_, err := dest.Put(ctx, keypath.GCSafePointPath(id), fmt.Sprintf(`{"safe_point":%d}`, tc.gc))
			require.NoError(t, err)
		}
	}, nil)
	generation := manager.activeGeneration.Load()
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := uint32(i + 1)
			err := manager.ensureMigratedTxnSafePoint(ctx, generation, id)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			resp, err := dest.Get(ctx, keypath.TxnSafePointPath(id))
			require.NoError(t, err)
			if tc.wantError == "" {
				require.Len(t, resp.Kvs, 1)
				require.Equal(t, tc.want, string(resp.Kvs[0].Value))
				require.Greater(t, resp.Kvs[0].ModRevision, cutoff)
			} else if tc.destination == missing {
				require.Empty(t, resp.Kvs)
			} else {
				require.Len(t, resp.Kvs, 1)
				require.Equal(t, tc.destination, string(resp.Kvs[0].Value))
				require.LessOrEqual(t, resp.Kvs[0].ModRevision, cutoff)
			}
		})
	}
}

func TestStarterGCMigrationConcurrentTakeover(t *testing.T) {
	ctx := context.Background()
	manager, _, _, _ := newStarterGCAPIFixture(t, 2)
	generation := manager.activeGeneration.Load()
	const hook = "github.com/tikv/pd/pkg/gc/starterGCMigrationSourceLoaded"
	reached, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	release := sync.OnceFunc(func() { close(resume) })
	require.NoError(t, failpoint.EnableCall(hook, func(id uint32) {
		if id == 1 && calls.Add(1) == 1 {
			close(reached)
			<-resume
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(hook)) }()
	defer release()

	start := func(ctx context.Context, id uint32) <-chan error {
		done := make(chan error, 1)
		go func() { done <- manager.ensureMigratedTxnSafePoint(ctx, generation, id) }()
		return done
	}
	wait := func(request string, done <-chan error) {
		t.Helper()
		select {
		case err := <-done:
			require.NoErrorf(t, err, "%s failed", request)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", request)
		}
	}
	first := start(ctx, 1)
	gcWarmupWait(t, reached)
	// A competing first use can commit and advance T while this one holds a stale snapshot.
	wait("competing migration for keyspace 1", start(ctx, 1))
	wait("migration for keyspace 2", start(ctx, 2))
	_, err := manager.AdvanceTxnSafePoint(1, 350, time.Now())
	require.NoError(t, err)
	_, _, err = manager.AdvanceGCSafePoint(1, 240)
	require.NoError(t, err)
	_, advancedRevision := readStarterSafePoint(t, manager, 1)
	release()
	wait("paused migration for keyspace 1", first)
	txn, rev := readStarterSafePoint(t, manager, 1)
	require.EqualValues(t, 350, txn)
	require.Equal(t, advancedRevision, rev)
}

func TestStarterGCMigrationFullBatchAmbiguousCommit(t *testing.T) {
	const count = endpoint.MaxGCSafePointBatchSize
	var loseReply atomic.Bool
	manager, dest, _, cutoff := newStarterGCFixture(t, count, nil, func(cfg *clientv3.Config) {
		cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoke(ctx, method, req, reply, cc, opts...)
			txn, ok := req.(*etcdserverpb.TxnRequest)
			if err == nil && ok && len(txn.Compare) > 0 && len(txn.Success) > 0 {
				if put := txn.Success[0].GetRequestPut(); put != nil && string(put.Key) == keypath.TxnSafePointPath(1) && loseReply.CompareAndSwap(true, false) {
					return errors.New("lost migration commit response")
				}
			}
			return err
		}))
	})
	loseReply.Store(true)
	ids := make([]uint32, count)
	for i := range ids {
		ids[i] = uint32(i + 1)
	}
	ctx := t.Context()
	result, err := manager.loadGCStateBatch(ctx, manager.activeGeneration.Load(), gcStateLoadCandidates(ids...))
	require.NoError(t, err)
	require.Empty(t, result.failed)
	require.ElementsMatch(t, ids, result.completed)
	require.False(t, loseReply.Load())
	// All 60 writes landed in the same transaction, and none was repeated after the lost reply.
	var revision int64
	for _, id := range ids {
		resp, err := dest.Get(ctx, keypath.TxnSafePointPath(id))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		row := resp.Kvs[0]
		require.Equal(t, strconv.FormatUint(uint64(100+id), 10), string(row.Value))
		require.EqualValues(t, 2, row.Version)
		if revision == 0 {
			revision = row.ModRevision
			require.Greater(t, revision, cutoff)
		}
		require.Equal(t, revision, row.ModRevision)
	}
}

func TestStarterGCMigrationBatchWithChangingRoute(t *testing.T) {
	m, dest, _, cutoff := newStarterGCAPIFixture(t, 2)
	const hook = "github.com/tikv/pd/pkg/gc/starterGCMigrationSourceLoaded"
	require.NoError(t, failpoint.EnableCall(hook, func(firstID uint32) {
		if firstID == 1 {
			// Change the route revision after every source read for keyspace 1.
			setStarterKeyspaceConfig(t, dest, 1, keyspace.MetaServiceGroupIDKey, "group1")
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(hook)) }()

	result, err := m.loadGCStateBatch(t.Context(), m.activeGeneration.Load(), gcStateLoadCandidates(1, 2))
	require.NoError(t, err)
	require.ErrorIs(t, result.failed[1], errs.ErrEtcdTxnConflict)
	require.Equal(t, []uint32{2}, result.completed)
	value, rev := readStarterSafePoint(t, m, 1)
	require.EqualValues(t, 10, value)
	require.LessOrEqual(t, rev, cutoff)
	value, rev = readStarterSafePoint(t, m, 2)
	require.EqualValues(t, 102, value)
	require.Greater(t, rev, cutoff)
}

func TestStarterGCMigrationSameBackend(t *testing.T) {
	ctx := t.Context()
	manager, dest, _, cutoff := newStarterGCFixture(t, 2, func(dest, _ *clientv3.Client) {
		// Keyspace 1 uses the default PD backend. Keyspace 2 uses an explicit group on the same etcd.
		setStarterKeyspaceConfig(t, dest, 1, keyspace.MetaServiceGroupIDKey, "")
	}, nil)
	groups := map[string]string{"group1": strings.Join(dest.Endpoints(), ",")}
	cfg, err := json.Marshal(&config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: groups}})
	require.NoError(t, err)
	require.NoError(t, manager.metaServiceGroupManager.UpdateGroupsSafely(ctx, groups, nil, func() error {
		_, err := dest.Put(ctx, keypath.ConfigPath(), string(cfg))
		return err
	}, nil))

	readRevision := func(id uint32) int64 {
		t.Helper()
		state, err := manager.GetGCState(id, true)
		require.NoError(t, err)
		require.EqualValues(t, 10, state.TxnSafePoint)
		_, revision := readStarterSafePoint(t, manager, id)
		return revision
	}
	first := make(map[uint32]int64)
	for _, id := range []uint32{1, 2} {
		first[id] = readRevision(id)
		require.Greater(t, first[id], cutoff)
	}

	// The next leader must serve both values without contacting the group or writing T again.
	require.NoError(t, manager.metaServiceGroupManager.Close())
	t.Cleanup(manager.OnNodeBecomesLeader())
	for id, revision := range first {
		require.Equal(t, revision, readRevision(id))
	}
}

func TestStarterGCMigrationGroupConfigChangedBeforeCommit(t *testing.T) {
	ctx := t.Context()
	manager, dest, source, cutoff := newStarterGCAPIFixture(t, 1)
	watcher, err := manager.WatchGCStates(ctx, true)
	require.NoError(t, err)
	defer watcher.Close()

	// Pause after borrowing a group client and reading its frozen T, before
	// the migration has a chance to commit with that configuration revision.
	const hook = "github.com/tikv/pd/pkg/gc/starterGCMigrationSourceLoaded"
	reached, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	pause := sync.OnceFunc(func() {
		close(reached)
		<-resume
	})
	require.NoError(t, failpoint.EnableCall(hook, func(uint32) { pause() }))
	defer func() { require.NoError(t, failpoint.Disable(hook)) }()
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := manager.GetGCState(1, true)
		done <- err
	}()
	gcWarmupWait(t, reached)

	// Route through an alias of the same backend. Simulate the interval
	// after config persistence but before this PD has reloaded its clients.
	aliases := map[string]string{"group1": strings.TrimPrefix(source.Endpoints()[0], "http://")}
	require.NotEqual(t, manager.metaServiceGroupManager.GetGroups()["group1"], aliases["group1"])
	cfg, err := json.Marshal(&config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: aliases}})
	require.NoError(t, err)
	_, err = dest.Put(ctx, keypath.ConfigPath(), string(cfg))
	require.NoError(t, err)
	release()
	require.ErrorIs(t, <-done, errs.ErrEtcdTxnConflict)
	value, revision := readStarterSafePoint(t, manager, 1)
	require.EqualValues(t, 10, value)
	require.LessOrEqual(t, revision, cutoff)
	_, cached := manager.gcStateCache.load(1)
	require.False(t, cached)
	_, published, err := watcher.receiveOne(false)
	require.NoError(t, err)
	require.False(t, published)

	require.NoError(t, manager.metaServiceGroupManager.UpdateGroupsSafely(ctx, aliases, nil, func() error { return nil }, nil))
	state, err := manager.GetGCState(1, true)
	require.NoError(t, err)
	require.EqualValues(t, 101, state.TxnSafePoint)
	_, revision = readStarterSafePoint(t, manager, 1)
	require.Greater(t, revision, cutoff)

	// A confirmed scope also survives a cache miss with its source offline.
	require.NoError(t, manager.metaServiceGroupManager.Close())
	for range 2 {
		state, err = manager.GetGCState(1, true)
		require.NoError(t, err)
		require.EqualValues(t, 101, state.TxnSafePoint)
		manager.gcStateCache.remove(1)
	}
}

func TestStarterGCMigrationStaleScope(t *testing.T) {
	manager, dest, _, cutoff := newStarterGCAPIFixture(t, 2)
	ctx, generation := t.Context(), manager.activeGeneration.Load()
	// The loader's candidates came from an earlier snapshot. Seed the metadata
	// it will actually read without using the API to change an immutable GC mode.
	next := gcStateLoadCandidates(1, 2)
	setStarterKeyspaceConfig(t, dest, 1, keyspace.GCManagementType, keyspace.UnifiedGC)
	for _, state := range []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE} {
		_, err := manager.keyspaceManager.UpdateKeyspaceStateByID(2, state, time.Now().Unix())
		require.NoError(t, err)
	}
	result, err := manager.loadGCStateBatch(ctx, generation, next)
	require.NoError(t, err)
	require.Empty(t, result.completed)
	require.ErrorIs(t, result.failed[1], errs.ErrGCOnInvalidKeyspace)
	require.ErrorIs(t, result.failed[2], errs.ErrKeyspaceNotFound)
	for _, id := range []uint32{1, 2} {
		_, cached := manager.gcStateCache.load(id)
		require.False(t, cached)
		_, revision := readStarterSafePoint(t, manager, id)
		require.LessOrEqual(t, revision, cutoff)
	}
}

func setStarterKeyspaceConfig(t *testing.T, dest *clientv3.Client, id uint32, field, value string) {
	t.Helper()
	key := keypath.KeyspaceMetaPath(id)
	resp, err := dest.Get(t.Context(), key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	var meta keyspacepb.KeyspaceMeta
	require.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &meta))
	if value == "" {
		delete(meta.Config, field)
	} else {
		meta.Config[field] = value
	}
	data, err := proto.Marshal(&meta)
	require.NoError(t, err)
	_, err = dest.Put(t.Context(), key, string(data))
	require.NoError(t, err)
}

// Observe the actual KV RPCs issued by a group's etcd client.
type recordingStarterKV struct {
	etcdserverpb.KVClient
	requests   [][]string
	beforeRead func() error
}

func (c *recordingStarterKV) Txn(ctx context.Context, req *etcdserverpb.TxnRequest, opts ...grpc.CallOption) (*etcdserverpb.TxnResponse, error) {
	var keys []string
	for _, op := range req.Success {
		keys = append(keys, string(op.GetRequestRange().GetKey()))
	}
	c.requests = append(c.requests, keys)
	if c.beforeRead != nil {
		if err := c.beforeRead(); err != nil {
			return nil, err
		}
	}
	return c.KVClient.Txn(ctx, req, opts...)
}

func recordStarterGroups(t *testing.T, m *GCStateManager, dest, source *clientv3.Client) (group1, group2 *recordingStarterKV) {
	t.Helper()
	ctx := t.Context()
	groups := map[string]string{"group1": source.Endpoints()[0], "group2": dest.Endpoints()[0]}
	cfg, err := json.Marshal(&config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: groups}})
	require.NoError(t, err)
	require.NoError(t, m.metaServiceGroupManager.UpdateGroupsSafely(ctx, groups, nil, func() error {
		_, err := dest.Put(ctx, keypath.ConfigPath(), string(cfg))
		return err
	}, nil))
	record := func(group string) *recordingStarterKV {
		client, _, err := m.metaServiceGroupManager.GetEtcdClientWithConfigRevision(ctx, group)
		require.NoError(t, err)
		rpc := &recordingStarterKV{KVClient: etcdserverpb.NewKVClient(client.ActiveConnection())}
		client.KV = clientv3.NewKVFromKVClient(rpc, client)
		return rpc
	}
	return record("group1"), record("group2")
}

func TestStarterGCMigrationWarmupGroupConcurrency(t *testing.T) {
	m, dest, source, _ := newStarterGCFixture(t, 2, func(dest, _ *clientv3.Client) {
		setStarterKeyspaceConfig(t, dest, 2, keyspace.MetaServiceGroupIDKey, "group2")
	}, nil)

	group1, group2 := recordStarterGroups(t, m, dest, source)
	entered, resume := make(chan struct{}, 2), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	blockRead := func() error {
		entered <- struct{}{}
		<-resume
		return nil
	}
	group1.beforeRead, group2.beforeRead = blockRead, blockRead

	stop := startStarterWarmup(m)
	defer stop()
	defer unblock()
	gcWarmupWait(t, entered)
	select {
	case <-entered:
		t.Fatal("a second source read started while the only warmup worker was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	gcWarmupWait(t, m.activeGeneration.Load().warmup.done)
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(1)}}, group1.requests)
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(2)}}, group2.requests)
}

func TestStarterGCMigrationMixedLoadBatch(t *testing.T) {
	ctx := t.Context()
	var localReads [][]string
	var readsMu sync.Mutex
	m, dest, source, cutoff := newStarterGCFixture(t, 8, func(dest, source *clientv3.Client) {
		// Keyspaces 1 and 2 use the second group. Keyspace 7 uses the PD default.
		setStarterKeyspaceConfig(t, dest, 1, keyspace.MetaServiceGroupIDKey, "group2")
		setStarterKeyspaceConfig(t, dest, 2, keyspace.MetaServiceGroupIDKey, "group2")
		setStarterKeyspaceConfig(t, dest, 7, keyspace.MetaServiceGroupIDKey, "")
		_, err := source.Put(ctx, keypath.TxnSafePointPath(5), "invalid")
		require.NoError(t, err)
	}, func(cfg *clientv3.Config) {
		cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			// The initial read starts at keyspace 1. Record subsequent snapshot reads for the healthy group.
			if txn, ok := req.(*etcdserverpb.TxnRequest); ok && len(txn.Compare) == 0 && len(txn.Success) > 0 {
				first := string(txn.Success[0].GetRequestRange().GetKey())
				if first == keypath.TxnSafePointPath(3) || first == keypath.TxnSafePointPath(4) {
					keys := make([]string, 0, len(txn.Success))
					for _, op := range txn.Success {
						keys = append(keys, string(op.GetRequestRange().GetKey()))
					}
					readsMu.Lock()
					localReads = append(localReads, keys)
					readsMu.Unlock()
				}
			}
			return invoke(ctx, method, req, reply, cc, opts...)
		}))
	})
	healthy, unavailable := recordStarterGroups(t, m, dest, source)
	// Advance G after the initial local snapshot, while the group read is in flight.
	healthy.beforeRead = func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
			return wb.SetGCSafePoint(8, 109)
		})
	}
	unavailable.beforeRead = func() error { return errors.New("group2 unavailable") }
	_, err := dest.Put(ctx, keypath.TxnSafePointPath(6), "106") // confirmed before this load
	require.NoError(t, err)
	result, err := m.loadGCStateBatch(ctx, m.activeGeneration.Load(), gcStateLoadCandidates(1, 2, 3, 4, 5, 6, 7, 8))
	require.NoError(t, err)
	readsMu.Lock()
	reads := localReads
	readsMu.Unlock()
	healthyKeys := []string{keypath.TxnSafePointPath(3), keypath.GCSafePointPath(3), keypath.TxnSafePointPath(4), keypath.GCSafePointPath(4)}
	require.Equal(t, [][]string{append(slices.Clone(healthyKeys), keypath.TxnSafePointPath(8), keypath.GCSafePointPath(8)), healthyKeys}, reads)
	require.ElementsMatch(t, []uint32{3, 4, 6, 7}, result.completed)
	require.Len(t, result.failed, 4)
	require.ErrorContains(t, result.failed[8], "GC safe point 109 exceeds frozen transaction safe point 108")
	for _, id := range []uint32{1, 2, 5, 8} {
		require.Error(t, result.failed[id])
		_, rev := readStarterSafePoint(t, m, id)
		require.LessOrEqual(t, rev, cutoff)
	}
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(3), keypath.TxnSafePointPath(4), keypath.TxnSafePointPath(5), keypath.TxnSafePointPath(8)}}, healthy.requests)
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(1), keypath.TxnSafePointPath(2)}}, unavailable.requests)
	for id, want := range map[uint32]uint64{3: 103, 4: 104, 6: 106, 7: 10} {
		value, rev := readStarterSafePoint(t, m, id)
		require.Equal(t, want, value)
		require.Greater(t, rev, cutoff)
	}
}
