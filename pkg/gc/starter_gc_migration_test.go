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
	manager, dest, _, revision := newStarterGCFixture(t, len(cases), func(dest, source *clientv3.Client) {
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
				require.Greater(t, resp.Kvs[0].ModRevision, revision)
			} else if tc.destination == missing {
				require.Empty(t, resp.Kvs)
			} else {
				require.Len(t, resp.Kvs, 1)
				require.Equal(t, tc.destination, string(resp.Kvs[0].Value))
				require.LessOrEqual(t, resp.Kvs[0].ModRevision, revision)
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
	first := start(ctx, 1)
	gcWarmupWait(t, reached)
	// A competing first use can commit and advance T while this one holds a stale snapshot.
	require.NoError(t, <-start(ctx, 1))
	require.NoError(t, <-start(ctx, 2))
	_, err := manager.AdvanceTxnSafePoint(1, 350, time.Now())
	require.NoError(t, err)
	_, _, err = manager.AdvanceGCSafePoint(1, 240)
	require.NoError(t, err)
	_, advancedRevision := readStarterSafePoint(t, manager, 1)
	release()
	require.NoError(t, <-first)
	txn, rev := readStarterSafePoint(t, manager, 1)
	require.EqualValues(t, 350, txn)
	require.Equal(t, advancedRevision, rev)
}

func TestStarterGCMigrationAmbiguousCommit(t *testing.T) {
	var loseReply atomic.Bool
	manager, dest, _, _ := newStarterGCFixture(t, 1, nil, func(cfg *clientv3.Config) {
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
	ctx := context.Background()
	require.NoError(t, manager.ensureMigratedTxnSafePoint(ctx, manager.activeGeneration.Load(), 1))
	require.False(t, loseReply.Load())
	resp, err := dest.Get(ctx, keypath.TxnSafePointPath(1))
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "101", string(resp.Kvs[0].Value))
	require.EqualValues(t, 2, resp.Kvs[0].Version)
}

func TestStarterGCMigrationSameBackend(t *testing.T) {
	ctx := t.Context()
	manager, dest, _, markerRevision := newStarterGCFixture(t, 2, func(dest, _ *clientv3.Client) {
		// Keyspace 1 uses the default PD backend. Keyspace 2 uses an explicit group on the same etcd.
		setStarterSourceGroup(t, dest, 1, "")
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
		require.Greater(t, first[id], markerRevision)
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
	manager, dest, source, markerRev := newStarterGCAPIFixture(t, 1)
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
	require.LessOrEqual(t, revision, markerRev)
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
	require.Greater(t, revision, markerRev)

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
	manager, _, _, markerRev := newStarterGCAPIFixture(t, 2)
	ctx, generation := t.Context(), manager.activeGeneration.Load()
	// An index or earlier metadata snapshot already queued these two independent
	// scopes when the control plane changed one to unified GC and retired the other.
	next := gcStateLoadCandidates(1, 2)
	_, err := manager.keyspaceManager.UpdateKeyspaceConfig("ks1", []*keyspace.Mutation{{Op: keyspace.OpPut, Key: keyspace.GCManagementType, Value: keyspace.UnifiedGC}})
	require.NoError(t, err)
	for _, state := range []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE} {
		_, err = manager.keyspaceManager.UpdateKeyspaceStateByID(2, state, time.Now().Unix())
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
		require.LessOrEqual(t, revision, markerRev)
	}
}

func setStarterSourceGroup(t *testing.T, dest *clientv3.Client, id uint32, group string) {
	t.Helper()
	key := keypath.KeyspaceMetaPath(id)
	resp, err := dest.Get(t.Context(), key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	var meta keyspacepb.KeyspaceMeta
	require.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &meta))
	if group == "" {
		delete(meta.Config, keyspace.MetaServiceGroupIDKey)
	} else {
		meta.Config[keyspace.MetaServiceGroupIDKey] = group
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

func TestStarterGCMigrationMixedLoadBatch(t *testing.T) {
	ctx := t.Context()
	var localReads [][]string
	var readsMu sync.Mutex
	m, dest, source, marker := newStarterGCFixture(t, 7, func(dest, source *clientv3.Client) {
		// Keyspaces 1 and 2 use the second group. Keyspace 7 uses the PD default.
		setStarterSourceGroup(t, dest, 1, "group2")
		setStarterSourceGroup(t, dest, 2, "group2")
		setStarterSourceGroup(t, dest, 7, "")
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
	// The first group is remote. The second uses the target etcd and will reject the batch.
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
	healthy, unavailable := record("group1"), record("group2")
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	unavailable.beforeRead = func() error { close(entered); <-resume; return errors.New("group2 unavailable") }
	_, err = dest.Put(ctx, keypath.TxnSafePointPath(6), "106") // confirmed before this load
	require.NoError(t, err)
	done := make(chan gcStateLoadResult, 1)
	go func() {
		r, err := m.loadGCStateBatch(ctx, m.activeGeneration.Load(), gcStateLoadCandidates(1, 2, 3, 4, 5, 6, 7))
		if err != nil {
			r.failed = map[uint32]error{0: err}
		}
		done <- r
	}()
	gcWarmupWait(t, entered)
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	state, err := m.getGCStateImpl(readCtx, 3, true)
	require.NoError(t, err)
	require.EqualValues(t, 103, state.TxnSafePoint)
	unblock()
	result := <-done
	readsMu.Lock()
	reads := localReads
	readsMu.Unlock()
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(3), keypath.GCSafePointPath(3), keypath.TxnSafePointPath(4), keypath.GCSafePointPath(4)}}, reads)
	require.ElementsMatch(t, []uint32{3, 4, 6, 7}, result.completed)
	require.Len(t, result.failed, 3)
	for _, id := range []uint32{1, 2, 5} {
		require.Error(t, result.failed[id])
		_, rev := readStarterSafePoint(t, m, id)
		require.LessOrEqual(t, rev, marker)
	}
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(3), keypath.TxnSafePointPath(4), keypath.TxnSafePointPath(5)}}, healthy.requests)
	require.Equal(t, [][]string{{keypath.TxnSafePointPath(1), keypath.TxnSafePointPath(2)}}, unavailable.requests)
	for id, want := range map[uint32]uint64{3: 103, 4: 104, 6: 106, 7: 10} {
		value, rev := readStarterSafePoint(t, m, id)
		require.Equal(t, want, value)
		require.Greater(t, rev, marker)
	}
}
