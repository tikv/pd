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
		{"target with missing source", "1", missing, 0, "", "frozen transaction safe point is missing"},
		{"G with missing source", missing, missing, 1, "", "frozen transaction safe point is missing"},
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
	manager, dest, _, markerRev := newStarterGCAPIFixture(t, 3)
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
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	first := start(readCtx, 1)
	gcWarmupWait(t, reached)
	second := start(ctx, 1)
	cancel()
	require.ErrorIs(t, <-first, context.Canceled)
	// An unrelated scope can finish while the first frozen source read is paused.
	require.NoError(t, <-start(ctx, 2))
	// Simulate another leader winning and already advancing the target before the
	// late source reader's CAS. A retry must use that local value.
	advanced, err := dest.Put(ctx, keypath.TxnSafePointPath(1), "350")
	require.NoError(t, err)
	// An advanced G must not turn the stale source snapshot into a data error.
	_, err = dest.Put(ctx, keypath.GCSafePointPath(1), `{"safe_point":240}`)
	require.NoError(t, err)
	release()
	require.NoError(t, <-second)
	require.EqualValues(t, 1, calls.Load())
	txn, rev, err := manager.gcMetaStorage.LoadTxnSafePointWithRevision(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 350, txn)
	require.Equal(t, advanced.Header.Revision, rev)

	// A different election of the same member must fence the old term even
	// before the old process has noticed its loss of leadership.
	_, err = dest.Put(ctx, keypath.ElectionPath(nil), "member1")
	require.NoError(t, err)
	err = manager.ensureMigratedTxnSafePoint(ctx, generation, 3)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	_, rev, err = manager.gcMetaStorage.LoadTxnSafePointWithRevision(ctx, 3)
	require.NoError(t, err)
	require.LessOrEqual(t, rev, markerRev)
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

func TestStarterGCMigrationInPlace(t *testing.T) {
	ctx := context.Background()
	manager, dest, _, markerRevision := newStarterGCFixture(t, 3, func(dest, _ *clientv3.Client) {
		for _, id := range []uint32{1, 2} {
			key := keypath.KeyspaceMetaPath(id)
			resp, err := dest.Get(ctx, key)
			require.NoError(t, err)
			require.Len(t, resp.Kvs, 1)
			var meta keyspacepb.KeyspaceMeta
			require.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &meta))
			delete(meta.Config, keyspace.MetaServiceGroupIDKey)
			data, err := proto.Marshal(&meta)
			require.NoError(t, err)
			_, err = dest.Put(ctx, key, string(data))
			require.NoError(t, err)
		}
	}, nil)
	groups := map[string]string{"group1": strings.Join(dest.Endpoints(), ",")}
	cfg, err := json.Marshal(&config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: groups}})
	require.NoError(t, err)
	require.NoError(t, manager.metaServiceGroupManager.UpdateGroupsSafely(ctx, groups, nil, func() error {
		_, err := dest.Put(ctx, keypath.ConfigPath(), string(cfg))
		return err
	}, nil))
	gen := manager.activeGeneration.Load()
	for _, id := range []uint32{1, 3} { // default PD path and explicit group on the same etcd
		require.NoError(t, manager.ensureMigratedTxnSafePoint(ctx, gen, id))
		ts, rev, err := manager.gcMetaStorage.LoadTxnSafePointWithRevision(ctx, id)
		require.NoError(t, err)
		require.EqualValues(t, 10, ts)
		require.LessOrEqual(t, rev, markerRevision)
		require.False(t, gen.migration.needsCheck(id, rev))
	}
	// A compare-only confirmation must also be fenced by the election.
	_, err = dest.Put(ctx, keypath.ElectionPath(nil), "member1")
	require.NoError(t, err)
	err = manager.ensureMigratedTxnSafePoint(ctx, gen, 2)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.True(t, gen.migration.needsCheck(2, 0))
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
	value, revision, err := manager.gcMetaStorage.LoadTxnSafePointWithRevision(ctx, 1)
	require.NoError(t, err)
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
	_, revision, err = manager.gcMetaStorage.LoadTxnSafePointWithRevision(ctx, 1)
	require.NoError(t, err)
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
		_, revision, err := manager.gcMetaStorage.LoadTxnSafePointWithRevision(ctx, id)
		require.NoError(t, err)
		require.LessOrEqual(t, revision, markerRev)
		require.True(t, generation.migration.needsCheck(id, revision))
	}
}
