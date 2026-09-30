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
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

const enabledKeyspaceTestPrefix = "/test/enabled-keyspaces/"

type pauseAfterFirstPageKV struct {
	clientv3.KV
	firstPage chan<- int64
	release   <-chan struct{}
	once      sync.Once
}

func (kv *pauseAfterFirstPageKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp, err := kv.KV.Get(ctx, key, opts...)
	if err == nil {
		kv.once.Do(func() {
			kv.firstPage <- resp.Header.Revision
			select {
			case <-kv.release:
			case <-ctx.Done():
			}
		})
	}
	return resp, err
}

type afterCommitTxn struct {
	clientv3.Txn
	after func(*clientv3.TxnResponse, error) (*clientv3.TxnResponse, error)
}

func (txn *afterCommitTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Then(ops...)
	return txn
}

func (txn *afterCommitTxn) Commit() (*clientv3.TxnResponse, error) {
	resp, err := txn.Txn.Commit()
	return txn.after(resp, err)
}

func (kv *pauseAfterFirstPageKV) Txn(ctx context.Context) clientv3.Txn {
	return &afterCommitTxn{Txn: kv.KV.Txn(ctx), after: func(resp *clientv3.TxnResponse, err error) (*clientv3.TxnResponse, error) {
		if err == nil {
			kv.once.Do(func() {
				kv.firstPage <- resp.Header.Revision
				select {
				case <-kv.release:
				case <-ctx.Done():
				}
			})
		}
		return resp, err
	}}
}

type pauseBeforeWatch struct {
	clientv3.Watcher
	started chan<- struct{}
	release <-chan struct{}
}

type signalWatchCreated struct {
	clientv3.Watcher
	created chan<- struct{}
}

func (w *signalWatchCreated) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	ch := w.Watcher.Watch(ctx, key, opts...)
	w.created <- struct{}{}
	return ch
}

type neverCreateWatchServer struct {
	pb.UnimplementedWatchServer
	started chan struct{}
}

func (s *neverCreateWatchServer) Watch(stream pb.Watch_WatchServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	close(s.started)
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (w *pauseBeforeWatch) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	w.started <- struct{}{}
	select {
	case <-w.release:
	case <-ctx.Done():
	}
	return w.Watcher.Watch(ctx, key, opts...)
}

func putEnabledKeyspaceTestMeta(t *testing.T, client *clientv3.Client, id uint32, state keyspacepb.KeyspaceState, gcType string) int64 {
	t.Helper()
	value, err := proto.Marshal(&keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
		State:    state,
		Config:   map[string]string{keyspace.GCManagementType: gcType},
	})
	require.NoError(t, err)
	resp, err := client.Put(context.Background(), fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, id), string(value))
	require.NoError(t, err)
	return resp.Header.Revision
}

func startEnabledKeyspaceTestCache(t *testing.T, client *clientv3.Client) *enabledKeyspaceCache {
	t.Helper()
	termCtx, cancel := context.WithCancel(context.Background())
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	done := make(chan struct{})
	go func() {
		cache.run(termCtx, enabledKeyspaceLoadHooks{})
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("cache did not stop after term cancellation")
		}
	})
	return cache
}

func TestEnabledKeyspaceCacheEmptySnapshotAndProgress(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	cache := startEnabledKeyspaceTestCache(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, cache.waitReady(ctx))
	initial, revision, err := cache.snapshotAtLeast(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, initial)
	require.Positive(t, revision)

	resp, err := client.Put(ctx, "/test/unrelated", "changed")
	require.NoError(t, err)
	list, applied, err := cache.snapshotAtLeast(ctx, resp.Header.Revision)
	require.NoError(t, err)
	require.Empty(t, list)
	require.GreaterOrEqual(t, applied, resp.Header.Revision)
}

func TestEnabledKeyspaceCacheAppliesMetadataChanges(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	putEnabledKeyspaceTestMeta(t, client, 3, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	putEnabledKeyspaceTestMeta(t, client, 2, keyspacepb.KeyspaceState_DISABLED, keyspace.UnifiedGC)
	putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	cache := startEnabledKeyspaceTestCache(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, cache.waitReady(ctx))
	list, _, err := cache.snapshotAtLeast(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, []enabledKeyspace{
		{id: 1, gcManagementType: keyspace.UnifiedGC},
		{id: 3, gcManagementType: keyspace.KeyspaceLevelGC},
	}, list)

	rev := putEnabledKeyspaceTestMeta(t, client, 2, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	list, _, err = cache.snapshotAtLeast(ctx, rev)
	require.NoError(t, err)
	require.Equal(t, []enabledKeyspace{
		{id: 1, gcManagementType: keyspace.UnifiedGC},
		{id: 2, gcManagementType: keyspace.KeyspaceLevelGC},
		{id: 3, gcManagementType: keyspace.KeyspaceLevelGC},
	}, list)

	rev = putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_DISABLED, keyspace.UnifiedGC)
	list, _, err = cache.snapshotAtLeast(ctx, rev)
	require.NoError(t, err)
	require.Equal(t, []enabledKeyspace{
		{id: 2, gcManagementType: keyspace.KeyspaceLevelGC},
		{id: 3, gcManagementType: keyspace.KeyspaceLevelGC},
	}, list)

	resp, err := client.Delete(ctx, fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, 3))
	require.NoError(t, err)
	list, _, err = cache.snapshotAtLeast(ctx, resp.Header.Revision)
	require.NoError(t, err)
	require.Equal(t, []enabledKeyspace{{id: 2, gcManagementType: keyspace.KeyspaceLevelGC}}, list)
}

func TestEnabledKeyspaceCacheLoadsAllPagesAtOneRevision(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ops := make([]clientv3.Op, 0, enabledKeyspacePageSize+1)
	for id := uint32(1); id <= enabledKeyspacePageSize+1; id++ {
		value, err := proto.Marshal(&keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
			State:    keyspacepb.KeyspaceState_ENABLED,
			Config:   map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
		})
		require.NoError(t, err)
		ops = append(ops, clientv3.OpPut(fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, id), string(value)))
		if len(ops) == 100 {
			_, err := client.Txn(ctx).Then(ops...).Commit()
			require.NoError(t, err)
			ops = ops[:0]
		}
	}
	resp, err := client.Txn(ctx).Then(ops...).Commit()
	require.NoError(t, err)
	revision := resp.Header.Revision
	firstPage := make(chan int64, 1)
	releasePage := make(chan struct{})
	clientWithPause := *client
	clientWithPause.KV = &pauseAfterFirstPageKV{KV: client.KV, firstPage: firstPage, release: releasePage}
	termCtx, stop := context.WithCancel(context.Background())
	defer stop()
	cache := newEnabledKeyspaceCache(&clientWithPause, enabledKeyspaceTestPrefix)
	type loadedSnapshot struct {
		entries  map[uint32]enabledKeyspace
		revision int64
		err      error
	}
	loaded := make(chan loadedSnapshot, 1)
	go func() {
		entries, loadedRevision, err := cache.load(termCtx, nil)
		loaded <- loadedSnapshot{entries: entries, revision: loadedRevision, err: err}
	}()
	select {
	case firstRevision := <-firstPage:
		require.Equal(t, revision, firstRevision)
	case <-ctx.Done():
		t.Fatal("first metadata page was not read")
	}
	_, err = client.Delete(ctx, fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, enabledKeyspacePageSize+1))
	require.NoError(t, err)
	insertedRevision := putEnabledKeyspaceTestMeta(t, client, 300, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	close(releasePage)
	var initial loadedSnapshot
	select {
	case initial = <-loaded:
	case <-ctx.Done():
		t.Fatal("fixed-revision metadata load did not finish")
	}
	require.NoError(t, initial.err)
	require.Equal(t, revision, initial.revision)
	require.Len(t, initial.entries, enabledKeyspacePageSize+1)
	require.Contains(t, initial.entries, uint32(enabledKeyspacePageSize+1))
	require.NotContains(t, initial.entries, uint32(300))

	cache.publish(termCtx, initial.entries, initial.revision)
	watchDone := make(chan error, 1)
	go func() { watchDone <- cache.watch(termCtx, initial.revision+1) }()
	list, applied, err := cache.snapshotAtLeast(ctx, insertedRevision)
	require.NoError(t, err)
	require.GreaterOrEqual(t, applied, insertedRevision)
	require.Len(t, list, enabledKeyspacePageSize+1)
	require.NotContains(t, list, enabledKeyspace{id: enabledKeyspacePageSize + 1, gcManagementType: keyspace.KeyspaceLevelGC})
	require.Equal(t, enabledKeyspace{id: 300, gcManagementType: keyspace.UnifiedGC}, list[len(list)-1])
	stop()
	select {
	case <-watchDone:
	case <-ctx.Done():
		t.Fatal("metadata watch did not stop")
	}
}

func TestEnabledKeyspaceCacheRejectsMalformedMetadataUntilReload(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	initial := putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	cache := startEnabledKeyspaceTestCache(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, cache.waitReady(ctx))

	resp, err := client.Put(ctx, fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, 2), "malformed protobuf")
	require.NoError(t, err)
	shortCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	_, _, err = cache.snapshotAtLeast(shortCtx, resp.Header.Revision)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	list, revision, err := cache.snapshotAtLeast(ctx, initial)
	require.NoError(t, err)
	require.Equal(t, initial, revision)
	require.Equal(t, []enabledKeyspace{{id: 1, gcManagementType: keyspace.KeyspaceLevelGC}}, list)

	fixed := putEnabledKeyspaceTestMeta(t, client, 2, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	list, revision, err = cache.snapshotAtLeast(ctx, fixed)
	require.NoError(t, err)
	require.GreaterOrEqual(t, revision, fixed)
	require.Equal(t, []enabledKeyspace{
		{id: 1, gcManagementType: keyspace.KeyspaceLevelGC},
		{id: 2, gcManagementType: keyspace.UnifiedGC},
	}, list)
}

func TestEnabledKeyspaceCacheReloadsAfterCompactedWatch(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	first := putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	termCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	watchStarting := make(chan struct{}, 1)
	releaseWatch := make(chan struct{})
	firstWatch := true
	cache.watcherFactory = func(client *clientv3.Client) clientv3.Watcher {
		watcher := clientv3.NewWatcher(client)
		if !firstWatch {
			return watcher
		}
		firstWatch = false
		return &pauseBeforeWatch{Watcher: watcher, started: watchStarting, release: releaseWatch}
	}
	var pageCalls, snapshotCalls atomic.Int32
	done := make(chan struct{})
	go func() {
		cache.run(termCtx, enabledKeyspaceLoadHooks{onPage: func([]enabledKeyspace) { pageCalls.Add(1) }, onInitialSnapshot: func([]enabledKeyspace) { snapshotCalls.Add(1) }})
		close(done)
	}()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	select {
	case <-watchStarting:
	case <-ctx.Done():
		t.Fatal("initial cache load did not reach watch startup")
	}
	list, revision, err := cache.snapshotAtLeast(ctx, first)
	require.NoError(t, err)
	require.Equal(t, first, revision)
	require.Equal(t, []enabledKeyspace{{id: 1, gcManagementType: keyspace.KeyspaceLevelGC}}, list)
	_, err = client.Delete(ctx, fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, 1))
	require.NoError(t, err)
	latest := putEnabledKeyspaceTestMeta(t, client, 2, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	_, err = client.Compact(ctx, latest, clientv3.WithCompactPhysical())
	require.NoError(t, err)
	close(releaseWatch)
	list, applied, err := cache.snapshotAtLeast(ctx, latest)
	require.NoError(t, err)
	require.GreaterOrEqual(t, applied, latest)
	require.Equal(t, []enabledKeyspace{{id: 2, gcManagementType: keyspace.UnifiedGC}}, list)
	require.EqualValues(t, 1, pageCalls.Load())
	require.EqualValues(t, 1, snapshotCalls.Load())
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("cache run did not stop")
	}
}

func TestEnabledKeyspaceCacheTermCancellationUnblocksWaiters(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	termCtx, cancel := context.WithCancel(context.Background())
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	done := make(chan struct{})
	go func() {
		cache.run(termCtx, enabledKeyspaceLoadHooks{})
		close(done)
	}()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	require.NoError(t, cache.waitReady(ctx))
	_, revision, err := cache.snapshotAtLeast(ctx, 0)
	require.NoError(t, err)
	waitResult := make(chan error, 1)
	go func() {
		_, _, err := cache.snapshotAtLeast(ctx, revision+100)
		waitResult <- err
	}()
	cancel()
	select {
	case err := <-waitResult:
		require.ErrorIs(t, err, context.Canceled, "waiting snapshot error: %v", err)
	case <-ctx.Done():
		t.Fatal("snapshot did not stop after term cancellation")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("cache run did not stop after term cancellation")
	}
}

func TestEnabledKeyspaceCacheWatchCreationTimesOut(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	backend := &neverCreateWatchServer{started: make(chan struct{})}
	pb.RegisterWatchServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{listener.Addr().String()}, DialTimeout: time.Second})
	require.NoError(t, err)
	defer client.Close()
	termCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	done := make(chan error, 1)
	go func() { done <- cache.watch(termCtx, 1) }()
	select {
	case <-backend.started:
	case <-time.After(3 * time.Second):
		t.Fatal("watch create request did not reach server")
	}
	select {
	case err := <-done:
		require.Error(t, err)
		require.NoError(t, termCtx.Err())
	case <-time.After(6 * time.Second):
		cancel()
		<-done
		t.Fatal("watch creation did not time out")
	}
}

func TestEnabledKeyspaceCacheReloadsAfterWatchCreationTimeout(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	first := putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	termCtx, cancel := context.WithCancel(context.Background())
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	watchStarted := make(chan struct{}, 1)
	releaseWatch := make(chan struct{})
	firstWatch := true
	cache.watcherFactory = func(client *clientv3.Client) clientv3.Watcher {
		watcher := clientv3.NewWatcher(client)
		if !firstWatch {
			return watcher
		}
		firstWatch = false
		return &pauseBeforeWatch{Watcher: watcher, started: watchStarted, release: releaseWatch}
	}
	done := make(chan struct{})
	go func() {
		cache.run(termCtx, enabledKeyspaceLoadHooks{})
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("cache did not stop")
		}
	}()
	ctx, stop := context.WithTimeout(context.Background(), 8*time.Second)
	defer stop()
	select {
	case <-watchStarted:
	case <-ctx.Done():
		t.Fatal("initial cache load did not reach watch creation")
	}
	initial, revision, err := cache.snapshotAtLeast(ctx, first)
	require.NoError(t, err)
	require.Equal(t, first, revision)
	require.Equal(t, []enabledKeyspace{{id: 1, gcManagementType: keyspace.KeyspaceLevelGC}}, initial)
	latest := putEnabledKeyspaceTestMeta(t, client, 2, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	list, applied, err := cache.snapshotAtLeast(ctx, latest)
	require.NoError(t, err)
	require.GreaterOrEqual(t, applied, latest)
	require.Equal(t, []enabledKeyspace{
		{id: 1, gcManagementType: keyspace.KeyspaceLevelGC},
		{id: 2, gcManagementType: keyspace.UnifiedGC},
	}, list)
}

func TestEnabledKeyspaceCacheWatchSurvivesCreationTimeout(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	termCtx, stop := context.WithCancel(context.Background())
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	created := make(chan struct{}, 1)
	cache.watcherFactory = func(client *clientv3.Client) clientv3.Watcher {
		return &signalWatchCreated{Watcher: clientv3.NewWatcher(client), created: created}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entries, revision, err := cache.load(termCtx, nil)
	require.NoError(t, err)
	require.True(t, cache.publish(termCtx, entries, revision))
	done := make(chan error, 1)
	go func() { done <- cache.watch(termCtx, revision+1) }()
	defer func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("watch did not stop")
		}
	}()
	select {
	case <-created:
	case <-ctx.Done():
		t.Fatal("watch was not created")
	}
	select {
	case err := <-done:
		t.Fatalf("watch ended after creation: %v", err)
	case <-time.After(4 * time.Second):
	}
	latest := putEnabledKeyspaceTestMeta(t, client, 3, keyspacepb.KeyspaceState_ENABLED, keyspace.UnifiedGC)
	list, applied, err := cache.snapshotAtLeast(ctx, latest)
	require.NoError(t, err)
	require.GreaterOrEqual(t, applied, latest)
	require.Equal(t, []enabledKeyspace{{id: 3, gcManagementType: keyspace.UnifiedGC}}, list)
}

func newEnabledKeyspaceInterceptClient(t *testing.T, client *clientv3.Client, interceptor grpc.UnaryClientInterceptor) *clientv3.Client {
	t.Helper()
	intercepted, err := clientv3.New(clientv3.Config{
		Endpoints:   client.Endpoints(),
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(interceptor)},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, intercepted.Close()) })
	return intercepted
}

func putEnabledKeyspaceTestIDs(t *testing.T, client *clientv3.Client, ids []uint32) {
	t.Helper()
	ops := make([]clientv3.Op, 0, 100)
	for i, id := range ids {
		value, err := proto.Marshal(&keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
			State:    keyspacepb.KeyspaceState_ENABLED,
			Config:   map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
		})
		require.NoError(t, err)
		ops = append(ops, clientv3.OpPut(fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, id), string(value)))
		if len(ops) == 100 || i == len(ids)-1 {
			_, err = client.Txn(context.Background()).Then(ops...).Commit()
			require.NoError(t, err)
			ops = ops[:0]
		}
	}
}

func enabledKeyspaceTestIDs(count int) []uint32 {
	ids := make([]uint32, count)
	for i := range ids {
		ids[i] = uint32(i)
	}
	return ids
}

func putEnabledKeyspaceTestWatermark(t *testing.T, client *clientv3.Client, watermark uint64) {
	t.Helper()
	value := binary.BigEndian.AppendUint64(nil, watermark)
	_, err := client.Put(context.Background(), keypath.KeyspaceAllocIDPath(), string(value))
	require.NoError(t, err)
}

func TestEnabledKeyspaceCacheFirstPageTransaction(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	for _, count := range []int{0, 1, 256} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			prefix := enabledKeyspaceTestPrefix
			if count == 0 {
				prefix = "/test/empty-keyspaces/"
			}
			_, err := client.Delete(context.Background(), enabledKeyspaceTestPrefix, clientv3.WithPrefix())
			require.NoError(t, err)
			putEnabledKeyspaceTestIDs(t, client, enabledKeyspaceTestIDs(count))
			if count != 0 {
				putEnabledKeyspaceTestWatermark(t, client, 10000)
			}
			var txns, ranges int
			var transactionRevision int64
			intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				switch request := req.(type) {
				case *pb.TxnRequest:
					txns++
					require.Empty(t, request.Compare)
					require.Len(t, request.Success, 2)
					metadata := request.Success[0].GetRequestRange()
					require.NotNil(t, metadata)
					require.Equal(t, prefix, string(metadata.Key))
					require.Equal(t, clientv3.GetPrefixRangeEnd(prefix), string(metadata.RangeEnd))
					require.EqualValues(t, 256, metadata.Limit)
					require.Zero(t, metadata.Revision)
					require.False(t, metadata.Serializable)
					allocator := request.Success[1].GetRequestRange()
					require.NotNil(t, allocator)
					require.Equal(t, keypath.KeyspaceAllocIDPath(), string(allocator.Key))
					require.Empty(t, allocator.RangeEnd)
					require.Zero(t, allocator.Revision)
					require.False(t, allocator.Serializable)
				case *pb.RangeRequest:
					ranges++
				}
				err := invoke(ctx, method, req, reply, cc, opts...)
				if response, ok := reply.(*pb.TxnResponse); ok && err == nil {
					transactionRevision = response.Header.Revision
				}
				return err
			})
			cache := newEnabledKeyspaceCache(intercepted, prefix)
			entries, revision, err := cache.load(context.Background(), nil)
			require.NoError(t, err)
			require.Len(t, entries, count)
			require.Positive(t, revision)
			require.Equal(t, transactionRevision, revision)
			require.Equal(t, 1, txns, "metadata and allocator must share the initial transaction")
			require.Zero(t, ranges, "More=false must not start range workers")
		})
	}
}

func TestEnabledKeyspaceCacheRangeCoverage(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	ids := append(enabledKeyspaceTestIDs(256), 256, 4095, 4096, 8191, 8192, 9999, 10000, 50000, constant.SystemKeyspaceID, constant.MaxValidKeyspaceID)
	putEnabledKeyspaceTestIDs(t, client, ids)
	end := clientv3.GetPrefixRangeEnd(enabledKeyspaceTestPrefix)
	key := func(s string) string { return enabledKeyspaceTestPrefix + s }
	for _, tc := range []struct {
		name      string
		watermark []byte
		want      [][2]string
	}{
		{"missing", nil, [][2]string{{key("00000255\x00"), end}}},
		{"malformed", []byte("123"), [][2]string{{key("00000255\x00"), end}}},
		{"out-of-range", binary.BigEndian.AppendUint64(nil, 1<<32), [][2]string{{key("00000255\x00"), end}}},
		{"overflow", binary.BigEndian.AppendUint64(nil, ^uint64(0)), [][2]string{{key("00000255\x00"), end}}},
		{"zero", binary.BigEndian.AppendUint64(nil, 0), [][2]string{{key("00000255\x00"), end}}},
		{"already-consumed", binary.BigEndian.AppendUint64(nil, 255), [][2]string{{key("00000255\x00"), end}}},
		{"first-boundary", binary.BigEndian.AppendUint64(nil, 4096), [][2]string{{key("00000255\x00"), end}}},
		{"exact-boundary", binary.BigEndian.AppendUint64(nil, 8192), [][2]string{{key("00000255\x00"), key("00004096")}, {key("00004096"), end}}},
		{"between-boundaries", binary.BigEndian.AppendUint64(nil, 10000), [][2]string{{key("00000255\x00"), key("00004096")}, {key("00004096"), key("00008192")}, {key("00008192"), end}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Delete(context.Background(), keypath.KeyspaceAllocIDPath())
			require.NoError(t, err)
			if tc.watermark != nil {
				_, err = client.Put(context.Background(), keypath.KeyspaceAllocIDPath(), string(tc.watermark))
				require.NoError(t, err)
			}
			var mu sync.Mutex
			var ranges [][2]string
			var revision int64
			intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if request, ok := req.(*pb.RangeRequest); ok {
					mu.Lock()
					ranges = append(ranges, [2]string{string(request.Key), string(request.RangeEnd)})
					mu.Unlock()
					if request.Revision != revision || request.Limit != 256 {
						return fmt.Errorf("page revision/limit = %d/%d, want %d/256", request.Revision, request.Limit, revision)
					}
				}
				err := invoke(ctx, method, req, reply, cc, opts...)
				if response, ok := reply.(*pb.TxnResponse); ok && err == nil {
					revision = response.Header.Revision
				}
				return err
			})
			cache := newEnabledKeyspaceCache(intercepted, enabledKeyspaceTestPrefix)
			entries, loadedRevision, err := cache.load(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, revision, loadedRevision)
			require.Len(t, entries, len(ids))
			for _, id := range ids {
				require.Contains(t, entries, id)
			}
			require.ElementsMatch(t, tc.want, ranges)
		})
	}
}

func TestEnabledKeyspaceCacheRollingWorkers(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	putEnabledKeyspaceTestIDs(t, client, append(enabledKeyspaceTestIDs(256), constant.SystemKeyspaceID))
	putEnabledKeyspaceTestWatermark(t, client, 49152)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := make(chan string, 32)
	releaseFirst := make(chan struct{})
	releaseOthers := make(chan struct{})
	var active, peak atomic.Int32
	intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if request, ok := req.(*pb.RangeRequest); ok {
			n := active.Add(1)
			defer active.Add(-1)
			for {
				previous := peak.Load()
				if n <= previous || peak.CompareAndSwap(previous, n) {
					break
				}
			}
			started <- string(request.RangeEnd)
			release := releaseOthers
			if string(request.Key) == enabledKeyspaceTestPrefix+"00000255\x00" {
				release = releaseFirst
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})
	cache := newEnabledKeyspaceCache(intercepted, enabledKeyspaceTestPrefix)
	loaded := make(chan error, 1)
	go func() {
		entries, _, err := cache.load(ctx, nil)
		if err == nil && len(entries) != 257 {
			err = fmt.Errorf("loaded %d entries", len(entries))
		}
		loaded <- err
	}()
	for range 4 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("four scan workers did not start")
		}
	}
	require.EqualValues(t, 4, peak.Load())
	close(releaseOthers)
	final := clientv3.GetPrefixRangeEnd(enabledKeyspaceTestPrefix)
	for reachedFinal := false; !reachedFinal; {
		select {
		case end := <-started:
			reachedFinal = end == final
		case <-ctx.Done():
			t.Fatal("slow first task prevented rolling dispatch")
		}
	}
	select {
	case err := <-loaded:
		t.Fatalf("partial load returned before first task: %v", err)
	default:
	}
	close(releaseFirst)
	require.NoError(t, <-loaded)
	require.EqualValues(t, 4, peak.Load())
	require.Zero(t, active.Load())
}

func TestEnabledKeyspaceCacheCancelsAttempt(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	putEnabledKeyspaceTestIDs(t, client, append(enabledKeyspaceTestIDs(256), constant.SystemKeyspaceID))
	putEnabledKeyspaceTestWatermark(t, client, uint64(constant.MaxValidKeyspaceID))
	for _, mode := range []string{"cancel", "read-error", "compacted", "decode-error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := make(chan struct{}, 4)
			fail := make(chan struct{})
			var active atomic.Int32
			injected := errors.New("injected page failure")
			intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if request, ok := req.(*pb.RangeRequest); ok {
					active.Add(1)
					defer active.Add(-1)
					started <- struct{}{}
					if strings.HasSuffix(string(request.Key), "00000255\x00") && mode != "cancel" {
						select {
						case <-fail:
						case <-ctx.Done():
							return ctx.Err()
						}
						switch mode {
						case "read-error":
							return injected
						case "compacted":
							return rpctypes.ErrCompacted
						case "decode-error":
							response := reply.(*pb.RangeResponse)
							response.Header = &pb.ResponseHeader{Revision: request.Revision}
							response.Kvs = []*mvccpb.KeyValue{{Key: []byte(enabledKeyspaceTestPrefix + "00000256"), Value: []byte("malformed")}}
							return nil
						}
					}
					<-ctx.Done()
					return ctx.Err()
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			})
			cache := newEnabledKeyspaceCache(intercepted, enabledKeyspaceTestPrefix)
			done := make(chan error, 1)
			go func() {
				entries, revision, err := cache.load(ctx, nil)
				if entries != nil || revision != 0 {
					done <- errors.New("failed attempt returned a partial snapshot")
					return
				}
				done <- err
			}()
			for range 4 {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("four blocked workers did not start")
				}
			}
			if mode == "cancel" {
				cancel()
			} else {
				close(fail)
			}
			select {
			case err := <-done:
				switch mode {
				case "cancel":
					require.ErrorIs(t, err, context.Canceled)
				case "read-error":
					require.ErrorIs(t, err, injected)
				case "compacted":
					require.ErrorIs(t, err, rpctypes.ErrCompacted)
				case "decode-error":
					require.ErrorContains(t, err, "decode keyspace metadata")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("failed attempt did not cancel blocked workers and producer")
			}
			require.Zero(t, active.Load())
			require.Zero(t, cache.appliedRevision())
		})
	}
}

func TestEnabledKeyspaceCacheLaggingFollowerWithoutWatermark(t *testing.T) {
	servers, _, clean := etcdutil.NewTestEtcdCluster(t, 3, nil)
	t.Cleanup(clean)
	testCtx, testCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer testCancel()
	follower := servers[1]
	if follower.Server.ID() == follower.Server.Leader() {
		follower = servers[2]
	}
	require.NotEqual(t, follower.Server.Leader(), follower.Server.ID())
	followerClient, err := clientv3.New(clientv3.Config{Endpoints: []string{follower.Config().ListenClientUrls[0].String()}, DialTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, followerClient.Close()) })
	before, err := followerClient.Get(testCtx, enabledKeyspaceTestPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, before.Kvs)
	for _, server := range servers {
		if server == follower {
			continue
		}
		server.Server.CutPeer(follower.Server.ID())
		follower.Server.CutPeer(server.Server.ID())
	}
	mend := sync.OnceFunc(func() {
		for _, server := range servers {
			if server == follower {
				continue
			}
			server.Server.MendPeer(follower.Server.ID())
			follower.Server.MendPeer(server.Server.ID())
		}
	})
	t.Cleanup(mend)
	// Keep writes pinned to the uncut member; the utility client's health
	// checker may otherwise add the isolated follower to its endpoints.
	writer, err := clientv3.New(clientv3.Config{Endpoints: []string{servers[0].Config().ListenClientUrls[0].String()}, DialTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	value, err := proto.Marshal(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, State: keyspacepb.KeyspaceState_ENABLED, Config: map[string]string{keyspace.GCManagementType: keyspace.UnifiedGC}})
	require.NoError(t, err)
	written, err := writer.Put(testCtx, enabledKeyspaceTestPrefix+"00000001", string(value))
	require.NoError(t, err)
	latest := written.Header.Revision
	stale, err := followerClient.Get(testCtx, enabledKeyspaceTestPrefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	require.NoError(t, err)
	require.Empty(t, stale.Kvs)
	require.Less(t, stale.Header.Revision, latest)
	cache := newEnabledKeyspaceCache(followerClient, enabledKeyspaceTestPrefix)
	ctx, cancel := context.WithTimeout(testCtx, 200*time.Millisecond)
	defer cancel()
	entries, revision, err := cache.load(ctx, nil)
	require.Error(t, err, "an isolated follower must not publish its stale empty prefix")
	require.Nil(t, entries)
	require.Zero(t, revision)
	mend()
	ctx, stop := context.WithTimeout(testCtx, 10*time.Second)
	defer stop()
	entries, revision, err = cache.load(ctx, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, revision, latest)
	require.Equal(t, map[uint32]enabledKeyspace{1: {id: 1, gcManagementType: keyspace.UnifiedGC}}, entries)
}

func TestEnabledKeyspaceCacheInitialHooks(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	putEnabledKeyspaceTestIDs(t, client, append(enabledKeyspaceTestIDs(256), 4096, 8192))
	putEnabledKeyspaceTestWatermark(t, client, 10000)
	termCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	cache := newEnabledKeyspaceCache(client, enabledKeyspaceTestPrefix)
	var mu sync.Mutex
	var pages [][]enabledKeyspace
	snapshots := make(chan []enabledKeyspace, 2)
	hooks := enabledKeyspaceLoadHooks{
		onPage: func(page []enabledKeyspace) {
			// Reentrant access must be safe, and no partial snapshot may be ready.
			if cache.appliedRevision() != 0 {
				panic("page callback after initial publication")
			}
			mu.Lock()
			pages = append(pages, page)
			mu.Unlock()
		},
		onInitialSnapshot: func(snapshot []enabledKeyspace) {
			if cache.appliedRevision() == 0 {
				panic("snapshot callback before publication")
			}
			snapshots <- snapshot
		},
	}
	done := make(chan struct{})
	go func() { cache.run(termCtx, hooks); close(done) }()
	defer func() { cancel(); <-done }()
	var initial []enabledKeyspace
	select {
	case initial = <-snapshots:
	case <-ctx.Done():
		t.Fatal("initial snapshot callback missing")
	}
	require.Len(t, initial, 258)
	mu.Lock()
	var pageEntries []enabledKeyspace
	for _, page := range pages {
		pageEntries = append(pageEntries, page...)
	}
	mu.Unlock()
	require.ElementsMatch(t, initial, pageEntries)
	require.True(t, slices.IsSortedFunc(initial, func(a, b enabledKeyspace) int { return int(a.id) - int(b.id) }))
	// A watch change must not mutate retained page/snapshot values or add hooks.
	latest := putEnabledKeyspaceTestMeta(t, client, 4096, keyspacepb.KeyspaceState_DISABLED, keyspace.KeyspaceLevelGC)
	current, _, err := cache.snapshotAtLeast(ctx, latest)
	require.NoError(t, err)
	require.Len(t, current, 257)
	require.Len(t, initial, 258)
	require.Contains(t, initial, enabledKeyspace{id: 4096, gcManagementType: keyspace.KeyspaceLevelGC})
	select {
	case <-snapshots:
		t.Fatal("initial snapshot callback repeated on watch change")
	default:
	}
	initial[0].id = 999999
	mu.Lock()
	for _, page := range pages {
		if len(page) != 0 {
			page[0].id = 999999
		}
	}
	mu.Unlock()
	current, _, err = cache.snapshotAtLeast(ctx, latest)
	require.NoError(t, err)
	require.Equal(t, uint32(0), current[0].id)
}

func TestEnabledKeyspaceCachePagesAfterConsumedRanges(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The first page ends exactly at 4096, after consuming the whole first
	// conceptual range. Both remaining tasks need more than one page.
	ids := make([]uint32, 0, 901)
	for id := uint32(3841); id <= 4440; id++ {
		ids = append(ids, id)
	}
	for id := uint32(8192); id <= 8491; id++ {
		ids = append(ids, id)
	}
	ids = append(ids, constant.SystemKeyspaceID)
	putEnabledKeyspaceTestIDs(t, client, ids)
	putEnabledKeyspaceTestWatermark(t, client, 12288)
	var mu sync.Mutex
	var ranges [][2]string
	var pageSizes []int
	var revision int64
	intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if request, ok := req.(*pb.RangeRequest); ok {
			if request.Revision != revision || request.Limit != 256 {
				return fmt.Errorf("page revision/limit = %d/%d, want %d/256", request.Revision, request.Limit, revision)
			}
			mu.Lock()
			ranges = append(ranges, [2]string{string(request.Key), string(request.RangeEnd)})
			mu.Unlock()
		}
		err := invoke(ctx, method, req, reply, cc, opts...)
		if response, ok := reply.(*pb.TxnResponse); ok && err == nil {
			revision = response.Header.Revision
			// Later page headers will be newer than R, but the returned snapshot
			// must still include the deleted entry and exclude the new entry.
			_, err = client.Delete(ctx, enabledKeyspaceTestPrefix+"00004440")
			if err != nil {
				return err
			}
			value, marshalErr := proto.Marshal(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 6000}, State: keyspacepb.KeyspaceState_ENABLED})
			if marshalErr != nil {
				return marshalErr
			}
			_, err = client.Put(ctx, enabledKeyspaceTestPrefix+"00006000", string(value))
		}
		return err
	})
	cache := newEnabledKeyspaceCache(intercepted, enabledKeyspaceTestPrefix)
	entries, loadedRevision, err := cache.load(ctx, func(page []enabledKeyspace) {
		mu.Lock()
		pageSizes = append(pageSizes, len(page))
		mu.Unlock()
	})
	require.NoError(t, err)
	require.Equal(t, revision, loadedRevision)
	require.Len(t, entries, len(ids))
	for _, id := range ids {
		require.Contains(t, entries, id)
	}
	require.NotContains(t, entries, uint32(6000))
	end := clientv3.GetPrefixRangeEnd(enabledKeyspaceTestPrefix)
	require.ElementsMatch(t, [][2]string{
		{enabledKeyspaceTestPrefix + "00004096\x00", enabledKeyspaceTestPrefix + "00008192"},
		{enabledKeyspaceTestPrefix + "00004352\x00", enabledKeyspaceTestPrefix + "00008192"},
		{enabledKeyspaceTestPrefix + "00008192", end},
		{enabledKeyspaceTestPrefix + "00008447\x00", end},
	}, ranges)
	require.ElementsMatch(t, []int{256, 256, 88, 256, 45}, pageSizes)
}
