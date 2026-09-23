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
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/etcdutil"
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

type pauseBeforeWatch struct {
	clientv3.Watcher
	started chan<- struct{}
	release <-chan struct{}
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

func startEnabledKeyspaceTestCache(t *testing.T, client *clientv3.Client) (*enabledKeyspaceCache, <-chan struct{}) {
	t.Helper()
	termCtx, cancel := context.WithCancel(context.Background())
	cache := newEnabledKeyspaceCache(termCtx, client, enabledKeyspaceTestPrefix)
	done := make(chan struct{})
	go func() {
		cache.run()
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
	return cache, done
}

func TestEnabledKeyspaceCacheEmptySnapshotAndProgress(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	cache, _ := startEnabledKeyspaceTestCache(t, client)

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
	cache, _ := startEnabledKeyspaceTestCache(t, client)

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
	var revision int64
	for id := uint32(1); id <= enabledKeyspacePageSize+1; id++ {
		value, err := proto.Marshal(&keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
			State:    keyspacepb.KeyspaceState_ENABLED,
			Config:   map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
		})
		require.NoError(t, err)
		ops = append(ops, clientv3.OpPut(fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, id), string(value)))
		if len(ops) == 100 {
			resp, err := client.Txn(ctx).Then(ops...).Commit()
			require.NoError(t, err)
			revision = resp.Header.Revision
			ops = ops[:0]
		}
	}
	resp, err := client.Txn(ctx).Then(ops...).Commit()
	require.NoError(t, err)
	revision = resp.Header.Revision
	firstPage := make(chan int64, 1)
	releasePage := make(chan struct{})
	clientWithPause := *client
	clientWithPause.KV = &pauseAfterFirstPageKV{KV: client.KV, firstPage: firstPage, release: releasePage}
	termCtx, stop := context.WithCancel(context.Background())
	defer stop()
	cache := newEnabledKeyspaceCache(termCtx, &clientWithPause, enabledKeyspaceTestPrefix)
	type loadedSnapshot struct {
		entries  map[uint32]enabledKeyspace
		revision int64
		err      error
	}
	loaded := make(chan loadedSnapshot, 1)
	go func() {
		entries, loadedRevision, err := cache.load()
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

	cache.publish(initial.entries, initial.revision)
	watchDone := make(chan error, 1)
	go func() { watchDone <- cache.watch(initial.revision + 1) }()
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
	cache, _ := startEnabledKeyspaceTestCache(t, client)
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
	cache := newEnabledKeyspaceCache(termCtx, client, enabledKeyspaceTestPrefix)
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
	done := make(chan struct{})
	go func() {
		cache.run()
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
	cache := newEnabledKeyspaceCache(termCtx, client, enabledKeyspaceTestPrefix)
	done := make(chan struct{})
	go func() {
		cache.run()
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
		require.True(t, errors.Is(err, context.Canceled), "waiting snapshot error: %v", err)
	case <-ctx.Done():
		t.Fatal("snapshot did not stop after term cancellation")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("cache run did not stop after term cancellation")
	}
}
