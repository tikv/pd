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

package keyspace

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

type bitmapTestStore struct {
	freeIDBitmapStore
	reads     int
	failRead  bool
	loseReply bool
	afterRead func()
	lastRead  []kv.RawTxnOp
}

type bitmapTestTxn struct {
	kv.RawTxn
	store *bitmapTestStore
}

func (s *bitmapTestStore) CreateRawTxnWithContext(ctx context.Context) kv.RawTxn {
	return &bitmapTestTxn{RawTxn: s.freeIDBitmapStore.CreateRawTxnWithContext(ctx), store: s}
}

func (txn *bitmapTestTxn) Then(ops ...kv.RawTxnOp) kv.RawTxn {
	txn.RawTxn = txn.RawTxn.Then(ops...)
	txn.store.lastRead = ops
	return txn
}

func (txn *bitmapTestTxn) Commit() (kv.RawTxnResponse, error) {
	txn.store.reads++
	if txn.store.failRead {
		return kv.RawTxnResponse{}, errors.New("injected read failure")
	}
	resp, err := txn.RawTxn.Commit()
	if txn.store.afterRead != nil {
		txn.store.afterRead()
	}
	return resp, err
}

func (s *bitmapTestStore) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	err := s.freeIDBitmapStore.RunInTxnWithConditions(ctx, conditions, f)
	if err == nil && s.loseReply {
		return errors.New("injected lost commit response")
	}
	return err
}

func TestFreeIDBitmap(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(cleanup)
	backend := kv.NewEtcdKVBase(client)
	ctx := context.Background()
	noMetadata := func(kv.Txn) error { return nil }
	newBitmap := func(t *testing.T) (*freeIDBitmap, *bitmapTestStore) {
		store := &bitmapTestStore{freeIDBitmapStore: backend}
		b, err := newFreeIDBitmap(store, "/"+t.Name()+"/", defaultFreeIDBucketBytes)
		require.NoError(t, err)
		require.NoError(t, b.initialize(ctx))
		return b, store
	}

	t.Run("construction is inert", func(t *testing.T) {
		store := &bitmapTestStore{freeIDBitmapStore: backend}
		b, err := newFreeIDBitmap(store, "/inert/", defaultFreeIDBucketBytes)
		require.NoError(t, err)
		require.Zero(t, store.reads)
		require.Empty(t, b.buckets)
		require.Error(t, b.loadNextPage(ctx, 64))
		require.Error(t, b.update(ctx, 1, true, noMetadata))
		resp, err := client.Get(ctx, "/inert/", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("layout survives restart", func(t *testing.T) {
		b, store := newBitmap(t)
		restarted, err := newFreeIDBitmap(store, b.prefix, defaultFreeIDBucketBytes)
		require.NoError(t, err)
		require.NoError(t, restarted.initialize(ctx))
		resized, err := newFreeIDBitmap(store, b.prefix, 128)
		require.NoError(t, err)
		require.ErrorContains(t, resized.initialize(ctx), "layout differs")
		for _, size := range []int{0, -1, 3, 8192} {
			_, err := newFreeIDBitmap(store, b.prefix, size)
			require.Error(t, err)
		}
	})

	t.Run("bounded loading and failed page", func(t *testing.T) {
		b, store := newBitmap(t)
		bits := make([]byte, b.bucketBytes)
		bits[0] = 1
		require.NoError(t, backend.Save(b.bucketKey(64), string(bits)))
		store.failRead = true
		require.Error(t, b.loadNextPage(ctx, 64))
		require.Zero(t, b.nextLoad)
		require.Empty(t, b.buckets)
		store.failRead = false
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.Equal(t, uint32(64), b.nextLoad)
		require.Equal(t, []kv.RawTxnOp{{Key: b.bucketKey(0), EndKey: b.bucketKey(64), Limit: 64, OpType: kv.RawTxnOpGetRange}}, store.lastRead)
		_, ok := b.firstFree(0, 65*512)
		require.False(t, ok)
		require.NoError(t, b.loadNextPage(ctx, 64))
		id, ok := b.firstFree(0, 65*512)
		require.True(t, ok)
		require.Equal(t, uint32(64*512), id)
		reads := store.reads
		b.firstFree(0, 65*512)
		require.Equal(t, reads, store.reads)
	})

	t.Run("empty index skips pages and accepts later updates", func(t *testing.T) {
		b, store := newBitmap(t)
		reads := store.reads
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.Equal(t, reads+1, store.reads)
		require.Equal(t, 1, store.lastRead[0].Limit)
		require.Equal(t, b.bucketCount(), b.nextLoad)
		require.Empty(t, b.buckets, "known-empty buckets need no cache entries")
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		require.Equal(t, reads+1, store.reads, "empty pages and later writes need no extra reads")
		id, ok := b.firstFree(0, 1024)
		require.True(t, ok)
		require.Equal(t, uint32(512), id)

		// A failed write still invalidates a bucket inside the known prefix.
		store.loseReply = true
		require.Error(t, b.update(ctx, 511, true, noMetadata))
		_, ok = b.firstFree(0, 1024)
		require.False(t, ok)
		store.loseReply = false
		require.NoError(t, b.loadNextPage(ctx, 64))
		id, ok = b.firstFree(0, 1024)
		require.True(t, ok)
		require.Equal(t, uint32(511), id)
	})

	t.Run("unknown earlier bucket blocks selection", func(t *testing.T) {
		b, _ := newBitmap(t)
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		_, ok := b.firstFree(0, 1024)
		require.False(t, ok)
		require.NoError(t, b.update(ctx, 511, true, noMetadata))
		id, ok := b.firstFree(0, 1024)
		require.True(t, ok)
		require.Equal(t, uint32(511), id)
		id, ok = b.firstFree(512, 1024)
		require.True(t, ok)
		require.Equal(t, uint32(512), id)
		require.NoError(t, b.update(ctx, 511, false, noMetadata))
		id, ok = b.firstFree(0, 1024)
		require.True(t, ok)
		require.Equal(t, uint32(512), id)
	})

	t.Run("metadata and bitmap commit together", func(t *testing.T) {
		b, store := newBitmap(t)
		owner := b.prefix + "owner"
		require.NoError(t, b.update(ctx, 1, true, func(txn kv.Txn) error { return txn.Save(owner, "free") }))
		resp, err := client.Get(ctx, b.bucketKey(0))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		firstRevision := resp.Kvs[0].ModRevision
		require.Equal(t, firstRevision, b.buckets[0].ModRevision)
		ownerResp, err := client.Get(ctx, owner)
		require.NoError(t, err)
		require.Equal(t, firstRevision, ownerResp.Kvs[0].ModRevision)
		reads := store.reads
		require.NoError(t, b.update(ctx, 2, true, noMetadata))
		require.Equal(t, reads, store.reads, "cached mutations must not reload the bucket")
		require.Greater(t, b.buckets[0].ModRevision, firstRevision)
		require.NoError(t, b.update(ctx, 1, false, func(txn kv.Txn) error { return txn.Save(owner, "new") }))
		require.NoError(t, b.update(ctx, 2, false, noMetadata))
		resp, err = client.Get(ctx, b.bucketKey(0))
		require.NoError(t, err)
		require.Empty(t, resp.Kvs, "empty buckets must be deleted")
		require.Zero(t, b.buckets[0].ModRevision)
		require.NoError(t, b.update(ctx, 3, true, noMetadata))
	})

	t.Run("callback failure rolls back both changes", func(t *testing.T) {
		b, _ := newBitmap(t)
		owner := b.prefix + "owner"
		err := b.update(ctx, 1, true, func(txn kv.Txn) error {
			if err := txn.Save(owner, "must not commit"); err != nil {
				return err
			}
			return errors.New("cancelled by owner")
		})
		require.ErrorContains(t, err, "cancelled by owner")
		for _, key := range []string{owner, b.bucketKey(0)} {
			value, err := backend.Load(key)
			require.NoError(t, err)
			require.Empty(t, value)
		}
		require.NotContains(t, b.buckets, uint32(0))
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.Empty(t, b.invalid)
	})

	t.Run("invalid earlier bucket blocks later free IDs", func(t *testing.T) {
		b, _ := newBitmap(t)
		require.NoError(t, b.update(ctx, 511, true, noMetadata))
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		require.NoError(t, backend.Save(b.bucketKey(0), b.buckets[0].Value))
		require.ErrorIs(t, b.update(ctx, 511, false, noMetadata), errs.ErrEtcdTxnConflict)
		_, ok := b.firstFree(0, 1024)
		require.False(t, ok)
		require.NoError(t, b.loadNextPage(ctx, 64))
		id, ok := b.firstFree(0, 1024)
		require.True(t, ok)
		require.Equal(t, uint32(511), id)
	})

	t.Run("same value rewrite invalidates stale cache", func(t *testing.T) {
		b, _ := newBitmap(t)
		require.NoError(t, b.update(ctx, 1, true, noMetadata))
		require.NoError(t, backend.Save(b.bucketKey(0), b.buckets[0].Value))
		owner := b.prefix + "owner"
		err := b.update(ctx, 1, false, func(txn kv.Txn) error { return txn.Save(owner, "must not commit") })
		require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
		value, err := backend.Load(owner)
		require.NoError(t, err)
		require.Empty(t, value)
		_, ok := b.firstFree(0, 512)
		require.False(t, ok)
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.NoError(t, b.update(ctx, 1, false, noMetadata))
	})

	t.Run("lost commit reply requires reload", func(t *testing.T) {
		b, store := newBitmap(t)
		require.NoError(t, b.update(ctx, 1, true, noMetadata))
		store.loseReply = true
		owner := b.prefix + "owner"
		err := b.update(ctx, 1, false, func(txn kv.Txn) error { return txn.Save(owner, "new") })
		require.ErrorContains(t, err, "lost commit response")
		value, err := backend.Load(owner)
		require.NoError(t, err)
		require.Equal(t, "new", value)
		require.NotContains(t, b.buckets, uint32(0))
		store.loseReply = false
		require.NoError(t, b.loadNextPage(ctx, 64))
		err = b.update(ctx, 1, false, func(kv.Txn) error { t.Fatal("must not claim twice"); return nil })
		require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	})

	t.Run("cancelled read is not published", func(t *testing.T) {
		b, store := newBitmap(t)
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		store.afterRead = cancel
		require.ErrorIs(t, b.loadNextPage(cancelCtx, 64), context.Canceled)
		require.Zero(t, b.nextLoad)
		require.Empty(t, b.buckets)
	})

	t.Run("corrupt bucket leaves entire page unavailable", func(t *testing.T) {
		b, _ := newBitmap(t)
		require.NoError(t, backend.Save(b.bucketKey(1), "bad"))
		require.ErrorContains(t, b.loadNextPage(ctx, 64), "invalid free ID bitmap")
		require.Empty(t, b.buckets)
		require.Zero(t, b.nextLoad)
	})

	t.Run("concurrent updates do not lose bits", func(t *testing.T) {
		b, _ := newBitmap(t)
		var wg sync.WaitGroup
		errorsCh := make(chan error, 32)
		for id := range uint32(32) {
			wg.Go(func() { errorsCh <- b.update(ctx, id, true, noMetadata) })
		}
		wg.Wait()
		close(errorsCh)
		for err := range errorsCh {
			require.NoError(t, err)
		}
		restarted, err := newFreeIDBitmap(backend, b.prefix, defaultFreeIDBucketBytes)
		require.NoError(t, err)
		require.NoError(t, restarted.initialize(ctx))
		require.NoError(t, restarted.loadNextPage(ctx, 1))
		for expected := range uint32(32) {
			id, ok := restarted.firstFree(0, 512)
			require.True(t, ok)
			require.Equal(t, expected, id)
			require.NoError(t, restarted.update(ctx, id, false, noMetadata))
		}
	})

	t.Run("loader stops on cancellation", func(t *testing.T) {
		b, _ := newBitmap(t)
		loaderCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- b.runLoader(loaderCtx, 1, time.Millisecond) }()
		require.Eventually(t, func() bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.nextLoad > 0
		}, time.Second*5, time.Millisecond*10)
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second * 5):
			t.Fatal("loader did not stop")
		}
	})

	for _, size := range []int{1, 32, 64, 256, 1024} {
		t.Run(fmt.Sprintf("bucket size %d", size), func(t *testing.T) {
			b, err := newFreeIDBitmap(backend, "/"+t.Name()+"/", size)
			require.NoError(t, err)
			require.NoError(t, b.initialize(ctx))
			last := uint32(b.bucketBytes*8 - 1)
			require.NoError(t, b.update(ctx, last, true, noMetadata))
			id, ok := b.firstFree(0, last+1)
			require.True(t, ok)
			require.Equal(t, last, id)
		})
	}
}
