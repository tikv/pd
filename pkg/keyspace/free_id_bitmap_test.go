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
	reads         int
	failRead      bool
	loseReply     bool
	delayCommit   bool
	pendingCommit func() error
	afterRead     func()
	lastRead      []kv.RawTxnOp
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
	if s.delayCommit {
		s.delayCommit = false
		return s.freeIDBitmapStore.RunInTxnWithConditions(ctx, conditions, func(txn kv.Txn) error {
			if err := f(txn); err != nil {
				return err
			}
			// Delayed test callbacks only stage writes. Replay them later with
			// the original CAS, as if a timed-out request reached etcd late.
			s.pendingCommit = func() error {
				return s.freeIDBitmapStore.RunInTxnWithConditions(context.Background(), conditions, f)
			}
			return context.DeadlineExceeded
		})
	}
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
		_, ok := b.candidates().Next()
		require.False(t, ok)
		require.NoError(t, b.loadNextPage(ctx, 64))
		id, ok := b.candidates().Next()
		require.True(t, ok)
		require.Equal(t, uint32(64*512), id)
		reads := store.reads
		b.candidates().Next()
		require.Equal(t, reads, store.reads)
	})

	t.Run("empty index loads bounded pages and accepts later updates", func(t *testing.T) {
		b, store := newBitmap(t)
		reads := store.reads
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.Equal(t, reads+1, store.reads)
		require.Equal(t, []kv.RawTxnOp{{Key: b.bucketKey(0), EndKey: b.bucketKey(64), Limit: 64, OpType: kv.RawTxnOpGetRange}}, store.lastRead)
		require.Equal(t, uint32(64), b.nextLoad)
		require.Empty(t, b.buckets, "loaded empty buckets need no cache entries")
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		require.Equal(t, reads+2, store.reads, "each page needs one read; cached writes need none")
		require.Equal(t, uint32(128), b.nextLoad)
		require.Equal(t, []kv.RawTxnOp{{Key: b.bucketKey(64), EndKey: b.bucketKey(128), Limit: 64, OpType: kv.RawTxnOpGetRange}}, store.lastRead)
		id, ok := b.candidates().Next()
		require.True(t, ok)
		require.Equal(t, uint32(512), id)

		// A failed write still invalidates a bucket inside the known prefix.
		store.loseReply = true
		require.Error(t, b.update(ctx, 510, true, noMetadata))
		id, ok = b.candidates().Next()
		require.True(t, ok)
		require.Equal(t, uint32(512), id)
		store.loseReply = false
		require.NoError(t, b.loadNextPage(ctx, 64))
		id, ok = b.candidates().Next()
		require.True(t, ok)
		require.Equal(t, uint32(510), id)

		for b.nextLoad < b.bucketCount() {
			start := b.nextLoad
			end := min(start+64, b.bucketCount())
			reads := store.reads
			require.NoError(t, b.loadNextPage(ctx, 64))
			require.Equal(t, reads+1, store.reads)
			require.Equal(t, end, b.nextLoad)
			require.Equal(t, []kv.RawTxnOp{{Key: b.bucketKey(start), EndKey: b.bucketKey(end), Limit: int(end - start), OpType: kv.RawTxnOpGetRange}}, store.lastRead)
		}
		require.Len(t, b.buckets, 2, "only the two nonempty buckets remain after full loading")
		reads = store.reads
		require.NoError(t, b.loadNextPage(ctx, 64))
		require.Equal(t, reads, store.reads, "fully loaded index needs no further reads")
	})

	t.Run("empty buckets outside the loaded prefix remain known", func(t *testing.T) {
		b, store := newBitmap(t)
		const bucket = uint32(200)
		cached, err := b.loadBucketLocked(ctx, bucket)
		require.NoError(t, err)
		require.Equal(t, b.bucketKey(bucket), cached.Key)
		require.Contains(t, b.buckets, bucket)
		free, known := b.isFreeLocked(bucket * 512)
		require.False(t, free)
		require.True(t, known)
		_, known = b.isFreeLocked((bucket - 1) * 512)
		require.False(t, known)

		// Reading an empty bucket ahead of the prefix avoids repeated reads.
		require.NoError(t, b.update(ctx, (bucket+1)*512, true, noMetadata))
		require.Equal(t, []uint32{bucket + 1}, b.freeBuckets)
		for b.nextLoad <= bucket {
			require.NoError(t, b.loadNextPage(ctx, 64))
		}
		require.NotContains(t, b.buckets, bucket)
		require.Equal(t, []uint32{bucket + 1}, b.freeBuckets)
		reads := store.reads
		cached, err = b.loadBucketLocked(ctx, bucket)
		require.NoError(t, err)
		require.Equal(t, b.bucketKey(bucket), cached.Key)
		require.Equal(t, reads, store.reads)

		// Invalidating one bucket leaves the next bucket available.
		b.invalidateBucketLocked(bucket)
		require.Equal(t, []uint32{bucket + 1}, b.freeBuckets)
		cached, err = b.loadBucketLocked(ctx, bucket)
		require.NoError(t, err)
		require.Equal(t, b.bucketKey(bucket), cached.Key)
		require.Zero(t, cached.ModRevision)
		require.NotContains(t, b.buckets, bucket)
		require.Equal(t, []uint32{bucket + 1}, b.freeBuckets)
		require.NoError(t, b.update(ctx, bucket*512, true, noMetadata))
	})

	t.Run("bucket start does not depend on its neighboring bucket", func(t *testing.T) {
		b, store := newBitmap(t)
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		for _, invalid := range []bool{false, true} {
			if invalid {
				b.invalidateBucketLocked(0)
			}
			reads := store.reads
			free, known := b.isFreeLocked(511)
			require.False(t, free)
			require.True(t, known)
			id, found := b.candidates().Next()
			require.True(t, found)
			require.Equal(t, uint32(512), id)
			require.Equal(t, reads, store.reads)
		}
		require.NoError(t, b.update(ctx, 512, false, noMetadata))
		require.Contains(t, b.invalid, uint32(0))
	})

	t.Run("cursor resumes without claiming skipped candidates", func(t *testing.T) {
		b, store := newBitmap(t)
		for _, id := range []uint32{7, 8, 510, 512, 515} {
			require.NoError(t, b.update(ctx, id, true, noMetadata))
		}
		reads := store.reads
		cursor := b.candidates()
		for _, expected := range []uint32{7, 510, 512, 515} {
			id, found := cursor.Next()
			require.True(t, found)
			require.Equal(t, expected, id)
		}
		_, found := cursor.Next()
		require.False(t, found)
		id, found := b.candidates().Next()
		require.True(t, found)
		require.Equal(t, uint32(7), id, "skipping leaves the free bit set")
		require.Equal(t, reads, store.reads)

		require.NoError(t, b.update(ctx, 513, true, noMetadata))
		_, found = cursor.Next()
		require.False(t, found, "exhausted cursors stay exhausted")
	})

	t.Run("consecutive candidates depend on the preceding claim", func(t *testing.T) {
		for _, first := range []uint32{7, 507, 512} {
			for _, succeeds := range []bool{false, true} {
				t.Run(fmt.Sprintf("first=%d/succeeds=%t", first, succeeds), func(t *testing.T) {
					b, store := newBitmap(t)
					for _, id := range []uint32{first, first + 1, first + 3} {
						require.NoError(t, b.update(ctx, id, true, noMetadata))
					}
					require.NoError(t, b.loadNextPage(ctx, 2))
					cursor := b.candidates()
					id, found := cursor.Next()
					require.True(t, found)
					require.Equal(t, first, id)

					err := b.update(ctx, first, false, func(kv.Txn) error {
						if !succeeds {
							return errors.New("claim failed")
						}
						return nil
					})
					expected := first + 1
					if succeeds {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "claim failed")
						// Reload the invalidated bucket: the failed claim left
						// the first ID free, so skip the rest of that run.
						require.NoError(t, b.loadNextPage(ctx, 2))
						expected = first + 3
					}
					reads := store.reads
					id, found = cursor.Next()
					require.True(t, found)
					require.Equal(t, expected, id)
					require.Equal(t, reads, store.reads)
				})
			}
		}
	})

	t.Run("cursor handles bucket removal and earlier insertion", func(t *testing.T) {
		b, _ := newBitmap(t)
		require.NoError(t, b.update(ctx, 510, true, noMetadata))
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		cursor := b.candidates()
		id, found := cursor.Next()
		require.True(t, found)
		require.Equal(t, uint32(510), id)
		require.NoError(t, b.update(ctx, 510, false, noMetadata))
		require.NoError(t, b.update(ctx, 7, true, noMetadata))
		id, found = cursor.Next()
		require.True(t, found)
		require.Equal(t, uint32(512), id)
		id, found = b.candidates().Next()
		require.True(t, found)
		require.Equal(t, uint32(7), id)
	})

	t.Run("cursor sees invalidation between calls", func(t *testing.T) {
		b, _ := newBitmap(t)
		require.NoError(t, b.update(ctx, 7, true, noMetadata))
		require.NoError(t, b.update(ctx, 513, true, noMetadata))
		cursor := b.candidates()
		id, found := cursor.Next()
		require.True(t, found)
		require.Equal(t, uint32(7), id)
		require.NoError(t, backend.Save(b.bucketKey(1), b.buckets[1].Value))
		require.ErrorIs(t, b.update(ctx, 513, false, noMetadata), errs.ErrEtcdTxnConflict)
		_, found = cursor.Next()
		require.False(t, found)
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

	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback failure preserves cache populated=%t", populated), func(t *testing.T) {
			b, store := newBitmap(t)
			require.NoError(t, b.loadNextPage(ctx, 1))
			if populated {
				require.NoError(t, b.update(ctx, 1, true, noMetadata))
			}
			cached := b.buckets[0]
			reads := store.reads
			owner := b.prefix + "owner"
			err := b.update(ctx, 1, !populated, func(txn kv.Txn) error {
				if err := txn.Save(owner, "must not commit"); err != nil {
					return err
				}
				return errors.New("cancelled by owner")
			})
			require.ErrorContains(t, err, "cancelled by owner")
			require.Equal(t, cached, b.buckets[0])
			require.Empty(t, b.invalid)
			_, found := b.candidates().Next()
			require.Equal(t, populated, found)
			value, err := backend.Load(owner)
			require.NoError(t, err)
			require.Empty(t, value)
			value, err = backend.Load(b.bucketKey(0))
			require.NoError(t, err)
			require.Equal(t, cached.Value, value)
			require.NoError(t, b.update(ctx, 1, !populated, noMetadata))
			require.Equal(t, reads, store.reads, "callback rejection must not force a reload")
		})
	}

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
		_, ok := b.candidates().Next()
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

	for _, reclaimFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("late reclaim in same bucket first=%t", reclaimFirst), func(t *testing.T) {
			b, store := newBitmap(t)
			require.NoError(t, b.update(ctx, 10, true, noMetadata))
			store.delayCommit = true
			require.ErrorIs(t, b.update(ctx, 9, true, noMetadata), context.DeadlineExceeded)
			require.NoError(t, b.loadNextPage(ctx, 1))
			id, ok := b.candidates().Next()
			require.True(t, ok)
			require.Equal(t, uint32(10), id)
			if reclaimFirst {
				require.NoError(t, store.pendingCommit())
				require.ErrorIs(t, b.update(ctx, id, false, noMetadata), errs.ErrEtcdTxnConflict)
				require.NoError(t, b.loadNextPage(ctx, 1))
				id, ok = b.candidates().Next()
				require.True(t, ok)
				require.Equal(t, uint32(9), id)
			} else {
				require.NoError(t, b.update(ctx, id, false, noMetadata))
				require.ErrorIs(t, store.pendingCommit(), errs.ErrEtcdTxnConflict)
			}
		})
	}

	t.Run("late reclaim after empty bucket recreation", func(t *testing.T) {
		b, store := newBitmap(t)
		store.delayCommit = true
		require.ErrorIs(t, b.update(ctx, 9, true, noMetadata), context.DeadlineExceeded)
		require.NoError(t, b.loadNextPage(ctx, 1))
		// Return the bucket to absent: the delayed revision-zero CAS can pass.
		require.NoError(t, b.update(ctx, 10, true, noMetadata))
		require.NoError(t, b.update(ctx, 10, false, noMetadata))
		require.NoError(t, store.pendingCommit())
		// A stale empty cache cannot claim anything from this bucket, and its
		// next write must reload. The neighboring bucket remains independent.
		require.ErrorIs(t, b.update(ctx, 10, true, noMetadata), errs.ErrEtcdTxnConflict)
		require.NoError(t, b.update(ctx, 512, true, noMetadata))
		require.NoError(t, b.update(ctx, 512, false, noMetadata))
		require.NoError(t, b.loadNextPage(ctx, 1))
		id, ok := b.candidates().Next()
		require.True(t, ok)
		require.Equal(t, uint32(9), id)
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
			id, ok := restarted.candidates().Next()
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
			callbacks := 0
			for _, id := range []uint32{last, last*2 + 1} {
				err := b.update(ctx, id, true, func(kv.Txn) error { callbacks++; return nil })
				require.ErrorContains(t, err, "bucket-end")
			}
			require.Zero(t, callbacks, "reserved IDs must not modify metadata")
			resp, err := client.Get(ctx, b.prefix+"buckets/", clientv3.WithPrefix())
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
			require.NoError(t, b.update(ctx, last-1, true, noMetadata))
			id, ok := b.candidates().Next()
			require.True(t, ok)
			require.Equal(t, last-1, id)

			// Old or corrupt data must not introduce a free boundary bit.
			bits := make([]byte, size)
			bits[size-1] = 0x80
			require.NoError(t, backend.Save(b.bucketKey(1), string(bits)))
			loaded := b.nextLoad
			require.ErrorContains(t, b.loadNextPage(ctx, 2), "reserved bucket-end")
			require.Equal(t, loaded, b.nextLoad)
		})
	}
}
