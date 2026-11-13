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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
)

// A 64-byte bucket covers 512 IDs, balancing etcd key count and rewrite size.
// Bucket width is persisted by initialize and must stay fixed for that index.
const defaultFreeIDBucketBytes = 64

type freeIDBitmapStore interface {
	kv.RawTxnWithContextCapable
	kv.ConditionalTxnRunner
}

// freeIDBitmap stores reclaimed keyspace IDs for future reuse. Each bit
// represents one ID: 1 means free, and 0 means unavailable. Consecutive IDs
// share a bucket; a 64-byte bucket covers 512 IDs. Only IDs whose reclamation
// has completed belong in this index.
//
// Buckets are persisted in etcd and loaded gradually into an in-memory cache.
// Reclamation sets an ID's bit; claiming it for a new keyspace clears the bit.
// update commits the bit change and the caller's keyspace metadata changes in
// one etcd transaction. Candidate lookup uses only the cache; updates compare
// the cached ModRevision and publish the new cached value after commit.
//
// Callers decide which IDs are eligible for reclamation or reuse and when to
// run the loader. Construct a new instance for each leadership term, route all
// bucket writes through update, and cancel and join runLoader before dropping
// the instance. The owner must fence writes against leadership changes; the
// local lock alone cannot exclude another process. This primitive validates
// bitmap consistency, while the caller validates keyspace state and Region
// boundaries. This primitive has no service lifecycle or keyspace metadata
// dependencies.
type freeIDBitmap struct {
	mu          sync.Mutex
	store       freeIDBitmapStore
	prefix      string
	bucketBytes int
	initialized bool
	buckets     map[uint32]kv.KeyValuePair // Includes loaded, absent buckets.
	freeBuckets []uint32                   // Sorted, nonempty buckets only.
	invalid     map[uint32]struct{}
	nextLoad    uint32 // Loaded prefix end; invalid entries override this coverage.
}

// newFreeIDBitmap creates an empty cache. prefix is the caller-provided etcd
// directory, including its trailing slash. With 64-byte buckets, storage is:
//
//	<prefix>layout           -> "v1:64"
//	<prefix>buckets/00000000 -> bitmap for IDs 0-511
//	<prefix>buckets/00000001 -> bitmap for IDs 512-1023
//
// Empty buckets are deleted from etcd. A missing cache entry is known empty
// only within the loaded prefix and only if it has not been invalidated.
// Construction has no storage or goroutine side effects.
func newFreeIDBitmap(store freeIDBitmapStore, prefix string, bucketBytes int) (*freeIDBitmap, error) {
	if prefix == "" || !strings.HasSuffix(prefix, "/") {
		return nil, errors.New("free ID bitmap prefix must end with a slash")
	}
	if bucketBytes <= 0 || bucketBytes > 4096 || bucketBytes&(bucketBytes-1) != 0 {
		return nil, errors.New("free ID bucket bytes must be a power of two between 1 and 4096")
	}
	return &freeIDBitmap{
		store: store, prefix: prefix, bucketBytes: bucketBytes,
		buckets: make(map[uint32]kv.KeyValuePair), invalid: make(map[uint32]struct{}),
	}, nil
}

func (b *freeIDBitmap) bucketCount() uint32 {
	return (constant.MaxValidKeyspaceID + 1) / uint32(b.bucketBytes*8)
}

func (b *freeIDBitmap) bucketKey(bucket uint32) string {
	return fmt.Sprintf("%sbuckets/%08d", b.prefix, bucket)
}

// initialize creates or checks the single layout record for this bitmap.
// "v1:<bucketBytes>" records the storage format version and bucket width, so
// restarts interpret each bit as the same ID. Existing layouts must match
// exactly; changing the version or width requires a separate migration.
func (b *freeIDBitmap) initialize(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.initialized {
		return nil
	}
	key, layout := b.prefix+"layout", fmt.Sprintf("v1:%d", b.bucketBytes)
	resp, err := b.store.CreateRawTxnWithContext(ctx).
		If(kv.RawTxnCondition{Key: key, CmpType: kv.RawTxnCmpNotExists}).
		Then(kv.RawTxnOp{Key: key, Value: layout, OpType: kv.RawTxnOpPut}).
		Else(kv.RawTxnOp{Key: key, OpType: kv.RawTxnOpGet}).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		if len(resp.Responses) != 1 || len(resp.Responses[0].KeyValuePairs) != 1 || resp.Responses[0].KeyValuePairs[0].Value != layout {
			return errors.New("free ID bitmap layout differs from configured bucket size")
		}
	}
	b.initialized = true
	return nil
}

// runLoader is called explicitly by its owner after initialize. An initial
// existence query skips paging when the whole index is empty. Otherwise each
// page covers at most pageBuckets numeric bucket positions, even for sparse
// data. Failed pages are retried without advancing coverage. Once fully loaded,
// only invalidated buckets require further reads; local writes maintain the cache.
func (b *freeIDBitmap) runLoader(ctx context.Context, pageBuckets uint32, interval time.Duration) error {
	if pageBuckets == 0 || interval <= 0 {
		return errors.New("free ID loader requires a positive page size and interval")
	}
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if err := b.loadNextPage(ctx, pageBuckets); err != nil && ctx.Err() == nil {
				log.Warn("failed to load free ID bitmap page", zap.Error(err))
			}
		}
	}
}

func (b *freeIDBitmap) loadNextPage(ctx context.Context, pageBuckets uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.initialized || pageBuckets == 0 {
		return errors.New("free ID bitmap is not initialized or page size is zero")
	}
	// Reload invalidated buckets before extending the loaded prefix.
	for bucket := range b.invalid {
		return b.loadRangeLocked(ctx, bucket, bucket+1)
	}
	// A fresh empty index needs one existence query, not a scan of every page.
	// The query covers the full bucket range; Limit bounds returned records.
	// Only a successful empty result establishes that the whole range is empty.
	// The lock excludes local writes; subsequent updates maintain this cache.
	if b.nextLoad == 0 {
		resp, err := b.store.CreateRawTxnWithContext(ctx).Then(kv.RawTxnOp{
			Key: b.bucketKey(0), EndKey: b.bucketKey(b.bucketCount()),
			OpType: kv.RawTxnOpGetRange, Limit: 1,
		}).Commit()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(resp.Responses) != 1 {
			return errors.New("invalid free ID bitmap range response")
		}
		if len(resp.Responses[0].KeyValuePairs) == 0 {
			b.nextLoad = b.bucketCount()
			return nil
		}
	}
	start := b.nextLoad
	end := start + min(pageBuckets, b.bucketCount()-start)
	if start == end {
		return nil
	}
	if err := b.loadRangeLocked(ctx, start, end); err != nil {
		return err
	}
	b.nextLoad = end
	return nil
}

func (b *freeIDBitmap) loadRangeLocked(ctx context.Context, start, end uint32) error {
	resp, err := b.store.CreateRawTxnWithContext(ctx).Then(kv.RawTxnOp{
		Key: b.bucketKey(start), EndKey: b.bucketKey(end),
		OpType: kv.RawTxnOpGetRange, Limit: int(end - start),
	}).Commit()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(resp.Responses) != 1 {
		return errors.New("invalid free ID bitmap range response")
	}
	loaded := make(map[uint32]kv.KeyValuePair, len(resp.Responses[0].KeyValuePairs))
	for _, value := range resp.Responses[0].KeyValuePairs {
		n, err := strconv.ParseUint(strings.TrimPrefix(value.Key, b.prefix+"buckets/"), 10, 32)
		bucket := uint32(n)
		if err != nil || bucket < start || bucket >= end || value.Key != b.bucketKey(bucket) || len(value.Value) != b.bucketBytes || value.ModRevision <= 0 {
			return errors.New("invalid free ID bitmap encoding or revision")
		}
		loaded[bucket] = value
	}
	for bucket := start; bucket < end; bucket++ {
		value := loaded[bucket]
		b.publishLocked(bucket, value.Value, value.ModRevision)
	}
	return nil
}

// firstFree returns the smallest cached free ID in [start, end). Unknown
// buckets before a candidate prevent selection, including across bucket keys.
// The search does no storage reads. Callers recheck eligibility and selection
// under mu before claiming through updateLocked, so reclamation cannot insert
// an earlier free ID between the final selection and the claim.
func (b *freeIDBitmap) firstFree(start, end uint32) (uint32, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.firstFreeLocked(start, end)
}

func (b *freeIDBitmap) firstFreeLocked(start, end uint32) (uint32, bool) {
	if start >= end || end > constant.MaxValidKeyspaceID+1 {
		return 0, false
	}
	width := uint32(b.bucketBytes * 8)
	index, _ := slices.BinarySearch(b.freeBuckets, start/width)
	checked := start / width
	for _, bucket := range b.freeBuckets[index:] {
		if bucket > (end-1)/width {
			break
		}
		for ; checked <= bucket; checked++ {
			if _, invalid := b.invalid[checked]; invalid {
				return 0, false
			}
			if _, loaded := b.buckets[checked]; !loaded && checked >= b.nextLoad {
				return 0, false
			}
		}
		value := b.buckets[bucket].Value
		for id := max(start, bucket*width); id < min(end, (bucket+1)*width); id++ {
			if value[(id%width)/8]&(1<<(id%8)) != 0 {
				return id, true
			}
		}
	}
	return 0, false
}

// update changes one bit and the caller's metadata in the same transaction.
// It holds the cache lock through commit and publication. The callback must not
// reenter this bitmap, modify bitmap keys, or wait for Region initialization.
// The caller must add its ownership/leadership checks to the same transaction.
// The callback must use txn for its persistent changes; cache changes belong
// after a successful return. The successful commit revision becomes the next
// CAS revision without another Get. On any error the bucket is unavailable
// until reloaded. An ambiguous
// commit also requires the caller to reconcile its own metadata before retry.
func (b *freeIDBitmap) update(ctx context.Context, id uint32, free bool, metadata func(kv.Txn) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.updateLocked(ctx, id, free, metadata)
}

func (b *freeIDBitmap) updateLocked(ctx context.Context, id uint32, free bool, metadata func(kv.Txn) error) error {
	if !b.initialized || id > constant.MaxValidKeyspaceID {
		return errors.New("free ID bitmap is not initialized or ID is out of range")
	}
	bucket := id / uint32(b.bucketBytes*8)
	value, loaded := b.buckets[bucket]
	if !loaded && bucket < b.nextLoad {
		_, invalid := b.invalid[bucket]
		loaded = !invalid
		value.Key = b.bucketKey(bucket)
	}
	if !loaded {
		if err := b.loadRangeLocked(ctx, bucket, bucket+1); err != nil {
			return err
		}
		value = b.buckets[bucket]
	}
	bits := make([]byte, b.bucketBytes)
	copy(bits, value.Value)
	bit := id % uint32(b.bucketBytes*8)
	mask := byte(1 << (bit % 8))
	if (bits[bit/8]&mask != 0) == free {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	if free {
		bits[bit/8] |= mask
	} else {
		bits[bit/8] &^= mask
	}
	updated := string(bits)
	if !slices.ContainsFunc(bits, func(v byte) bool { return v != 0 }) {
		updated = ""
	}
	var committed kv.CommittedRevisionTxn
	err := b.store.RunInTxnWithConditions(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(value.Key), "=", value.ModRevision),
	}, func(txn kv.Txn) error {
		var ok bool
		committed, ok = txn.(kv.CommittedRevisionTxn)
		if !ok {
			return errors.New("free ID bitmap requires the committed transaction revision")
		}
		if err := metadata(txn); err != nil {
			return err
		}
		if updated == "" {
			return txn.Remove(value.Key)
		}
		return txn.Save(value.Key, updated)
	})
	if err != nil {
		b.publishLocked(bucket, "", 0)
		delete(b.buckets, bucket)
		b.invalid[bucket] = struct{}{}
		return err
	}
	b.publishLocked(bucket, updated, committed.CommitRevision())
	return nil
}

func (b *freeIDBitmap) publishLocked(bucket uint32, value string, revision int64) {
	index, present := slices.BinarySearch(b.freeBuckets, bucket)
	if value == "" && present {
		b.freeBuckets = slices.Delete(b.freeBuckets, index, index+1)
	} else if value != "" && !present {
		b.freeBuckets = slices.Insert(b.freeBuckets, index, bucket)
	}
	if value == "" {
		revision = 0
	}
	b.buckets[bucket] = kv.KeyValuePair{Key: b.bucketKey(bucket), Value: value, ModRevision: revision}
	delete(b.invalid, bucket)
}
