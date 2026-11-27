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

// Background
//
// Normal creation allocates increasing 24-bit keyspace IDs, allowing about
// 16.8 million IDs. Frequent creation and deletion can exhaust this space
// even when few keyspaces remain active. This mechanism reclaims IDs from
// deleted keyspaces.
//
// Reclamation workflow
//
// This file provides the bitmap storage and cache. Reclamation and Create
// integration will be added in subsequent changes.
//
// After a keyspace enters TOMBSTONE, an external caller may select it from
// a cached view of PD metadata and request reclamation. PD rechecks eligibility,
// removes the old metadata and name mapping, and marks the ID free in an
// etcd-backed bitmap. The Create path may then look up and claim a free ID.
// Bitmap and metadata changes commit atomically in the same transaction.
//
// Design trade-off
//
// An alternative is to retain TOMBSTONE metadata, check reuse eligibility
// during Create, and replace the old metadata and name mapping using CAS.
// This avoids a persisted bitmap, but keeps one metadata record per candidate
// and puts reclamation work on Create. Rebuilding the candidate index after
// restart requires scanning all keyspace metadata, unless a separate index
// is persisted.
//
// The bitmap provides a compact persisted index of IDs that have passed
// reclamation checks and supports gradual loading from compact buckets.
// Its cost is an extra bucket CAS/write in the metadata transaction and
// a persisted layout to maintain.

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
// The last ID in each bucket is intentionally left unreclaimed to simplify
// the implementation. This breaks free runs at bucket boundaries, so selection
// and claiming depend only on the current bucket. This costs one reusable ID
// per bucket (1/512 by default) and lets a single bucket CAS protect the
// predecessor check, even if a timed-out write commits after a reload.
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
	buckets     map[uint32]kv.KeyValuePair // Empty entries are needed only outside nextLoad.
	freeBuckets []uint32                   // Sorted nonempty buckets; each contains a candidate.
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

// runLoader is called explicitly by its owner after initialize. Each page
// covers at most pageBuckets numeric bucket positions, including empty ranges.
// Failed pages are retried without advancing coverage. Once fully loaded,
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
	start := b.nextLoad
	end := start + min(pageBuckets, b.bucketCount()-start)
	if start == end {
		return nil
	}
	return b.loadRangeLocked(ctx, start, end)
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
		if value.Value[b.bucketBytes-1]&0x80 != 0 {
			return errors.New("free ID bitmap contains a reserved bucket-end ID")
		}
		loaded[bucket] = value
	}
	// Extend contiguous coverage only after the entire range is validated.
	// Publication can then omit empty entries inside the loaded prefix.
	if start == b.nextLoad {
		b.nextLoad = end
	}
	for bucket := start; bucket < end; bucket++ {
		value := loaded[bucket]
		b.publishLocked(bucket, value.Value, value.ModRevision)
	}
	return nil
}

// freeIDCandidates is a per-attempt cursor over cached free IDs. It keeps only
// the next ID to visit, so changes to the bucket list cannot shift its position.
// It is not a snapshot: newly loaded or reclaimed IDs behind the cursor are
// left for the next attempt. Once exhausted, the cursor remains exhausted.
type freeIDCandidates struct {
	bitmap *freeIDBitmap
	nextID uint32
}

// candidates creates a cursor without storage reads or changes to the free bits.
// Callers may skip a candidate by calling Next again. Eligibility and the free
// bit must be rechecked under the bitmap lock when actually claiming an ID.
func (b *freeIDBitmap) candidates() *freeIDCandidates {
	return &freeIDCandidates{bitmap: b}
}

// Next returns the next cached candidate, releasing the lock before the caller
// checks its Region. Within a bucket, only the first ID of each free run is
// selectable: splitting in its middle could leave earlier IDs unusable.
// The reserved last bit breaks runs between buckets. Each lookup scans at most
// the resumed bucket and the next nonempty bucket.
func (c *freeIDCandidates) Next() (uint32, bool) {
	b := c.bitmap
	b.mu.Lock()
	defer b.mu.Unlock()
	idsPerBucket := uint32(b.bucketBytes * 8)
	resumeBucket := c.nextID / idsPerBucket
	index, _ := slices.BinarySearch(b.freeBuckets, resumeBucket)
	for _, bucket := range b.freeBuckets[index:] {
		firstBit := uint32(0)
		if bucket == resumeBucket {
			firstBit = c.nextID % idsPerBucket
		}
		if bit, found := b.nextCandidateBitLocked(bucket, firstBit); found {
			id := bucket*idsPerBucket + bit
			c.nextID = id + 1
			return id, true
		}
	}
	c.nextID = constant.MaxValidKeyspaceID + 1
	return 0, false
}

// isFreeLocked reports an ID's cached free bit and whether it is known.
// Unknown or invalidated buckets require a reload before their bits can be used.
func (b *freeIDBitmap) isFreeLocked(id uint32) (free, known bool) {
	idsPerBucket := uint32(b.bucketBytes * 8)
	if id%idsPerBucket == idsPerBucket-1 {
		return false, true
	}
	bucket := id / idsPerBucket
	if _, invalid := b.invalid[bucket]; invalid {
		return false, false
	}
	cached, loaded := b.buckets[bucket]
	if !loaded && bucket >= b.nextLoad {
		return false, false
	}
	if cached.Value == "" {
		return false, true
	}
	bit := id % idsPerBucket
	return cached.Value[bit/8]&(1<<(bit%8)) != 0, true
}

// update changes one bit and the caller's metadata in the same transaction.
// It holds the cache lock through commit and publication. The callback must not
// reenter this bitmap, modify bitmap keys, or wait for Region initialization.
// The caller must add its ownership/leadership checks to the same transaction.
// The callback must use txn for its persistent changes; cache changes belong
// after a successful return. The successful commit revision becomes the next
// CAS revision without another Get. If commit returns an error, reload the
// bucket before using it again; each update checks its revision with CAS.
func (b *freeIDBitmap) update(ctx context.Context, id uint32, free bool, metadata func(kv.Txn) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.updateLocked(ctx, id, free, metadata)
}

func (b *freeIDBitmap) updateLocked(ctx context.Context, id uint32, free bool, metadata func(kv.Txn) error) error {
	if !b.initialized || id > constant.MaxValidKeyspaceID {
		return errors.New("free ID bitmap is not initialized or ID is out of range")
	}
	idsPerBucket := uint32(b.bucketBytes * 8)
	if id%idsPerBucket == idsPerBucket-1 {
		return errors.New("bucket-end keyspace IDs cannot be reclaimed or claimed")
	}
	bucket := id / idsPerBucket
	cached, err := b.loadBucketLocked(ctx, bucket)
	if err != nil {
		return err
	}
	updated, err := b.updatedBucketValue(cached.Value, id%idsPerBucket, free)
	if err != nil {
		return err
	}

	// Commit the bit and metadata together, provided the cached bucket is current.
	var committedTxn kv.CommittedRevisionTxn
	commitReady := false
	err = b.store.RunInTxnWithConditions(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(cached.Key), "=", cached.ModRevision),
	}, func(txn kv.Txn) error {
		var ok bool
		committedTxn, ok = txn.(kv.CommittedRevisionTxn)
		if !ok {
			return errors.New("free ID bitmap requires the committed transaction revision")
		}
		if err := metadata(txn); err != nil {
			return err
		}
		var err error
		if updated == "" {
			err = txn.Remove(cached.Key)
		} else {
			err = txn.Save(cached.Key, updated)
		}
		commitReady = err == nil
		return err
	})
	if err != nil {
		// Callback errors abort before commit and leave the cache valid.
		// Once commit is possible, an error requires a reload.
		if commitReady {
			b.invalidateBucketLocked(bucket)
		}
		return err
	}
	b.publishLocked(bucket, updated, committedTxn.CommitRevision())
	return nil
}

// loadBucketLocked returns a cached bucket, reading storage only if its state
// is unknown. An absent bucket inside the loaded prefix has revision zero.
func (b *freeIDBitmap) loadBucketLocked(ctx context.Context, bucket uint32) (kv.KeyValuePair, error) {
	if cached, loaded := b.buckets[bucket]; loaded {
		return cached, nil
	}
	_, invalid := b.invalid[bucket]
	if bucket < b.nextLoad && !invalid {
		return kv.KeyValuePair{Key: b.bucketKey(bucket)}, nil
	}
	if err := b.loadRangeLocked(ctx, bucket, bucket+1); err != nil {
		return kv.KeyValuePair{}, err
	}
	cached := b.buckets[bucket]
	if cached.Key == "" {
		cached.Key = b.bucketKey(bucket)
	}
	return cached, nil
}

// updatedBucketValue changes one bucket-relative bit in a copy of the bitmap.
// Repeating a claim or reclaim is a conflict. An empty result deletes the bucket.
func (b *freeIDBitmap) updatedBucketValue(value string, bitOffset uint32, free bool) (string, error) {
	bitmap := make([]byte, b.bucketBytes)
	copy(bitmap, value)
	byteIndex := bitOffset / 8
	bitMask := byte(1 << (bitOffset % 8))
	currentlyFree := bitmap[byteIndex]&bitMask != 0
	if currentlyFree == free {
		return "", errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	if free {
		bitmap[byteIndex] |= bitMask
	} else {
		bitmap[byteIndex] &^= bitMask
	}
	if !slices.ContainsFunc(bitmap, func(v byte) bool { return v != 0 }) {
		return "", nil
	}
	return string(bitmap), nil
}

// invalidateBucketLocked removes the cached value and its allocation candidates.
// The invalid marker overrides loaded-prefix coverage until the bucket is reread.
func (b *freeIDBitmap) invalidateBucketLocked(bucket uint32) {
	delete(b.buckets, bucket)
	b.invalid[bucket] = struct{}{}
	b.updateFreeBucketIndexLocked(bucket)
}

func (b *freeIDBitmap) publishLocked(bucket uint32, value string, revision int64) {
	if value == "" {
		revision = 0
	}
	if value == "" && bucket < b.nextLoad {
		delete(b.buckets, bucket)
	} else {
		b.buckets[bucket] = kv.KeyValuePair{Key: b.bucketKey(bucket), Value: value, ModRevision: revision}
	}
	delete(b.invalid, bucket)
	// Every nonempty bucket has a candidate because its predecessor ends in 0.
	b.updateFreeBucketIndexLocked(bucket)
}

// updateFreeBucketIndexLocked maintains the sorted list of nonempty cached buckets.
func (b *freeIDBitmap) updateFreeBucketIndexLocked(bucket uint32) {
	nonempty := b.buckets[bucket].Value != ""
	index, present := slices.BinarySearch(b.freeBuckets, bucket)
	if nonempty && !present {
		b.freeBuckets = slices.Insert(b.freeBuckets, index, bucket)
	} else if !nonempty && present {
		b.freeBuckets = slices.Delete(b.freeBuckets, index, index+1)
	}
}

// nextCandidateBitLocked finds a free run's first bit within one bucket.
func (b *freeIDBitmap) nextCandidateBitLocked(bucket, firstBit uint32) (uint32, bool) {
	bitmap := b.buckets[bucket].Value
	for bit := firstBit; bit < uint32(len(bitmap)*8); bit++ {
		if bitmap[bit/8]&(1<<(bit%8)) == 0 {
			continue
		}
		if bit > 0 && bitmap[(bit-1)/8]&(1<<((bit-1)%8)) != 0 {
			continue
		}
		return bit, true
	}
	return 0, false
}
