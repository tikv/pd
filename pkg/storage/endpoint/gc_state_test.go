// Copyright 2024 TiKV Project Authors.
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

package endpoint

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func newEtcdStorageEndpoint(t *testing.T) (se *StorageEndpoint, clean func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	kvBase := kv.NewEtcdKVBase(client, rootPath)

	s := NewStorageEndpoint(kvBase, nil)
	return s, clean
}

func TestGCBarriersConversions(t *testing.T) {
	re := require.New(t)

	t1 := time.Date(2025, 2, 20, 15, 30, 00, 0, time.Local)
	t2 := t1.Add(time.Minute)
	t3 := t1.Add(time.Millisecond)
	t4 := t1.Add(time.Millisecond * 999)

	gcBarriers := []*GCBarrier{
		NewGCBarrier(constant.NullKeyspaceID, "a", 1, nil),
		NewGCBarrier(0, "b", 2, &t1),
		NewGCBarrier(1000, "c", uint64(t1.UnixMilli())<<18, &t2),
		NewGCBarrier(1000, "d", math.MaxUint64-1, &t3),
		NewGCBarrier(constant.NullKeyspaceID, "e", 456139133457530881, &t4),
	}

	// Check t3 & t4 are rounded
	t3Rounded := time.Date(2025, 2, 20, 15, 30, 01, 0, time.Local)
	re.Equal(t3Rounded, *gcBarriers[3].ExpirationTime)
	re.Equal(t3Rounded, *gcBarriers[4].ExpirationTime)

	serviceSafePoints := []*ServiceSafePoint{
		{ServiceID: "a", ExpiredAt: math.MaxInt64, SafePoint: 1, KeyspaceID: constant.NullKeyspaceID},
		{ServiceID: "b", ExpiredAt: t1.Unix(), SafePoint: 2, KeyspaceID: 0},
		{ServiceID: "c", ExpiredAt: t2.Unix(), SafePoint: uint64(t1.UnixMilli()) << 18, KeyspaceID: 1000},
		{ServiceID: "d", ExpiredAt: t3Rounded.Unix(), SafePoint: math.MaxUint64 - 1, KeyspaceID: 1000},
		{ServiceID: "e", ExpiredAt: t3Rounded.Unix(), SafePoint: 456139133457530881, KeyspaceID: constant.NullKeyspaceID},
	}

	// Test representing GC barriers by service safe points.
	for i, gcBarrier := range gcBarriers {
		expectedServiceSafePoint := serviceSafePoints[i]
		serviceSafePoint := gcBarrier.toServiceSafePoint()
		re.Equal(expectedServiceSafePoint, serviceSafePoint)
	}

	for i, serviceSafePoint := range serviceSafePoints {
		expectedGCBarrier := gcBarriers[i]
		gcBarrier := gcBarrierFromServiceSafePoint(serviceSafePoint)
		re.Equal(expectedGCBarrier, gcBarrier)
	}
}

func loadValue(re *require.Assertions, se *StorageEndpoint, key string) string {
	v, err := se.Load(key)
	re.NoError(err)
	return v
}

func TestGCStateJSONUtil(t *testing.T) {
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	p := newGCStateProvider(se)
	re := require.New(t)

	writeJSON := func(key string, value any) {
		err := p.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
			return wb.writeJson(key, value)
		})
		re.NoError(err)
	}

	loadValue := func(key string) string {
		return loadValue(re, se, key)
	}

	writeJSON("dir1/k1", int64(1))
	writeJSON("dir1/k2", int64(2))
	re.Equal("1", loadValue("dir1/k1"))
	re.Equal("2", loadValue("dir1/k2"))
	vint, err := loadJSON[int64](se, "dir1/k1")
	re.NoError(err)
	re.Equal(int64(1), vint)
	vint, err = loadJSON[int64](se, "dir1/k2")
	re.NoError(err)
	re.Equal(int64(2), vint)
	keys, vints, err := loadJSONByPrefix[int64](se, "dir1/", 0)
	re.NoError(err)
	re.Equal([]string{"dir1/k1", "dir1/k2"}, keys)
	re.Equal([]int64{1, 2}, vints)
	// Non-zero limit takes effect
	keys, vints, err = loadJSONByPrefix[int64](se, "dir1/", 1)
	re.NoError(err)
	re.Equal([]string{"dir1/k1"}, keys)
	re.Equal([]int64{1}, vints)

	writeJSON("dir2/k1", "str")
	re.Equal(`"str"`, loadValue("dir2/k1"))
	vstr, err := loadJSON[string](se, "dir2/k1")
	re.NoError(err)
	re.Equal("str", vstr)

	writeJSON("dir3/k1", new(int64))
	writeJSON("dir3/k2", nil)
	re.Equal("0", loadValue("dir3/k1"))
	re.Equal("null", loadValue("dir3/k2"))
	vpint, err := loadJSON[*int64](se, "dir3/k1")
	re.NoError(err)
	re.Equal(int64(0), *vpint)
	vpint, err = loadJSON[*int64](se, "dir3/k2")
	re.NoError(err)
	re.Nil(vpint)
	keys, vpints, err := loadJSONByPrefix[*int64](se, "dir3/", 0)
	re.NoError(err)
	re.Equal([]string{"dir3/k1", "dir3/k2"}, keys)
	re.Equal([]*int64{new(int64), nil}, vpints)

	ssp := &ServiceSafePoint{
		ServiceID:  "testsvc",
		ExpiredAt:  math.MaxInt64,
		SafePoint:  456139133457530881,
		KeyspaceID: constant.NullKeyspaceID,
	}
	writeJSON("dir4/k1", ssp)
	re.Equal(`{"service_id":"testsvc","expired_at":9223372036854775807,"safe_point":456139133457530881,"keyspace_id":4294967295}`, loadValue("dir4/k1"))
	loadedSsp, err := loadJSON[*ServiceSafePoint](se, "dir4/k1")
	re.NoError(err)
	re.Equal(ssp, loadedSsp)
	keys, loadedSsps, err := loadJSONByPrefix[*ServiceSafePoint](se, "dir4/", 0)
	re.NoError(err)
	re.Equal([]string{"dir4/k1"}, keys)
	re.Equal([]*ServiceSafePoint{ssp}, loadedSsps)
}

func TestGCStateTransactionACID(t *testing.T) {
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	provider := se.GetGCStateProvider()
	re := require.New(t)

	const prefix = "dir1/"
	allKeys := []string{prefix + "k1", prefix + "k2", prefix + "k3", prefix + "k4", prefix + "k5"}

	// Set initial values
	re.NoError(provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
		for _, key := range allKeys {
			err := wb.writeJson(key, 0)
			if err != nil {
				return err
			}
		}
		return nil
	}))

	// +1 to all keys
	var addCount atomic.Int64
	adder := func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			err := provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				_, values, err1 := loadJSONByPrefix[int](se, prefix, 0)
				if err1 != nil {
					return err1
				}
				for i, key := range allKeys {
					err1 = wb.writeJson(key, values[i]+1)
					if err1 != nil {
						return err1
					}
				}
				return nil
			})
			if err != nil {
				if errors.ErrorNotEqual(err, errs.ErrEtcdTxnConflict) {
					return errors.AddStack(err)
				}
			} else {
				addCount.Add(1)
			}
		}
	}

	// Transfer between two keys
	var transferCount atomic.Int64
	transferrer := func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			from := rand.Intn(len(allKeys))
			to := rand.Intn(len(allKeys) - 1)
			if to >= from {
				to++
			}
			err := provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				v1, err1 := loadJSON[int](se, allKeys[from])
				if err1 != nil {
					return err1
				}
				v2, err1 := loadJSON[int](se, allKeys[to])
				if err1 != nil {
					return err1
				}
				err1 = wb.writeJson(allKeys[from], v1-1)
				if err1 != nil {
					return err1
				}
				err1 = wb.writeJson(allKeys[to], v2+1)
				if err1 != nil {
					return err1
				}
				return nil
			})
			if err != nil {
				if errors.ErrorNotEqual(err, errs.ErrEtcdTxnConflict) {
					return errors.AddStack(err)
				}
			} else {
				transferCount.Add(1)
			}
		}
	}

	// Check invariant: the sum is always multiple of 5 (the count of keys).
	var checkCount atomic.Int64
	checker := func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			// Check by range read
			err := provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				_, values, err1 := loadJSONByPrefix[int](se, prefix, 0)
				if err1 != nil {
					return err1
				}
				sum := 0
				for _, v := range values {
					sum += v
				}
				if sum%5 != 0 {
					return errors.Errorf("invariant check: unexpected sum %v", sum)
				}
				return nil
			})
			if err != nil {
				if errors.ErrorNotEqual(err, errs.ErrEtcdTxnConflict) {
					return errors.AddStack(err)
				}
			}
			// Check by single-key reads
			checkBySingleKeySum := 0
			err = provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				for _, key := range allKeys {
					v, err1 := loadJSON[int](se, key)
					if err1 != nil {
						return err1
					}
					checkBySingleKeySum += v
				}
				return nil
			})
			if err != nil {
				if errors.ErrorNotEqual(err, errs.ErrEtcdTxnConflict) {
					return errors.AddStack(err)
				}
			} else if checkBySingleKeySum%5 != 0 {
				return errors.Errorf("invariant check: unexpected sum %v", checkBySingleKeySum)

			}
			// A single range read that's out of transaction should also read atomically.
			_, values, err := loadJSONByPrefix[int](se, prefix, 0)
			if err != nil {
				return err
			}
			sum := 0
			for _, v := range values {
				sum += v
			}
			if sum%5 != 0 {
				return errors.Errorf("invariant check: unexpected sum %v", sum)
			}

			checkCount.Add(1)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 3)
	goroutines := []func(ctx context.Context) error{adder, transferrer, checker}
	for _, f := range goroutines {
		go func() {
			errCh <- f(ctx)
		}()
	}

	// Run for at least 1 second and until each thread has iterated successfully at least once.
	for {
		select {
		case <-time.After(time.Second):
		case err := <-errCh:
			re.Fail("goroutine exited unexpectedly", err)
		}
		if addCount.Load() > 0 && transferCount.Load() > 0 && checkCount.Load() > 0 {
			break
		}
	}
	cancel()
	// Wait for all 3 goroutines to finish and expect no error.
	for range goroutines {
		re.NoError(<-errCh)
	}

	// Check the final sum
	keys, values, err := loadJSONByPrefix[int](se, prefix, 0)
	re.NoError(err)
	re.Equal(allKeys, keys)
	sum := 0
	for _, v := range values {
		sum += v
	}
	re.Equal(int(addCount.Load())*len(allKeys), sum)

	// Check revision: All transactions (except readonly ones) increases the revision.
	// The non-readonly transactions we have performed are: adder, transferrer, and the initialization of the initial
	// data at the beginning of the test.
	revision := loadValue(re, se, keypath.GCMetaRevisionPath())
	re.Equal(fmt.Sprintf("%d", addCount.Load()+transferCount.Load()+1), revision)
}

func TestGCSafePoint(t *testing.T) {
	re := require.New(t)
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	provider := se.GetGCStateProvider()
	testData := []uint64{0, 1, 2, 233, 2333, 23333333333, math.MaxUint64}
	keyspaceIDs := []uint32{constant.NullKeyspaceID, 0, 1000}

	for _, keyspaceID := range keyspaceIDs {
		res, err := provider.LoadGCSafePoint(keyspaceID)
		re.NoError(err)
		re.Equal(uint64(0), res)
		for _, gcSafePoint := range testData {
			// For checking physical data representation.
			expectedKey := "gc/safe_point"
			expectedValue := fmt.Sprintf("%x", gcSafePoint)
			if keyspaceID != constant.NullKeyspaceID {
				expectedKey = fmt.Sprintf("keyspaces/gc_safe_point/%08d", keyspaceID)
				expectedValue = fmt.Sprintf(`{"keyspace_id":%d,"safe_point":%d}`, keyspaceID, gcSafePoint)
			}

			// Check data representation before updating (to ensure not incorrectly updated when updating other keyspaces).
			re.NotEqual(expectedValue, loadValue(re, se, expectedKey))

			err = provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				return wb.SetGCSafePoint(keyspaceID, gcSafePoint)
			})
			re.NoError(err)

			// Check data representation after updating.
			re.Equal(expectedValue, loadValue(re, se, expectedKey))

			newGCSafePoint, err := provider.LoadGCSafePoint(keyspaceID)
			re.NoError(err)
			re.Equal(newGCSafePoint, gcSafePoint)
		}
	}
}

func TestGCBarrier(t *testing.T) {
	re := require.New(t)
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	provider := se.GetGCStateProvider()
	expirationTime := time.Unix(1740127928, 0)

	// GCStateProvider is only a storage layer and should not manipulate the internal data, while the detailed logic
	// (e.g. maintaining invariants between different data) should be done in outer modules. So it allows the keyspaceID
	// mismatches the keyspace to store it. Its responsibility is just to store and read it as is.
	// Also note that for the NullKeyspace, the old data from previous version may not contain the keyspaceID field,
	// which means it is actually possible that the KeyspaceID field mismatches the actual keyspace it belongs to.
	gcBarriers := []*GCBarrier{
		{BarrierID: "1", BarrierTS: 1, ExpirationTime: &expirationTime, KeyspaceID: constant.NullKeyspaceID},
		{BarrierID: "2", BarrierTS: 2, ExpirationTime: nil, KeyspaceID: 1000},
		{BarrierID: "3", BarrierTS: 3, ExpirationTime: &expirationTime, KeyspaceID: 0},
	}

	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, 0, 1000} {
		// Empty.
		loadedBarriers, err := provider.LoadAllGCBarriers(keyspaceID)
		re.NoError(err)
		re.Empty(loadedBarriers)

		// Loading not existing GC barrier results in nils.
		for _, gcBarrier := range gcBarriers {
			loadedBarrier, err := provider.LoadGCBarrier(keyspaceID, gcBarrier.BarrierID)
			re.NoError(err)
			re.Nil(loadedBarrier)
		}

		for _, gcBarrier := range gcBarriers {
			err := provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				return wb.SetGCBarrier(keyspaceID, gcBarrier)
			})
			re.NoError(err)
		}

		// Check the raw data.
		pathPrefix := "gc/safe_point/service"
		if keyspaceID != constant.NullKeyspaceID {
			pathPrefix = fmt.Sprintf("keyspaces/service_safe_point/%08d", keyspaceID)
		}
		re.Equal(`{"service_id":"1","expired_at":1740127928,"safe_point":1,"keyspace_id":4294967295}`,
			loadValue(re, se, pathPrefix+"/1"))
		re.Equal(`{"service_id":"2","expired_at":9223372036854775807,"safe_point":2,"keyspace_id":1000}`,
			loadValue(re, se, pathPrefix+"/2"))
		re.Equal(`{"service_id":"3","expired_at":1740127928,"safe_point":3,"keyspace_id":0}`,
			loadValue(re, se, pathPrefix+"/3"))

		// Check with the GC barrier API.
		loadedBarriers, err = provider.LoadAllGCBarriers(keyspaceID)
		re.NoError(err)
		re.Len(loadedBarriers, 3)
		for i, barrier := range loadedBarriers {
			re.Equal(gcBarriers[i].BarrierID, barrier.BarrierID)
			re.Equal(gcBarriers[i].BarrierTS, barrier.BarrierTS)
			re.Equal(gcBarriers[i].ExpirationTime, barrier.ExpirationTime)
			re.Equal(gcBarriers[i].KeyspaceID, barrier.KeyspaceID)

			// Check key matches.
			b, err := provider.LoadGCBarrier(keyspaceID, barrier.BarrierID)
			re.NoError(err)
			re.Equal(barrier.BarrierID, b.BarrierID)
		}

		if keyspaceID == constant.NullKeyspaceID {
			// Check by the legacy service safe point API for null keyspace.
			keys, ssps, err := provider.CompatibleLoadAllServiceGCSafePoints()
			re.NoError(err)
			re.Len(keys, 3)
			re.Len(ssps, 3)

			for i, key := range keys {
				re.True(strings.HasSuffix(key, gcBarriers[i].BarrierID))

				ssp := ssps[i]
				re.Equal(gcBarriers[i].BarrierID, ssp.ServiceID)
				if gcBarriers[i].ExpirationTime == nil {
					re.Equal(int64(math.MaxInt64), ssp.ExpiredAt)
				} else {
					re.Equal(gcBarriers[i].ExpirationTime.Unix(), ssp.ExpiredAt)
				}
				re.Equal(gcBarriers[i].BarrierTS, ssp.SafePoint)
				re.Equal(gcBarriers[i].KeyspaceID, ssp.KeyspaceID)
			}
		}

		// Test deletion
		for _, gcBarrier := range gcBarriers {
			err = provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				return wb.DeleteGCBarrier(keyspaceID, gcBarrier.BarrierID)
			})
			re.NoError(err)

			// Not exist anymore.
			loadedBarrier, err := provider.LoadGCBarrier(keyspaceID, gcBarrier.BarrierID)
			re.NoError(err)
			re.Nil(loadedBarrier)
		}

		// After deletion, reading range returns empty again.
		loadedBarriers, err = provider.LoadAllGCBarriers(keyspaceID)
		re.NoError(err)
		re.Empty(loadedBarriers)
	}
}

//func TestLoadMinServiceGCSafePoint(t *testing.T) {
//	re := require.New(t)
//	storage := NewStorageWithMemoryBackend()
//	expireAt := time.Now().Add(1000 * time.Second).Unix()
//	serviceSafePoints := []*endpoint.ServiceSafePoint{
//		{ServiceID: "1", ExpiredAt: 0, SafePoint: 1},
//		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
//		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
//	}
//
//	for _, ssp := range serviceSafePoints {
//		re.NoError(storage.SaveServiceGCSafePoint(ssp))
//	}
//
//	// gc_worker's safepoint will be automatically inserted when loading service safepoints. Here the returned
//	// safepoint can be either of "gc_worker" or "2".
//	ssp, err := storage.LoadMinServiceGCSafePoint(time.Now())
//	re.NoError(err)
//	re.Equal(uint64(2), ssp.SafePoint)
//
//	// Advance gc_worker's safepoint
//	re.NoError(storage.SaveServiceGCSafePoint(&endpoint.ServiceSafePoint{
//		ServiceID: "gc_worker",
//		ExpiredAt: math.MaxInt64,
//		SafePoint: 10,
//	}))
//
//	ssp, err = storage.LoadMinServiceGCSafePoint(time.Now())
//	re.NoError(err)
//	re.Equal("2", ssp.ServiceID)
//	re.Equal(expireAt, ssp.ExpiredAt)
//	re.Equal(uint64(2), ssp.SafePoint)
//}

func TestTxnSafePoint(t *testing.T) {
	re := require.New(t)
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	provider := se.GetGCStateProvider()

	testData := []uint64{0, 1, 2, 233, 2333, 23333333333, math.MaxUint64}
	keyspaceIDs := []uint32{constant.NullKeyspaceID, 0, 1000}

	for _, keyspaceID := range keyspaceIDs {
		res, err := provider.LoadTxnSafePoint(keyspaceID)
		re.NoError(err)
		re.Equal(uint64(0), res)

		for _, txnSafePoint := range testData {
			// For checking physical data representation.
			expectedKey := "/tidb/store/gcworker/saved_safe_point"
			expectedValue := fmt.Sprintf("%d", txnSafePoint)
			if keyspaceID != constant.NullKeyspaceID {
				expectedKey = fmt.Sprintf("/keyspaces/tidb/%d/tidb/store/gcworker/saved_safe_point", keyspaceID)
			}

			// Check data representation before updating (to ensure not incorrectly updated when updating other keyspaces).
			re.NotEqual(expectedValue, loadValue(re, se, expectedKey))

			err = provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
				return wb.SetTxnSafePoint(keyspaceID, txnSafePoint)
			})
			re.NoError(err)

			// Check data representation after updating.
			re.Equal(expectedValue, loadValue(re, se, expectedKey))

			newTxnSafePoint, err := provider.LoadGCSafePoint(keyspaceID)
			re.NoError(err)
			re.Equal(newTxnSafePoint, txnSafePoint)
		}
	}
}

func mustSaveKey(re *require.Assertions, se *StorageEndpoint, key string, value string) {
	err := se.Save(key, value)
	re.NoError(err)
}

func mustRemoveKey(re *require.Assertions, se *StorageEndpoint, key string) {
	err := se.Remove(key)
	re.NoError(err)
}

func TestTiDBMinStartTS(t *testing.T) {
	re := require.New(t)
	se, clean := newEtcdStorageEndpoint(t)
	defer clean()
	provider := se.GetGCStateProvider()

	genKey := func(keyspaceID uint32, instanceIdentifier string) string {
		if keyspaceID == constant.NullKeyspaceID {
			return "/tidb/server/minstartts/" + instanceIdentifier
		}
		return fmt.Sprintf("/keyspaces/tidb/%d/tidb/server/minstartts/%s", keyspaceID, instanceIdentifier)
	}

	keyspaceIDs := []uint32{constant.NullKeyspaceID, 0, 1000}

	checkResult := func(keyspaceID uint32, expectedInstanceKey string, expectedValue uint64) {
		key, res, err := provider.CompatibleLoadTiDBMinStartTS(keyspaceID)
		re.NoError(err)
		re.Equal(expectedInstanceKey, key)
		re.Equal(expectedValue, res)
	}

	for _, keyspaceID := range keyspaceIDs {
		checkResult(keyspaceID, "", 0)

		mustSaveKey(re, se, genKey(keyspaceID, "instance1"), "10")
		checkResult(keyspaceID, genKey(keyspaceID, "instance1"), 10)

		mustSaveKey(re, se, genKey(keyspaceID, "instance1"), "15")
		checkResult(keyspaceID, genKey(keyspaceID, "instance1"), 15)

		mustSaveKey(re, se, genKey(keyspaceID, "instance2"), "20")
		checkResult(keyspaceID, genKey(keyspaceID, "instance1"), 15)

		mustSaveKey(re, se, genKey(keyspaceID, "instance2"), "14")
		checkResult(keyspaceID, genKey(keyspaceID, "instance2"), 14)

		mustSaveKey(re, se, genKey(keyspaceID, "instance1"), strconv.FormatUint(math.MaxUint64, 10))
		checkResult(keyspaceID, genKey(keyspaceID, "instance2"), 14)

		mustRemoveKey(re, se, genKey(keyspaceID, "instance2"))
		checkResult(keyspaceID, genKey(keyspaceID, "instance1"), math.MaxUint64)

		mustRemoveKey(re, se, genKey(keyspaceID, "instance1"))
		checkResult(keyspaceID, "", 0)
	}
}

func TestDataPhysicalRepresentation(t *testing.T) {
	re := require.New(t)

	// The following data is possible to be stored by current version of PD. Test storing on them.
	// Note that newEtcdStorageEndpoint uses 100 as the cluster ID.
	writableKvPairs := []kv.KeyValuePair{
		{"/pd/100/gc/safepoint", "654882009e40000" /* 456139133457530880 */},
		{"/pd/100/keyspaces/gc_safe_point/00001111", `{"keyspace_id":1111,"safe_point":456139133457530881}`},
		{"/tidb/store/gcworker/saved_safe_point", "456139133457530882"},
		{"/keyspaces/tidb/2222/tidb/store/gcworker/saved_safe_point", "456139133457530883"},
		{"/pd/100/gc/safepoint/service/gc_worker", `{"service_id":"gc_worker","expired_at":9223372036854775807,"safe_point":456139133457530884,"keyspace_id":4294967295}`},
		{"/pd/100/gc/safepoint/service/svc1", `{"service_id":"svc1","expired_at":1740127928,"safe_point":456139133457530885,"keyspace_id":4294967295}`},
		{"/pd/100/keyspaces/service_safe_point/00003333/svc2", `{"service_id":"svc2","expired_at":1740127928,"safe_point":456139133457530886,"keyspace_id":2222}`},
	}

	slices.SortFunc(writableKvPairs, func(lhs, rhs kv.KeyValuePair) bool {
		return lhs.Key < rhs.Key
	})

	// Test storing
	func() {
		se, clean := newEtcdStorageEndpoint(t)
		defer clean()
		provider := se.GetGCStateProvider()
		err := provider.RunInGCStateTransaction(func(wb *GCStateWriteBatch) error {
			re.NoError(wb.SetGCSafePoint(constant.NullKeyspaceID, 456139133457530880))
			re.NoError(wb.SetGCSafePoint(1111, 456139133457530881))
			re.NoError(wb.SetTxnSafePoint(constant.NullKeyspaceID, 456139133457530882))
			re.NoError(wb.SetTxnSafePoint(2222, 456139133457530883))
			re.NoError(wb.SetGCBarrier(constant.NullKeyspaceID, NewGCBarrier(constant.NullKeyspaceID, "gc_worker", 456139133457530884, nil)))
			expirationTime := time.Unix(1740127928, 0)
			re.NoError(wb.SetGCBarrier(constant.NullKeyspaceID, NewGCBarrier(constant.NullKeyspaceID, "svc1", 456139133457530885, &expirationTime)))
			re.NoError(wb.SetGCBarrier(3333, NewGCBarrier(3333, "svc2", 456139133457530886, &expirationTime)))
			return nil
		})
		re.NoError(err)

		keys, values, err := se.LoadRange("", "", 0)
		re.NoError(err)
		re.Len(keys, len(writableKvPairs), fmt.Sprintf("data length mismatches, expected kvpairs: %v, actual keys: %v, acutal values: %v", writableKvPairs, keys, values))
		for i, key := range keys {
			value := values[i]
			re.Equal(writableKvPairs[i].Key, key)
			re.Equal(writableKvPairs[i].Value, value)
		}
	}()

	// The following data is only possible to be read by the current version, and might be written by old versions PD or
	// other components (e.g., tidb).
	readableKvPairs := []kv.KeyValuePair{
		// MinStartTS reported by TiDB
		{"/tidb/server/minstartts/instance1", "456139133457530887"},
		{"/keyspaces/tidb/4444/tidb/server/minstartts/instance2", "456139133457530888"},
		// Service safe points written by old PDs that doesn't have the keyspaceID field.
		{"/pd/100/gc/safepoint/service/svc3", `{"service_id":"svc1","expired_at":1740127928,"safe_point":456139133457530889}`},
	}

	func() {
		se, clean := newEtcdStorageEndpoint(t)
		defer clean()
		provider := se.GetGCStateProvider()

		var allKvPairs []kv.KeyValuePair
		allKvPairs = append(allKvPairs, writableKvPairs...)
		allKvPairs = append(allKvPairs, readableKvPairs...)
		for _, kvPair := range allKvPairs {
			err := se.Save(kvPair.Key, kvPair.Value)
			re.NoError(err)
		}

		gcSafePoint, err := provider.LoadGCSafePoint(constant.NullKeyspaceID)
		re.NoError(err)
		re.Equal(uint64(456139133457530880), gcSafePoint)
		gcSafePoint, err = provider.LoadGCSafePoint(1111)
		re.NoError(err)
		re.Equal(uint64(456139133457530881), gcSafePoint)
		txnSafePoint, err := provider.LoadTxnSafePoint(constant.NullKeyspaceID)
		re.NoError(err)
		re.Equal(uint64(456139133457530882), txnSafePoint)
		txnSafePoint, err = provider.LoadTxnSafePoint(2222)
		re.NoError(err)
		re.Equal(uint64(456139133457530883), txnSafePoint)
		gcBarrier, err := provider.LoadGCBarrier(constant.NullKeyspaceID, "gc_worker")
		re.NoError(err)
		re.Equal(NewGCBarrier(constant.NullKeyspaceID, "gc_worker", 456139133457530884, nil), gcBarrier)
		gcBarrier, err = provider.LoadGCBarrier(constant.NullKeyspaceID, "svc1")
		re.NoError(err)
		expirationTime := time.Unix(1740127928, 0)
		re.Equal(NewGCBarrier(constant.NullKeyspaceID, "svc1", 456139133457530885, &expirationTime), gcBarrier)
		gcBarrier, err = provider.LoadGCBarrier(3333, "svc2")
		re.NoError(err)
		re.Equal(NewGCBarrier(3333, "svc2", 456139133457530886, &expirationTime), gcBarrier)

		key, minStartTS, err := provider.CompatibleLoadTiDBMinStartTS(constant.NullKeyspaceID)
		re.NoError(err)
		re.Equal("/tidb/server/minstartts/instance1", key)
		re.Equal(uint64(456139133457530887), minStartTS)
		key, minStartTS, err = provider.CompatibleLoadTiDBMinStartTS(4444)
		re.NoError(err)
		re.Equal("/keyspaces/tidb/4444/tidb/server/minstartts/instance2", key)
		re.Equal(uint64(456139133457530888), minStartTS)
		gcBarrier, err = provider.LoadGCBarrier(constant.NullKeyspaceID, "svc3")
		re.NoError(err)
		re.Equal(NewGCBarrier(constant.NullKeyspaceID, "svc1", 456139133457530889, &expirationTime), gcBarrier)

		keys, ssps, err := provider.CompatibleLoadAllServiceGCSafePoints()
		re.NoError(err)
		re.Equal([]string{
			"/pd/100/gc/safepoint/service/gc_worker",
			"/pd/100/gc/safepoint/service/svc1",
			"/pd/100/gc/safepoint/service/svc3",
		}, keys)
		re.Equal([]*ServiceSafePoint{
			{
				ServiceID:  "gc_worker",
				ExpiredAt:  math.MaxInt64,
				SafePoint:  456139133457530884,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "svc1",
				ExpiredAt:  1740127928,
				SafePoint:  456139133457530885,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID: "svc3",
				ExpiredAt: 1740127928,
				SafePoint: 456139133457530889,
				// The storage layer is not responsible for fixing this field.
			},
		}, ssps)
	}()
}
