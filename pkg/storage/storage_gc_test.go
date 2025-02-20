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

package storage

import (
	"math"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func newTestEtcdStorage(t *testing.T) (storage Storage, clean func()) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	s := NewStorageWithEtcdBackend(client, rootPath)
	return s, clean
}

func TestLoadGCSafePoint(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestEtcdStorage(t)
	defer clean()
	provider := storage.GetGCStateProvider()
	testData := []uint64{0, 1, 2, 233, 2333, 23333333333, math.MaxUint64}

	r, e := provider.LoadGCSafePoint(constant.NullKeyspaceID)
	re.Equal(uint64(0), r)
	re.NoError(e)
	for _, gcSafePoint := range testData {
		err := provider.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
			return wb.SetGCSafePoint(constant.NullKeyspaceID, gcSafePoint)
		})
		re.NoError(err)
		newGCSafePoint, err := provider.LoadGCSafePoint(constant.NullKeyspaceID)
		re.NoError(err)
		re.Equal(newGCSafePoint, gcSafePoint)
	}
}

func TestSetGCBarrier(t *testing.T) {
	re := require.New(t)
	storage, clean := newTestEtcdStorage(t)
	defer clean()
	provider := storage.GetGCStateProvider()
	expirationTime := time.Now().Add(100 * time.Second).Round(time.Second)
	gcBarriers := []endpoint.GCBarrier{
		{BarrierID: "1", BarrierTS: 1, ExpirationTime: &expirationTime, KeyspaceID: constant.NullKeyspaceID},
		{BarrierID: "2", BarrierTS: 2, ExpirationTime: &expirationTime, KeyspaceID: constant.NullKeyspaceID},
		{BarrierID: "3", BarrierTS: 3, ExpirationTime: &expirationTime, KeyspaceID: constant.NullKeyspaceID},
	}

	for _, gcBarrier := range gcBarriers {
		err := provider.RunInGCMetaTransaction(func(wb *endpoint.GCStateWriteBatch) error {
			return wb.SetGCBarrier(constant.NullKeyspaceID, gcBarrier)
		})
		re.NoError(err)
	}

	// Check with the GC barrier API.
	barriers, err := provider.LoadAllGCBarriers(constant.NullKeyspaceID)
	re.NoError(err)
	re.Len(barriers, 3)
	for i, barrier := range barriers {
		re.Equal(gcBarriers[i].BarrierID, barrier.BarrierID)
		re.Equal(gcBarriers[i].BarrierTS, barrier.BarrierTS)
		re.Equal(gcBarriers[i].ExpirationTime, barrier.ExpirationTime)

		// Check key matches.
		b, err := provider.LoadGCBarrier(constant.NullKeyspaceID, barrier.BarrierID)
		re.NoError(err)
		re.Equal(barrier.BarrierID, b.BarrierID)
	}

	// Check by the legacy service safe point API.
	//prefix := keypath.ServiceGCSafePointPrefix()
	//keys, values, err := storage.LoadRange(prefix, prefixEnd, len(gcBarriers))
	keys, ssps, err := provider.CompatibleLoadAllServiceGCSafePoints()
	re.NoError(err)
	re.Len(keys, 3)
	re.Len(ssps, 3)

	for i, key := range keys {
		re.True(strings.HasSuffix(key, gcBarriers[i].BarrierID))

		ssp := ssps[i]
		re.Equal(gcBarriers[i].BarrierID, ssp.ServiceID)
		re.Equal(gcBarriers[i].ExpirationTime.Unix(), ssp.ExpiredAt)
		re.Equal(gcBarriers[i].BarrierTS, ssp.SafePoint)
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
