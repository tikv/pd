// Copyright 2022 TiKV Project Authors.
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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.etcd.io/etcd/clientv3"
)

func TestBasic(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	re.Equal("raft/s/00000000000000000123", endpoint.StorePath(123))
	re.Equal("raft/r/00000000000000000123", endpoint.RegionPath(123))

	meta := &metapb.Cluster{Id: 123}
	ok, err := storage.LoadMeta(meta)
	re.False(ok)
	re.NoError(err)
	re.NoError(storage.SaveMeta(meta))
	newMeta := &metapb.Cluster{}
	ok, err = storage.LoadMeta(newMeta)
	re.True(ok)
	re.NoError(err)
	re.Equal(meta, newMeta)

	store := &metapb.Store{Id: 123}
	ok, err = storage.LoadStore(123, store)
	re.False(ok)
	re.NoError(err)
	re.NoError(storage.SaveStore(store))
	newStore := &metapb.Store{}
	ok, err = storage.LoadStore(123, newStore)
	re.True(ok)
	re.NoError(err)
	re.Equal(store, newStore)

	region := &metapb.Region{Id: 123}
	ok, err = storage.LoadRegion(123, region)
	re.False(ok)
	re.NoError(err)
	re.NoError(storage.SaveRegion(region))
	newRegion := &metapb.Region{}
	ok, err = storage.LoadRegion(123, newRegion)
	re.True(ok)
	re.NoError(err)
	re.Equal(region, newRegion)
	err = storage.DeleteRegion(region)
	re.NoError(err)
	ok, err = storage.LoadRegion(123, newRegion)
	re.False(ok)
	re.NoError(err)
}

func mustSaveStores(re *require.Assertions, s Storage, n int) []*metapb.Store {
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		re.NoError(s.SaveStore(store))
	}

	return stores
}

func TestLoadStores(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	cache := core.NewStoresInfo()

	n := 10
	stores := mustSaveStores(re, storage, n)
	re.NoError(storage.LoadStores(cache.SetStore))

	re.Equal(n, cache.GetStoreCount())
	for _, store := range cache.GetMetaStores() {
		re.Equal(stores[store.GetId()], store)
	}
}

func TestStoreWeight(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	cache := core.NewStoresInfo()
	const n = 3

	mustSaveStores(re, storage, n)
	re.NoError(storage.SaveStoreWeight(1, 2.0, 3.0))
	re.NoError(storage.SaveStoreWeight(2, 0.2, 0.3))
	re.NoError(storage.LoadStores(cache.SetStore))
	leaderWeights := []float64{1.0, 2.0, 0.2}
	regionWeights := []float64{1.0, 3.0, 0.3}
	for i := 0; i < n; i++ {
		re.Equal(leaderWeights[i], cache.GetStore(uint64(i)).GetLeaderWeight())
		re.Equal(regionWeights[i], cache.GetStore(uint64(i)).GetRegionWeight())
	}
}

func TestLoadGCSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	testData := []uint64{0, 1, 2, 233, 2333, 23333333333, math.MaxUint64}

	r, e := storage.LoadGCSafePoint()
	re.Equal(uint64(0), r)
	re.NoError(e)
	for _, safePoint := range testData {
		err := storage.SaveGCSafePoint(safePoint)
		re.NoError(err)
		safePoint1, err := storage.LoadGCSafePoint()
		re.NoError(err)
		re.Equal(safePoint1, safePoint)
	}
}

func TestSaveServiceGCSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}

	for _, ssp := range serviceSafePoints {
		re.NoError(storage.SaveServiceGCSafePoint(ssp))
	}

	prefix := endpoint.GCSafePointServicePrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := storage.LoadRange(prefix, prefixEnd, len(serviceSafePoints))
	re.NoError(err)
	re.Len(keys, 3)
	re.Len(values, 3)

	ssp := &endpoint.ServiceSafePoint{}
	for i, key := range keys {
		re.True(strings.HasSuffix(key, serviceSafePoints[i].ServiceID))

		re.NoError(json.Unmarshal([]byte(values[i]), ssp))
		re.Equal(serviceSafePoints[i].ServiceID, ssp.ServiceID)
		re.Equal(serviceSafePoints[i].ExpiredAt, ssp.ExpiredAt)
		re.Equal(serviceSafePoints[i].SafePoint, ssp.SafePoint)
	}
}

func TestLoadMinServiceGCSafePoint(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(1000 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: 0, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}

	for _, ssp := range serviceSafePoints {
		re.NoError(storage.SaveServiceGCSafePoint(ssp))
	}

	// gc_worker's safepoint will be automatically inserted when loading service safepoints. Here the returned
	// safepoint can be either of "gc_worker" or "2".
	ssp, err := storage.LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal(uint64(2), ssp.SafePoint)

	// Advance gc_worker's safepoint
	re.NoError(storage.SaveServiceGCSafePoint(&endpoint.ServiceSafePoint{
		ServiceID: "gc_worker",
		ExpiredAt: math.MaxInt64,
		SafePoint: 10,
	}))

	ssp, err = storage.LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("2", ssp.ServiceID)
	re.Equal(expireAt, ssp.ExpiredAt)
	re.Equal(uint64(2), ssp.SafePoint)
}

func TestLoadRegions(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	cache := core.NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(re, storage, n)
	re.NoError(storage.LoadRegions(context.Background(), cache.SetRegion))

	re.Equal(n, cache.GetRegionCount())
	for _, region := range cache.GetMetaRegions() {
		re.Equal(regions[region.GetId()], region)
	}
}

func mustSaveRegions(re *require.Assertions, s endpoint.RegionStorage, n int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := newTestRegionMeta(uint64(i))
		regions = append(regions, region)
	}

	for _, region := range regions {
		re.NoError(s.SaveRegion(region))
	}

	return regions
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:   []byte(fmt.Sprintf("%20d", regionID+1)),
	}
}

func TestLoadRegionsToCache(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	cache := core.NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(re, storage, n)
	re.NoError(storage.LoadRegionsOnce(context.Background(), cache.SetRegion))

	re.Equal(n, cache.GetRegionCount())
	for _, region := range cache.GetMetaRegions() {
		re.Equal(regions[region.GetId()], region)
	}

	n = 20
	mustSaveRegions(re, storage, n)
	re.NoError(storage.LoadRegionsOnce(context.Background(), cache.SetRegion))
	re.Equal(n, cache.GetRegionCount())
}

func TestLoadRegionsExceedRangeLimit(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/storage/kv/withRangeLimit", "return(500)"))
	storage := NewStorageWithMemoryBackend()
	cache := core.NewRegionsInfo()

	n := 1000
	regions := mustSaveRegions(re, storage, n)
	re.NoError(storage.LoadRegions(context.Background(), cache.SetRegion))
	re.Equal(n, cache.GetRegionCount())
	for _, region := range cache.GetMetaRegions() {
		re.Equal(regions[region.GetId()], region)
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/storage/kv/withRangeLimit"))
}
