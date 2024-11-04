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
	"math/rand"
	"sort"
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
	re.NoError(TryLoadRegionsOnce(context.Background(), storage, cache.SetRegion))

	re.Equal(n, cache.GetRegionCount())
	for _, region := range cache.GetMetaRegions() {
		re.Equal(regions[region.GetId()], region)
	}

	n = 20
	mustSaveRegions(re, storage, n)
	re.NoError(TryLoadRegionsOnce(context.Background(), storage, cache.SetRegion))
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

func TestTrySwitchRegionStorage(t *testing.T) {
	re := require.New(t)
	defaultStorage := NewStorageWithMemoryBackend()
	localStorage := NewStorageWithMemoryBackend()
	storage := NewCoreStorage(defaultStorage, localStorage)
	defaultCache := core.NewRegionsInfo()
	localCache := core.NewRegionsInfo()

	TrySwitchRegionStorage(storage, false)
	regions10 := mustSaveRegions(re, storage, 10)
	re.NoError(defaultStorage.LoadRegions(context.Background(), defaultCache.SetRegion))
	re.NoError(localStorage.LoadRegions(context.Background(), localCache.SetRegion))
	re.Empty(localCache.GetMetaRegions())
	re.Len(defaultCache.GetMetaRegions(), 10)
	for _, region := range defaultCache.GetMetaRegions() {
		re.Equal(regions10[region.GetId()], region)
	}

	TrySwitchRegionStorage(storage, true)
	regions20 := mustSaveRegions(re, storage, 20)
	re.NoError(defaultStorage.LoadRegions(context.Background(), defaultCache.SetRegion))
	re.NoError(localStorage.LoadRegions(context.Background(), localCache.SetRegion))
	re.Len(defaultCache.GetMetaRegions(), 10)
	re.Len(localCache.GetMetaRegions(), 20)
	for _, region := range defaultCache.GetMetaRegions() {
		re.Equal(regions10[region.GetId()], region)
	}
	for _, region := range localCache.GetMetaRegions() {
		re.Equal(regions20[region.GetId()], region)
	}
}

const (
	keyChars = "abcdefghijklmnopqrstuvwxyz"
	keyLen   = 20
)

func generateKeys(size int) []string {
	m := make(map[string]struct{}, size)
	for len(m) < size {
		k := make([]byte, keyLen)
		for i := range k {
			k[i] = keyChars[rand.Intn(len(keyChars))]
		}
		m[string(k)] = struct{}{}
	}

	v := make([]string, 0, size)
	for k := range m {
		v = append(v, k)
	}
	sort.Strings(v)
	return v
}

func randomMerge(regions []*metapb.Region, n int, ratio int) {
	rand.New(rand.NewSource(6))
	note := make(map[int]bool)
	for i := 0; i < n*ratio/100; i++ {
		pos := rand.Intn(n - 1)
		for {
			if _, ok := note[pos]; !ok {
				break
			}
			pos = rand.Intn(n - 1)
		}
		note[pos] = true

		mergeIndex := pos + 1
		for mergeIndex < n {
			_, ok := note[mergeIndex]
			if ok {
				mergeIndex++
			} else {
				break
			}
		}
		regions[mergeIndex].StartKey = regions[pos].StartKey
		if regions[pos].GetRegionEpoch().GetVersion() > regions[mergeIndex].GetRegionEpoch().GetVersion() {
			regions[mergeIndex].GetRegionEpoch().Version = regions[pos].GetRegionEpoch().GetVersion()
		}
		regions[mergeIndex].GetRegionEpoch().Version++
	}
}

func saveRegions(lb *levelDBBackend, n int, ratio int) error {
	keys := generateKeys(n)
	regions := make([]*metapb.Region, 0, n)
	for i := uint64(0); i < uint64(n); i++ {
		var region *metapb.Region
		if i == 0 {
			region = &metapb.Region{
				Id:       i,
				StartKey: []byte("aaaaaaaaaaaaaaaaaaaa"),
				EndKey:   []byte(keys[i]),
				RegionEpoch: &metapb.RegionEpoch{
					Version: 1,
				},
			}
		} else {
			region = &metapb.Region{
				Id:       i,
				StartKey: []byte(keys[i-1]),
				EndKey:   []byte(keys[i]),
				RegionEpoch: &metapb.RegionEpoch{
					Version: 1,
				},
			}
		}
		regions = append(regions, region)
	}
	if ratio != 0 {
		randomMerge(regions, n, ratio)
	}

	for _, region := range regions {
		err := lb.SaveRegion(region)
		if err != nil {
			return err
		}
	}
	return lb.Flush()
}

func benchmarkLoadRegions(b *testing.B, n int, ratio int) {
	re := require.New(b)
	ctx := context.Background()
	dir := b.TempDir()
	lb, err := newLevelDBBackend(ctx, dir, nil)
	if err != nil {
		b.Fatal(err)
	}
	cluster := core.NewBasicCluster()
	err = saveRegions(lb, n, ratio)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = lb.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	b.ResetTimer()
	err = lb.LoadRegions(context.Background(), cluster.CheckAndPutRegion)
	re.NoError(err)
}

var volumes = []struct {
	input int
}{
	{input: 10000},
	{input: 100000},
	{input: 1000000},
}

func BenchmarkLoadRegionsByVolume(b *testing.B) {
	for _, v := range volumes {
		b.Run(fmt.Sprintf("input size %d", v.input), func(b *testing.B) {
			benchmarkLoadRegions(b, v.input, 0)
		})
	}
}

var ratios = []struct {
	ratio int
}{
	{ratio: 0},
	{ratio: 20},
	{ratio: 40},
	{ratio: 60},
	{ratio: 80},
}

func BenchmarkLoadRegionsByRandomMerge(b *testing.B) {
	for _, r := range ratios {
		b.Run(fmt.Sprintf("merge ratio %d", r.ratio), func(b *testing.B) {
			benchmarkLoadRegions(b, 1000000, r.ratio)
		})
	}
}
