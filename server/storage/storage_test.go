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

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	endpoint "github.com/tikv/pd/server/storage/endpoint"
	"go.etcd.io/etcd/clientv3"
)

func TestStorage(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStorageSuite{})

type testStorageSuite struct {
}

func (s *testStorageSuite) TestBasic(c *C) {
	storage := NewStorageWithMemoryBackend()

	c.Assert(endpoint.StorePath(123), Equals, "raft/s/00000000000000000123")
	c.Assert(endpoint.RegionPath(123), Equals, "raft/r/00000000000000000123")

	meta := &metapb.Cluster{Id: 123}
	ok, err := storage.LoadMeta(meta)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(storage.SaveMeta(meta), IsNil)
	newMeta := &metapb.Cluster{}
	ok, err = storage.LoadMeta(newMeta)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newMeta, DeepEquals, meta)

	store := &metapb.Store{Id: 123}
	ok, err = storage.LoadStore(123, store)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(storage.SaveStore(store), IsNil)
	newStore := &metapb.Store{}
	ok, err = storage.LoadStore(123, newStore)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newStore, DeepEquals, store)

	region := &metapb.Region{Id: 123}
	ok, err = storage.LoadRegion(123, region)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(storage.SaveRegion(region), IsNil)
	newRegion := &metapb.Region{}
	ok, err = storage.LoadRegion(123, newRegion)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newRegion, DeepEquals, region)
	err = storage.DeleteRegion(region)
	c.Assert(err, IsNil)
	ok, err = storage.LoadRegion(123, newRegion)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
}

func mustSaveStores(c *C, s Storage, n int) []*metapb.Store {
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(s.SaveStore(store), IsNil)
	}

	return stores
}

func (s *testStorageSuite) TestLoadStores(c *C) {
	storage := NewStorageWithMemoryBackend()
	cache := core.NewStoresInfo()

	n := 10
	stores := mustSaveStores(c, storage, n)
	c.Assert(storage.LoadStores(cache.SetStore), IsNil)

	c.Assert(cache.GetStoreCount(), Equals, n)
	for _, store := range cache.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
}

func (s *testStorageSuite) TestStoreWeight(c *C) {
	storage := NewStorageWithMemoryBackend()
	cache := core.NewStoresInfo()
	const n = 3

	mustSaveStores(c, storage, n)
	c.Assert(storage.SaveStoreWeight(1, 2.0, 3.0), IsNil)
	c.Assert(storage.SaveStoreWeight(2, 0.2, 0.3), IsNil)
	c.Assert(storage.LoadStores(cache.SetStore), IsNil)
	leaderWeights := []float64{1.0, 2.0, 0.2}
	regionWeights := []float64{1.0, 3.0, 0.3}
	for i := 0; i < n; i++ {
		c.Assert(cache.GetStore(uint64(i)).GetLeaderWeight(), Equals, leaderWeights[i])
		c.Assert(cache.GetStore(uint64(i)).GetRegionWeight(), Equals, regionWeights[i])
	}
}

func (s *testStorageSuite) TestLoadGCSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	testData := []uint64{0, 1, 2, 233, 2333, 23333333333, math.MaxUint64}

	r, e := storage.LoadGCSafePoint()
	c.Assert(r, Equals, uint64(0))
	c.Assert(e, IsNil)
	for _, safePoint := range testData {
		err := storage.SaveGCSafePoint(safePoint)
		c.Assert(err, IsNil)
		safePoint1, err := storage.LoadGCSafePoint()
		c.Assert(err, IsNil)
		c.Assert(safePoint, Equals, safePoint1)
	}
}

func (s *testStorageSuite) TestSaveServiceGCSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: expireAt, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}

	for _, ssp := range serviceSafePoints {
		c.Assert(storage.SaveServiceGCSafePoint(ssp), IsNil)
	}

	prefix := endpoint.GCSafePointServicePrefixPath()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := storage.LoadRange(prefix, prefixEnd, len(serviceSafePoints))
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 3)
	c.Assert(values, HasLen, 3)

	ssp := &endpoint.ServiceSafePoint{}
	for i, key := range keys {
		c.Assert(strings.HasSuffix(key, serviceSafePoints[i].ServiceID), IsTrue)

		c.Assert(json.Unmarshal([]byte(values[i]), ssp), IsNil)
		c.Assert(ssp.ServiceID, Equals, serviceSafePoints[i].ServiceID)
		c.Assert(ssp.ExpiredAt, Equals, serviceSafePoints[i].ExpiredAt)
		c.Assert(ssp.SafePoint, Equals, serviceSafePoints[i].SafePoint)
	}
}

func (s *testStorageSuite) TestLoadMinServiceGCSafePoint(c *C) {
	storage := NewStorageWithMemoryBackend()
	expireAt := time.Now().Add(1000 * time.Second).Unix()
	serviceSafePoints := []*endpoint.ServiceSafePoint{
		{ServiceID: "1", ExpiredAt: 0, SafePoint: 1},
		{ServiceID: "2", ExpiredAt: expireAt, SafePoint: 2},
		{ServiceID: "3", ExpiredAt: expireAt, SafePoint: 3},
	}

	for _, ssp := range serviceSafePoints {
		c.Assert(storage.SaveServiceGCSafePoint(ssp), IsNil)
	}

	// gc_worker's safepoint will be automatically inserted when loading service safepoints. Here the returned
	// safepoint can be either of "gc_worker" or "2".
	ssp, err := storage.LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(ssp.SafePoint, Equals, uint64(2))

	// Advance gc_worker's safepoint
	c.Assert(storage.SaveServiceGCSafePoint(&endpoint.ServiceSafePoint{
		ServiceID: "gc_worker",
		ExpiredAt: math.MaxInt64,
		SafePoint: 10,
	}), IsNil)

	ssp, err = storage.LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(ssp.ServiceID, Equals, "2")
	c.Assert(ssp.ExpiredAt, Equals, expireAt)
	c.Assert(ssp.SafePoint, Equals, uint64(2))
}

func (s *testStorageSuite) TestLoadRegions(c *C) {
	storage := NewStorageWithMemoryBackend()
	cache := core.NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegions(context.Background(), cache.SetRegion), IsNil)

	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func mustSaveRegions(c *C, s endpoint.RegionStorage, n int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := newTestRegionMeta(uint64(i))
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(s.SaveRegion(region), IsNil)
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

func (s *testStorageSuite) TestLoadRegionsToCache(c *C) {
	storage := NewStorageWithMemoryBackend()
	cache := core.NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegionsOnce(context.Background(), cache.SetRegion), IsNil)

	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}

	n = 20
	mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegionsOnce(context.Background(), cache.SetRegion), IsNil)
	c.Assert(cache.GetRegionCount(), Equals, n)
}

func (s *testStorageSuite) TestLoadRegionsExceedRangeLimit(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/kv/withRangeLimit", "return(500)"), IsNil)
	storage := NewStorageWithMemoryBackend()
	cache := core.NewRegionsInfo()

	n := 1000
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegions(context.Background(), cache.SetRegion), IsNil)
	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/kv/withRangeLimit"), IsNil)
}
