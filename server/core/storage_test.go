// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
)

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
}

func (s *testKVSuite) TestBasic(c *C) {
	storage := NewStorage(kv.NewMemoryKV())

	c.Assert(storage.storePath(123), Equals, "raft/s/00000000000000000123")
	c.Assert(regionPath(123), Equals, "raft/r/00000000000000000123")

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

func mustSaveStores(c *C, s *Storage, n int) []*metapb.Store {
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

func (s *testKVSuite) TestLoadStores(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewStoresInfo()

	n := 10
	stores := mustSaveStores(c, storage, n)
	c.Assert(storage.LoadStores(cache.SetStore), IsNil)

	c.Assert(cache.GetStoreCount(), Equals, n)
	for _, store := range cache.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
}

func (s *testKVSuite) TestStoreWeight(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewStoresInfo()
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

func mustSaveRegions(c *C, s *Storage, n int) []*metapb.Region {
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

func (s *testKVSuite) TestLoadRegions(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegions(cache.SetRegion), IsNil)

	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func (s *testKVSuite) TestLoadRegionsToCache(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegionsOnce(cache.SetRegion), IsNil)

	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}

	n = 20
	mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegionsOnce(cache.SetRegion), IsNil)
	c.Assert(cache.GetRegionCount(), Equals, n)
}

func (s *testKVSuite) TestLoadRegionsExceedRangeLimit(c *C) {
	storage := NewStorage(&KVWithMaxRangeLimit{Base: kv.NewMemoryKV(), rangeLimit: 500})
	cache := NewRegionsInfo()

	n := 1000
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegions(cache.SetRegion), IsNil)
	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func (s *testKVSuite) TestLoadGCSafePoint(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
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

func (s *testKVSuite) TestSaveServiceGCSafePoint(c *C) {
	mem := kv.NewMemoryKV()
	storage := NewStorage(mem)
	expireAt := time.Now().Add(100 * time.Second).Unix()
	serviceSafePoints := []*ServiceSafePoint{
		{"1", expireAt, 1},
		{"2", expireAt, 2},
		{"3", expireAt, 3},
	}

	for _, ssp := range serviceSafePoints {
		c.Assert(storage.SaveServiceGCSafePoint(ssp), IsNil)
	}

	prefix := path.Join(gcPath, "safe_point", "service") + "/"
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := mem.LoadRange(prefix, prefixEnd, len(serviceSafePoints))
	c.Assert(err, IsNil)
	c.Assert(len(keys), Equals, 3)
	c.Assert(len(values), Equals, 3)

	ssp := &ServiceSafePoint{}
	for i, key := range keys {
		c.Assert(strings.HasSuffix(key, serviceSafePoints[i].ServiceID), IsTrue)

		c.Assert(json.Unmarshal([]byte(values[i]), ssp), IsNil)
		c.Assert(ssp.ServiceID, Equals, serviceSafePoints[i].ServiceID)
		c.Assert(ssp.ExpiredAt, Equals, serviceSafePoints[i].ExpiredAt)
		c.Assert(ssp.SafePoint, Equals, serviceSafePoints[i].SafePoint)
	}
}

func (s *testKVSuite) TestLoadMinServiceGCSafePoint(c *C) {
	mem := kv.NewMemoryKV()
	storage := NewStorage(mem)
	expireAt := time.Now().Add(1000 * time.Second).Unix()
	serviceSafePoints := []*ServiceSafePoint{
		{"1", 0, 1},
		{"2", expireAt, 2},
		{"3", expireAt, 3},
	}

	for _, ssp := range serviceSafePoints {
		c.Assert(storage.SaveServiceGCSafePoint(ssp), IsNil)
	}

	ssp, err := storage.LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(ssp.ServiceID, Equals, "2")
	c.Assert(ssp.ExpiredAt, Equals, expireAt)
	c.Assert(ssp.SafePoint, Equals, uint64(2))
}

type KVWithMaxRangeLimit struct {
	kv.Base
	rangeLimit int
}

func (kv *KVWithMaxRangeLimit) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	if limit > kv.rangeLimit {
		return nil, nil, errors.Errorf("limit %v exceed max rangeLimit %v", limit, kv.rangeLimit)
	}
	return kv.Base.LoadRange(key, endKey, limit)
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:   []byte(fmt.Sprintf("%20d", regionID+1)),
	}
}
