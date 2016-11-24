// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
	server  *Server
	cleanup cleanUpFunc
}

func (s *testKVSuite) SetUpTest(c *C) {
	s.server, s.cleanup = mustRunTestServer(c)
}

func (s *testKVSuite) TearDownTest(c *C) {
	s.cleanup()
}

func (s *testKVSuite) TestBasic(c *C) {
	kv := newKV(s.server)

	clusterID := s.server.clusterID
	storePath := fmt.Sprintf("/pd/%v/raft/s/00000000000000000123", clusterID)
	regionPath := fmt.Sprintf("/pd/%v/raft/r/00000000000000000123", clusterID)
	mergePath := fmt.Sprintf("/pd/%v/raft/merge/00000000000000000123", clusterID)
	shutdownPath := fmt.Sprintf("/pd/%v/raft/shutdown/00000000000000000123", clusterID)
	c.Assert(kv.storePath(123), Equals, storePath)
	c.Assert(kv.regionPath(123), Equals, regionPath)
	c.Assert(kv.mergePath(123), Equals, mergePath)
	c.Assert(kv.shutdownPath(123), Equals, shutdownPath)

	meta := &metapb.Cluster{Id: 123}
	ok, err := kv.loadMeta(meta)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(kv.saveMeta(meta), IsNil)
	newMeta := &metapb.Cluster{}
	ok, err = kv.loadMeta(newMeta)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newMeta, DeepEquals, meta)

	store := &metapb.Store{Id: 123}
	ok, err = kv.loadStore(123, store)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(kv.saveStore(store), IsNil)
	newStore := &metapb.Store{}
	ok, err = kv.loadStore(123, newStore)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newStore, DeepEquals, store)

	region := &metapb.Region{Id: 123}
	ok, err = kv.loadRegion(123, region)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(kv.saveRegion(region), IsNil)
	newRegion := &metapb.Region{}
	ok, err = kv.loadRegion(123, newRegion)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newRegion, DeepEquals, region)

	// Merge.

	value, err := kv.load(kv.mergePath(1))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	c.Assert(kv.markRegionMerge(1, 2), IsNil)
	intoID, ok, err := kv.loadRegionMerge(1)
	c.Assert(intoID, Equals, uint64(2))
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	c.Assert(kv.clearRegionMerge(1), IsNil)
	value, err = kv.load(kv.mergePath(1))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	region1 := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	region2 := &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}
	c.Assert(kv.saveRegion(region1), IsNil)
	c.Assert(kv.saveRegion(region2), IsNil)
	c.Assert(kv.markRegionMerge(1, 2), IsNil)
	region2 = &metapb.Region{Id: 2, StartKey: []byte("a"), EndKey: []byte("c")}
	c.Assert(kv.commitRegionMerge(1, region2), IsNil)
	value, err = kv.load(kv.shutdownPath(1))
	c.Assert(err, IsNil)
	c.Assert(value, NotNil)
	value, err = kv.load(kv.mergePath(1))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)
	newRegion = &metapb.Region{}
	ok, err = kv.loadRegion(1, newRegion)
	c.Assert(ok, IsFalse)
	c.Assert(err, IsNil)
	ok, err = kv.loadRegion(2, newRegion)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)
	c.Assert(newRegion, DeepEquals, region2)
}

func mustSaveStores(c *C, kv *kv, n int) []*metapb.Store {
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(kv.saveStore(store), IsNil)
	}

	return stores
}

func (s *testKVSuite) TestLoadStores(c *C) {
	kv := newKV(s.server)
	cache := newStoresInfo()

	n := 10
	stores := mustSaveStores(c, kv, n)
	c.Assert(kv.loadStores(cache, 3), IsNil)

	c.Assert(cache.getStoreCount(), Equals, n)
	for _, store := range cache.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
}

func mustSaveRegions(c *C, kv *kv, n int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := &metapb.Region{Id: uint64(i)}
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(kv.saveRegion(region), IsNil)
	}

	return regions
}

func mustMergeRegions(c *C, kv *kv, regions []*metapb.Region) {
	for i := 0; i < len(regions); i += 2 {
		c.Assert(kv.markRegionMerge(regions[i].GetId(), regions[i+1].GetId()), IsNil)
	}
	for i := 2; i < len(regions); i += 4 {
		c.Assert(kv.commitRegionMerge(regions[i].GetId(), regions[i+1]), IsNil)
	}
}

func (s *testKVSuite) TestLoadRegions(c *C) {
	kv := newKV(s.server)
	cache := newRegionsInfo()

	n := 10
	regions := mustSaveRegions(c, kv, n)
	c.Assert(kv.loadRegions(cache, 3), IsNil)

	c.Assert(cache.getRegionCount(), Equals, n)
	for _, region := range cache.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}

	mustMergeRegions(c, kv, regions)
	c.Assert(kv.loadMergeRegions(cache, 2), IsNil)
	c.Assert(kv.loadShutdownRegions(cache, 1), IsNil)

	c.Assert(cache.getMergingRegionCount(), Equals, 6)
	c.Assert(cache.getShutdownRegionCount(), Equals, 2)
	for i := 0; i < len(regions); i += 4 {
		c.Assert(cache.isRegionMerging(regions[i].GetId()), IsTrue)
		c.Assert(cache.isRegionMerging(regions[i+1].GetId()), IsTrue)
	}
	for i := 2; i < len(regions); i += 4 {
		c.Assert(cache.isRegionShutdown(regions[i].GetId()), IsTrue)
		c.Assert(cache.isRegionShutdown(regions[i+1].GetId()), IsFalse)
	}
}
