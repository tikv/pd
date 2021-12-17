// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/kv"
)

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
}

func (s *testKVSuite) TestLoadRegions(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewRegionsInfo()

	n := 10
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegions(context.Background(), cache.SetRegion), IsNil)

	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
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

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:   []byte(fmt.Sprintf("%20d", regionID+1)),
	}
}

func (s *testKVSuite) TestLoadRegionsToCache(c *C) {
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewRegionsInfo()

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

func (s *testKVSuite) TestLoadRegionsExceedRangeLimit(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/kv/withRangeLimit", "return(500)"), IsNil)
	storage := NewStorage(kv.NewMemoryKV())
	cache := NewRegionsInfo()

	n := 1000
	regions := mustSaveRegions(c, storage, n)
	c.Assert(storage.LoadRegions(context.Background(), cache.SetRegion), IsNil)
	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/kv/withRangeLimit"), IsNil)
}
