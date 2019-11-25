package syncer

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testLoadRegionsCacheSuite{})

type testLoadRegionsCacheSuite struct{}

func (s *testLoadRegionsCacheSuite) TestLoadRegionsCache(c *C) {
	cache := core.NewRegionsInfo()
	n := 10
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := newTestRegionMeta(uint64(i))
		regions = append(regions, region)
	}
	regionSyner := &RegionSyncer{
		regionsCache: regions,
	}
	c.Assert(regionSyner.LoadRegionsCache(cache.SetRegion), IsNil)
	c.Assert(cache.GetRegionCount(), Equals, n)
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}
func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:   []byte(fmt.Sprintf("%20d", regionID+1)),
	}
}
