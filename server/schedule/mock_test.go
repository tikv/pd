package schedule

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"sync/atomic"
)

// Below are mock data functions

// mockIDAllocator mocks IDAllocator and it is only used for test.
type mockIDAllocator struct {
	base uint64
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{base: 0}
}

func (alloc *mockIDAllocator) Alloc() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}

type mockCluster struct {
	id mockIDAllocator
}

func newMockClusterInfo() *mockCluster {
	return &mockCluster{
		id: *newMockIDAllocator(),
	}
}

func (c *mockCluster) RandFollowerRegion(storeID uint64) *core.RegionInfo {
	return nil
}
func (c *mockCluster) RandLeaderRegion(storeID uint64) *core.RegionInfo            { return nil }
func (c *mockCluster) GetStores() []*core.StoreInfo                                { return nil }
func (c *mockCluster) GetStore(id uint64) *core.StoreInfo                          { return nil }
func (c *mockCluster) GetRegion(id uint64) *core.RegionInfo                        { return nil }
func (c *mockCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo   { return nil }
func (c *mockCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo { return nil }
func (c *mockCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo      { return nil }
func (c *mockCluster) BlockStore(id uint64) error                                  { return nil }
func (c *mockCluster) UnblockStore(id uint64)                                      {}
func (c *mockCluster) IsRegionHot(id uint64) bool                                  { return false }
func (c *mockCluster) RegionWriteStats() []*core.RegionStat                        { return nil }
func (c *mockCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, _ := c.id.Alloc()
	return &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}, nil

}

func newMockRegionInfo(id uint64) *core.RegionInfo {
	return &core.RegionInfo{
		Region: &metapb.Region{
			Id: id,
			Peers: []*metapb.Peer{
				newMockPeer(1, 1),
				newMockPeer(2, 2),
				newMockPeer(3, 3),
			},
		},
	}
}

func newMockPeer(id uint64, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		Id:      id,
		StoreId: storeID,
	}
}
