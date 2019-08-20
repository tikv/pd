package statistics

import "github.com/pingcap/pd/server/core"

// RegionStatInformer provides access to a shared informer of statistics.
type RegionStatInformer interface {
	IsRegionHot(region *core.RegionInfo) bool
	RegionWriteStats() map[uint64][]*HotSpotPeerStat
	RegionReadStats() map[uint64][]*HotSpotPeerStat
	RandHotRegionFromStore(store uint64, kind FlowKind) *core.RegionInfo
}
