package statistics

import (
	"time"

	"github.com/pingcap/pd/server/core"
)

// HotSpotPeerStatGenerator used to produce new hotspot statistics.
type HotSpotPeerStatGenerator interface {
	GenHotSpotPeerStats(stats *StoresStats) *HotSpotPeerStat
}

// hotSpotPeerStatBuilder used to produce new hotspot statistics.
type hotSpotPeerStatGenerator struct {
	Region    *core.RegionInfo
	StoreID   uint64
	FlowKeys  uint64
	FlowBytes uint64
	Expired   bool
	Kind      FlowKind

	lastHotSpotPeerStats *HotSpotPeerStat
}

const rollingWindowsSize = 5

// GenHotSpotPeerStats implements HotSpotPeerStatsGenerator.
func (flowStats *hotSpotPeerStatGenerator) GenHotSpotPeerStats(stats *StoresStats) *HotSpotPeerStat {
	var hotRegionThreshold uint64
	switch flowStats.Kind {
	case WriteFlow:
		hotRegionThreshold = calculateWriteHotThresholdWithStore(stats, flowStats.StoreID)
	case ReadFlow:
		hotRegionThreshold = calculateReadHotThresholdWithStore(stats, flowStats.StoreID)
	}
	flowBytes := flowStats.FlowBytes
	oldItem := flowStats.lastHotSpotPeerStats
	region := flowStats.Region
	newItem := &HotSpotPeerStat{
		RegionID:       region.GetID(),
		FlowBytes:      flowStats.FlowBytes,
		FlowKeys:       flowStats.FlowKeys,
		LastUpdateTime: time.Now(),
		StoreID:        flowStats.StoreID,
		Version:        region.GetMeta().GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
		Kind:           flowStats.Kind,
		needDelete:     flowStats.Expired,
	}

	if region.GetLeader().GetStoreId() == flowStats.StoreID {
		newItem.isLeader = true
	}

	if newItem.IsNeedDelete() {
		return newItem
	}

	if oldItem != nil {
		newItem.HotDegree = oldItem.HotDegree + 1
		newItem.Stats = oldItem.Stats
	}

	if flowBytes >= hotRegionThreshold {
		if oldItem == nil {
			newItem.Stats = NewRollingStats(rollingWindowsSize)
		}
		newItem.isNew = true
		newItem.Stats.Add(float64(flowBytes))
		return newItem
	}

	// smaller than hotRegionThreshold
	if oldItem == nil {
		return nil
	}
	if oldItem.AntiCount <= 0 {
		newItem.needDelete = true
		return newItem
	}
	// eliminate some noise
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	newItem.Stats.Add(float64(flowBytes))
	return newItem
}
