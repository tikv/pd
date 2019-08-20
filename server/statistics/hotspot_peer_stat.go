package statistics

import "time"

// HotSpotPeerStat records each hot region's statistics
type HotSpotPeerStat struct {
	RegionID  uint64 `json:"region_id"`
	FlowBytes uint64 `json:"flow_bytes"`
	FlowKeys  uint64 `json:"flow_keys"`
	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	// StoreID is the store id of the region peer
	StoreID uint64   `json:"store_id"`
	Kind    FlowKind `json:"kind"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int
	// Version used to check the region split times
	Version uint64
	// Stats is a rolling statistics, recording some recently added records.
	Stats *RollingStats

	needDelete bool
	isLeader   bool
	isNew      bool
}

// IsNeedDelete to delete the item in cache.
func (stat HotSpotPeerStat) IsNeedDelete() bool {
	return stat.needDelete
}

// IsLeader indicaes the item belong to the leader.
func (stat HotSpotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// IsNew indicaes the item is first update in the cache of the region.
func (stat HotSpotPeerStat) IsNew() bool {
	return stat.isNew
}
