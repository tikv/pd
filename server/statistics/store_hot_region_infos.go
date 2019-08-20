package statistics

// StoreHotRegionInfos : used to get human readable description for hot regions.
type StoreHotRegionInfos struct {
	AsPeer   StoreHotRegionsStat `json:"as_peer"`
	AsLeader StoreHotRegionsStat `json:"as_leader"`
}

// StoreHotRegionsStat used to record the hot region statistics group by store
type StoreHotRegionsStat map[uint64]*HotRegionsStat
