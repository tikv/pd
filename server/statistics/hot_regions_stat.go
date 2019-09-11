package statistics

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	TotalFlowBytes uint64        `json:"total_flow_bytes"`
	RegionsCount   int           `json:"regions_count"`
	RegionsStat    []HotPeerStat `json:"statistics"`
}
