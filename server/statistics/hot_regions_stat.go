package statistics

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	TotalFlowBytes uint64      `json:"total_flow_bytes"`
	RegionsCount   int         `json:"regions_count"`
	RegionsStat    RegionsStat `json:"statistics"`
}

// RegionsStat is a list of a group region state type
type RegionsStat []HotSpotPeerStat

func (m RegionsStat) Len() int           { return len(m) }
func (m RegionsStat) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m RegionsStat) Less(i, j int) bool { return m[i].FlowBytes < m[j].FlowBytes }
