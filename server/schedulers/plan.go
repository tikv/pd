package schedulers

import (
	"strconv"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
)

type schedulePlan struct {
	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo

	// hot region only
	// TODO: remove it
	srcPeerStat *statistics.HotPeerStat

	score int64
}

func newSchedulePlan() *schedulePlan {
	return &schedulePlan{}
}

func (p *schedulePlan) SourceMetricLabel() string {
	return strconv.FormatUint(p.source.GetID(), 10)
}

func (p *schedulePlan) TargetMetricLabel() string {
	return strconv.FormatUint(p.target.GetID(), 10)
}
