package schedule

import (
	"strconv"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
)

type Plan struct {
	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo

	// hot region only
	// TODO: remove it
	srcPeerStat *statistics.HotPeerStat

	score int64
}

func NewSchedulePlan() *Plan {
	return &Plan{}
}

func (p *Plan) SourceMetricLabel() string {
	return strconv.FormatUint(p.source.GetID(), 10)
}

func (p *Plan) TargetMetricLabel() string {
	return strconv.FormatUint(p.target.GetID(), 10)
}
