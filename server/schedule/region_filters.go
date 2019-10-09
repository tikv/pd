package schedule

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/opt"
)

type RegionFilter interface {
	Type() string
	FilterSource(opt opt.Options, region *core.RegionInfo, interval *TimeInterval, regionIDs []uint64) bool
	FilterTarget(opt opt.Options, region *core.RegionInfo, interval *TimeInterval, regionIDs []uint64) bool
}

// FilterSource checks if region can pass all Filters as source region.
func RegionFilterSource(opt opt.Options, region *core.RegionInfo, filters []RegionFilter, interval *TimeInterval, regionIDs []uint64) bool {
	for _, filter := range filters {
		if filter.FilterSource(opt, region, interval, regionIDs) {
			return true
		}
	}
	return false
}
