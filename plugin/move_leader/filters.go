package main

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/opt"
	"time"
)

type violentFilter struct {
}

func NewViolentFilter() schedule.RegionFilter {
	return &violentFilter{}
}

func (f *violentFilter) Type() string {
	return "violent-filter"
}

func (f *violentFilter) FilterSource(opt opt.Options, region *core.RegionInfo, interval *schedule.TimeInterval, regionIDs []uint64) bool {
	if interval != nil {
		currentTime := time.Now()
		if currentTime.After(interval.End) || interval.Begin.After(currentTime) {
			return false
		}
	}
	return f.isExists(region.GetID(), regionIDs)
}

func (f *violentFilter) FilterTarget(opt opt.Options, region *core.RegionInfo, interval *schedule.TimeInterval, regionIDs []uint64) bool {
	if interval != nil {
		currentTime := time.Now()
		if currentTime.After(interval.End) || interval.Begin.After(currentTime) {
			return false
		}
	}
	return f.isExists(region.GetID(), regionIDs)
}

func (f *violentFilter) isExists(regionID uint64, regionIDs []uint64) bool {
	for _, id := range regionIDs {
		if id == regionID {
			return true
		}
	}
	return false
}
