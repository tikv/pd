package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/pd/server/schedule"
)

var scheduleInfo []moveRegion
var scheduleInfoLock = sync.RWMutex{}

type moveRegion struct {
	startKey  string
	endKey    string
	storeIDs  []uint64
	startTime time.Time
	endTime   time.Time
}

func init() {
	storeIDs := []uint64{2, 3}
	moveRegionInfo1 := moveRegion{
		startKey:  "757365727461626C653A7573657235333338323038333332323133373236333037",
		endKey:    "757365727461626C653A7573657236303233353630333138313739393833353231",
		storeIDs:  storeIDs,
		startTime: time.Now().Add(-8 * time.Hour),
		endTime:   time.Now().Add(8 * time.Hour),
	}
	scheduleInfo = append(scheduleInfo, moveRegionInfo1)
	moveRegionInfo2 := moveRegion{
		startKey:  "757365727461626C653A7573657234363734383136383931393933333735323231",
		endKey:    "757365727461626C653A7573657235333338323038333332323133373236333037",
		storeIDs:  storeIDs,
		startTime: time.Now().Add(-8 * time.Hour),
		endTime:   time.Now().Add(8 * time.Hour),
	}
	scheduleInfo = append(scheduleInfo, moveRegionInfo2)
}

// CreateScheduler create scheduler based on schedule info
func CreateScheduler(opController *schedule.OperatorController, cluster schedule.Cluster) []schedule.Scheduler {
	schedules := []schedule.Scheduler{}

	scheduleInfoLock.Lock()
	defer scheduleInfoLock.Unlock()
	// produce schedulers
	for id, info := range scheduleInfo {
		name := "move-region-use-scheduler-" + strconv.Itoa(id)
		schedules = append(schedules,
			newMoveRegionUserScheduler(opController, name,
				info.startKey, info.endKey, info.storeIDs, info.startTime, info.endTime))
	}

	return schedules
}
