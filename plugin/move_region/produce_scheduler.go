package main

import (
	"strconv"
	"time"

	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.ScheduleInfoLock.Lock()
	defer schedule.ScheduleInfoLock.Unlock()

	storeIDs := []uint64{2, 3, 4}
	moveRegionInfo := schedule.MoveRegion{
		StartKey:  "",
		EndKey:    "757365727461626C653A7573657238373036373638343832343630373733313835",
		StoreIDs:  storeIDs,
		StartTime: time.Now().Add(-8 * time.Hour),
		EndTime:   time.Now().Add(8 * time.Hour),
	}
	schedule.ScheduleInfo = append(schedule.ScheduleInfo, moveRegionInfo)
}

// CreateUserScheduler create scheduler based on schedule info
func CreateUserScheduler(opController *schedule.OperatorController, cluster schedule.Cluster) []schedule.Scheduler {
	schedule.ScheduleInfoLock.Lock()
	defer schedule.ScheduleInfoLock.Unlock()
	schedulers := []schedule.Scheduler{}

	// produce schedulers
	for id, info := range schedule.ScheduleInfo {
		name := "move-region-use-scheduler-" + strconv.Itoa(id)
		schedulers = append(schedulers,
			newMoveRegionUserScheduler(opController, name,
				info.StartKey, info.EndKey, info.StoreIDs, info.StartTime, info.EndTime))
	}

	return schedulers
}
