package main

import (
	"github.com/pingcap/pd/server/schedule"
	"strings"
)

func ProduceScheduler(cfg schedule.Config, opController *schedule.OperatorController, cluster schedule.Cluster) []schedule.Scheduler {
	storeMap := cfg.GetStoreId(cluster)
	intervalMaps := cfg.GetInterval()
	schedules := []schedule.Scheduler{}

	schedule.PluginsInfoMapLock.Lock()
	defer schedule.PluginsInfoMapLock.Unlock()
	// produce schedulers
	for str, storeIDs := range storeMap {
		schedule.PluginsInfoMap[str].UpdateStoreIDs(cluster)
		s := strings.Split(str, "-")
		name := "move-region-use-scheduler-" + s[1]
		schedules = append(schedules,
			newMoveRegionUserScheduler(opController, name,
				schedule.PluginsInfoMap[str].GetKeyStart(), schedule.PluginsInfoMap[str].GetKeyEnd(), storeIDs, intervalMaps[str]))
	}

	return schedules
}
