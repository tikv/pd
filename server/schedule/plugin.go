package schedule

import (
	"fmt"
	"path/filepath"
	"plugin"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// ScheduleInfo save all schedule information
var ScheduleInfo []MoveRegion

// ScheduleInfoLock is a lock for ScheduleInfo
var ScheduleInfoLock = sync.RWMutex{}

// MoveRegion is a schedule information structure
type MoveRegion struct {
	RegionIDs []uint64
	StoreIDs  []uint64
	StartTime time.Time
	EndTime   time.Time
}

// IsPredictedHotRegion determine whether the region is the hot spot of prediction
func IsPredictedHotRegion(cluster Cluster, regionID uint64) bool {
	for _, info := range ScheduleInfo {
		currentTime := time.Now()
		if currentTime.After(info.EndTime) || currentTime.Before(info.StartTime) {
			continue
		}
		for _, id := range info.RegionIDs {
			if id == regionID {
				log.Info("region is predicted hot", zap.Uint64("region-id", regionID))
				return true
			}
		}
	}
	return false
}

// PluginMap save all scheduler plugin
var PluginMap = make(map[string]*plugin.Plugin)

// PluginMapLock is a lock for PluginMap
var PluginMapLock = sync.RWMutex{}

// GetFunction gets func by funcName from plugin(.so)
func GetFunction(path string, funcName string) (plugin.Symbol, error) {
	PluginMapLock.Lock()
	if PluginMap[path] == nil {
		//open plugin
		filePath, err := filepath.Abs(path)
		if err != nil {
			PluginMapLock.Unlock()
			return nil, err
		}
		log.Info("open plugin file", zap.String("file-path", filePath))
		p, err := plugin.Open(filePath)
		if err != nil {
			PluginMapLock.Unlock()
			return nil, err
		}
		PluginMap[path] = p
	}
	PluginMapLock.Unlock()
	PluginMapLock.RLock()
	defer PluginMapLock.RUnlock()
	//get func from plugin
	f, err := PluginMap[path].Lookup(funcName)
	if err != nil {
		fmt.Println("Lookup func error!")
		return nil, err
	}
	return f, nil
}
