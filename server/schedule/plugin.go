package schedule

import (
	"encoding/hex"
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
	StartKey  string
	EndKey    string
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
		regionIDs := GetRegionIDs(cluster, info.StartKey, info.EndKey)
		for _, id := range regionIDs {
			if id == regionID {
				log.Info("region is predicted hot", zap.Uint64("region-id", regionID))
				return true
			}
		}
	}
	return false
}

// GetRegionIDs get all regions within the specified key range
func GetRegionIDs(cluster Cluster, keyStart, keyEnd string) []uint64 {
	regionIDs := []uint64{}
	//decode key form string to []byte
	startKey, err := hex.DecodeString(keyStart)
	if err != nil {
		log.Error("can not decode", zap.String("key:", keyStart))
		return regionIDs
	}
	endKey, err := hex.DecodeString(keyEnd)
	if err != nil {
		log.Info("can not decode", zap.String("key:", keyEnd))
		return regionIDs
	}

	lastKey := []byte{}
	regions := cluster.ScanRegions(startKey, endKey, 0)
	for _, region := range regions {
		regionIDs = append(regionIDs, region.GetID())
		lastKey = region.GetEndKey()
	}

	if len(regions) == 0 {
		lastRegion := cluster.ScanRegions(startKey, []byte{}, 1)
		if len(lastRegion) == 0 {
			return regionIDs
		} else {
			//if get the only one region, exclude it
			if len(lastRegion[0].GetStartKey()) == 0 && len(lastRegion[0].GetEndKey()) == 0 {
				return regionIDs
			} else {
				//      startKey         endKey
				//         |			   |
				//     -----------------------------
				// ...|	  region0  |    region1  | ...
				//    -----------------------------
				// key range span two regions
				// choose region1
				regionIDs = append(regionIDs, lastRegion[0].GetID())
				return regionIDs
			}
		}
	} else {
		if len(lastKey) == 0 {
			// if regions last one is the last region
			return regionIDs
		} else {
			//            startKey		                            endKey
			//         	   |                                         |
			//      -----------------------------------------------------------
			// ... |	  region_i   |   ...   |     region_j   |    region_j+1   | ...
			//     -----------------------------------------------------------
			// ScanRangeWithEndKey(startKey, endKey) will get region i+1 to j
			// lastKey = region_j's EndKey
			// ScanRegions(lastKey, 1) then get region_j+1
			// so finally get region i+1 to j+1
			lastRegion := cluster.ScanRegions(lastKey, []byte{}, 1)
			if len(lastRegion) != 0 {
				regionIDs = append(regionIDs, lastRegion[0].GetID())
			}
			return regionIDs
		}
	}
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
