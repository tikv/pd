package main

import (
	"encoding/hex"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"go.uber.org/zap"
)

type moveRegionUserScheduler struct {
	*userBaseScheduler
	opController *schedule.OperatorController
	name         string
	startKey     string
	endKey       string
	storeIDs     []uint64
	startTime    time.Time
	endTime      time.Time
}

// Only use for register scheduler
// newMoveRegionUserScheduler() will be called manually
func init() {
	schedule.RegisterScheduler("move-region-user", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newMoveRegionUserScheduler(opController, "", "", "", []uint64{}, time.Time{}, time.Time{}), nil
	})
}

func newMoveRegionUserScheduler(opController *schedule.OperatorController, name, startKey, endKey string, storeIDs []uint64, startTime, endTime time.Time) schedule.Scheduler {
	base := newUserBaseScheduler(opController)
	log.Info("", zap.String("New", name), zap.Strings("key range", []string{startKey, endKey}))
	return &moveRegionUserScheduler{
		userBaseScheduler: base,
		opController:      opController,
		name:              name,
		startKey:          startKey,
		endKey:            endKey,
		storeIDs:          storeIDs,
		startTime:         startTime,
		endTime:           endTime,
	}
}

func (r *moveRegionUserScheduler) GetName() string {
	return r.name
}

func (r *moveRegionUserScheduler) GetType() string {
	return "move-region-user"
}

func (r *moveRegionUserScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	currentTime := time.Now()
	if currentTime.Before(r.startTime) || currentTime.After(r.endTime) {
		return false
	}
	return r.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (r *moveRegionUserScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	// When region ids change, re-output scheduler's regions and stores
	regionIDs := getRegionIDs(cluster, r.startKey, r.endKey)
	// log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Regions", regionIDs))
	// log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Stores", r.storeIDs))

	if len(r.storeIDs) == 0 {
		return nil
	}

	filters := []filter.Filter{
		filter.StoreStateFilter{MoveRegion: true},
	}

	storeMap := make(map[uint64]struct{})
	storeIDs := []uint64{}
	// filter target stores first
	for _, storeID := range r.storeIDs {
		if filter.Target(cluster, cluster.GetStore(storeID), filters) {
			log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("store-id", storeID))
		} else {
			storeMap[storeID] = struct{}{}
			storeIDs = append(storeIDs, storeID)
		}
	}

	if len(storeMap) == 0 {
		return nil
	}

	replicas := cluster.GetMaxReplicas()
	for _, regionID := range regionIDs {
		region := cluster.GetRegion(regionID)
		if region == nil {
			log.Info("region not exist", zap.Uint64("region-id", regionID))
			continue
		}
		// If filtered target stores all contain a region peer,
		// it means user's rules has been met,
		// then do nothing
		if !allExist(storeIDs, region) {
			// if region max-replicas > target stores length,
			// add the store where the original peer is located sequentially,
			// until target stores length = max-replicas
			for storeID := range region.GetStoreIds() {
				if replicas > len(storeMap) {
					if _, ok := storeMap[storeID]; !ok {
						if filter.Target(cluster, cluster.GetStore(storeID), filters) {
							log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("store-id", storeID))
						} else {
							storeMap[storeID] = struct{}{}
						}
					}
				} else {
					break
				}
			}
			// if replicas still > target stores length, do nothing
			if replicas > len(storeMap) {
				log.Info("replicas > len(storeMap)", zap.String("scheduler", r.GetName()))
				continue
			}
			op, err := operator.CreateMoveRegionOperator(r.name, cluster, region, operator.OpAdmin, storeMap)
			if err != nil {
				log.Error("CreateMoveRegionOperator Err", zap.String("scheduler", r.GetName()), zap.Error(err))
				continue
			}
			return []*operator.Operator{op}
		}
	}
	return nil
}

// allExist(storeIDs, region) determine if all storeIDs contain a region peer
func allExist(storeIDs []uint64, region *core.RegionInfo) bool {
	for _, storeID := range storeIDs {
		if _, ok := region.GetStoreIds()[storeID]; ok {
			continue
		} else {
			return false
		}
	}
	return true
}

func getRegionIDs(cluster schedule.Cluster, keyStart, keyEnd string) []uint64 {
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
