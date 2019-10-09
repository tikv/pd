package main

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/filter"
	"go.uber.org/zap"
)

type moveRegionUserScheduler struct {
	*userBaseScheduler
	name         string
	opController *schedule.OperatorController
	storeIDs     []uint64
	keyStart     string
	keyEnd       string
	timeInterval *schedule.TimeInterval
}

// Only use for register scheduler
// newMoveRegionUserScheduler() will be called manually
func init() {
	schedule.RegisterScheduler("move-region-user", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newMoveRegionUserScheduler(opController, "", "", "", []uint64{}, nil), nil
	})
}

func newMoveRegionUserScheduler(opController *schedule.OperatorController, name, keyStart, keyEnd string, storeIDs []uint64, interval *schedule.TimeInterval) schedule.Scheduler {
	base := newUserBaseScheduler(opController)
	log.Info("", zap.String("New", name), zap.Strings("key range", []string{keyStart, keyEnd}))
	return &moveRegionUserScheduler{
		userBaseScheduler: base,
		name:              name,
		storeIDs:          storeIDs,
		keyStart:          keyStart,
		keyEnd:            keyEnd,
		timeInterval:      interval,
		opController:      opController,
	}
}

func (r *moveRegionUserScheduler) GetName() string {
	return r.name
}

func (r *moveRegionUserScheduler) GetType() string {
	return "move-region-user"
}

func (r *moveRegionUserScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return r.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (r *moveRegionUserScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedule.PluginsInfoMapLock.RLock()
	defer schedule.PluginsInfoMapLock.RUnlock()
	// Determine if there is a time limit
	if r.timeInterval != nil {
		currentTime := time.Now()
		if currentTime.After(r.timeInterval.GetEnd()) || r.timeInterval.GetBegin().After(currentTime) {
			return nil
		}
	}
	// When region ids change, re-output scheduler's regions and stores
	regionIDs := schedule.GetRegionIDs(cluster, r.keyStart, r.keyEnd)
//	log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Regions", regionIDs))
//	log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Stores", r.storeIDs))

	if len(r.storeIDs) == 0 {
		return nil
	}

	filters := []filter.Filter {
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
		if !r.allExist(storeIDs, region) {
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
			op, err := operator.CreateMoveRegionOperator("move-region-user", cluster, region, operator.OpAdmin, storeMap)
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
func (r *moveRegionUserScheduler) allExist(storeIDs []uint64, region *core.RegionInfo) bool {
	for _, storeID := range storeIDs {
		if _, ok := region.GetStoreIds()[storeID]; ok {
			continue
		} else {
			return false
		}
	}
	return true
}
