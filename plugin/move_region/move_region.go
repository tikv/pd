package main

import (
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
	regionIDs    []uint64
	storeIDs     []uint64
	startTime    time.Time
	endTime      time.Time
}

// Only use for register scheduler
// newMoveRegionUserScheduler() will be called manually
func init() {
	schedule.RegisterScheduler("move-region-user", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newMoveRegionUserScheduler(opController, "", []uint64{}, []uint64{}, time.Time{}, time.Time{}), nil
	})
}

func newMoveRegionUserScheduler(opController *schedule.OperatorController, name string, regionIDs []uint64, storeIDs []uint64, startTime, endTime time.Time) schedule.Scheduler {
	base := newUserBaseScheduler(opController)
	log.Info("", zap.String("New", name), zap.Any("region Ids", regionIDs))
	return &moveRegionUserScheduler{
		userBaseScheduler: base,
		opController:      opController,
		name:              name,
		regionIDs:         regionIDs,
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
	log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Regions", r.regionIDs))
	// log.Info("", zap.String("Schedule()", r.GetName()), zap.Uint64s("Stores", r.storeIDs))

	if len(r.storeIDs) == 0 {
		return nil
	}
	for _, regionID := range r.regionIDs {
		region := cluster.GetRegion(regionID)
		if region == nil {
			log.Info("region not exist", zap.Uint64("region-id", regionID))
			continue
		}
		sourceID := region.GetLeader().GetStoreId()
		source := cluster.GetStore(sourceID)
		// If leader is in target stores,
		// it means user's rules has been met,
		// then do nothing
		if !isExists(sourceID, r.storeIDs) {
			// Let "seq" store be the target first
			targetID := r.storeIDs[0]
			target := cluster.GetStore(targetID)
			if _, ok := region.GetStoreIds()[targetID]; ok {
				// target store has region peer, so do "transfer leader"
				filters := []filter.Filter{
					filter.StoreStateFilter{TransferLeader: true},
				}
				if filter.Source(cluster, source, filters) {
					//log.Info("filter source", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", sourceID))
					continue
				}
				if filter.Target(cluster, target, filters) {
					//log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", targetID))
					continue
				}
				op := operator.CreateTransferLeaderOperator(r.name, region, sourceID, targetID, operator.OpLeader)
				op.SetPriorityLevel(core.HighPriority)
				return []*operator.Operator{op}
			} else {
				// target store doesn't have region peer, so do "move leader"
				filters := []filter.Filter{
					filter.StoreStateFilter{MoveRegion: true},
				}
				if filter.Source(cluster, source, filters) {
					//log.Info("filter source", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", sourceID))
					continue
				}
				if filter.Target(cluster, target, filters) {
					//log.Info("filter target", zap.String("scheduler", r.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("store-id", targetID))
					continue
				}
				destPeer, err := cluster.AllocPeer(targetID)
				if err != nil {
					log.Error("failed to allocate peer", zap.Error(err))
					continue
				}
				op, err := operator.CreateMoveLeaderOperator(r.name, cluster, region, operator.OpAdmin, sourceID, targetID, destPeer.GetId())
				if err != nil {
					log.Error("CreateMoveLeaderOperator Err",
						zap.String("scheduler", r.GetName()),
						zap.Error(err))
					continue
				}
				op.SetPriorityLevel(core.HighPriority)
				return []*operator.Operator{op}
			}
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

// isExists(ID , IDs) determine if the ID is in IDs
func isExists(ID uint64, IDs []uint64) bool {
	for _, id := range IDs {
		if id == ID {
			return true
		}
	}
	return false
}
