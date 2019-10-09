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

type moveLeaderUserScheduler struct {
	*userBaseScheduler
	name         string
	opController *schedule.OperatorController
	storeIDs     []uint64
	keyStart     string
	keyEnd       string
	storeSeq     int
	timeInterval *schedule.TimeInterval
}

// Only use for register scheduler
// newMoveLeaderUserScheduler() will be called manually
func init() {
	schedule.RegisterScheduler("move-leader-user", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newMoveLeaderUserScheduler(opController, "", "", "", []uint64{}, nil), nil
	})
}

func newMoveLeaderUserScheduler(opController *schedule.OperatorController, name, keyStart, keyEnd string, storeIDs []uint64, interval *schedule.TimeInterval) schedule.Scheduler {
	log.Info("", zap.String("New", name), zap.Strings("key range", []string{keyStart, keyEnd}))
	base := newUserBaseScheduler(opController)
	return &moveLeaderUserScheduler{
		userBaseScheduler: base,
		name:              name,
		storeIDs:          storeIDs,
		keyStart:          keyStart,
		keyEnd:            keyEnd,
		storeSeq:          0,
		timeInterval:      interval,
		opController:      opController,
	}
}

func (l *moveLeaderUserScheduler) GetName() string {
	return l.name
}

func (l *moveLeaderUserScheduler) GetType() string {
	return "move-leader-user"
}

func (l *moveLeaderUserScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (l *moveLeaderUserScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedule.PluginsInfoMapLock.RLock()
	defer schedule.PluginsInfoMapLock.RUnlock()
	// Determine if there is a time limit
	if l.timeInterval != nil {
		currentTime := time.Now()
		if currentTime.After(l.timeInterval.GetEnd()) || l.timeInterval.GetBegin().After(currentTime) {
			return nil
		}
	}
	// When region ids change, re-output scheduler's regions and stores
	regionIDs := schedule.GetRegionIDs(cluster, l.keyStart, l.keyEnd)
	//log.Info("", zap.String("Schedule()", l.GetName()), zap.Uint64s("Regions", regionIDs))
	//log.Info("", zap.String("Schedule()", l.GetName()), zap.Uint64s("Stores", l.storeIDs))

	if len(l.storeIDs) == 0 {
		return nil
	}

	for _, regionID := range regionIDs {
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
		if !l.isExists(sourceID, l.storeIDs) {
			// Let "seq" store be the target first
			targetID := l.storeIDs[l.storeSeq]
			// seq increase
			if l.storeSeq < len(l.storeIDs)-1 {
				l.storeSeq++
			} else {
				l.storeSeq = 0
			}
			target := cluster.GetStore(targetID)
			if _, ok := region.GetStoreIds()[targetID]; ok {
				// target store has region peer, so do "transfer leader"
				filters := []filter.Filter{
					filter.StoreStateFilter{TransferLeader: true},
				}
				if filter.Source(cluster, source, filters) {
					log.Info("filter source",
						zap.String("scheduler", l.GetName()),
						zap.Uint64("region-id", regionID),
						zap.Uint64("store-id", sourceID))
					continue
				}
				if filter.Target(cluster, target, filters) {
					log.Info("filter target",
						zap.String("scheduler", l.GetName()),
						zap.Uint64("region-id", regionID),
						zap.Uint64("store-id", targetID))
					continue
				}
				op := operator.CreateTransferLeaderOperator("move-leader-user", region, sourceID, targetID, operator.OpLeader)
				op.SetPriorityLevel(core.HighPriority)
				return []*operator.Operator{op}
			} else {
				// target store doesn't have region peer, so do "move leader"
				filters := []filter.Filter{
					filter.StoreStateFilter{MoveRegion: true},
				}
				if filter.Source(cluster, source, filters) {
					log.Info("filter source",
						zap.String("scheduler", l.GetName()),
						zap.Uint64("region-id", regionID),
						zap.Uint64("store-id", sourceID))
					continue
				}
				if filter.Target(cluster, target, filters) {
					log.Info("filter target",
						zap.String("scheduler", l.GetName()),
						zap.Uint64("region-id", regionID),
						zap.Uint64("store-id", targetID))
					continue
				}
				destPeer, err := cluster.AllocPeer(targetID)
				if err != nil {
					log.Error("failed to allocate peer", zap.Error(err))
					continue
				}
				op, err := operator.CreateMoveLeaderOperator("move-leader-user", cluster, region, operator.OpAdmin, sourceID, targetID, destPeer.GetId())
				if err != nil {
					log.Error("CreateMoveLeaderOperator Err",
						zap.String("scheduler", l.GetName()),
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

// isExists(ID , IDs) determine if the ID is in IDs
func (l *moveLeaderUserScheduler) isExists(ID uint64, IDs []uint64) bool {
	for _, id := range IDs {
		if id == ID {
			return true
		}
	}
	return false
}
