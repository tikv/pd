// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	// ShuffleHotRegionName is shuffle hot region scheduler name.
	ShuffleHotRegionName = "shuffle-hot-region-scheduler"
	// ShuffleHotRegionType is shuffle hot region scheduler type.
	ShuffleHotRegionType = "shuffle-hot-region"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(ShuffleHotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				conf.Limit = limit
			}
			conf.Name = ShuffleHotRegionName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleHotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleHotRegionScheduler(opController, conf), nil
	})
}

type shuffleHotRegionSchedulerConfig struct {
	Name  string `json:"name"`
	Limit uint64 `json:"limit"`
}

// ShuffleHotRegionScheduler mainly used to test.
// It will randomly pick a hot peer, and move the peer
// to a random store, and then transfer the leader to
// the hot peer.
type shuffleHotRegionScheduler struct {
	*BaseScheduler
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
	r           *rand.Rand
	conf        *shuffleHotRegionSchedulerConfig
	types       []rwType
	regionOpRecord    map[uint64]*opRecord
	schStatus         scheduleStatus
}

// newShuffleHotRegionScheduler creates an admin scheduler that random balance hot regions
func newShuffleHotRegionScheduler(opController *schedule.OperatorController, conf *shuffleHotRegionSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	ret := &shuffleHotRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		types:         []rwType{read, write},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func (s *shuffleHotRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *shuffleHotRegionScheduler) GetType() string {
	return ShuffleHotRegionType
}

func (s *shuffleHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *shuffleHotRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpHotRegion) < s.conf.Limit &&
		s.OpController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit() &&
		s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *shuffleHotRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	// i := s.r.Int() % len(s.types)
	// return s.dispatch(s.types[i], cluster)
	return s.runSch(write, cluster)
}

func (s *shuffleHotRegionScheduler) runSch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	for {
		switch s.schStatus {
		case scheduleInit:
			log.Info("start shuffle region")
			storesStats := cluster.GetStoresStats()
			minHotDegree := cluster.GetOpts().GetHotRegionCacheHitsThreshold()
			hotRegionThreshold := getHotRegionThreshold(storesStats, write)
			s.stLoadInfos[writePeer] = summaryStoresLoad(
				storesStats.GetStoresBytesWriteStat(),
				storesStats.GetStoresKeysWriteStat(),
				storesStats.GetStoresOpsWriteStat(),
				map[uint64]Influence{},
				cluster.RegionWriteStats(),
				minHotDegree,
				hotRegionThreshold,
				write, core.RegionKind, mixed)
			
			if cluster.GetOpts().GetHotSchedulerMode() % 10 > 0 {
				s.randomHotPeer(cluster, s.stLoadInfos[writePeer])
			} else {
				s.revertBalance(cluster, s.stLoadInfos[writePeer])
			}
			s.schStatus+= 1
		case scheduleSplit:
			pendingOps, done := s.processPendingOps(cluster)
			if !done {
				return pendingOps
			}
			log.Info("finish shuffle region")
			s.schStatus++
		default:
			return nil
		}
	}
	return nil
}

func (s *shuffleHotRegionScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	storesStats := cluster.GetStoresStats()
	minHotDegree := cluster.GetOpts().GetHotRegionCacheHitsThreshold()
	switch typ {
	case read:
		hotRegionThreshold := getHotRegionThreshold(storesStats, read)
		s.stLoadInfos[readLeader] = summaryStoresLoad(
			storesStats.GetStoresBytesReadStat(),
			storesStats.GetStoresKeysReadStat(),
			storesStats.GetStoresOpsReadStat(),
			map[uint64]Influence{},
			cluster.RegionReadStats(),
			minHotDegree,
			hotRegionThreshold,
			read, core.LeaderKind, mixed)
		return s.randomSchedule(cluster, s.stLoadInfos[readLeader])
	case write:
		hotRegionThreshold := getHotRegionThreshold(storesStats, write)
		s.stLoadInfos[writeLeader] = summaryStoresLoad(
			storesStats.GetStoresBytesWriteStat(),
			storesStats.GetStoresKeysWriteStat(),
			storesStats.GetStoresOpsWriteStat(),
			map[uint64]Influence{},
			cluster.RegionWriteStats(),
			minHotDegree,
			hotRegionThreshold,
			write, core.LeaderKind, mixed)
		return s.randomSchedule(cluster, s.stLoadInfos[writeLeader])
	}
	return nil
}

func (s *shuffleHotRegionScheduler) randomSchedule(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) []*operator.Operator {
	for _, detail := range loadDetail {
		if len(detail.HotPeers) < 1 {
			continue
		}
		i := s.r.Intn(len(detail.HotPeers))
		r := detail.HotPeers[i]
		// select src region
		srcRegion := cluster.GetRegion(r.RegionID)
		if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
			continue
		}
		srcStoreID := srcRegion.GetLeader().GetStoreId()
		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		}

		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewPlacementSafeguard(s.GetName(), cluster, srcRegion, srcStore),
		}
		stores := cluster.GetStores()
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if !filter.Target(cluster.GetOpts(), store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetID())
		}
		if len(destStoreIDs) == 0 {
			return nil
		}
		// random pick a dest store
		destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
		if destStoreID == 0 {
			return nil
		}
		srcPeer := srcRegion.GetStorePeer(srcStoreID)
		if srcPeer == nil {
			return nil
		}
		destPeer := &metapb.Peer{StoreId: destStoreID}
		op, err := operator.CreateMoveLeaderOperator("random-move-hot-leader", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStoreID, destPeer)
		if err != nil {
			log.Debug("fail to create move leader operator", errs.ZapError(err))
			return nil
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
	return nil
}

type loadDetailID struct {
	storeID uint64
	detail *storeLoadDetail
	flow float64
	expFlow float64
}

func (s *shuffleHotRegionScheduler) randomHotPeer(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) {
	loadType := 0
	s.regionOpRecord = make(map[uint64]*opRecord)

	for storeID, detail := range loadDetail {
		peers := detail.HotPeers
		if len(peers) < 1 {
			continue
		}

		sortedPeers := make([]*statistics.HotPeerStat, len(peers))
		copy(sortedPeers, peers)
		if loadType == 0 {
			sort.Slice(sortedPeers, func(i, j int) bool {
				return sortedPeers[i].GetByteRate() > sortedPeers[j].GetByteRate()
			})
		} else {
			sort.Slice(sortedPeers, func(i, j int) bool {
				return sortedPeers[i].GetKeyRate() > sortedPeers[j].GetKeyRate()
			})
		}

		maxCount := len(sortedPeers)
		if maxCount > 20 {
			maxCount = 20
		}
		for j := 0; j < 5; j++ {
			i := s.r.Intn(maxCount)
			peer := sortedPeers[i]
	
			srcStoreID := storeID
			// srcStore := cluster.GetStore(srcStoreID)
			srcRegion := cluster.GetRegion(peer.RegionID)
			if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
				continue
			}
	
			filters := []filter.Filter{
				filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
				filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
				// filter.NewPlacementSafeguard(s.GetName(), cluster, srcRegion, srcStore),
			}
			stores := cluster.GetStores()
			destStoreIDs := make([]uint64, 0, len(stores))
			for _, store := range stores {
				if !filter.Target(cluster.GetOpts(), store, filters) {
					continue
				}
				destStoreIDs = append(destStoreIDs, store.GetID())
			}
	
			destStoreID := destStoreIDs[s.r.Intn(len(destStoreIDs))]
			if destStoreID == 0 {
				continue
			}
	
			log.Info("move region info", 
				zap.Uint64("srcStoreID", srcStoreID), 
				zap.Uint64("dstStoreID", destStoreID),
				zap.Uint64("regionID", peer.RegionID),
			)
	
			s.regionOpRecord[peer.RegionID] = &opRecord{
				region: &regionInfo{
					regionID: peer.RegionID,
					srcStoreID: srcStoreID,
					dstStoreID: destStoreID,
				},
			}
		}

	}
}

func (s *shuffleHotRegionScheduler) revertBalance(cluster opt.Cluster, loadDetail map[uint64]*storeLoadDetail) {
	loadType := 0
	maxFlowThreshold := 30
	
	s.regionOpRecord = make(map[uint64]*opRecord)

	var details []*loadDetailID
	for storeID, detail := range loadDetail {
		flow := detail.LoadPred.Current.ByteRate
		if loadType == 1 {
			flow = detail.LoadPred.Current.KeyRate
		}
		details = append(details, &loadDetailID{
			storeID: storeID,
			detail: detail,
			flow: flow,
		})
	}

	sort.Slice(details, func(i, j int) bool {
		if loadType == 0 {
			return details[i].detail.LoadPred.Current.ByteRate < details[j].detail.LoadPred.Current.ByteRate
		} else {
			return details[i].detail.LoadPred.Current.KeyRate < details[j].detail.LoadPred.Current.KeyRate
		}
	})

	moveRatios := make([]float64, len(details))
	for i := range moveRatios {
		moveRatios[i] = float64(s.r.Intn(maxFlowThreshold) + 1) / 100.0
	}
	sort.Slice(moveRatios, func(i, j int) bool {
		return moveRatios[i] > moveRatios[j]
	})

	for i := uint64(0); i < uint64(len(details) - 1); i++ {
		peers := details[i].detail.HotPeers
		if len(peers) < 1 {
			continue
		}

		expByteRate := details[i].detail.LoadPred.Future.ExpByteRate
		expKeyRate := details[i].detail.LoadPred.Future.ExpKeyRate

		sortedPeers := make([]*statistics.HotPeerStat, len(peers))
		copy(sortedPeers, peers)
		if loadType == 0 {
			sort.Slice(sortedPeers, func(i, j int) bool {
				// return sortedPeers[i].GetByteRate() > sortedPeers[j].GetByteRate()

				normByteI := sortedPeers[i].GetByteRate() / expByteRate
				normByteJ := sortedPeers[j].GetByteRate() / expByteRate
				normKeyI := sortedPeers[i].GetKeyRate() / expKeyRate
				normKeyJ := sortedPeers[j].GetKeyRate() / expKeyRate
				if normByteI > normKeyI {
					if normByteJ > normKeyJ {
						return normByteI > normByteJ
					} else {
						return true
					}
				} else {
					if normByteJ > normKeyJ {
						return false
					} else {
						return normByteI > normByteJ
					}
				}
			})
		} else {
			sort.Slice(sortedPeers, func(i, j int) bool {
				// return sortedPeers[i].GetKeyRate() > sortedPeers[j].GetKeyRate()

				normByteI := sortedPeers[i].GetByteRate() / expByteRate
				normByteJ := sortedPeers[j].GetByteRate() / expByteRate
				normKeyI := sortedPeers[i].GetKeyRate() / expKeyRate
				normKeyJ := sortedPeers[j].GetKeyRate() / expKeyRate
				if normByteI < normKeyI {
					if normByteJ < normKeyJ {
						return normKeyI > normKeyJ
					} else {
						return true
					}
				} else {
					if normByteJ < normKeyJ {
						return false
					} else {
						return normKeyI > normKeyJ
					}
				}
			})
		}

		details[i].expFlow = (1 - moveRatios[i]) * details[i].flow
		log.Info("store flow info", 
			zap.Uint64("storeID", details[i].storeID), 
			zap.Float64("expFlow", details[i].expFlow), 
			zap.Float64("curFlow", details[i].flow),
			zap.Float64("ratio", moveRatios[i]),
		)

		for _, peer := range sortedPeers {
			if details[i].flow < details[i].expFlow {
				break
			}
			if _, ok := s.regionOpRecord[peer.RegionID]; ok {
				continue
			}
			
			srcStoreID := details[i].storeID
			srcRegion := cluster.GetRegion(peer.RegionID)
			if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
				continue
			}
			movedFlow := 0.0
			if loadType == 0 {
				movedFlow = peer.GetByteRate()
			} else {
				movedFlow = peer.GetKeyRate()
			}

			randNum := s.r.Intn(len(details) - 1 - int(i))
			dstID := uint64(randNum + int(i) + 1)
			destStoreID := details[dstID].storeID
			destPeer := srcRegion.GetStorePeer(destStoreID)
			if destPeer != nil {
				continue
			}

			details[i].flow -= movedFlow
			details[dstID].flow += movedFlow

			log.Info("move region info", 
				zap.Uint64("srcStoreID", srcStoreID), 
				zap.Uint64("dstStoreID", destStoreID),
				zap.Uint64("regionID", peer.RegionID),
				zap.Float64("regionFlow", movedFlow), 
			)

			s.regionOpRecord[peer.RegionID] = &opRecord{
				region: &regionInfo{
					regionID: peer.RegionID,
					srcStoreID: srcStoreID,
					dstStoreID: destStoreID,
				},
			}
		}
	}
}

func (s *shuffleHotRegionScheduler) processPendingOps(cluster opt.Cluster) ([]*operator.Operator, bool) {
	var pendingOps []*operator.Operator
	var runningOps []*operator.Operator

	for regionID, record := range s.regionOpRecord {
		if record.isFinish || record.pendingOp == nil {
		} else if record.pendingOp.CheckSuccess() {
			record.isFinish = true
		} else if record.pendingOp.Status() > operator.SUCCESS { // some operators may be canceled or expired, try to reschedule them
			if record.retry >= operationRetryLimit {
				log.Info("cancel failed operation",
					zap.Uint64("regionID", regionID),
					zap.String("desc", record.pendingOp.Desc()))
				record.isFinish = true
			} else {
				log.Info("try to reschedule failed operation",
					zap.Uint64("regionID", regionID),
					zap.String("status", operator.OpStatusToString(record.pendingOp.Status())),
					zap.String("opType", record.pendingOp.Kind().String()))
				record.retry++
				op := s.generateOperator(cluster, record.region)
				if op != nil {
					record.pendingOp = op
					pendingOps = append(pendingOps, record.pendingOp)
				} else {
					record.isFinish = true
				}
			}
		} else {
			runningOps = append(runningOps, record.pendingOp)
		}
	}
	for _, record := range s.regionOpRecord {
		if !record.isFinish && record.pendingOp == nil && len(pendingOps)+len(runningOps) < batchOperationLimit { // operators not yet to be created
			op := s.generateOperator(cluster, record.region)
			if op != nil {
				record.pendingOp = op
				pendingOps = append(pendingOps, record.pendingOp)
			} else {
				record.isFinish = true
			}
		}
	}
	if len(pendingOps) > 0 {
		log.Info("process pending operations", zap.Int("count", len(pendingOps)))
	}

	done := len(pendingOps)+len(runningOps) == 0
	return pendingOps, done
}

func (s *shuffleHotRegionScheduler) generateOperator(cluster opt.Cluster, ri *regionInfo) *operator.Operator {
	regionID := ri.regionID
	srcStoreID := ri.srcStoreID
	dstStoreID := ri.dstStoreID

	region := cluster.GetRegion(regionID)
	destPeer := &metapb.Peer{StoreId: dstStoreID}
	
	op, err := operator.CreateMovePeerOperator("random-move-hot-peer", cluster, region, operator.OpRegion, srcStoreID, destPeer)
	if err != nil {
		log.Info("fail to create move peer operator", errs.ZapError(err))
		return nil
	}
	op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))

	return op
}