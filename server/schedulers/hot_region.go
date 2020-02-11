// Copyright 2017 PingCAP, Inc.
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
	"container/list"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/statistics"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(HotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler(HotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotScheduler(opController), nil
	})
	// FIXME: remove this two schedule after the balance test move in schedulers package
	schedule.RegisterScheduler(HotWriteRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotWriteScheduler(opController), nil
	})
	schedule.RegisterScheduler(HotReadRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotReadScheduler(opController), nil
	})
}

const (
	hotRegionLimitFactor    = 0.75
	storeHotPeersDefaultLen = 100
	hotRegionScheduleFactor = 0.95
	// HotRegionName is balance hot region scheduler name.
	HotRegionName = "balance-hot-region-scheduler"
	// HotRegionType is balance hot region scheduler type.
	HotRegionType = "hot-region"
	// HotReadRegionType is hot read region scheduler type.
	HotReadRegionType = "hot-read-region"
	// HotWriteRegionType is hot write region scheduler type.
	HotWriteRegionType               = "hot-write-region"
	minFlowBytes                     = 128 * 1024
	maxZombieDur       time.Duration = statistics.StoreHeartBeatReportInterval * time.Second
)

// BalanceType : the perspective of balance
type BalanceType int

const (
	hotWrite BalanceType = iota
	hotRead
)

type storeStatistics struct {
	readStatAsLeader  statistics.StoreHotPeersStat
	writeStatAsPeer   statistics.StoreHotPeersStat
	writeStatAsLeader statistics.StoreHotPeersStat
}

func newStoreStatistics() *storeStatistics {
	return &storeStatistics{
		readStatAsLeader:  make(statistics.StoreHotPeersStat),
		writeStatAsLeader: make(statistics.StoreHotPeersStat),
		writeStatAsPeer:   make(statistics.StoreHotPeersStat),
	}
}

type hotScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []BalanceType

	// store id -> hot regions statistics as the role of leader
	stats           *storeStatistics
	r               *rand.Rand
	readPendings    map[*pendingInfluence]struct{}
	writePendings   map[*pendingInfluence]struct{}
	readPendingSum  map[uint64]Influence
	writePendingSum map[uint64]Influence
	readScores      *ScoreInfos
	writeScores     *ScoreInfos
	pendingOpInfos  map[uint64]*list.List
}

func newHotScheduler(opController *schedule.OperatorController) *hotScheduler {
	base := NewBaseScheduler(opController)
	pendingOpInfos := make(map[uint64]*list.List)
	return &hotScheduler{
		name:           HotRegionName,
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		stats:          newStoreStatistics(),
		types:          []BalanceType{hotWrite, hotRead},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		readScores:     NewScoreInfos(),
		writeScores:    NewScoreInfos(),
		readPendings:   map[*pendingInfluence]struct{}{},
		writePendings:  map[*pendingInfluence]struct{}{},
		pendingOpInfos: pendingOpInfos,
	}
}

func newHotReadScheduler(opController *schedule.OperatorController) *hotScheduler {
	base := NewBaseScheduler(opController)
	pendingOpInfos := make(map[uint64]*list.List)
	return &hotScheduler{
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		stats:          newStoreStatistics(),
		types:          []BalanceType{hotRead},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		readScores:     NewScoreInfos(),
		writeScores:    NewScoreInfos(),
		readPendings:   map[*pendingInfluence]struct{}{},
		writePendings:  map[*pendingInfluence]struct{}{},
		pendingOpInfos: pendingOpInfos,
	}
}

func newHotWriteScheduler(opController *schedule.OperatorController) *hotScheduler {
	base := NewBaseScheduler(opController)
	pendingOpInfos := make(map[uint64]*list.List)
	return &hotScheduler{
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		stats:          newStoreStatistics(),
		types:          []BalanceType{hotWrite},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		readScores:     NewScoreInfos(),
		writeScores:    NewScoreInfos(),
		readPendings:   map[*pendingInfluence]struct{}{},
		writePendings:  map[*pendingInfluence]struct{}{},
		pendingOpInfos: pendingOpInfos,
	}
}

func (h *hotScheduler) GetName() string {
	return h.name
}

func (h *hotScheduler) GetType() string {
	return HotRegionType
}

func (h *hotScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return h.allowBalanceLeader(cluster) || h.allowBalanceRegion(cluster)
}

func (h *hotScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < minUint64(h.leaderLimit, cluster.GetHotRegionScheduleLimit()) &&
		h.OpController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (h *hotScheduler) allowBalanceRegion(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < minUint64(h.peerLimit, cluster.GetHotRegionScheduleLimit())
}

func (h *hotScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[h.r.Int()%len(h.types)], cluster)
}

func (h *hotScheduler) dispatch(typ BalanceType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()
	h.summaryPendingInfluence()
	h.readScores = h.analyzeStoreLoad(cluster, hotRead)
	h.writeScores = h.analyzeStoreLoad(cluster, hotWrite)
	h.gcPendingOpInfos()
	storesStat := cluster.GetStoresStats()
	switch typ {
	case hotRead:
		asLeader := calcScore(cluster.RegionReadStats(), storesStat.GetStoresBytesReadStat(), cluster, core.LeaderKind)
		h.stats.readStatAsLeader = h.calcPendingInfluence(asLeader, h.readPendingSum)
		return h.balanceHotReadRegions(cluster)
	case hotWrite:
		regionWriteStats := cluster.RegionWriteStats()
		storeWriteStats := storesStat.GetStoresBytesWriteStat()
		asLeader := calcScore(regionWriteStats, storeWriteStats, cluster, core.LeaderKind)
		h.stats.writeStatAsLeader = h.calcPendingInfluence(asLeader, h.writePendingSum)
		asPeer := calcScore(regionWriteStats, storeWriteStats, cluster, core.RegionKind)
		h.stats.writeStatAsPeer = h.calcPendingInfluence(asPeer, h.writePendingSum)
		return h.balanceHotWriteRegions(cluster)
	}
	return nil
}

func (h *hotScheduler) calcPendingInfluence(storeStat statistics.StoreHotPeersStat, pending map[uint64]Influence) statistics.StoreHotPeersStat {
	for id, stat := range storeStat {
		stat.FutureBytesRate += pending[id].ByteRate
	}
	return storeStat
}

func filterUnhealthyStore(cluster opt.Cluster, storeStatsMap map[uint64]float64) {
	stores := cluster.GetStores()
	for _, store := range stores {
		if store.IsTombstone() ||
			store.DownTime() > cluster.GetMaxStoreDownTime() {
			delete(storeStatsMap, store.GetID())
		}
	}
}

func (h *hotScheduler) updateStatsByPendingOpInfo(storeStatsMap map[uint64]float64, balanceType BalanceType) {
	for storeID := range storeStatsMap {
		if balanceType == hotRead {
			storeStatsMap[storeID] += h.readPendingSum[storeID].ByteRate
		} else {
			storeStatsMap[storeID] += h.writePendingSum[storeID].ByteRate
		}
	}
}

func (h *hotScheduler) gcPendingOpInfos() {
	for regionID, pendingOpInfos := range h.pendingOpInfos {
		var n *list.Element
		for e := pendingOpInfos.Front(); e != nil; e = n {
			n = e.Next()
			opInfo := e.Value.(*pendingInfluence)
			if opInfo.isDone() {
				if time.Now().After(opInfo.op.GetCreateTime().Add(statistics.StoreHeartBeatReportInterval * time.Second)) {
					schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Dec()
					pendingOpInfos.Remove(e)
				}
			}
		}
		if pendingOpInfos.Len() == 0 {
			delete(h.pendingOpInfos, regionID)
		}
	}
}

func (h *hotScheduler) analyzeStoreLoad(cluster opt.Cluster, balanceType BalanceType) *ScoreInfos {
	storesStats := cluster.GetStoresStats()
	var storeStatsMap map[uint64]float64
	if balanceType == hotRead {
		storeStatsMap = storesStats.GetStoresBytesReadStat()
	} else {
		storeStatsMap = storesStats.GetStoresBytesWriteStat()
	}
	flowMean := MeanStoresStats(storeStatsMap)
	if flowMean <= minFlowBytes {
		for id := range storeStatsMap {
			storeStatsMap[id] = 0
		}
	} else {
		filterUnhealthyStore(cluster, storeStatsMap)
		h.updateStatsByPendingOpInfo(storeStatsMap, balanceType)
	}
	return ConvertStoresStats(storeStatsMap)
}

func (h *hotScheduler) addPendingInfluence(op *operator.Operator, srcStore, dstStore, regionID uint64, infl Influence, balanceType BalanceType, isTransferLeader bool) {
	influence := newPendingInfluence(op, srcStore, dstStore, infl, isTransferLeader)
	if balanceType == hotRead {
		h.readPendings[influence] = struct{}{}
	} else {
		h.writePendings[influence] = struct{}{}
	}
	if _, ok := h.pendingOpInfos[regionID]; !ok {
		h.pendingOpInfos[regionID] = list.New()
	}
	h.pendingOpInfos[regionID].PushBack(influence)
	schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Inc()
}
func (h *hotScheduler) getPendingInfluence(regionID uint64) *pendingInfluence {
	if l, ok := h.pendingOpInfos[regionID]; ok {
		if l.Len() != 0 {
			return l.Back().Value.(*pendingInfluence)
		}
	}
	return nil
}

func (h *hotScheduler) balanceHotReadRegions(cluster opt.Cluster) []*operator.Operator {
	balanceType := hotRead
	// balance by leader
	srcRegion, newLeader, infl := h.balanceByLeader(cluster, h.stats.readStatAsLeader, balanceType)
	if srcRegion != nil {
		srcStore := srcRegion.GetLeader().GetStoreId()
		dstStore := newLeader.GetStoreId()
		op, err := operator.CreateTransferLeaderOperator("transfer-hot-read-leader", cluster, srcRegion, srcStore, dstStore, operator.OpHotRegion)
		if err != nil {
			log.Debug("fail to create transfer hot read leader operator", zap.Error(err))
			return nil
		}
		op.SetPriorityLevel(core.HighPriority)
		op.Counters = append(op.Counters,
			schedulerCounter.WithLabelValues(h.GetName(), "new-operator"),
			schedulerCounter.WithLabelValues(h.GetName(), "move-leader"),
		)
		h.addPendingInfluence(op, srcStore, dstStore, srcRegion.GetID(), infl, balanceType, true /*isTransferLeader*/)
		return []*operator.Operator{op}
	}

	// balance by peer
	srcRegion, srcPeer, dstPeer, infl := h.balanceByPeer(cluster, h.stats.readStatAsLeader, balanceType)
	if srcRegion != nil {
		op, err := operator.CreateMoveLeaderOperator("move-hot-read-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), dstPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
			return nil
		}
		op.SetPriorityLevel(core.HighPriority)
		op.Counters = append(op.Counters,
			schedulerCounter.WithLabelValues(h.GetName(), "new-operator"),
			schedulerCounter.WithLabelValues(h.GetName(), "move-peer"),
		)
		h.addPendingInfluence(op, srcPeer.GetStoreId(), dstPeer.GetStoreId(), srcRegion.GetID(), infl, balanceType, false /*isTransferLeader*/)
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

// balanceHotRetryLimit is the limit to retry schedule for selected balance strategy.
const balanceHotRetryLimit = 10

func (h *hotScheduler) balanceHotWriteRegions(cluster opt.Cluster) []*operator.Operator {
	balanceType := hotWrite
	balancePeer := h.r.Int()%2 == 0
	for i := 0; i < balanceHotRetryLimit; i++ {
		balancePeer = !balancePeer
		if h.allowBalanceRegion(cluster) && (!h.allowBalanceLeader(cluster) || balancePeer) {
			// balance by peer
			srcRegion, srcPeer, dstPeer, infl := h.balanceByPeer(cluster, h.stats.writeStatAsPeer, balanceType)
			if srcRegion != nil {
				op, err := operator.CreateMovePeerOperator("move-hot-write-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), dstPeer)
				if err != nil {
					schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
					return nil
				}
				op.SetPriorityLevel(core.HighPriority)
				op.Counters = append(op.Counters,
					schedulerCounter.WithLabelValues(h.GetName(), "new-operator"),
					schedulerCounter.WithLabelValues(h.GetName(), "move-peer"),
				)
				h.addPendingInfluence(op, srcPeer.GetStoreId(), dstPeer.GetStoreId(), srcRegion.GetID(), infl, balanceType, false /*isTransferLeader*/)
				return []*operator.Operator{op}
			}
		} else if h.allowBalanceLeader(cluster) {
			// balance by leader
			srcRegion, newLeader, infl := h.balanceByLeader(cluster, h.stats.writeStatAsLeader, balanceType)
			if srcRegion != nil {
				srcStore := srcRegion.GetLeader().GetStoreId()
				dstStore := newLeader.GetStoreId()
				op, err := operator.CreateTransferLeaderOperator("transfer-hot-write-leader", cluster, srcRegion, srcStore, dstStore, operator.OpHotRegion)
				if err != nil {
					log.Debug("fail to create transfer hot write leader operator", zap.Error(err))
					return nil
				}
				op.SetPriorityLevel(core.HighPriority)
				op.Counters = append(op.Counters,
					schedulerCounter.WithLabelValues(h.GetName(), "new-operator"),
					schedulerCounter.WithLabelValues(h.GetName(), "move-leader"),
				)
				// transfer leader do not influence the byte rate
				infl.ByteRate = 0
				h.addPendingInfluence(op, srcStore, dstStore, srcRegion.GetID(), infl, balanceType, true /*isTransferLeader*/)
				return []*operator.Operator{op}
			}
		} else {
			break
		}
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func calcScore(storeHotPeers map[uint64][]*statistics.HotPeerStat, storeBytesStat map[uint64]float64, cluster opt.Cluster, kind core.ResourceKind) statistics.StoreHotPeersStat {
	stats := make(statistics.StoreHotPeersStat)
	for storeID, items := range storeHotPeers {
		hotPeers, ok := stats[storeID]
		if !ok {
			hotPeers = &statistics.HotPeersStat{
				Stats: make([]statistics.HotPeerStat, 0, storeHotPeersDefaultLen),
			}
			stats[storeID] = hotPeers
		}

		for _, r := range items {
			if kind == core.LeaderKind && !r.IsLeader() {
				continue
			}
			// HotDegree is the update times on the hot cache. If the heartbeat report
			// the flow of the region exceeds the threshold, the scheduler will update the region in
			// the hot cache and the hot degree of the region will increase.
			if r.HotDegree < cluster.GetHotRegionCacheHitsThreshold() {
				continue
			}

			regionInfo := cluster.GetRegion(r.RegionID)
			if regionInfo == nil {
				continue
			}

			s := statistics.HotPeerStat{
				StoreID:        storeID,
				RegionID:       r.RegionID,
				HotDegree:      r.HotDegree,
				AntiCount:      r.AntiCount,
				BytesRate:      r.GetBytesRate(),
				LastUpdateTime: r.LastUpdateTime,
				Version:        r.Version,
			}
			hotPeers.TotalBytesRate += r.GetBytesRate()
			hotPeers.Count++
			hotPeers.Stats = append(hotPeers.Stats, s)
		}
	}
	for id, rate := range storeBytesStat {
		hotPeers, ok := stats[id]
		if !ok {
			hotPeers = &statistics.HotPeersStat{
				Stats: make([]statistics.HotPeerStat, 0, storeHotPeersDefaultLen),
			}
			stats[id] = hotPeers
		}
		hotPeers.StoreBytesRate = rate
		hotPeers.FutureBytesRate = rate
	}
	return stats
}

// balanceByPeer balances the peer distribution of hot regions.
func (h *hotScheduler) balanceByPeer(cluster opt.Cluster, storesStat statistics.StoreHotPeersStat, balanceType BalanceType) (*core.RegionInfo, *metapb.Peer, *metapb.Peer, Influence) {
	if !h.allowBalanceRegion(cluster) {
		return nil, nil, nil, Influence{}
	}

	srcStoreID := h.selectSrcStoreByScore(storesStat, balanceType)
	if srcStoreID == 0 {
		return nil, nil, nil, Influence{}
	}
	// get one source region and a target store.
	// For each region in the source store, we try to find the best target store;
	// If we can find a target store, then return from this method.
	stores := cluster.GetStores()
	var dstStoreID uint64
	for _, i := range h.r.Perm(len(storesStat[srcStoreID].Stats)) {
		rs := storesStat[srcStoreID].Stats[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no-region").Inc()
			continue
		}

		if pendingOpInfo := h.getPendingInfluence(srcRegion.GetID()); pendingOpInfo != nil {
			if !pendingOpInfo.isTransferLeader || !pendingOpInfo.isDone() {
				continue
			}
		}

		if !opt.IsHealthyAllowPending(cluster, srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy-replica").Inc()
			continue
		}

		if !opt.IsRegionReplicated(cluster, srcRegion) {
			log.Debug("region has abnormal replica count", zap.String("scheduler", h.GetName()), zap.Uint64("region-id", srcRegion.GetID()))
			schedulerCounter.WithLabelValues(h.GetName(), "abnormal-replica").Inc()
			continue
		}

		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID))
		}

		srcPeer := srcRegion.GetStorePeer(srcStoreID)
		if srcPeer == nil {
			log.Debug("region does not peer on source store, maybe stat out of date", zap.Uint64("region-id", rs.RegionID))
			continue
		}

		var scoreGuard filter.Filter
		if cluster.IsPlacementRulesEnabled() {
			scoreGuard = filter.NewRuleFitFilter(h.GetName(), cluster, srcRegion, srcStoreID)
		} else {
			scoreGuard = filter.NewDistinctScoreFilter(h.GetName(), cluster.GetLocationLabels(), cluster.GetRegionStores(srcRegion), srcStore)
		}

		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: h.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(h.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewHealthFilter(h.GetName()),
			scoreGuard,
		}
		candidateStoreIDs := make(map[uint64]struct{}, len(stores))
		for _, store := range stores {
			if filter.Target(cluster, store, filters) {
				continue
			}
			candidateStoreIDs[store.GetID()] = struct{}{}
		}
		if len(candidateStoreIDs) == 0 {
			continue
		}

		regionBytesRate := rs.GetBytesRate()
		dstStoreID = h.selectDstStoreByScore(candidateStoreIDs, regionBytesRate, balanceType)
		if dstStoreID != 0 {
			h.peerLimit = h.adjustBalanceLimit(srcStoreID, storesStat)
			srcPeer := srcRegion.GetStorePeer(srcStoreID)
			if srcPeer == nil {
				return nil, nil, nil, Influence{}
			}
			dstPeer := &metapb.Peer{StoreId: dstStoreID, IsLearner: srcPeer.IsLearner}
			return srcRegion, srcPeer, dstPeer, Influence{ByteRate: regionBytesRate}
		}
	}

	return nil, nil, nil, Influence{}
}

// balanceByLeader balances the leader distribution of hot regions.
func (h *hotScheduler) balanceByLeader(cluster opt.Cluster, storesStat statistics.StoreHotPeersStat, balanceType BalanceType) (*core.RegionInfo, *metapb.Peer, Influence) {
	if !h.allowBalanceLeader(cluster) {
		return nil, nil, Influence{}
	}
	var (
		srcStoreID, dstStoreID uint64
	)

	if balanceType == hotWrite {
		srcStoreID = h.selectSrcStoreByHot(storesStat)
	} else {
		srcStoreID = h.selectSrcStoreByScore(storesStat, balanceType)
	}

	if srcStoreID == 0 {
		return nil, nil, Influence{}
	}

	// select dstPeer
	for _, i := range h.r.Perm(len(storesStat[srcStoreID].Stats)) {
		rs := storesStat[srcStoreID].Stats[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no-region").Inc()
			continue
		}

		if _, ok := h.pendingOpInfos[srcRegion.GetID()]; ok {
			continue
		}

		if !opt.IsHealthyAllowPending(cluster, srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy-replica").Inc()
			continue
		}

		filters := []filter.Filter{filter.StoreStateFilter{ActionScope: h.GetName(), TransferLeader: true}}
		filters = append(filters, filter.NewHealthFilter(h.GetName()))
		candidateStoreIDs := make(map[uint64]struct{}, len(srcRegion.GetPeers())-1)
		for _, store := range cluster.GetFollowerStores(srcRegion) {
			if !filter.Target(cluster, store, filters) {
				candidateStoreIDs[store.GetID()] = struct{}{}
			}
		}
		if len(candidateStoreIDs) == 0 {
			continue
		}

		regionBytesRate := rs.GetBytesRate()
		if balanceType == hotWrite {
			dstStoreID = h.selectDstStoreByHot(candidateStoreIDs, regionBytesRate, srcStoreID, storesStat)
		} else {
			dstStoreID = h.selectDstStoreByScore(candidateStoreIDs, regionBytesRate, balanceType)
		}

		if dstStoreID != 0 {
			dstPeer := srcRegion.GetStoreVoter(dstStoreID)
			if dstPeer != nil {
				h.leaderLimit = h.adjustBalanceLimit(srcStoreID, storesStat)
				return srcRegion, dstPeer, Influence{ByteRate: rs.GetBytesRate()}
			}
		}
	}
	return nil, nil, Influence{}
}

// Select the store to move hot regions from.
// We choose the store with the maximum number of hot region first.
// Inside these stores, we choose the one with maximum flow bytes.
func (h *hotScheduler) selectSrcStoreByHot(stats statistics.StoreHotPeersStat) (srcStoreID uint64) {
	var (
		maxFlowBytes float64
		maxCount     int
	)

	for storeID, stat := range stats {
		count, flowBytes := len(stat.Stats), stat.FutureBytesRate
		if count <= 1 {
			continue
		}
		// pick by count
		if count > maxCount || (count == maxCount && flowBytes > maxFlowBytes) {
			maxCount = count
			maxFlowBytes = flowBytes
			srcStoreID = storeID
		}
	}
	return
}

// selectDstStoreByHot selects a target store to hold the region of the source region.
// We choose a target store based on the hot region number and flow bytes of this store.
func (h *hotScheduler) selectDstStoreByHot(candidates map[uint64]struct{}, regionBytesRate float64, srcStoreID uint64, storesStat statistics.StoreHotPeersStat) (dstStoreID uint64) {
	srcBytesRate := storesStat[srcStoreID].FutureBytesRate
	srcCount := len(storesStat[srcStoreID].Stats)
	var (
		minBytesRate = srcBytesRate*hotRegionScheduleFactor - regionBytesRate
		minCount     = math.MaxInt32
	)
	for storeID := range candidates {
		if s, ok := storesStat[storeID]; ok {
			dstCount, dstBytesRate := len(s.Stats), math.Max(s.StoreBytesRate, s.FutureBytesRate)
			// pick by count
			if srcCount < dstCount+2 { // ensure srcCount >= dstCount after the operation.
				continue
			}
			if minCount > dstCount || (minCount == dstCount && minBytesRate > dstBytesRate) {
				dstStoreID = storeID
				minBytesRate = dstBytesRate
				minCount = dstCount
			}
		} else {
			return storeID
		}
	}
	return dstStoreID
}

func (h *hotScheduler) selectSrcStoreByScore(stats statistics.StoreHotPeersStat, balanceType BalanceType) uint64 {
	var storesScore *ScoreInfos
	if balanceType == hotRead {
		storesScore = h.readScores
	} else {
		storesScore = h.writeScores
	}
	maxScore := storesScore.Max()                 // with sort
	for i := storesScore.Len() - 1; i >= 0; i-- { // select from large to small
		scoreInfo := storesScore.scoreInfos[i]
		if scoreInfo.GetScore() >= maxScore*hotRegionScheduleFactor {
			storeID := scoreInfo.GetStoreID()
			if stat, ok := stats[storeID]; ok && len(stat.Stats) > 1 {
				return storeID
			}
		}
	}
	return 0
}

func (h *hotScheduler) selectDstStoreByScore(candidates map[uint64]struct{}, regionBytesRate float64, balanceType BalanceType) uint64 {
	var storesScore *ScoreInfos
	if balanceType == hotRead {
		storesScore = h.readScores
	} else {
		storesScore = h.writeScores
	}
	maxScore := storesScore.Max()                      // with sort
	for _, scoreInfo := range storesScore.scoreInfos { // select from small to large
		if scoreInfo.GetScore()+regionBytesRate < maxScore*hotRegionScheduleFactor {
			dstStoreID := scoreInfo.GetStoreID()
			if _, ok := candidates[dstStoreID]; ok {
				return dstStoreID
			}
		}
	}
	return 0
}

func (h *hotScheduler) adjustBalanceLimit(storeID uint64, storesStat statistics.StoreHotPeersStat) uint64 {
	srcStoreStatistics := storesStat[storeID]

	var hotRegionTotalCount int
	for _, m := range storesStat {
		hotRegionTotalCount += len(m.Stats)
	}

	avgRegionCount := float64(hotRegionTotalCount) / float64(len(storesStat))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(len(srcStoreStatistics.Stats)) - avgRegionCount) * hotRegionLimitFactor)
	return maxUint64(limit, 1)
}

func (h *hotScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stats.readStatAsLeader))
	for id, stat := range h.stats.readStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
	}
}

func (h *hotScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stats.writeStatAsLeader))
	asPeer := make(statistics.StoreHotPeersStat, len(h.stats.writeStatAsPeer))
	for id, stat := range h.stats.writeStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	for id, stat := range h.stats.writeStatAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

func (h *hotScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(hotWrite)
}

func (h *hotScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(hotRead)
}

func (h *hotScheduler) copyPendingInfluence(typ BalanceType) map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	var pendingSum map[uint64]Influence
	if typ == hotRead {
		pendingSum = h.readPendingSum
	} else {
		pendingSum = h.writePendingSum
	}
	ret := make(map[uint64]Influence, len(pendingSum))
	for id, infl := range pendingSum {
		ret[id] = infl
	}
	return ret
}

func (h *hotScheduler) GetReadScores() map[uint64]float64 {
	return h.GetStoresScore(hotRead)
}

func (h *hotScheduler) GetWriteScores() map[uint64]float64 {
	return h.GetStoresScore(hotWrite)
}

func (h *hotScheduler) GetStoresScore(typ BalanceType) map[uint64]float64 {
	h.RLock()
	defer h.RUnlock()
	storesScore := make(map[uint64]float64)
	var infos []*ScoreInfo
	if typ == hotRead {
		infos = h.readScores.ToSlice()
	} else {
		infos = h.writeScores.ToSlice()
	}
	for _, info := range infos {
		storesScore[info.GetStoreID()] = info.GetScore()
	}
	return storesScore
}

func calcPendingWeight(op *operator.Operator) float64 {
	if op.CheckExpired() || op.CheckTimeout() {
		return 0
	}
	status := op.Status()
	if !operator.IsEndStatus(status) {
		return 1
	}
	switch status {
	case operator.SUCCESS:
		zombieDur := time.Since(op.GetReachTimeOf(status))
		if zombieDur >= maxZombieDur {
			return 0
		}
		// TODO: use store statistics update time to make a more accurate estimation
		return float64(maxZombieDur-zombieDur) / float64(maxZombieDur)
	default:
		return 0
	}
}

func (h *hotScheduler) summaryPendingInfluence() {
	h.readPendingSum = summaryPendingInfluence(h.readPendings, calcPendingWeight)
	h.writePendingSum = summaryPendingInfluence(h.writePendings, calcPendingWeight)
}

func (h *hotScheduler) clearPendingInfluence() {
	h.readPendings = map[*pendingInfluence]struct{}{}
	h.writePendings = map[*pendingInfluence]struct{}{}
	h.readPendingSum = nil
	h.writePendingSum = nil
	h.pendingOpInfos = make(map[uint64]*list.List)
}
