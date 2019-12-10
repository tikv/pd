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
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(HotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler(HotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {

		return newBalanceHotRegionsScheduler(opController), nil
	})
	// FIXME: remove this two schedule after the balance test move in schedulers package
	schedule.RegisterScheduler(HotWriteRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceHotWriteRegionsScheduler(opController), nil
	})
	schedule.RegisterScheduler(HotReadRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceHotReadRegionsScheduler(opController), nil
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
	hotWriteRegionBalance BalanceType = iota
	hotReadRegionBalance
)

type storeStatistics struct {
	readStatAsLeader  statistics.StoreHotPeersStat
	writeStatAsPeer   statistics.StoreHotPeersStat
	writeStatAsLeader statistics.StoreHotPeersStat
}

func newStoreStaticstics() *storeStatistics {
	return &storeStatistics{
		readStatAsLeader:  make(statistics.StoreHotPeersStat),
		writeStatAsLeader: make(statistics.StoreHotPeersStat),
		writeStatAsPeer:   make(statistics.StoreHotPeersStat),
	}
}

type balanceHotRegionsScheduler struct {
	name string
	*baseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []BalanceType

	// store id -> hot regions statistics as the role of leader
	stats *storeStatistics
	r     *rand.Rand
	// ScoreInfos stores storeID and score of all stores.
	scoreInfos      *ScoreInfos
	readPendings    map[*pendingInfluence]struct{}
	writePendings   map[*pendingInfluence]struct{}
	readPendingSum  map[uint64]Influence
	writePendingSum map[uint64]Influence
}

func newBalanceHotRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		name:          HotRegionName,
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotWriteRegionBalance, hotReadRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		scoreInfos:    NewScoreInfos(),
		readPendings:  map[*pendingInfluence]struct{}{},
		writePendings: map[*pendingInfluence]struct{}{},
	}
}

func newBalanceHotReadRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotReadRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		scoreInfos:    NewScoreInfos(),
		readPendings:  map[*pendingInfluence]struct{}{},
		writePendings: map[*pendingInfluence]struct{}{},
	}
}

func newBalanceHotWriteRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotWriteRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		scoreInfos:    NewScoreInfos(),
		readPendings:  map[*pendingInfluence]struct{}{},
		writePendings: map[*pendingInfluence]struct{}{},
	}
}

func (h *balanceHotRegionsScheduler) GetName() string {
	return h.name
}

func (h *balanceHotRegionsScheduler) GetType() string {
	return HotRegionType
}

func (h *balanceHotRegionsScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return h.allowBalanceLeader(cluster) || h.allowBalanceRegion(cluster)
}

func (h *balanceHotRegionsScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	return h.opController.OperatorCount(operator.OpHotRegion) < minUint64(h.leaderLimit, cluster.GetHotRegionScheduleLimit()) &&
		h.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (h *balanceHotRegionsScheduler) allowBalanceRegion(cluster opt.Cluster) bool {
	return h.opController.OperatorCount(operator.OpHotRegion) < minUint64(h.peerLimit, cluster.GetHotRegionScheduleLimit())
}

func (h *balanceHotRegionsScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[h.r.Int()%len(h.types)], cluster)
}

func (h *balanceHotRegionsScheduler) dispatch(typ BalanceType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()
	h.analyzeStoreLoad(cluster.GetStoresStats())
	storesStat := cluster.GetStoresStats()
	h.summaryPendingInfluence()
	switch typ {
	case hotReadRegionBalance:
		asLeader := calcScore(cluster.RegionReadStats(), storesStat.GetStoresBytesReadStat(), cluster, core.LeaderKind)
		h.stats.readStatAsLeader = h.calcPendingInfluence(asLeader, h.readPendingSum)
		return h.balanceHotReadRegions(cluster)
	case hotWriteRegionBalance:
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

func (h *balanceHotRegionsScheduler) calcPendingInfluence(storeStat statistics.StoreHotPeersStat, pending map[uint64]Influence) statistics.StoreHotPeersStat {
	for id, stat := range storeStat {
		stat.FutureBytesRate += pending[id].ByteRate
	}
	return storeStat
}

func (h *balanceHotRegionsScheduler) analyzeStoreLoad(storesStats *statistics.StoresStats) {
	readFlowScoreInfos := NormalizeStoresStats(storesStats.GetStoresBytesReadStat())
	writeFlowScoreInfos := NormalizeStoresStats(storesStats.GetStoresBytesWriteStat())
	readFlowMean := MeanStoresStats(storesStats.GetStoresBytesReadStat())
	writeFlowMean := MeanStoresStats(storesStats.GetStoresBytesWriteStat())

	var weights []float64
	means := readFlowMean + writeFlowMean
	if means <= minFlowBytes {
		weights = append(weights, 0, 0)
	} else {
		weights = append(weights, readFlowMean/means, writeFlowMean/means)
	}

	h.scoreInfos = AggregateScores([]*ScoreInfos{readFlowScoreInfos, writeFlowScoreInfos}, weights)
}

func (h *balanceHotRegionsScheduler) balanceHotReadRegions(cluster opt.Cluster) []*operator.Operator {
	// balance by leader
	srcRegion, newLeader, infl := h.balanceByLeader(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move-leader").Inc()
		srcStore := srcRegion.GetLeader().GetStoreId()
		dstStore := newLeader.GetStoreId()
		op := operator.CreateTransferLeaderOperator("transfer-hot-read-leader", srcRegion, srcStore, dstStore, operator.OpHotRegion)
		op.SetPriorityLevel(core.HighPriority)
		h.readPendings[newPendingInfluence(op, srcStore, dstStore, infl)] = struct{}{}
		return []*operator.Operator{op}
	}

	// balance by peer
	srcRegion, srcPeer, destPeer, infl := h.balanceByPeer(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		op, err := operator.CreateMoveLeaderOperator("move-hot-read-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), destPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
			return nil
		}
		op.SetPriorityLevel(core.HighPriority)
		schedulerCounter.WithLabelValues(h.GetName(), "move-peer").Inc()
		h.readPendings[newPendingInfluence(op, srcPeer.GetStoreId(), destPeer.GetStoreId(), infl)] = struct{}{}
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

// balanceHotRetryLimit is the limit to retry schedule for selected balance strategy.
const balanceHotRetryLimit = 10

func (h *balanceHotRegionsScheduler) balanceHotWriteRegions(cluster opt.Cluster) []*operator.Operator {
	balancePeer := h.r.Int()%2 == 0
	for i := 0; i < balanceHotRetryLimit; i++ {
		balancePeer = !balancePeer
		if h.allowBalanceRegion(cluster) && (!h.allowBalanceLeader(cluster) || balancePeer) {
			// balance by peer
			srcRegion, srcPeer, dstPeer, infl := h.balanceByPeer(cluster, h.stats.writeStatAsPeer)
			if srcRegion != nil {
				op, err := operator.CreateMovePeerOperator("move-hot-write-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), dstPeer)
				if err != nil {
					schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
					return nil
				}
				op.SetPriorityLevel(core.HighPriority)
				schedulerCounter.WithLabelValues(h.GetName(), "move-peer").Inc()
				h.writePendings[newPendingInfluence(op, srcPeer.GetStoreId(), dstPeer.GetStoreId(), infl)] = struct{}{}
				return []*operator.Operator{op}
			}
		} else if h.allowBalanceLeader(cluster) {
			// balance by leader
			srcRegion, newLeader, infl := h.balanceByLeader(cluster, h.stats.writeStatAsLeader)
			if srcRegion != nil {
				schedulerCounter.WithLabelValues(h.GetName(), "move-leader").Inc()
				srcStore := srcRegion.GetLeader().GetStoreId()
				dstStore := newLeader.GetStoreId()
				op := operator.CreateTransferLeaderOperator("transfer-hot-write-leader", srcRegion, srcStore, dstStore, operator.OpHotRegion)
				op.SetPriorityLevel(core.HighPriority)
				// transfer leader do not influence the byte rate
				infl.ByteRate = 0
				h.writePendings[newPendingInfluence(op, srcStore, dstStore, infl)] = struct{}{}
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
func (h *balanceHotRegionsScheduler) balanceByPeer(cluster opt.Cluster, storesStat statistics.StoreHotPeersStat) (*core.RegionInfo, *metapb.Peer, *metapb.Peer, Influence) {
	if !h.allowBalanceRegion(cluster) {
		return nil, nil, nil, Influence{}
	}

	srcStoreID := h.selectSrcStore(storesStat)
	if srcStoreID == 0 {
		return nil, nil, nil, Influence{}
	}

	// get one source region and a target store.
	// For each region in the source store, we try to find the best target store;
	// If we can find a target store, then return from this method.
	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(len(storesStat[srcStoreID].Stats)) {
		rs := storesStat[srcStoreID].Stats[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no-region").Inc()
			continue
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
		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: h.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(h.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewDistinctScoreFilter(h.GetName(), cluster.GetLocationLabels(), cluster.GetRegionStores(srcRegion), srcStore),
		}
		candidateStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if filter.Target(cluster, store, filters) {
				continue
			}
			candidateStoreIDs = append(candidateStoreIDs, store.GetID())
		}

		destStoreID = h.selectDestStore(candidateStoreIDs, rs.GetBytesRate(), srcStoreID, storesStat)
		if destStoreID != 0 {
			h.peerLimit = h.adjustBalanceLimit(srcStoreID, storesStat)

			srcPeer := srcRegion.GetStorePeer(srcStoreID)
			if srcPeer == nil {
				return nil, nil, nil, Influence{}
			}

			// When the target store is decided, we allocate a peer ID to hold the source region,
			// because it doesn't exist in the system right now.
			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Error("failed to allocate peer", zap.Error(err))
				return nil, nil, nil, Influence{}
			}

			return srcRegion, srcPeer, destPeer, Influence{ByteRate: rs.GetBytesRate()}
		}
	}

	return nil, nil, nil, Influence{}
}

// balanceByLeader balances the leader distribution of hot regions.
func (h *balanceHotRegionsScheduler) balanceByLeader(cluster opt.Cluster, storesStat statistics.StoreHotPeersStat) (*core.RegionInfo, *metapb.Peer, Influence) {
	if !h.allowBalanceLeader(cluster) {
		return nil, nil, Influence{}
	}

	srcStoreID := h.selectSrcStore(storesStat)
	if srcStoreID == 0 {
		return nil, nil, Influence{}
	}

	// select destPeer
	for _, i := range h.r.Perm(len(storesStat[srcStoreID].Stats)) {
		rs := storesStat[srcStoreID].Stats[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no-region").Inc()
			continue
		}

		if !opt.IsHealthyAllowPending(cluster, srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy-replica").Inc()
			continue
		}

		filters := []filter.Filter{filter.StoreStateFilter{ActionScope: h.GetName(), TransferLeader: true}}
		candidateStoreIDs := make([]uint64, 0, len(srcRegion.GetPeers())-1)
		for _, store := range cluster.GetFollowerStores(srcRegion) {
			if !filter.Target(cluster, store, filters) {
				candidateStoreIDs = append(candidateStoreIDs, store.GetID())
			}
		}
		if len(candidateStoreIDs) == 0 {
			continue
		}
		destStoreID := h.selectDestStore(candidateStoreIDs, rs.GetBytesRate(), srcStoreID, storesStat)
		if destStoreID == 0 {
			continue
		}

		destPeer := srcRegion.GetStoreVoter(destStoreID)
		if destPeer != nil {
			h.leaderLimit = h.adjustBalanceLimit(srcStoreID, storesStat)

			return srcRegion, destPeer, Influence{ByteRate: rs.GetBytesRate()}
		}
	}
	return nil, nil, Influence{}
}

// Select the store to move hot regions from.
// We choose the store with the maximum number of hot region first.
// Inside these stores, we choose the one with maximum flow bytes.
func (h *balanceHotRegionsScheduler) selectSrcStore(stats statistics.StoreHotPeersStat) (srcStoreID uint64) {
	var (
		maxFlowBytes float64
		maxCount     int
	)

	for storeID, stat := range stats {
		count, flowBytes := len(stat.Stats), stat.FutureBytesRate
		if count <= 1 {
			continue
		}
		if flowBytes > maxFlowBytes || (flowBytes == maxFlowBytes && count > maxCount) {
			maxCount = count
			maxFlowBytes = flowBytes
			srcStoreID = storeID
		}
	}
	return
}

// selectDestStore selects a target store to hold the region of the source region.
// We choose a target store based on the hot region number and flow bytes of this store.
func (h *balanceHotRegionsScheduler) selectDestStore(candidateStoreIDs []uint64, regionBytesRate float64, srcStoreID uint64, storesStat statistics.StoreHotPeersStat) (destStoreID uint64) {
	srcBytesRate := storesStat[srcStoreID].FutureBytesRate

	var (
		minBytesRate = srcBytesRate*hotRegionScheduleFactor - regionBytesRate
		minCount     = math.MaxInt32
	)
	for _, storeID := range candidateStoreIDs {
		if s, ok := storesStat[storeID]; ok {
			count, dstBytesRate := len(s.Stats), math.Max(s.StoreBytesRate, s.FutureBytesRate)
			if minBytesRate > dstBytesRate || (minBytesRate == dstBytesRate && minCount > count) {
				minCount = count
				minBytesRate = dstBytesRate
				destStoreID = storeID
			}
		} else {
			destStoreID = storeID
			return
		}
	}
	return
}

func (h *balanceHotRegionsScheduler) adjustBalanceLimit(storeID uint64, storesStat statistics.StoreHotPeersStat) uint64 {
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

func (h *balanceHotRegionsScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
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

func (h *balanceHotRegionsScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
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

func (h *balanceHotRegionsScheduler) GetWritePendingInfluence() map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	ret := make(map[uint64]Influence, len(h.writePendingSum))
	for id, infl := range h.writePendingSum {
		ret[id] = infl
	}
	return ret
}

func (h *balanceHotRegionsScheduler) GetReadPendingInfluence() map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	ret := make(map[uint64]Influence, len(h.readPendingSum))
	for id, infl := range h.readPendingSum {
		ret[id] = infl
	}
	return ret
}

func (h *balanceHotRegionsScheduler) GetStoresScore() map[uint64]float64 {
	h.RLock()
	defer h.RUnlock()
	storesScore := make(map[uint64]float64)
	for _, info := range h.scoreInfos.ToSlice() {
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

func (h *balanceHotRegionsScheduler) summaryPendingInfluence() {
	h.readPendingSum = summaryPendingInfluence(h.readPendings, calcPendingWeight)
	h.writePendingSum = summaryPendingInfluence(h.writePendings, calcPendingWeight)
}

func (h *balanceHotRegionsScheduler) clearPendingInfluence() {
	h.readPendings = map[*pendingInfluence]struct{}{}
	h.writePendings = map[*pendingInfluence]struct{}{}
	h.readPendingSum = nil
	h.writePendingSum = nil
}
