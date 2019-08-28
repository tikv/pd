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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterScheduler("hot-region", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newBalanceHotRegionsScheduler(opController), nil
	})
	// FIXME: remove this two schedule after the balance test move in schedulers package
	schedule.RegisterScheduler("hot-write-region", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newBalanceHotWriteRegionsScheduler(opController), nil
	})
	schedule.RegisterScheduler("hot-read-region", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newBalanceHotReadRegionsScheduler(opController), nil
	})
}

const (
	hotRegionLimitFactor      = 0.75
	storeHotRegionsDefaultLen = 100
	hotRegionScheduleFactor   = 0.9
	minFlowBytes              = 128 * 1024
	minScoreLimit             = 0.35

	opMaxZombieTime = statistics.StoreHeartBeatReportInterval * 2.5 * time.Second
)

// BalanceType : the perspective of balance
type BalanceType int

const (
	hotWriteRegionBalance BalanceType = iota
	hotReadRegionBalance
)

type storeStatistics struct {
	readStatAsLeader  statistics.StoreHotRegionsStat
	writeStatAsPeer   statistics.StoreHotRegionsStat
	writeStatAsLeader statistics.StoreHotRegionsStat
}

func newStoreStaticstics() *storeStatistics {
	return &storeStatistics{
		readStatAsLeader:  make(statistics.StoreHotRegionsStat),
		writeStatAsLeader: make(statistics.StoreHotRegionsStat),
		writeStatAsPeer:   make(statistics.StoreHotRegionsStat),
	}
}

const (
	created = iota
	unstarted
	started
	finished
)

func statusStr(status uint32) string {
	switch status {
	case created:
		return "new"
	case unstarted:
		return "unstarted"
	case started:
		return "started"
	case finished:
		return "finished"
	}
	return ""
}

type opInfluence struct {
	typ        string
	status     uint32
	from, to   uint64
	bytesRead  map[uint64]float64
	bytesWrite map[uint64]float64
}

func newOpInfluence(typ string, from, to uint64) *opInfluence {
	return &opInfluence{
		typ:        typ,
		status:     created,
		from:       from,
		to:         to,
		bytesRead:  map[uint64]float64{},
		bytesWrite: map[uint64]float64{},
	}
}

func (oi *opInfluence) unstarted() {
	if atomic.CompareAndSwapUint32(&oi.status, created, unstarted) {
		oi.incMetric(unstarted)
	}
}

func (oi *opInfluence) started() {
	if atomic.CompareAndSwapUint32(&oi.status, created, started) {
		oi.incMetric(started)
	} else if atomic.CompareAndSwapUint32(&oi.status, unstarted, started) {
		oi.decMetric(unstarted)
		oi.incMetric(started)
	}
}

func (oi *opInfluence) finished() {
	if atomic.CompareAndSwapUint32(&oi.status, created, finished) {
		return
	} else if atomic.CompareAndSwapUint32(&oi.status, unstarted, finished) {
		oi.decMetric(unstarted)
	} else if atomic.CompareAndSwapUint32(&oi.status, started, finished) {
		oi.decMetric(started)
	}
}

func (oi *opInfluence) incMetric(status uint32) {
	hotspotPendingOpGauge.WithLabelValues(oi.typ, statusStr(status), u64Str(oi.from), u64Str(oi.to)).Inc()
}

func (oi *opInfluence) decMetric(status uint32) {
	hotspotPendingOpGauge.WithLabelValues(oi.typ, statusStr(status), u64Str(oi.from), u64Str(oi.to)).Dec()
}

func (oi *opInfluence) applyFunc(f func(float64) float64, dst *opInfluence) *opInfluence {
	if dst == nil {
		dst = newOpInfluence("", 0, 0)
	}
	for k, v := range oi.bytesRead {
		dst.bytesRead[k] = f(v)
	}
	for k, v := range oi.bytesWrite {
		dst.bytesWrite[k] = f(v)
	}
	return dst
}

func (oi *opInfluence) multWeight(w float64, dst *opInfluence) *opInfluence {
	return oi.applyFunc(func(v float64) float64 { return v * w }, dst)
}

func (oi *opInfluence) Add(other *opInfluence, w float64) {
	mapAddWeight(oi.bytesRead, other.bytesRead, w)
	mapAddWeight(oi.bytesWrite, other.bytesWrite, w)
}

func mapAddWeight(a, b map[uint64]float64, w float64) {
	for k, v := range b {
		a[k] += v * w
	}
}

type balanceHotRegionsScheduler struct {
	*baseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []BalanceType

	// store id -> hot regions statistics as the role of leader
	stats      *storeStatistics
	r          *rand.Rand
	pendingOps map[*operator.Operator]*opInfluence
	influence  *opInfluence

	storesScore *ScorePairSlice
}

func newBalanceHotRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotWriteRegionBalance, hotReadRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		pendingOps:    map[*operator.Operator]*opInfluence{},
		influence:     newOpInfluence("hotspot-summary", 0, 0),
		storesScore:   NewScorePairSlice(),
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
		pendingOps:    map[*operator.Operator]*opInfluence{},
		influence:     newOpInfluence("hotspot-read-summary", 0, 0),
		storesScore:   NewScorePairSlice(),
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
		pendingOps:    map[*operator.Operator]*opInfluence{},
		influence:     newOpInfluence("hotspot-write-summary", 0, 0),
		storesScore:   NewScorePairSlice(),
	}
}

func (h *balanceHotRegionsScheduler) GetName() string {
	return "balance-hot-region-scheduler"
}

func (h *balanceHotRegionsScheduler) GetType() string {
	return "hot-region"
}

func (h *balanceHotRegionsScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	h.observePendingOps()
	return h.allowBalanceLeader(cluster) || h.allowBalanceRegion(cluster)
}

func (h *balanceHotRegionsScheduler) allowBalanceLeader(cluster schedule.Cluster) bool {
	return h.opController.OperatorCount(operator.OpHotRegion) < minUint64(h.leaderLimit, cluster.GetHotRegionScheduleLimit()) &&
		h.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (h *balanceHotRegionsScheduler) allowBalanceRegion(cluster schedule.Cluster) bool {
	return h.opController.OperatorCount(operator.OpHotRegion) < minUint64(h.peerLimit, cluster.GetHotRegionScheduleLimit())
}

func (h *balanceHotRegionsScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[h.r.Int()%len(h.types)], cluster)
}

func (h *balanceHotRegionsScheduler) observePendingOps() {
	h.RLock()
	defer h.RUnlock()
	var unstartedCnt uint = 0
	var unstartedSum time.Duration = 0
	for op, infl := range h.pendingOps {
		if op.IsDropped() {
			infl.finished()
			continue
		}
		if op.IsStarted() {
			infl.started()
		} else {
			infl.unstarted()
			unstartedCnt++
			unstartedSum += op.ElapsedTime()
		}
	}
	pendingInfluenceGauge.WithLabelValues("unstarted_cnt", "--").Set(float64(unstartedCnt))
	pendingInfluenceGauge.WithLabelValues("unstarted_sum", "--").Set(float64(unstartedSum) / float64(time.Millisecond))
}

func (h *balanceHotRegionsScheduler) updatePendingInfluence() {
	h.influence.multWeight(0, h.influence)
	for op, infl := range h.pendingOps {
		if op.IsDropped() {
			infl.finished()
			infl = calcZombieInfluence(op, infl)
			if infl == nil {
				delete(h.pendingOps, op)
				continue
			}
		} else if op.IsStarted() {
			infl.started()
		} else {
			infl.unstarted()
		}
		h.influence.Add(infl, 1.0)
	}
	for store, value := range h.influence.bytesRead {
		pendingInfluenceGauge.WithLabelValues("bytes_read", storeStr(store)).Set(value)
	}
	for store, value := range h.influence.bytesWrite {
		pendingInfluenceGauge.WithLabelValues("bytes_write", storeStr(store)).Set(value)
	}
}

func calcZombieInfluence(op *operator.Operator, infl *opInfluence) *opInfluence {
	zombieTime := time.Since(op.GetDropTime())
	if op.IsStarted() && zombieTime < opMaxZombieTime {
		return infl.multWeight(float64(zombieTime)/float64(opMaxZombieTime), nil)
	}
	return nil
}

func (h *balanceHotRegionsScheduler) dispatch(typ BalanceType, cluster schedule.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()
	h.analyzeStoreLoad(cluster.GetStoresStats())
	h.updatePendingInfluence()
	switch typ {
	case hotReadRegionBalance:
		h.stats.readStatAsLeader = calcScore(cluster, h.influence, typ, core.LeaderKind)
		return h.balanceHotReadRegions(cluster)
	case hotWriteRegionBalance:
		h.stats.writeStatAsLeader = calcScore(cluster, h.influence, typ, core.LeaderKind)
		h.stats.writeStatAsPeer = calcScore(cluster, h.influence, typ, core.RegionKind)
		return h.balanceHotWriteRegions(cluster)
	}
	return nil
}

func (h *balanceHotRegionsScheduler) analyzeStoreLoad(storesStats *statistics.StoresStats) {
	bytesReadStat := storesStats.GetStoresBytesReadStat()
	bytesWriteStat := storesStats.GetStoresBytesWriteStat()

	readFlowMean := MeanStoresStats(bytesReadStat)
	writeFlowMean := MeanStoresStats(bytesWriteStat)
	readFlowScorePairs := NormalizeStoresStats(bytesReadStat)
	writeFlowScorePairs := NormalizeStoresStats(bytesWriteStat)

	weights := []float64{}
	means := readFlowMean + writeFlowMean
	if means <= minFlowBytes {
		weights = append(weights, 0, 0)
	} else {
		weights = append(weights, readFlowMean/means, writeFlowMean/means)
	}

	scorePairSliceVec := []*ScorePairSlice{readFlowScorePairs, writeFlowScorePairs}
	h.storesScore = AggregateScores(scorePairSliceVec, weights)
}

func (h *balanceHotRegionsScheduler) balanceHotReadRegions(cluster schedule.Cluster) []*operator.Operator {
	// balance by leader
	srcRegion, newLeader, influence := h.balanceByLeader(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		op := operator.CreateTransferLeaderOperator("transfer-hot-read-leader", srcRegion, srcRegion.GetLeader().GetStoreId(), newLeader.GetStoreId(), operator.OpHotRegion)
		op.SetPriorityLevel(core.HighPriority)
		schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()

		opInf := newOpInfluence(op.Desc(), srcRegion.GetLeader().GetStoreId(), newLeader.GetStoreId())
		opInf.bytesRead = influence
		h.pendingOps[op] = opInf
		hotspotOpCounter.WithLabelValues(op.Desc(), u64Str(srcRegion.GetLeader().GetStoreId()), u64Str(newLeader.GetStoreId())).Inc()

		return []*operator.Operator{op}
	}

	// balance by peer
	srcRegion, srcPeer, destPeer, influence := h.balanceByPeer(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		op, err := operator.CreateMovePeerOperator("move-hot-read-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
		if err != nil {
			schedulerCounter.WithLabelValues(h.GetName(), "create_operator_fail").Inc()
			return nil
		}
		op.SetPriorityLevel(core.HighPriority)
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()

		opInf := newOpInfluence(op.Desc(), srcPeer.GetStoreId(), destPeer.GetStoreId())
		opInf.bytesRead = influence
		h.pendingOps[op] = opInf
		hotspotOpCounter.WithLabelValues(op.Desc(), u64Str(srcPeer.GetStoreId()), u64Str(destPeer.GetStoreId())).Inc()

		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

// balanceHotRetryLimit is the limit to retry schedule for selected balance strategy.
const balanceHotRetryLimit = 10

func (h *balanceHotRegionsScheduler) balanceHotWriteRegions(cluster schedule.Cluster) []*operator.Operator {
	for i := 0; i < balanceHotRetryLimit; i++ {
		switch h.r.Intn(cluster.GetMaxReplicas() + balanceHotRetryLimit) {
		case -1:
			// balance by leader
			srcRegion, newLeader, influence := h.balanceByLeader(cluster, h.stats.writeStatAsLeader)
			if srcRegion != nil {
				op := operator.CreateTransferLeaderOperator("transfer-hot-write-leader", srcRegion, srcRegion.GetLeader().GetStoreId(), newLeader.GetStoreId(), operator.OpHotRegion)
				op.SetPriorityLevel(core.HighPriority)
				schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()

				opInf := newOpInfluence(op.Desc(), srcRegion.GetLeader().GetStoreId(), newLeader.GetStoreId())
				opInf.bytesWrite = influence
				opInf.multWeight(0.01, opInf)
				h.pendingOps[op] = opInf
				hotspotOpCounter.WithLabelValues(op.Desc(), u64Str(srcRegion.GetLeader().GetStoreId()), u64Str(newLeader.GetStoreId())).Inc()

				return []*operator.Operator{op}
			}
		default:
			// balance by peer
			srcRegion, srcPeer, destPeer, influence := h.balanceByPeer(cluster, h.stats.writeStatAsPeer)
			if srcRegion != nil {
				op, err := operator.CreateMovePeerOperator("move-hot-write-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
				if err != nil {
					schedulerCounter.WithLabelValues(h.GetName(), "create_operator_fail").Inc()
					return nil
				}
				op.SetPriorityLevel(core.HighPriority)
				schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()

				opInf := newOpInfluence(op.Desc(), srcPeer.GetStoreId(), destPeer.GetStoreId())
				opInf.bytesWrite = influence
				h.pendingOps[op] = opInf
				hotspotOpCounter.WithLabelValues(op.Desc(), u64Str(srcPeer.GetStoreId()), u64Str(destPeer.GetStoreId())).Inc()

				return []*operator.Operator{op}
			}
		}
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func calcScore(cluster schedule.Cluster, pendingInf *opInfluence, typ BalanceType, kind core.ResourceKind) statistics.StoreHotRegionsStat {
	var storeItems map[uint64][]*statistics.HotSpotPeerStat
	switch typ {
	case hotWriteRegionBalance:
		storeItems = cluster.RegionWriteStats()
	case hotReadRegionBalance:
		storeItems = cluster.RegionReadStats()
	}
	stats := make(statistics.StoreHotRegionsStat)
	storesStats := cluster.GetStoresStats()
	for storeID, items := range storeItems {
		// HotDegree is the update times on the hot cache. If the heartbeat report
		// the flow of the region exceeds the threshold, the scheduler will update the region in
		// the hot cache and the hotdegree of the region will increase.
		storeStat, ok := stats[storeID]
		if !ok {
			storeStat = &statistics.HotRegionsStat{
				RegionsStat: make(statistics.RegionsStat, 0, storeHotRegionsDefaultLen),
			}
			stats[storeID] = storeStat
		}

		for _, r := range items {
			if kind == core.LeaderKind && !r.IsLeader() {
				continue
			}
			if r.HotDegree < cluster.GetHotRegionCacheHitsThreshold() {
				continue
			}

			regionInfo := cluster.GetRegion(r.RegionID)
			if regionInfo == nil {
				continue
			}

			s := statistics.HotSpotPeerStat{
				RegionID:       r.RegionID,
				FlowBytes:      r.GetFlowBytes(),
				HotDegree:      r.HotDegree,
				LastUpdateTime: r.LastUpdateTime,
				StoreID:        storeID,
				AntiCount:      r.AntiCount,
				Version:        r.Version,
			}
			storeStat.TotalFlowBytes += r.GetFlowBytes()
			storeStat.RegionsCount++
			storeStat.RegionsStat = append(storeStat.RegionsStat, s)
		}

		switch typ {
		case hotReadRegionBalance:
			{
				_, readBytesRate := storesStats.GetStoreBytesRate(storeID)
				storeStat.StoreFlowBytes = uint64(readBytesRate)
				readBytesRate += pendingInf.bytesRead[storeID]
				storeStat.StoreFutureBytes = uint64(readBytesRate)
			}
		case hotWriteRegionBalance:
			{
				writeBytesRate, _ := storesStats.GetStoreBytesRate(storeID)
				storeStat.StoreFlowBytes = uint64(writeBytesRate)
				writeBytesRate += pendingInf.bytesWrite[storeID]
				storeStat.StoreFutureBytes = uint64(writeBytesRate)
			}
		}
	}
	return stats
}

// balanceByPeer balances the peer distribution of hot regions.
func (h *balanceHotRegionsScheduler) balanceByPeer(cluster schedule.Cluster, storesStat statistics.StoreHotRegionsStat) (*core.RegionInfo, *metapb.Peer, *metapb.Peer, map[uint64]float64) {
	if !h.allowBalanceRegion(cluster) {
		return nil, nil, nil, nil
	}

	srcStoreID := h.selectSrcStore(storesStat)
	if srcStoreID == 0 {
		return nil, nil, nil, nil
	}

	// get one source region and a target store.
	// For each region in the source store, we try to find the best target store;
	// If we can find a target store, then return from this method.
	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(storesStat[srcStoreID].RegionsStat.Len()) {
		rs := storesStat[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no_region").Inc()
			continue
		}

		if isRegionUnhealthy(srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy_replica").Inc()
			continue
		}

		if len(srcRegion.GetPeers()) != cluster.GetMaxReplicas() {
			log.Debug("region has abnormal replica count", zap.String("scheduler", h.GetName()), zap.Uint64("region-id", srcRegion.GetID()))
			schedulerCounter.WithLabelValues(h.GetName(), "abnormal_replica").Inc()
			continue
		}

		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID))
		}
		filters := []filter.Filter{
			filter.StoreStateFilter{MoveRegion: true},
			filter.NewExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewDistinctScoreFilter(cluster.GetLocationLabels(), cluster.GetRegionStores(srcRegion), srcStore),
		}
		candidateStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if filter.Target(cluster, store, filters) {
				continue
			}
			candidateStoreIDs = append(candidateStoreIDs, store.GetID())
		}

		destStoreID = h.selectDestStore(candidateStoreIDs, rs.GetFlowBytes(), srcStoreID, storesStat)
		if destStoreID != 0 {
			h.peerLimit = h.adjustBalanceLimit(srcStoreID, storesStat)

			srcPeer := srcRegion.GetStorePeer(srcStoreID)
			if srcPeer == nil {
				return nil, nil, nil, nil
			}

			// When the target store is decided, we allocate a peer ID to hold the source region,
			// because it doesn't exist in the system right now.
			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Error("failed to allocate peer", zap.Error(err))
				return nil, nil, nil, nil
			}

			influence := map[uint64]float64{
				srcPeer.GetStoreId():  -float64(rs.GetFlowBytes()),
				destPeer.GetStoreId(): float64(rs.GetFlowBytes()),
			}

			return srcRegion, srcPeer, destPeer, influence
		}
	}

	return nil, nil, nil, nil
}

// balanceByLeader balances the leader distribution of hot regions.
func (h *balanceHotRegionsScheduler) balanceByLeader(cluster schedule.Cluster, storesStat statistics.StoreHotRegionsStat) (*core.RegionInfo, *metapb.Peer, map[uint64]float64) {
	if !h.allowBalanceLeader(cluster) {
		return nil, nil, nil
	}

	srcStoreID := h.selectSrcStore(storesStat)
	if srcStoreID == 0 {
		return nil, nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(storesStat[srcStoreID].RegionsStat.Len()) {
		rs := storesStat[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no_region").Inc()
			continue
		}

		if isRegionUnhealthy(srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy_replica").Inc()
			continue
		}

		filters := []filter.Filter{filter.StoreStateFilter{TransferLeader: true}}
		candidateStoreIDs := make([]uint64, 0, len(srcRegion.GetPeers())-1)
		for _, store := range cluster.GetFollowerStores(srcRegion) {
			if !filter.Target(cluster, store, filters) {
				candidateStoreIDs = append(candidateStoreIDs, store.GetID())
			}
		}
		if len(candidateStoreIDs) == 0 {
			continue
		}
		destStoreID := h.selectDestStore(candidateStoreIDs, rs.GetFlowBytes(), srcStoreID, storesStat)
		if destStoreID == 0 {
			continue
		}

		destPeer := srcRegion.GetStoreVoter(destStoreID)
		if destPeer != nil {
			h.leaderLimit = h.adjustBalanceLimit(srcStoreID, storesStat)
			influence := map[uint64]float64{
				srcRegion.GetLeader().GetStoreId(): -float64(rs.GetFlowBytes()),
				destPeer.GetStoreId():              float64(rs.GetFlowBytes()),
			}
			return srcRegion, destPeer, influence
		}
	}
	return nil, nil, nil
}

// Select the store to move hot regions from.
// We choose the store with the maximum number of hot region first.
// Inside these stores, we choose the one with maximum flow bytes.
func (h *balanceHotRegionsScheduler) selectSrcStore(stats statistics.StoreHotRegionsStat) (srcStoreID uint64) {
	var (
		maxScore uint64
		maxCount int
	)

	for storeID, stat := range stats {
		score := stat.StoreFutureBytes
		count := stat.RegionsStat.Len()
		if count < 1 {
			continue
		}
		if score > maxScore || (score == maxScore && count > maxCount) {
			maxCount = count
			maxScore = score
			srcStoreID = storeID
		}
	}
	return
}

// selectDestStore selects a target store to hold the region of the source region.
// We choose a target store based on the hot region number and flow bytes of this store.
func (h *balanceHotRegionsScheduler) selectDestStore(candidateStoreIDs []uint64, regionFlowBytes uint64, srcStoreID uint64, storesStat statistics.StoreHotRegionsStat) (destStoreID uint64) {
	sr := storesStat[srcStoreID]
	srcScore := minUint64(sr.StoreFlowBytes, sr.StoreFutureBytes)

	var (
		// Prevent overflow.
		minDstScore uint64 = maxUint64(uint64(float64(srcScore)*hotRegionScheduleFactor), regionFlowBytes) - regionFlowBytes
		minCount           = int(math.MaxInt32)
	)
	for _, storeID := range candidateStoreIDs {
		if s, ok := storesStat[storeID]; ok {
			dstScore := maxUint64(s.StoreFlowBytes, s.StoreFutureBytes)
			dstCount := s.RegionsStat.Len()
			if minDstScore > dstScore || (minDstScore == dstScore && dstCount < minCount) {
				destStoreID = storeID
				minDstScore = dstScore
				minCount = dstCount
			}
		} else {
			// Assume that stores with no hot region are cold.
			destStoreID = storeID
			return
		}
	}
	return
}

func (h *balanceHotRegionsScheduler) adjustBalanceLimit(storeID uint64, storesStat statistics.StoreHotRegionsStat) uint64 {
	srcStoreStatistics := storesStat[storeID]

	var hotRegionTotalCount float64
	for _, m := range storesStat {
		hotRegionTotalCount += float64(m.RegionsStat.Len())
	}

	avgRegionCount := hotRegionTotalCount / float64(len(storesStat))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(srcStoreStatistics.RegionsStat.Len()) - avgRegionCount) * hotRegionLimitFactor)
	return maxUint64(limit, 1)
}

func (h *balanceHotRegionsScheduler) GetHotReadStatus() *statistics.StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotRegionsStat, len(h.stats.readStatAsLeader))
	for id, stat := range h.stats.readStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &statistics.StoreHotRegionInfos{
		AsLeader: asLeader,
	}
}

func (h *balanceHotRegionsScheduler) GetHotWriteStatus() *statistics.StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotRegionsStat, len(h.stats.writeStatAsLeader))
	asPeer := make(statistics.StoreHotRegionsStat, len(h.stats.writeStatAsPeer))
	for id, stat := range h.stats.writeStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	for id, stat := range h.stats.writeStatAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	return &statistics.StoreHotRegionInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

func (h *balanceHotRegionsScheduler) GetStoresScore() map[uint64]float64 {
	h.RLock()
	defer h.RUnlock()
	storesScore := make(map[uint64]float64, 0)
	for _, pair := range h.storesScore.GetPairs() {
		storesScore[pair.GetStoreID()] = pair.GetScore()
	}
	return storesScore
}

func u64Str(x uint64) string {
	return fmt.Sprintf("%d", x)
}

func storeStr(x uint64) string {
	return fmt.Sprintf("store-%d", x)
}
