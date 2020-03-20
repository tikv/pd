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
	"net/http"
	"sort"
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
		conf := initHotRegionScheduleConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newHotScheduler(opController, conf), nil
	})

	// FIXME: remove this two schedule after the balance test move in schedulers package
	{
		schedule.RegisterScheduler(HotWriteRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
			return newHotWriteScheduler(opController, initHotRegionScheduleConfig()), nil
		})
		schedule.RegisterScheduler(HotReadRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
			return newHotReadScheduler(opController, initHotRegionScheduleConfig()), nil
		})

	}
}

const (
	// HotRegionName is balance hot region scheduler name.
	HotRegionName = "balance-hot-region-scheduler"
	// HotRegionType is balance hot region scheduler type.
	HotRegionType = "hot-region"
	// HotReadRegionType is hot read region scheduler type.
	HotReadRegionType = "hot-read-region"
	// HotWriteRegionType is hot write region scheduler type.
	HotWriteRegionType = "hot-write-region"

	hotRegionLimitFactor = 0.75
)

type hotScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []rwType
	r           *rand.Rand

	// states across multiple `Schedule` calls
	pendings       [resourceTypeLen]map[*pendingInfluence]struct{}
	regionPendings map[uint64][2]*operator.Operator

	// temporary states but exported to API or metrics
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
	pendingSums [resourceTypeLen]map[uint64]Influence
	// config of hot scheduler
	conf *hotRegionSchedulerConfig
}

func newHotScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	base := NewBaseScheduler(opController)
	ret := &hotScheduler{
		name:           HotRegionName,
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		types:          []rwType{write, read},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		regionPendings: make(map[uint64][2]*operator.Operator),
		conf:           conf,
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.pendings[ty] = map[*pendingInfluence]struct{}{}
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func newHotReadScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{read}
	return ret
}

func newHotWriteScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{write}
	return ret
}

func (h *hotScheduler) GetName() string {
	return h.name
}

func (h *hotScheduler) GetType() string {
	return HotRegionType
}

func (h *hotScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.conf.ServeHTTP(w, r)
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

func (h *hotScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()

	h.prepareForBalance(cluster)

	switch typ {
	case read:
		return h.balanceHotReadRegions(cluster)
	case write:
		return h.balanceHotWriteRegions(cluster)
	}
	return nil
}

func (h *hotScheduler) prepareForBalance(cluster opt.Cluster) {
	h.summaryPendingInfluence()

	storesStat := cluster.GetStoresStats()

	minHotDegree := cluster.GetHotRegionCacheHitsThreshold()
	{ // update read statistics
		regionRead := cluster.RegionReadStats()
		storeByte := storesStat.GetStoresBytesReadStat()
		storeKey := storesStat.GetStoresKeysReadStat()

		h.stLoadInfos[readLeader] = summaryStoresLoad(
			storeByte,
			storeKey,
			h.pendingSums[readLeader],
			regionRead,
			minHotDegree,
			read, core.LeaderKind)
	}

	{ // update write statistics
		regionWrite := cluster.RegionWriteStats()
		storeByte := storesStat.GetStoresBytesWriteStat()
		storeKey := storesStat.GetStoresKeysWriteStat()

		h.stLoadInfos[writeLeader] = summaryStoresLoad(
			storeByte,
			storeKey,
			h.pendingSums[writeLeader],
			regionWrite,
			minHotDegree,
			write, core.LeaderKind)

		h.stLoadInfos[writePeer] = summaryStoresLoad(
			storeByte,
			storeKey,
			h.pendingSums[writePeer],
			regionWrite,
			minHotDegree,
			write, core.RegionKind)
	}
}

func (h *hotScheduler) summaryPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendingSums[ty] = summaryPendingInfluence(h.pendings[ty], h.calcPendingWeight)
	}
	h.gcRegionPendings()
}

func (h *hotScheduler) gcRegionPendings() {
	for regionID, pendings := range h.regionPendings {
		empty := true
		for ty, op := range pendings {
			if op != nil && op.IsEnd() {
				if time.Now().After(op.GetCreateTime().Add(h.conf.GetMaxZombieDuration())) {
					schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Dec()
					pendings[ty] = nil
				}
			}
			if pendings[ty] != nil {
				empty = false
			}
		}
		if empty {
			delete(h.regionPendings, regionID)
		} else {
			h.regionPendings[regionID] = pendings
		}
	}
}

// Load information of all available stores.
func summaryStoresLoad(
	storeByteRate map[uint64]float64,
	storeKeyRate map[uint64]float64,
	pendings map[uint64]Influence,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	minHotDegree int,
	rwTy rwType,
	kind core.ResourceKind,
) map[uint64]*storeLoadDetail {
	loadDetail := make(map[uint64]*storeLoadDetail, len(storeByteRate))

	// Stores without byte rate statistics is not available to schedule.
	for id, byteRate := range storeByteRate {
		keyRate := storeKeyRate[id]

		// Find all hot peers first
		hotPeers := make([]*statistics.HotPeerStat, 0)
		{
			byteSum := 0.0
			keySum := 0.0
			for _, peer := range filterHotPeers(kind, minHotDegree, storeHotPeers[id]) {
				byteSum += peer.GetByteRate()
				keySum += peer.GetKeyRate()
				hotPeers = append(hotPeers, peer.Clone())
			}
			// Use sum of hot peers to estimate leader-only byte rate.
			if kind == core.LeaderKind && rwTy == write {
				byteRate = byteSum
				keyRate = keySum
			}

			// Metric for debug.
			{
				ty := "byte-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(byteSum)
			}
			{
				ty := "key-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(keySum)
			}
		}

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&storeLoad{
			ByteRate: byteRate,
			KeyRate:  keyRate,
			Count:    float64(len(hotPeers)),
		}).ToLoadPred(pendings[id])

		// Construct store load info.
		loadDetail[id] = &storeLoadDetail{
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
		}
	}
	return loadDetail
}

func filterHotPeers(
	kind core.ResourceKind,
	minHotDegree int,
	peers []*statistics.HotPeerStat,
) []*statistics.HotPeerStat {
	ret := make([]*statistics.HotPeerStat, 0)
	for _, peer := range peers {
		if (kind == core.LeaderKind && !peer.IsLeader()) ||
			peer.HotDegree < minHotDegree {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}

func (h *hotScheduler) addPendingInfluence(op *operator.Operator, srcStore, dstStore uint64, infl Influence, rwTy rwType, opTy opType) bool {
	regionID := op.RegionID()
	_, ok := h.regionPendings[regionID]
	if ok {
		schedulerStatus.WithLabelValues(h.GetName(), "pending_op_fails").Inc()
		return false
	}

	influence := newPendingInfluence(op, srcStore, dstStore, infl)
	rcTy := toResourceType(rwTy, opTy)
	h.pendings[rcTy][influence] = struct{}{}

	h.regionPendings[regionID] = [2]*operator.Operator{nil, nil}
	{ // h.pendingOpInfos[regionID][ty] = influence
		tmp := h.regionPendings[regionID]
		tmp[opTy] = op
		h.regionPendings[regionID] = tmp
	}

	schedulerStatus.WithLabelValues(h.GetName(), "pending_op_create").Inc()
	return true
}

func (h *hotScheduler) balanceHotReadRegions(cluster opt.Cluster) []*operator.Operator {
	// prefer to balance by leader
	leaderSolver := newBalanceSolver(h, cluster, read, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	peerSolver := newBalanceSolver(h, cluster, read, movePeer)
	ops = peerSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *hotScheduler) balanceHotWriteRegions(cluster opt.Cluster) []*operator.Operator {
	// prefer to balance by peer
	peerSolver := newBalanceSolver(h, cluster, write, movePeer)
	ops := peerSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	leaderSolver := newBalanceSolver(h, cluster, write, transferLeader)
	ops = leaderSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

type balanceSolver struct {
	sche         *hotScheduler
	cluster      opt.Cluster
	stLoadDetail map[uint64]*storeLoadDetail
	rwTy         rwType
	opTy         opType

	cur *solution

	maxSrc   *storeLoad
	minDst   *storeLoad
	rankStep *storeLoad
}

type solution struct {
	srcStoreID  uint64
	srcPeerStat *statistics.HotPeerStat
	region      *core.RegionInfo
	dstStoreID  uint64

	// progressiveRank measures the contribution for balance.
	// The smaller the rank, the better this solution is.
	// If rank < 0, this solution makes thing better.
	progressiveRank int64
}

func (bs *balanceSolver) init() {
	switch toResourceType(bs.rwTy, bs.opTy) {
	case writePeer:
		bs.stLoadDetail = bs.sche.stLoadInfos[writePeer]
	case writeLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[writeLeader]
	case readLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[readLeader]
	}
	for _, id := range getUnhealthyStores(bs.cluster) {
		delete(bs.stLoadDetail, id)
	}

	bs.maxSrc = &storeLoad{}
	bs.minDst = &storeLoad{
		ByteRate: math.MaxFloat64,
		KeyRate:  math.MaxFloat64,
		Count:    math.MaxFloat64,
	}
	maxCur := &storeLoad{}

	for _, detail := range bs.stLoadDetail {
		bs.maxSrc = maxLoad(bs.maxSrc, detail.LoadPred.min())
		bs.minDst = minLoad(bs.minDst, detail.LoadPred.max())
		maxCur = maxLoad(maxCur, &detail.LoadPred.Current)
	}

	bs.rankStep = &storeLoad{
		ByteRate: maxCur.ByteRate * bs.sche.conf.GetByteRankStepRatio(),
		KeyRate:  maxCur.KeyRate * bs.sche.conf.GetKeyRankStepRatio(),
		Count:    maxCur.Count * bs.sche.conf.GetCountRankStepRatio(),
	}
}

func getUnhealthyStores(cluster opt.Cluster) []uint64 {
	ret := make([]uint64, 0)
	stores := cluster.GetStores()
	for _, store := range stores {
		if store.IsTombstone() ||
			store.DownTime() > cluster.GetMaxStoreDownTime() {
			ret = append(ret, store.GetID())
		}
	}
	return ret
}

func newBalanceSolver(sche *hotScheduler, cluster opt.Cluster, rwTy rwType, opTy opType) *balanceSolver {
	solver := &balanceSolver{
		sche:    sche,
		cluster: cluster,
		rwTy:    rwTy,
		opTy:    opTy,
	}
	solver.init()
	return solver
}

func (bs *balanceSolver) isValid() bool {
	if bs.cluster == nil || bs.sche == nil || bs.stLoadDetail == nil {
		return false
	}
	switch bs.rwTy {
	case write, read:
	default:
		return false
	}
	switch bs.opTy {
	case movePeer, transferLeader:
	default:
		return false
	}
	return true
}

func (bs *balanceSolver) solve() []*operator.Operator {
	if !bs.isValid() || !bs.allowBalance() {
		return nil
	}
	bs.cur = &solution{}
	var (
		best  *solution
		ops   []*operator.Operator
		infls []Influence
	)

	for srcStoreID := range bs.filterSrcStores() {
		bs.cur.srcStoreID = srcStoreID

		for _, srcPeerStat := range bs.filterHotPeers() {
			bs.cur.srcPeerStat = srcPeerStat
			bs.cur.region = bs.getRegion()
			if bs.cur.region == nil {
				continue
			}

			for dstStoreID := range bs.filterDstStores() {
				bs.cur.dstStoreID = dstStoreID
				bs.calcProgressiveRank()
				if bs.cur.progressiveRank < 0 && bs.betterThan(best) {
					if newOps, newInfls := bs.buildOperators(); len(newOps) > 0 {
						ops = newOps
						infls = newInfls
						clone := *bs.cur
						best = &clone
					}
				}
			}
		}
	}

	for i := 0; i < len(ops); i++ {
		// TODO: multiple operators need to be atomic.
		if !bs.sche.addPendingInfluence(ops[i], best.srcStoreID, best.dstStoreID, infls[i], bs.rwTy, bs.opTy) {
			return nil
		}
	}
	return ops
}

func (bs *balanceSolver) allowBalance() bool {
	switch bs.opTy {
	case movePeer:
		return bs.sche.allowBalanceRegion(bs.cluster)
	case transferLeader:
		return bs.sche.allowBalanceLeader(bs.cluster)
	default:
		return false
	}
}

func (bs *balanceSolver) filterSrcStores() map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail)
	for id, detail := range bs.stLoadDetail {
		if bs.cluster.GetStore(id) == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", id))
			continue
		}
		if len(detail.HotPeers) == 0 {
			continue
		}
		ret[id] = detail
	}
	return ret
}

func (bs *balanceSolver) filterHotPeers() []*statistics.HotPeerStat {
	ret := bs.stLoadDetail[bs.cur.srcStoreID].HotPeers
	// Return at most MaxPeerNum peers, to prevent balanceSolver.solve() too slow.
	maxPeerNum := bs.sche.conf.GetMaxPeerNumber()

	// filter pending region
	appendItem := func(items []*statistics.HotPeerStat, item *statistics.HotPeerStat) []*statistics.HotPeerStat {
		if _, ok := bs.sche.regionPendings[item.ID()]; !ok {
			items = append(items, item)
		}
		return items
	}
	if len(ret) <= maxPeerNum {
		nret := make([]*statistics.HotPeerStat, 0, len(ret))
		for _, peer := range ret {
			nret = appendItem(nret, peer)
		}
		return nret
	}

	byteSort := make([]*statistics.HotPeerStat, len(ret))
	copy(byteSort, ret)
	sort.Slice(byteSort, func(i, j int) bool {
		return byteSort[i].GetByteRate() > byteSort[j].GetByteRate()
	})
	keySort := make([]*statistics.HotPeerStat, len(ret))
	copy(keySort, ret)
	sort.Slice(keySort, func(i, j int) bool {
		return keySort[i].GetKeyRate() > keySort[j].GetKeyRate()
	})

	union := make(map[*statistics.HotPeerStat]struct{}, maxPeerNum)
	for len(union) < maxPeerNum {
		for len(byteSort) > 0 {
			peer := byteSort[0]
			byteSort = byteSort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
		for len(keySort) > 0 {
			peer := keySort[0]
			keySort = keySort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
	}
	ret = make([]*statistics.HotPeerStat, 0, len(union))
	for peer := range union {
		ret = appendItem(ret, peer)
	}
	return ret
}

// isRegionAvailable checks whether the given region is not available to schedule.
func (bs *balanceSolver) isRegionAvailable(region *core.RegionInfo) bool {
	if region == nil {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "no-region").Inc()
		return false
	}

	if pendings, ok := bs.sche.regionPendings[region.GetID()]; ok {
		if bs.opTy == transferLeader {
			return false
		}
		if pendings[movePeer] != nil ||
			(pendings[transferLeader] != nil && !pendings[transferLeader].IsEnd()) {
			return false
		}
	}

	if !opt.IsHealthyAllowPending(bs.cluster, region) {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "unhealthy-replica").Inc()
		return false
	}

	if !opt.IsRegionReplicated(bs.cluster, region) {
		log.Debug("region has abnormal replica count", zap.String("scheduler", bs.sche.GetName()), zap.Uint64("region-id", region.GetID()))
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "abnormal-replica").Inc()
		return false
	}

	return true
}

func (bs *balanceSolver) getRegion() *core.RegionInfo {
	region := bs.cluster.GetRegion(bs.cur.srcPeerStat.ID())
	if !bs.isRegionAvailable(region) {
		return nil
	}

	switch bs.opTy {
	case movePeer:
		srcPeer := region.GetStorePeer(bs.cur.srcStoreID)
		if srcPeer == nil {
			log.Debug("region does not have a peer on source store, maybe stat out of date", zap.Uint64("region-id", bs.cur.srcPeerStat.ID()))
			return nil
		}
	case transferLeader:
		if region.GetLeader().GetStoreId() != bs.cur.srcStoreID {
			log.Debug("region leader is not on source store, maybe stat out of date", zap.Uint64("region-id", bs.cur.srcPeerStat.ID()))
			return nil
		}
	default:
		return nil
	}

	return region
}

func (bs *balanceSolver) filterDstStores() map[uint64]*storeLoadDetail {
	var (
		filters    []filter.Filter
		candidates []*core.StoreInfo
	)

	switch bs.opTy {
	case movePeer:
		var scoreGuard filter.Filter
		if bs.cluster.IsPlacementRulesEnabled() {
			scoreGuard = filter.NewRuleFitFilter(bs.sche.GetName(), bs.cluster, bs.cur.region, bs.cur.srcStoreID)
		} else {
			srcStore := bs.cluster.GetStore(bs.cur.srcStoreID)
			if srcStore == nil {
				return nil
			}
			scoreGuard = filter.NewDistinctScoreFilter(bs.sche.GetName(), bs.cluster.GetLocationLabels(), bs.cluster.GetRegionStores(bs.cur.region), srcStore)
		}

		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.region.GetStoreIds(), bs.cur.region.GetStoreIds()),
			filter.NewHealthFilter(bs.sche.GetName()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
			scoreGuard,
		}

		candidates = bs.cluster.GetStores()

	case transferLeader:
		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true},
			filter.NewHealthFilter(bs.sche.GetName()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
		}

		candidates = bs.cluster.GetFollowerStores(bs.cur.region)

	default:
		return nil
	}

	ret := make(map[uint64]*storeLoadDetail, len(candidates))
	for _, store := range candidates {
		if filter.Target(bs.cluster, store, filters) {
			ret[store.GetID()] = bs.stLoadDetail[store.GetID()]
		}
	}
	return ret
}

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
func (bs *balanceSolver) calcProgressiveRank() {
	srcLd := bs.stLoadDetail[bs.cur.srcStoreID].LoadPred.min()
	dstLd := bs.stLoadDetail[bs.cur.dstStoreID].LoadPred.max()
	peer := bs.cur.srcPeerStat
	rank := int64(0)
	if bs.rwTy == write && bs.opTy == transferLeader {
		// In this condition, CPU usage is the matter.
		// Only consider about count and key rate.
		if srcLd.Count > dstLd.Count &&
			srcLd.KeyRate >= dstLd.KeyRate+peer.GetKeyRate() {
			rank = -1
		}
	} else {
		keyDecRatio := (dstLd.KeyRate + peer.GetKeyRate()) / (srcLd.KeyRate + 1)
		keyHot := peer.GetKeyRate() >= bs.sche.conf.GetMinHotKeyRate()
		byteDecRatio := (dstLd.ByteRate + peer.GetByteRate()) / (srcLd.ByteRate + 1)
		byteHot := peer.GetByteRate() > bs.sche.conf.GetMinHotByteRate()
		greatDecRatio, minorDecRatio := bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorGreatDecRatio()
		switch {
		case byteHot && byteDecRatio <= greatDecRatio && keyHot && keyDecRatio <= greatDecRatio:
			// Both byte rate and key rate are balanced, the best choice.
			rank = -3
		case byteDecRatio <= minorDecRatio && keyHot && keyDecRatio <= greatDecRatio:
			// Byte rate is not worsened, key rate is balanced.
			rank = -2
		case byteHot && byteDecRatio <= greatDecRatio:
			// Byte rate is balanced, ignore the key rate.
			rank = -1
		}
	}
	bs.cur.progressiveRank = rank
}

// betterThan checks if `bs.cur` is a better solution than `old`.
func (bs *balanceSolver) betterThan(old *solution) bool {
	if old == nil {
		return true
	}

	switch {
	case bs.cur.progressiveRank < old.progressiveRank:
		return true
	case bs.cur.progressiveRank > old.progressiveRank:
		return false
	}

	if r := bs.compareSrcStore(bs.cur.srcStoreID, old.srcStoreID); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r := bs.compareDstStore(bs.cur.dstStoreID, old.dstStoreID); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if bs.cur.srcPeerStat != old.srcPeerStat {
		// compare region

		if bs.rwTy == write && bs.opTy == transferLeader {
			switch {
			case bs.cur.srcPeerStat.GetKeyRate() > old.srcPeerStat.GetKeyRate():
				return true
			case bs.cur.srcPeerStat.GetKeyRate() < old.srcPeerStat.GetKeyRate():
				return false
			}
		} else {
			byteRkCmp := rankCmp(bs.cur.srcPeerStat.GetByteRate(), old.srcPeerStat.GetByteRate(), stepRank(0, 100))
			keyRkCmp := rankCmp(bs.cur.srcPeerStat.GetKeyRate(), old.srcPeerStat.GetKeyRate(), stepRank(0, 10))

			switch bs.cur.progressiveRank {
			case -2: // greatDecRatio < byteDecRatio <= minorDecRatio && keyDecRatio <= greatDecRatio
				if keyRkCmp != 0 {
					return keyRkCmp > 0
				}
				if byteRkCmp != 0 {
					// prefer smaller byte rate, to reduce oscillation
					return byteRkCmp < 0
				}
			case -3: // byteDecRatio <= greatDecRatio && keyDecRatio <= greatDecRatio
				if keyRkCmp != 0 {
					return keyRkCmp > 0
				}
				fallthrough
			case -1: // byteDecRatio <= greatDecRatio
				if byteRkCmp != 0 {
					// prefer region with larger byte rate, to converge faster
					return byteRkCmp > 0
				}
			}
		}
	}

	return false
}

// smaller is better
func (bs *balanceSolver) compareSrcStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare source store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(bs.maxSrc.Count, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
				))),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				)),
			)
		} else {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
					stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
				))),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				),
			)
		}

		lp1 := bs.stLoadDetail[st1].LoadPred
		lp2 := bs.stLoadDetail[st2].LoadPred
		return lpCmp(lp1, lp2)
	}
	return 0
}

// smaller is better
func (bs *balanceSolver) compareDstStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare destination store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(bs.minDst.Count, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
				)),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				)))
		} else {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRank(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
					stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
				)),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
				),
			)
		}

		lp1 := bs.stLoadDetail[st1].LoadPred
		lp2 := bs.stLoadDetail[st2].LoadPred
		return lpCmp(lp1, lp2)
	}
	return 0
}

func stepRank(rk0 float64, step float64) func(float64) int64 {
	return func(rate float64) int64 {
		return int64((rate - rk0) / step)
	}
}

func (bs *balanceSolver) isReadyToBuild() bool {
	if bs.cur.srcStoreID == 0 || bs.cur.dstStoreID == 0 ||
		bs.cur.srcPeerStat == nil || bs.cur.region == nil {
		return false
	}
	if bs.cur.srcStoreID != bs.cur.srcPeerStat.StoreID ||
		bs.cur.region.GetID() != bs.cur.srcPeerStat.ID() {
		return false
	}
	return true
}

func (bs *balanceSolver) buildOperators() ([]*operator.Operator, []Influence) {
	if !bs.isReadyToBuild() {
		return nil, nil
	}
	var (
		op  *operator.Operator
		err error
	)

	switch bs.opTy {
	case movePeer:
		srcPeer := bs.cur.region.GetStorePeer(bs.cur.srcStoreID) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: bs.cur.dstStoreID, IsLearner: srcPeer.IsLearner}
		bs.sche.peerLimit = bs.sche.adjustBalanceLimit(bs.cur.srcStoreID, bs.stLoadDetail)
		op, err = operator.CreateMovePeerOperator(
			"move-hot-"+bs.rwTy.String()+"-region",
			bs.cluster,
			bs.cur.region,
			operator.OpHotRegion,
			bs.cur.srcStoreID,
			dstPeer)
	case transferLeader:
		if bs.cur.region.GetStoreVoter(bs.cur.dstStoreID) == nil {
			return nil, nil
		}
		bs.sche.leaderLimit = bs.sche.adjustBalanceLimit(bs.cur.srcStoreID, bs.stLoadDetail)
		op, err = operator.CreateTransferLeaderOperator(
			"transfer-hot-"+bs.rwTy.String()+"-leader",
			bs.cluster,
			bs.cur.region,
			bs.cur.srcStoreID,
			bs.cur.dstStoreID,
			operator.OpHotRegion)
	}

	if err != nil {
		log.Debug("fail to create operator", zap.Error(err), zap.Stringer("rwType", bs.rwTy), zap.Stringer("opType", bs.opTy))
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "create-operator-fail").Inc()
		return nil, nil
	}

	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(bs.sche.GetName(), bs.opTy.String()))

	infl := Influence{
		ByteRate: bs.cur.srcPeerStat.GetByteRate(),
		KeyRate:  bs.cur.srcPeerStat.GetKeyRate(),
		Count:    1,
	}

	return []*operator.Operator{op}, []Influence{infl}
}

func (h *hotScheduler) adjustBalanceLimit(storeID uint64, loadDetail map[uint64]*storeLoadDetail) uint64 {
	srcStoreStatistics := loadDetail[storeID]

	var hotRegionTotalCount int
	for _, m := range loadDetail {
		hotRegionTotalCount += len(m.HotPeers)
	}

	avgRegionCount := float64(hotRegionTotalCount) / float64(len(loadDetail))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(len(srcStoreStatistics.HotPeers)) - avgRegionCount) * hotRegionLimitFactor)
	return maxUint64(limit, 1)
}

func (h *hotScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stLoadInfos[readLeader]))
	for id, detail := range h.stLoadInfos[readLeader] {
		asLeader[id] = detail.toHotPeersStat()
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
	}
}

func (h *hotScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stLoadInfos[writeLeader]))
	asPeer := make(statistics.StoreHotPeersStat, len(h.stLoadInfos[writePeer]))
	for id, detail := range h.stLoadInfos[writeLeader] {
		asLeader[id] = detail.toHotPeersStat()
	}
	for id, detail := range h.stLoadInfos[writePeer] {
		asPeer[id] = detail.toHotPeersStat()
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

func (h *hotScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(writePeer)
}

func (h *hotScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(readLeader)
}

func (h *hotScheduler) copyPendingInfluence(ty resourceType) map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	pendingSum := h.pendingSums[ty]
	ret := make(map[uint64]Influence, len(pendingSum))
	for id, infl := range pendingSum {
		ret[id] = infl
	}
	return ret
}

func (h *hotScheduler) calcPendingWeight(op *operator.Operator) float64 {
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
		maxZombieDur := h.conf.GetMaxZombieDuration()
		if zombieDur >= maxZombieDur {
			return 0
		}
		// TODO: use store statistics update time to make a more accurate estimation
		return float64(maxZombieDur-zombieDur) / float64(maxZombieDur)
	default:
		return 0
	}
}

func (h *hotScheduler) clearPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendings[ty] = map[*pendingInfluence]struct{}{}
		h.pendingSums[ty] = nil
	}
	h.regionPendings = make(map[uint64][2]*operator.Operator)
}

// rwType : the perspective of balance
type rwType int

const (
	write rwType = iota
	read
)

func (rw rwType) String() string {
	switch rw {
	case read:
		return "read"
	case write:
		return "write"
	default:
		return ""
	}
}

type opType int

const (
	movePeer opType = iota
	transferLeader
)

func (ty opType) String() string {
	switch ty {
	case movePeer:
		return "move-peer"
	case transferLeader:
		return "transfer-leader"
	default:
		return ""
	}
}

type resourceType int

const (
	writePeer resourceType = iota
	writeLeader
	readLeader
	resourceTypeLen
)

func toResourceType(rwTy rwType, opTy opType) resourceType {
	switch rwTy {
	case write:
		switch opTy {
		case movePeer:
			return writePeer
		case transferLeader:
			return writeLeader
		}
	case read:
		return readLeader
	}
	panic(fmt.Sprintf("invalid arguments for toResourceType: rwTy = %v, opTy = %v", rwTy, opTy))
}
