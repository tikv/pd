// Copyright 2017 TiKV Project Authors.
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
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
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

	minHotScheduleInterval = time.Second
	maxHotScheduleInterval = 20 * time.Second
)

// schedulePeerPr the probability of schedule the hot peer.
var schedulePeerPr = 0.66

type hotScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	types []rwType
	r     *rand.Rand

	// states across multiple `Schedule` calls
	pendings map[*pendingInfluence]struct{}
	// regionPendings stores regionID -> Operator
	// this records regionID which have pending Operator by operation type. During filterHotPeers, the hot peers won't
	// be selected if its owner region is tracked in this attribute.
	regionPendings map[uint64]*operator.Operator

	// temporary states but exported to API or metrics
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
	// This stores the pending Influence for each store by resource type.
	pendingSums map[uint64]*Influence
	// config of hot scheduler
	conf *hotRegionSchedulerConfig
}

func newHotScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	base := NewBaseScheduler(opController)
	ret := &hotScheduler{
		name:           HotRegionName,
		BaseScheduler:  base,
		types:          []rwType{write, read},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		pendings:       map[*pendingInfluence]struct{}{},
		regionPendings: make(map[uint64]*operator.Operator),
		conf:           conf,
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
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

func (h *hotScheduler) GetMinInterval() time.Duration {
	return minHotScheduleInterval
}
func (h *hotScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(h.GetMinInterval(), maxHotScheduleInterval, exponentialGrowth)
}

func (h *hotScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	allowed := h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(h.GetType(), operator.OpHotRegion.String()).Inc()
	}
	return allowed
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

// prepareForBalance calculate the summary of pending Influence for each store and prepare the load detail for
// each store
func (h *hotScheduler) prepareForBalance(cluster opt.Cluster) {
	h.summaryPendingInfluence()

	stores := cluster.GetStores()
	storesLoads := cluster.GetStoresLoads()

	{ // update read statistics
		regionRead := cluster.RegionReadStats()
		h.stLoadInfos[readLeader] = summaryStoresLoad(
			stores,
			storesLoads,
			h.pendingSums,
			regionRead,
			read, core.LeaderKind)
		h.stLoadInfos[readPeer] = summaryStoresLoad(
			stores,
			storesLoads,
			h.pendingSums,
			regionRead,
			read, core.RegionKind)
	}

	{ // update write statistics
		regionWrite := cluster.RegionWriteStats()
		h.stLoadInfos[writeLeader] = summaryStoresLoad(
			stores,
			storesLoads,
			h.pendingSums,
			regionWrite,
			write, core.LeaderKind)

		h.stLoadInfos[writePeer] = summaryStoresLoad(
			stores,
			storesLoads,
			h.pendingSums,
			regionWrite,
			write, core.RegionKind)
	}
}

// summaryPendingInfluence calculate the summary of pending Influence for each store
// and clean the region from regionInfluence if they have ended operator.
func (h *hotScheduler) summaryPendingInfluence() {
	h.pendingSums = summaryPendingInfluence(h.pendings, h.calcPendingWeight)
	h.gcRegionPendings()
}

// gcRegionPendings check the region whether it need to be deleted from regionPendings depended on whether it have
// ended operator
func (h *hotScheduler) gcRegionPendings() {
	for regionID, op := range h.regionPendings {
		if op != nil && op.IsEnd() {
			if time.Now().After(op.GetCreateTime().Add(h.conf.GetMaxZombieDuration())) {
				log.Debug("gc pending influence in hot region scheduler", zap.Uint64("region-id", regionID), zap.Time("create", op.GetCreateTime()), zap.Time("now", time.Now()), zap.Duration("zombie", h.conf.GetMaxZombieDuration()))
				schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Dec()
				delete(h.regionPendings, regionID)
			}
		}
	}
}

// summaryStoresLoad Load information of all available stores.
// it will filtered the hot peer and calculate the current and future stat(byte/key rate,count) for each store
func summaryStoresLoad(
	stores []*core.StoreInfo,
	storesLoads map[uint64][]float64,
	storePendings map[uint64]*Influence,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	rwTy rwType,
	kind core.ResourceKind,
) map[uint64]*storeLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(key/byte rate,count)
	loadDetail := make(map[uint64]*storeLoadDetail, len(storesLoads))
	allLoadSum := make([]float64, statistics.DimLen)
	allCount := 0.0

	// Stores without byte rate statistics is not available to schedule.
	for _, store := range stores {
		id := store.GetID()
		storeLoads, ok := storesLoads[id]
		if !ok {
			continue
		}
		loads := make([]float64, statistics.DimLen)
		switch rwTy {
		case read:
			loads[statistics.ByteDim] = storeLoads[statistics.StoreReadBytes]
			loads[statistics.KeyDim] = storeLoads[statistics.StoreReadKeys]
			loads[statistics.QueryDim] = storeLoads[statistics.StoreReadQuery]
		case write:
			loads[statistics.ByteDim] = storeLoads[statistics.StoreWriteBytes]
			loads[statistics.KeyDim] = storeLoads[statistics.StoreWriteKeys]
			loads[statistics.QueryDim] = storeLoads[statistics.StoreWriteQuery]
		}
		// Find all hot peers first
		var hotPeers []*statistics.HotPeerStat
		{
			peerLoadSum := make([]float64, statistics.DimLen)
			// TODO: To remove `filterHotPeers`, we need to:
			// HotLeaders consider `Write{Bytes,Keys}`, so when we schedule `writeLeader`, all peers are leader.
			for _, peer := range filterHotPeers(kind, storeHotPeers[id]) {
				for i := range peerLoadSum {
					peerLoadSum[i] += peer.GetLoad(getRegionStatKind(rwTy, i))
				}
				hotPeers = append(hotPeers, peer.Clone())
			}
			// Use sum of hot peers to estimate leader-only byte rate.
			// For write requests, Write{Bytes, Keys} is applied to all Peers at the same time, while the Leader and Follower are under different loads (usually the Leader consumes more CPU).
			// But none of the current dimension reflect this difference, so we create a new dimension to reflect it.
			if kind == core.LeaderKind && rwTy == write {
				loads = peerLoadSum
			}

			// Metric for debug.
			{
				ty := "byte-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.ByteDim])
			}
			{
				ty := "key-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.KeyDim])
			}
			{
				ty := "query-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.QueryDim])
			}
		}
		for i := range allLoadSum {
			allLoadSum[i] += loads[i]
		}
		allCount += float64(len(hotPeers))

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&storeLoad{
			Loads: loads,
			Count: float64(len(hotPeers)),
		}).ToLoadPred(rwTy, storePendings[id])

		// Construct store load info.
		loadDetail[id] = &storeLoadDetail{
			Store:    store,
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
		}
	}
	storeLen := float64(len(storesLoads))
	// store expectation byte/key rate and count for each store-load detail.
	for id, detail := range loadDetail {
		expectLoads := make([]float64, len(allLoadSum))
		for i := range expectLoads {
			expectLoads[i] = allLoadSum[i] / storeLen
		}
		expectCount := allCount / storeLen
		detail.LoadPred.Expect.Loads = expectLoads
		detail.LoadPred.Expect.Count = expectCount
		// Debug
		{
			ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectLoads[statistics.ByteDim])
		}
		{
			ty := "exp-key-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectLoads[statistics.KeyDim])
		}
		{
			ty := "exp-query-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectLoads[statistics.QueryDim])
		}
		{
			ty := "exp-count-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectCount)
		}
	}
	return loadDetail
}

// filterHotPeers filter the peer whose hot degree is less than minHotDegress
func filterHotPeers(
	kind core.ResourceKind,
	peers []*statistics.HotPeerStat,
) []*statistics.HotPeerStat {
	ret := make([]*statistics.HotPeerStat, 0, len(peers))
	for _, peer := range peers {
		if kind == core.LeaderKind && !peer.IsLeader() {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}

func (h *hotScheduler) addPendingInfluence(op *operator.Operator, srcStore, dstStore uint64, infl Influence) bool {
	regionID := op.RegionID()
	_, ok := h.regionPendings[regionID]
	if ok {
		schedulerStatus.WithLabelValues(h.GetName(), "pending_op_fails").Inc()
		return false
	}

	influence := newPendingInfluence(op, srcStore, dstStore, infl)
	h.pendings[influence] = struct{}{}
	h.regionPendings[regionID] = op

	schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Inc()
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
	s := h.r.Intn(100)
	switch {
	case s < int(schedulePeerPr*100):
		peerSolver := newBalanceSolver(h, cluster, write, movePeer)
		ops := peerSolver.solve()
		if len(ops) > 0 {
			return ops
		}
	default:
	}

	leaderSolver := newBalanceSolver(h, cluster, write, transferLeader)
	ops := leaderSolver.solve()
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

	byteIsBetter bool
	keyIsBetter  bool
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
	case readPeer:
		bs.stLoadDetail = bs.sche.stLoadInfos[readPeer]
	}
	// And it will be unnecessary to filter unhealthy store, because it has been solved in process heartbeat

	bs.maxSrc = &storeLoad{Loads: make([]float64, statistics.DimLen)}
	bs.minDst = &storeLoad{
		Loads: make([]float64, statistics.DimLen),
		Count: math.MaxFloat64,
	}
	for i := range bs.minDst.Loads {
		bs.minDst.Loads[i] = math.MaxFloat64
	}
	maxCur := &storeLoad{Loads: make([]float64, statistics.DimLen)}

	for _, detail := range bs.stLoadDetail {
		bs.maxSrc = maxLoad(bs.maxSrc, detail.LoadPred.min())
		bs.minDst = minLoad(bs.minDst, detail.LoadPred.max())
		maxCur = maxLoad(maxCur, &detail.LoadPred.Current)
	}

	rankStepRatios := []float64{bs.sche.conf.GetByteRankStepRatio(), bs.sche.conf.GetKeyRankStepRatio(), bs.sche.conf.GetQueryRateRankStepRatio()}
	stepLoads := make([]float64, statistics.DimLen)
	for i := range stepLoads {
		stepLoads[i] = maxCur.Loads[i] * rankStepRatios[i]
	}
	bs.rankStep = &storeLoad{
		Loads: stepLoads,
		Count: maxCur.Count * bs.sche.conf.GetCountRankStepRatio(),
	}
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

// solve travels all the src stores, hot peers, dst stores and select each one of them to make a best scheduling solution.
// The comparing between solutions is based on calcProgressiveRank.
func (bs *balanceSolver) solve() []*operator.Operator {
	if !bs.isValid() {
		return nil
	}
	bs.cur = &solution{}
	var (
		best *solution
		op   *operator.Operator
		infl Influence
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
					if newOp, newInfl := bs.buildOperator(); newOp != nil {
						op = newOp
						infl = *newInfl
						clone := *bs.cur
						best = &clone
					}
				}
			}
		}
	}

	if best == nil || !bs.sche.addPendingInfluence(op, best.srcStoreID, best.dstStoreID, infl) {
		return nil
	}

	return []*operator.Operator{op}
}

// filterSrcStores compare the min rate and the ratio * expectation rate, if both key and byte rate is greater than
// its expectation * ratio, the store would be selected as hot source store
func (bs *balanceSolver) filterSrcStores() map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail)
	for id, detail := range bs.stLoadDetail {
		if len(detail.HotPeers) == 0 {
			continue
		}
		minLoad := detail.LoadPred.min()
		if slice.AllOf(minLoad.Loads, func(i int) bool {
			if statistics.IsSelectedDim(i) {
				return minLoad.Loads[i] > bs.sche.conf.GetSrcToleranceRatio()*detail.LoadPred.Expect.Loads[i]
			}
			return true
		}) {
			ret[id] = detail
			hotSchedulerResultCounter.WithLabelValues("src-store-succ", strconv.FormatUint(id, 10)).Inc()
		}
		hotSchedulerResultCounter.WithLabelValues("src-store-failed", strconv.FormatUint(id, 10)).Inc()
	}
	return ret
}

// filterHotPeers filtered hot peers from statistics.HotPeerStat and deleted the peer if its region is in pending status.
// The returned hotPeer count in controlled by `max-peer-number`.
func (bs *balanceSolver) filterHotPeers() []*statistics.HotPeerStat {
	ret := bs.stLoadDetail[bs.cur.srcStoreID].HotPeers
	// Return at most MaxPeerNum peers, to prevent balanceSolver.solve() too slow.
	maxPeerNum := bs.sche.conf.GetMaxPeerNumber()

	// filter pending region
	appendItem := func(items []*statistics.HotPeerStat, item *statistics.HotPeerStat) []*statistics.HotPeerStat {
		minHotDegree := bs.cluster.GetOpts().GetHotRegionCacheHitsThreshold()
		if _, ok := bs.sche.regionPendings[item.ID()]; !ok && !item.IsNeedCoolDownTransferLeader(minHotDegree) {
			// no in pending operator and no need cool down after transfer leader
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

	union := bs.sortHotPeers(ret, maxPeerNum)
	ret = make([]*statistics.HotPeerStat, 0, len(union))
	for peer := range union {
		ret = appendItem(ret, peer)
	}
	return ret
}

func (bs *balanceSolver) sortHotPeers(ret []*statistics.HotPeerStat, maxPeerNum int) map[*statistics.HotPeerStat]struct{} {
	byteSort := make([]*statistics.HotPeerStat, len(ret))
	copy(byteSort, ret)
	sort.Slice(byteSort, func(i, j int) bool {
		k := getRegionStatKind(bs.rwTy, statistics.ByteDim)
		return byteSort[i].GetLoad(k) > byteSort[j].GetLoad(k)
	})
	keySort := make([]*statistics.HotPeerStat, len(ret))
	copy(keySort, ret)
	sort.Slice(keySort, func(i, j int) bool {
		k := getRegionStatKind(bs.rwTy, statistics.KeyDim)
		return keySort[i].GetLoad(k) > keySort[j].GetLoad(k)
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
		for len(union) < maxPeerNum && len(keySort) > 0 {
			peer := keySort[0]
			keySort = keySort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
	}
	return union
}

// isRegionAvailable checks whether the given region is not available to schedule.
func (bs *balanceSolver) isRegionAvailable(region *core.RegionInfo) bool {
	if region == nil {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "no-region").Inc()
		return false
	}

	if op, ok := bs.sche.regionPendings[region.GetID()]; ok {
		if bs.opTy == transferLeader {
			return false
		}
		if op.Kind()&operator.OpRegion != 0 ||
			(op.Kind()&operator.OpLeader != 0 && !op.IsEnd()) {
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

// filterDstStores select the candidate store by filters
func (bs *balanceSolver) filterDstStores() map[uint64]*storeLoadDetail {
	var (
		filters    []filter.Filter
		candidates []*storeLoadDetail
	)
	srcStore := bs.stLoadDetail[bs.cur.srcStoreID].Store
	switch bs.opTy {
	case movePeer:
		filters = []filter.Filter{
			&filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.region.GetStoreIds(), bs.cur.region.GetStoreIds()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
			filter.NewPlacementSafeguard(bs.sche.GetName(), bs.cluster, bs.cur.region, srcStore),
		}

		for _, detail := range bs.stLoadDetail {
			candidates = append(candidates, detail)
		}

	case transferLeader:
		filters = []filter.Filter{
			&filter.StoreStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true},
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
		}
		if leaderFilter := filter.NewPlacementLeaderSafeguard(bs.sche.GetName(), bs.cluster, bs.cur.region, srcStore); leaderFilter != nil {
			filters = append(filters, leaderFilter)
		}

		for _, peer := range bs.cur.region.GetFollowers() {
			if detail, ok := bs.stLoadDetail[peer.GetStoreId()]; ok {
				candidates = append(candidates, detail)
			}
		}

	default:
		return nil
	}
	return bs.pickDstStores(filters, candidates)
}

func (bs *balanceSolver) pickDstStores(filters []filter.Filter, candidates []*storeLoadDetail) map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail, len(candidates))
	dstToleranceRatio := bs.sche.conf.GetDstToleranceRatio()
	for _, detail := range candidates {
		store := detail.Store
		if filter.Target(bs.cluster.GetOpts(), store, filters) {
			maxLoads := detail.LoadPred.max().Loads
			if slice.AllOf(maxLoads, func(i int) bool {
				if statistics.IsSelectedDim(i) {
					return maxLoads[i]*dstToleranceRatio < detail.LoadPred.Expect.Loads[i]
				}
				return true
			}) {
				ret[store.GetID()] = detail
				hotSchedulerResultCounter.WithLabelValues("dst-store-succ", strconv.FormatUint(store.GetID(), 10)).Inc()
			}
			hotSchedulerResultCounter.WithLabelValues("dst-store-fail", strconv.FormatUint(store.GetID(), 10)).Inc()
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
		// Only consider about key rate.
		srcKeyRate := srcLd.Loads[statistics.KeyDim]
		dstKeyRate := dstLd.Loads[statistics.KeyDim]
		peerKeyRate := peer.GetLoad(getRegionStatKind(bs.rwTy, statistics.KeyDim))
		if srcKeyRate-peerKeyRate >= dstKeyRate+peerKeyRate {
			rank = -1
		}
	} else {
		firstPriorityDimHot, firstPriorityDecRatio, secondPriorityDimHot, secondPriorityDecRatio := bs.getHotDecRatioByPriorities()
		greatDecRatio, minorDecRatio := bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorGreatDecRatio()
		switch {
		case firstPriorityDimHot && firstPriorityDecRatio <= greatDecRatio && secondPriorityDimHot && secondPriorityDecRatio <= greatDecRatio:
			// If belong to the case, both byte rate and key rate will be more balanced, the best choice.
			rank = -3
			bs.byteIsBetter = true
			bs.keyIsBetter = true
		case firstPriorityDecRatio <= minorDecRatio && secondPriorityDimHot && secondPriorityDecRatio <= greatDecRatio:
			// If belong to the case, byte rate will be not worsened, key rate will be more balanced.
			rank = -2
			bs.keyIsBetter = true
		case firstPriorityDimHot && firstPriorityDecRatio <= greatDecRatio:
			// If belong to the case, byte rate will be more balanced, ignore the key rate.
			rank = -1
			bs.byteIsBetter = true
		}
	}
	log.Debug("calcProgressiveRank",
		zap.Uint64("region-id", bs.cur.region.GetID()),
		zap.Uint64("from-store-id", bs.cur.srcStoreID),
		zap.Uint64("to-store-id", bs.cur.dstStoreID),
		zap.Int64("rank", rank))
	bs.cur.progressiveRank = rank
}

func (bs *balanceSolver) getHotDecRatioByPriorities() (bool, float64, bool, float64) {
	// we use DecRatio(Decline Ratio) to expect that the dst store's (key/byte) rate should still be less
	// than the src store's (key/byte) rate after scheduling one peer.
	getSrcDecRate := func(a, b float64) float64 {
		if a-b <= 0 {
			return 1
		}
		return a - b
	}
	srcLd := bs.stLoadDetail[bs.cur.srcStoreID].LoadPred.min()
	dstLd := bs.stLoadDetail[bs.cur.dstStoreID].LoadPred.max()
	peer := bs.cur.srcPeerStat
	checkHot := func(dim int) (bool, float64) {
		srcRate := srcLd.Loads[dim]
		dstRate := dstLd.Loads[dim]
		peerRate := peer.GetLoad(getRegionStatKind(bs.rwTy, dim))
		decRatio := (dstRate + peerRate) / getSrcDecRate(srcRate, peerRate)
		isHot := peerRate >= bs.getMinRate(dim)
		return isHot, decRatio
	}
	keyHot, keyDecRatio := checkHot(statistics.KeyDim)
	byteHot, byteDecRatio := checkHot(statistics.ByteDim)
	switch bs.preferPriority() {
	case BytePriority:
		return byteHot, byteDecRatio, keyHot, keyDecRatio
	case KeyPriority:
		return keyHot, keyDecRatio, byteHot, byteDecRatio
	}
	return byteHot, byteDecRatio, keyHot, keyDecRatio
}

func (bs *balanceSolver) getMinRate(dim int) float64 {
	switch dim {
	case statistics.KeyDim:
		return bs.sche.conf.GetMinHotKeyRate()
	case statistics.ByteDim:
		return bs.sche.conf.GetMinHotByteRate()
	}
	return -1
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
			case bs.cur.srcPeerStat.GetLoad(statistics.RegionWriteKeys) > old.srcPeerStat.GetLoad(statistics.RegionWriteKeys):
				return true
			case bs.cur.srcPeerStat.GetLoad(statistics.RegionWriteKeys) < old.srcPeerStat.GetLoad(statistics.RegionWriteKeys):
				return false
			}
		} else {
			firstCmp, secondCmp := bs.getRkCmpPriorities(old)
			switch bs.cur.progressiveRank {
			case -2: // greatDecRatio < byteDecRatio <= minorDecRatio && keyDecRatio <= greatDecRatio
				if secondCmp != 0 {
					return secondCmp > 0
				}
				if firstCmp != 0 {
					// prefer smaller byte rate, to reduce oscillation
					return firstCmp < 0
				}
			case -3: // byteDecRatio <= greatDecRatio && keyDecRatio <= greatDecRatio
				if secondCmp != 0 {
					return secondCmp > 0
				}
				fallthrough
			case -1: // byteDecRatio <= greatDecRatio
				if firstCmp != 0 {
					// prefer region with larger byte rate, to converge faster
					return firstCmp > 0
				}
			}
		}
	}

	return false
}

func (bs *balanceSolver) getRkCmpPriorities(old *solution) (int, int) {
	bk, kk := getRegionStatKind(bs.rwTy, statistics.ByteDim), getRegionStatKind(bs.rwTy, statistics.KeyDim)
	byteRkCmp := rankCmp(bs.cur.srcPeerStat.GetLoad(bk), old.srcPeerStat.GetLoad(bk), stepRank(0, 100))
	keyRkCmp := rankCmp(bs.cur.srcPeerStat.GetLoad(kk), old.srcPeerStat.GetLoad(kk), stepRank(0, 10))
	switch bs.preferPriority() {
	case BytePriority:
		return byteRkCmp, keyRkCmp
	case KeyPriority:
		return keyRkCmp, byteRkCmp
	}
	return byteRkCmp, keyRkCmp
}

// smaller is better
func (bs *balanceSolver) compareSrcStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare source store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.Loads[statistics.KeyDim], bs.rankStep.Loads[statistics.KeyDim])),
					stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.Loads[statistics.ByteDim], bs.rankStep.Loads[statistics.ByteDim])),
				))),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.Loads[statistics.KeyDim])),
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.Loads[statistics.ByteDim])),
				)),
			)
		} else {
			firstLdRankCmp, secondLdRankCmp, firstLdRankCmpZeroStep := bs.getLdRankPriorities()
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					firstLdRankCmp,
					secondLdRankCmp,
				))),
				diffCmp(firstLdRankCmpZeroStep),
			)
		}
		lp1 := bs.stLoadDetail[st1].LoadPred
		lp2 := bs.stLoadDetail[st2].LoadPred
		return lpCmp(lp1, lp2)
	}
	return 0
}

func (bs *balanceSolver) getLdRankPriorities() (storeLoadCmp, storeLoadCmp, storeLoadCmp) {
	switch bs.preferPriority() {
	case BytePriority:
		return stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.Loads[statistics.ByteDim], bs.rankStep.Loads[statistics.ByteDim])),
			stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.Loads[statistics.KeyDim], bs.rankStep.Loads[statistics.KeyDim])),
			stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.Loads[statistics.ByteDim]))
	case KeyPriority:
		return stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.Loads[statistics.KeyDim], bs.rankStep.Loads[statistics.KeyDim])),
			stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.Loads[statistics.ByteDim], bs.rankStep.Loads[statistics.ByteDim])),
			stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.Loads[statistics.KeyDim]))
	}
	return stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.Loads[statistics.ByteDim], bs.rankStep.Loads[statistics.ByteDim])),
		stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.Loads[statistics.KeyDim], bs.rankStep.Loads[statistics.KeyDim])),
		stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.Loads[statistics.ByteDim]))
}

// smaller is better
func (bs *balanceSolver) compareDstStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare destination store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.Loads[statistics.KeyDim], bs.rankStep.Loads[statistics.KeyDim])),
					stLdRankCmp(stLdByteRate, stepRank(bs.minDst.Loads[statistics.ByteDim], bs.rankStep.Loads[statistics.ByteDim])),
				)),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRank(0, bs.rankStep.Loads[statistics.KeyDim])),
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.Loads[statistics.ByteDim])),
				)))
		} else {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRank(bs.minDst.Loads[statistics.ByteDim], bs.rankStep.Loads[statistics.ByteDim])),
					stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.Loads[statistics.KeyDim], bs.rankStep.Loads[statistics.KeyDim])),
				)),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.Loads[statistics.ByteDim])),
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

func (bs *balanceSolver) buildOperator() (op *operator.Operator, infl *Influence) {
	if !bs.isReadyToBuild() {
		return nil, nil
	}
	var (
		err         error
		typ         string
		sourceLabel string
		targetLabel string
	)

	switch bs.opTy {
	case movePeer:
		srcPeer := bs.cur.region.GetStorePeer(bs.cur.srcStoreID) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: bs.cur.dstStoreID, Role: srcPeer.Role}
		sourceLabel = strconv.FormatUint(bs.cur.srcStoreID, 10)
		targetLabel = strconv.FormatUint(dstPeer.GetStoreId(), 10)

		if bs.rwTy == read && bs.cur.region.GetLeader().StoreId == bs.cur.srcStoreID { // move read leader
			op, err = operator.CreateMoveLeaderOperator(
				"move-hot-read-leader",
				bs.cluster,
				bs.cur.region,
				operator.OpHotRegion,
				bs.cur.srcStoreID,
				dstPeer)
			typ = "move-leader"
		} else {
			desc := "move-hot-" + bs.rwTy.String() + "-peer"
			typ = "move-peer"
			op, err = operator.CreateMovePeerOperator(
				desc,
				bs.cluster,
				bs.cur.region,
				operator.OpHotRegion,
				bs.cur.srcStoreID,
				dstPeer)
		}
	case transferLeader:
		if bs.cur.region.GetStoreVoter(bs.cur.dstStoreID) == nil {
			return nil, nil
		}
		desc := "transfer-hot-" + bs.rwTy.String() + "-leader"
		sourceLabel = strconv.FormatUint(bs.cur.srcStoreID, 10)
		targetLabel = strconv.FormatUint(bs.cur.dstStoreID, 10)
		op, err = operator.CreateTransferLeaderOperator(
			desc,
			bs.cluster,
			bs.cur.region,
			bs.cur.srcStoreID,
			bs.cur.dstStoreID,
			operator.OpHotRegion)
	}

	if err != nil {
		log.Debug("fail to create operator", zap.Stringer("rw-type", bs.rwTy), zap.Stringer("op-type", bs.opTy), errs.ZapError(err))
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "create-operator-fail").Inc()
		return nil, nil
	}

	dim := ""
	if bs.byteIsBetter && bs.keyIsBetter {
		dim = "both"
	} else if bs.byteIsBetter {
		dim = "byte"
	} else if bs.keyIsBetter {
		dim = "key"
	}

	op.SetPriorityLevel(core.HighPriority)
	op.FinishedCounters = append(op.FinishedCounters,
		hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), sourceLabel, "out", dim),
		hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), targetLabel, "in", dim),
		balanceDirectionCounter.WithLabelValues(bs.sche.GetName(), sourceLabel, targetLabel))
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(bs.sche.GetName(), bs.opTy.String()))

	infl = &Influence{
		Loads: append(bs.cur.srcPeerStat.Loads[:0:0], bs.cur.srcPeerStat.Loads...),
		Count: 1,
	}
	return op, infl
}

func (h *hotScheduler) GetHotStatus(typ string) *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	var leaderTyp, peerTyp resourceType
	switch typ {
	case HotReadRegionType:
		leaderTyp, peerTyp = readLeader, readPeer
	case HotWriteRegionType:
		leaderTyp, peerTyp = writeLeader, writePeer
	}
	asLeader := make(statistics.StoreHotPeersStat, len(h.stLoadInfos[leaderTyp]))
	asPeer := make(statistics.StoreHotPeersStat, len(h.stLoadInfos[peerTyp]))
	for id, detail := range h.stLoadInfos[leaderTyp] {
		asLeader[id] = detail.toHotPeersStat()
	}
	for id, detail := range h.stLoadInfos[peerTyp] {
		asPeer[id] = detail.toHotPeersStat()
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

func (h *hotScheduler) GetPendingInfluence() map[uint64]*Influence {
	h.RLock()
	defer h.RUnlock()
	ret := make(map[uint64]*Influence, len(h.pendingSums))
	for id, infl := range h.pendingSums {
		ret[id] = infl.add(infl, 0) // copy
	}
	return ret
}

// calcPendingWeight return the calculate weight of one Operator, the value will between [0,1]
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
	h.pendings = map[*pendingInfluence]struct{}{}
	h.pendingSums = nil
	h.regionPendings = make(map[uint64]*operator.Operator)
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
	readPeer
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
		switch opTy {
		case movePeer:
			return readPeer
		case transferLeader:
			return readLeader
		}
	}
	panic(fmt.Sprintf("invalid arguments for toResourceType: rwTy = %v, opTy = %v", rwTy, opTy))
}

func getRegionStatKind(rwTy rwType, dim int) statistics.RegionStatKind {
	switch {
	case rwTy == read && dim == statistics.ByteDim:
		return statistics.RegionReadBytes
	case rwTy == read && dim == statistics.KeyDim:
		return statistics.RegionReadKeys
	case rwTy == write && dim == statistics.ByteDim:
		return statistics.RegionWriteBytes
	case rwTy == write && dim == statistics.KeyDim:
		return statistics.RegionWriteKeys
	case rwTy == write && dim == statistics.QueryDim:
		return statistics.RegionWriteQuery
	case rwTy == read && dim == statistics.QueryDim:
		return statistics.RegionReadQuery
	}
	return 0
}

func (bs *balanceSolver) preferPriority() string {
	priorities := bs.sche.conf.WritePriorities
	if bs.rwTy == read {
		priorities = bs.sche.conf.ReadPriorities
	}
	return priorities[0]
}
