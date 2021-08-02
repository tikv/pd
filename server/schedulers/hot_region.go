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
	"github.com/tikv/pd/server/versioninfo"
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

// tikv below 5.2.0 does not report query information, we will use byte and key as the scheduling dimensions
var prioritiesWithoutQuery = []string{"byte", "key"}
var writeLeaderPrioritiesWithoutQuery = []string{"key", "byte"}

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
// it will filter the hot peer and calculate the current and future stat(rate,count) for each store
func summaryStoresLoad(
	stores []*core.StoreInfo,
	storesLoads map[uint64][]float64,
	storePendings map[uint64]*Influence,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	rwTy rwType,
	kind core.ResourceKind,
) map[uint64]*storeLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(rate,count)
	loadDetail := make(map[uint64]*storeLoadDetail, len(storesLoads))
	allLoadSum := make([]float64, statistics.DimLen)
	allCount := 0.0

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
	// store expectation rate and count for each store-load detail.
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

	firstPriority             int
	secondPriority            int
	writeLeaderFirstPriority  int
	writeLeaderSecondPriority int
	isSelectedDim             func(int) bool

	firstPriorityIsBetter  bool
	secondPriorityIsBetter bool
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

	rankStepRatios := []float64{
		statistics.ByteDim:  bs.sche.conf.GetByteRankStepRatio(),
		statistics.KeyDim:   bs.sche.conf.GetKeyRankStepRatio(),
		statistics.QueryDim: bs.sche.conf.GetQueryRateRankStepRatio()}
	stepLoads := make([]float64, statistics.DimLen)
	for i := range stepLoads {
		stepLoads[i] = maxCur.Loads[i] * rankStepRatios[i]
	}
	bs.rankStep = &storeLoad{
		Loads: stepLoads,
		Count: maxCur.Count * bs.sche.conf.GetCountRankStepRatio(),
	}

	// For read, transfer-leader and move-peer have the same priority config
	// For write, they are different
	if bs.rwTy == read {
		bs.firstPriority, bs.secondPriority = bs.adjustConfig(bs.sche.conf.ReadPriorities, prioritiesWithoutQuery)
	} else {
		bs.firstPriority, bs.secondPriority = bs.adjustConfig(bs.sche.conf.WritePeerPriorities, prioritiesWithoutQuery)
		bs.writeLeaderFirstPriority, bs.writeLeaderSecondPriority = bs.adjustConfig(bs.sche.conf.WriteLeaderPriorities, writeLeaderPrioritiesWithoutQuery)
	}

	bs.isSelectedDim = func(dim int) bool {
		return dim == bs.firstPriority || dim == bs.secondPriority
	}
}

func (bs *balanceSolver) adjustConfig(origins, defaults []string) (first, second int) {
	querySupport := bs.cluster.IsFeatureSupported(versioninfo.HotScheduleWithQuery)
	priorities := origins
	if !querySupport && slice.AnyOf(origins, func(i int) bool {
		return origins[i] == QueryPriority
	}) {
		priorities = defaults
	}
	return prioritiesTodim(priorities)
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

// filterSrcStores compare the min rate and the ratio * expectation rate, if two dim rate is greater than
// its expectation * ratio, the store would be selected as hot source store
func (bs *balanceSolver) filterSrcStores() map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail)
	for id, detail := range bs.stLoadDetail {
		if len(detail.HotPeers) == 0 {
			continue
		}
		minLoad := detail.LoadPred.min()
		if bs.checkSrcByDimPriorityAndTolerance(minLoad, &detail.LoadPred.Expect) {
			ret[id] = detail
			hotSchedulerResultCounter.WithLabelValues("src-store-succ", strconv.FormatUint(id, 10)).Inc()
		} else {
			hotSchedulerResultCounter.WithLabelValues("src-store-failed", strconv.FormatUint(id, 10)).Inc()
		}
	}
	return ret
}

func (bs *balanceSolver) checkSrcByDimPriorityAndTolerance(minLoad, expectLoad *storeLoad) bool {
	if bs.sche.conf.StrictPickingStore {
		return slice.AllOf(minLoad.Loads, func(i int) bool {
			if bs.isSelectedDim(i) {
				return minLoad.Loads[i] > bs.sche.conf.GetSrcToleranceRatio()*expectLoad.Loads[i]
			}
			return true
		})
	}
	return minLoad.Loads[bs.firstPriority] > bs.sche.conf.GetSrcToleranceRatio()*expectLoad.Loads[bs.firstPriority]
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
	firstSort := make([]*statistics.HotPeerStat, len(ret))
	copy(firstSort, ret)
	sort.Slice(firstSort, func(i, j int) bool {
		k := getRegionStatKind(bs.rwTy, bs.firstPriority)
		return firstSort[i].GetLoad(k) > firstSort[j].GetLoad(k)
	})
	secondSort := make([]*statistics.HotPeerStat, len(ret))
	copy(secondSort, ret)
	sort.Slice(secondSort, func(i, j int) bool {
		k := getRegionStatKind(bs.rwTy, bs.secondPriority)
		return secondSort[i].GetLoad(k) > secondSort[j].GetLoad(k)
	})
	union := make(map[*statistics.HotPeerStat]struct{}, maxPeerNum)
	for len(union) < maxPeerNum {
		for len(firstSort) > 0 {
			peer := firstSort[0]
			firstSort = firstSort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
		for len(union) < maxPeerNum && len(secondSort) > 0 {
			peer := secondSort[0]
			secondSort = secondSort[1:]
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
	for _, detail := range candidates {
		id := detail.Store.GetID()
		maxLoad := detail.LoadPred.max()
		if filter.Target(bs.cluster.GetOpts(), detail.Store, filters) {
			if bs.checkDstByPriorityAndTolerance(maxLoad, &detail.LoadPred.Expect) {
				ret[id] = detail
				hotSchedulerResultCounter.WithLabelValues("src-store-succ", strconv.FormatUint(id, 10)).Inc()
			} else {
				hotSchedulerResultCounter.WithLabelValues("src-store-failed", strconv.FormatUint(id, 10)).Inc()
			}
		}
	}
	return ret
}

func (bs *balanceSolver) checkDstByPriorityAndTolerance(maxLoad, expect *storeLoad) bool {
	dstToleranceRatio := bs.sche.conf.GetDstToleranceRatio()
	if bs.sche.conf.StrictPickingStore {
		return slice.AllOf(maxLoad.Loads, func(i int) bool {
			if bs.isSelectedDim(i) {
				return maxLoad.Loads[i]*dstToleranceRatio < expect.Loads[i]
			}
			return true
		})
	}
	return maxLoad.Loads[bs.firstPriority]*dstToleranceRatio < expect.Loads[bs.firstPriority]
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
		// Only consider about key rate or query rate.
		srcRate := srcLd.Loads[bs.writeLeaderFirstPriority]
		dstRate := dstLd.Loads[bs.writeLeaderFirstPriority]
		peerRate := peer.GetLoad(getRegionStatKind(bs.rwTy, bs.writeLeaderFirstPriority))
		if srcRate-peerRate >= dstRate+peerRate {
			rank = -1
		}
	} else {
		firstPriorityDimHot, firstPriorityDecRatio, secondPriorityDimHot, secondPriorityDecRatio := bs.getHotDecRatioByPriorities(srcLd, dstLd, peer)
		greatDecRatio, minorDecRatio := bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorGreatDecRatio()
		switch {
		case firstPriorityDimHot && firstPriorityDecRatio <= greatDecRatio && secondPriorityDimHot && secondPriorityDecRatio <= greatDecRatio:
			// If belong to the case, two dim will be more balanced, the best choice.
			rank = -3
			bs.firstPriorityIsBetter = true
			bs.secondPriorityIsBetter = true
		case firstPriorityDecRatio <= minorDecRatio && secondPriorityDimHot && secondPriorityDecRatio <= greatDecRatio:
			// If belong to the case, first priority dim will be not worsened, second priority dim will be more balanced.
			rank = -2
			bs.secondPriorityIsBetter = true
		case firstPriorityDimHot && firstPriorityDecRatio <= greatDecRatio:
			// If belong to the case, first priority dim will be more balanced, ignore the second priority dim.
			rank = -1
			bs.firstPriorityIsBetter = true
		}
	}
	bs.cur.progressiveRank = rank
}

func (bs *balanceSolver) getHotDecRatioByPriorities(srcLd, dstLd *storeLoad, peer *statistics.HotPeerStat) (bool, float64, bool, float64) {
	// we use DecRatio(Decline Ratio) to expect that the dst store's rate should still be less
	// than the src store's rate after scheduling one peer.
	getSrcDecRate := func(a, b float64) float64 {
		if a-b <= 0 {
			return 1
		}
		return a - b
	}
	checkHot := func(dim int) (bool, float64) {
		srcRate := srcLd.Loads[dim]
		dstRate := dstLd.Loads[dim]
		peerRate := peer.GetLoad(getRegionStatKind(bs.rwTy, dim))
		decRatio := (dstRate + peerRate) / getSrcDecRate(srcRate, peerRate)
		isHot := peerRate >= bs.getMinRate(dim)
		return isHot, decRatio
	}
	firstHot, firstDecRatio := checkHot(bs.firstPriority)
	secondHot, secondDecRatio := checkHot(bs.secondPriority)
	return firstHot, firstDecRatio, secondHot, secondDecRatio
}

func (bs *balanceSolver) getMinRate(dim int) float64 {
	switch dim {
	case statistics.KeyDim:
		return bs.sche.conf.GetMinHotKeyRate()
	case statistics.ByteDim:
		return bs.sche.conf.GetMinHotByteRate()
	case statistics.QueryDim:
		return bs.sche.conf.GetMinHotQueryRate()
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
			kind := getRegionStatKind(write, bs.writeLeaderFirstPriority)
			switch {
			case bs.cur.srcPeerStat.GetLoad(kind) > old.srcPeerStat.GetLoad(kind):
				return true
			case bs.cur.srcPeerStat.GetLoad(kind) < old.srcPeerStat.GetLoad(kind):
				return false
			}
		} else {
			firstCmp, secondCmp := bs.getRkCmpPriorities(old)
			switch bs.cur.progressiveRank {
			case -2: // greatDecRatio < firstPriorityDecRatio <= minorDecRatio && secondPriorityDecRatio <= greatDecRatio
				if secondCmp != 0 {
					return secondCmp > 0
				}
				if firstCmp != 0 {
					// prefer smaller first priority rate, to reduce oscillation
					return firstCmp < 0
				}
			case -3: // firstPriorityDecRatio <= greatDecRatio && secondPriorityDecRatio <= greatDecRatio
				if secondCmp != 0 {
					return secondCmp > 0
				}
				fallthrough
			case -1: // firstPriorityDecRatio <= greatDecRatio
				if firstCmp != 0 {
					// prefer region with larger first priority rate, to converge faster
					return firstCmp > 0
				}
			}
		}
	}

	return false
}

func (bs *balanceSolver) getRkCmpPriorities(old *solution) (firstCmp int, secondCmp int) {
	fk, sk := getRegionStatKind(bs.rwTy, bs.firstPriority), getRegionStatKind(bs.rwTy, bs.secondPriority)
	dimToStep := func(priority int) float64 {
		switch priority {
		case statistics.ByteDim:
			return 100
		case statistics.KeyDim:
			return 10
		case statistics.QueryDim:
			return 10
		}
		return 100
	}
	fkRkCmp := rankCmp(bs.cur.srcPeerStat.GetLoad(fk), old.srcPeerStat.GetLoad(fk), stepRank(0, dimToStep(bs.firstPriority)))
	skRkCmp := rankCmp(bs.cur.srcPeerStat.GetLoad(sk), old.srcPeerStat.GetLoad(sk), stepRank(0, dimToStep(bs.secondPriority)))
	return fkRkCmp, skRkCmp
}

// smaller is better
func (bs *balanceSolver) compareSrcStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare source store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdRate(bs.writeLeaderFirstPriority), stepRank(bs.maxSrc.Loads[bs.writeLeaderFirstPriority], bs.rankStep.Loads[bs.writeLeaderFirstPriority])),
					stLdRankCmp(stLdRate(bs.writeLeaderSecondPriority), stepRank(bs.maxSrc.Loads[bs.writeLeaderSecondPriority], bs.rankStep.Loads[bs.writeLeaderSecondPriority])),
				))),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdRate(bs.writeLeaderFirstPriority), stepRank(0, bs.rankStep.Loads[bs.writeLeaderFirstPriority])),
					stLdRankCmp(stLdRate(bs.writeLeaderSecondPriority), stepRank(0, bs.rankStep.Loads[bs.writeLeaderSecondPriority])),
				)),
			)
		} else {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(bs.maxSrc.Loads[bs.firstPriority], bs.rankStep.Loads[bs.firstPriority])),
					stLdRankCmp(stLdRate(bs.secondPriority), stepRank(bs.maxSrc.Loads[bs.secondPriority], bs.rankStep.Loads[bs.secondPriority])),
				))),
				diffCmp(
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(0, bs.rankStep.Loads[bs.firstPriority])),
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
					stLdRankCmp(stLdRate(bs.writeLeaderFirstPriority), stepRank(bs.minDst.Loads[bs.writeLeaderFirstPriority], bs.rankStep.Loads[bs.writeLeaderFirstPriority])),
					stLdRankCmp(stLdRate(bs.writeLeaderSecondPriority), stepRank(bs.minDst.Loads[bs.writeLeaderSecondPriority], bs.rankStep.Loads[bs.writeLeaderSecondPriority])),
				)),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdRate(bs.writeLeaderFirstPriority), stepRank(0, bs.rankStep.Loads[bs.writeLeaderFirstPriority])),
					stLdRankCmp(stLdRate(bs.writeLeaderSecondPriority), stepRank(0, bs.rankStep.Loads[bs.writeLeaderSecondPriority])),
				)))
		} else {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(bs.minDst.Loads[bs.firstPriority], bs.rankStep.Loads[bs.firstPriority])),
					stLdRankCmp(stLdRate(bs.secondPriority), stepRank(bs.minDst.Loads[bs.secondPriority], bs.rankStep.Loads[bs.secondPriority])),
				)),
				diffCmp(
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(0, bs.rankStep.Loads[bs.firstPriority])),
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
		typ = "transfer-leader"
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
	if bs.firstPriorityIsBetter && bs.secondPriorityIsBetter {
		dim = "all"
	} else if bs.firstPriorityIsBetter {
		dim = dimToString(bs.firstPriority)
	} else if bs.secondPriorityIsBetter {
		dim = dimToString(bs.secondPriority)
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

func stringToDim(name string) int {
	switch name {
	case BytePriority:
		return statistics.ByteDim
	case KeyPriority:
		return statistics.KeyDim
	case QueryPriority:
		return statistics.QueryDim
	}
	return statistics.ByteDim
}

func dimToString(dim int) string {
	switch dim {
	case statistics.ByteDim:
		return BytePriority
	case statistics.KeyDim:
		return KeyPriority
	case statistics.QueryDim:
		return QueryPriority
	default:
		return ""
	}
}

func prioritiesTodim(priorities []string) (firstPriority int, secondPriority int) {
	return stringToDim(priorities[0]), stringToDim(priorities[1])
}
