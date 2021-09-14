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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

		var data map[string]interface{}
		if err := decoder(&data); err != nil {
			return nil, err
		}
		if len(data) != 0 {
			// After upgrading, use compatible config.
			// For clusters with the initial version >= v5.2, it will be overwritten by the default config.
			conf.apply(compatibleConfig)
			if err := decoder(conf); err != nil {
				return nil, err
			}
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

var (
	// schedulePeerPr the probability of schedule the hot peer.
	schedulePeerPr = 0.66
	// pendingAmpFactor will amplify the impact of pending influence, making scheduling slower or even serial when two stores are close together
	pendingAmpFactor = 2.0
)

type hotScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	types []rwType
	r     *rand.Rand

	// regionPendings stores regionID -> pendingInfluence
	// this records regionID which have pending Operator by operation type. During filterHotPeers, the hot peers won't
	// be selected if its owner region is tracked in this attribute.
	regionPendings map[uint64]*pendingInfluence

	// store information, including pending Influence by resource type
	// Every time `Schedule()` will recalculate it.
	stInfos map[uint64]*storeSummaryInfo
	// temporary states but exported to API or metrics
	// Every time `Schedule()` will recalculate it.
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail

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
		regionPendings: make(map[uint64]*pendingInfluence),
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

	h.prepareForBalance(typ, cluster)

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
func (h *hotScheduler) prepareForBalance(typ rwType, cluster opt.Cluster) {
	h.stInfos = summaryStoreInfos(cluster)
	h.summaryPendingInfluence()
	storesLoads := cluster.GetStoresLoads()
	isTraceRegionFlow := cluster.GetOpts().IsTraceRegionFlow()

	switch typ {
	case read:
		// update read statistics
		regionRead := cluster.RegionReadStats()
		h.stLoadInfos[readLeader] = summaryStoresLoad(
			h.stInfos,
			storesLoads,
			regionRead,
			isTraceRegionFlow,
			read, core.LeaderKind)
		h.stLoadInfos[readPeer] = summaryStoresLoad(
			h.stInfos,
			storesLoads,
			regionRead,
			isTraceRegionFlow,
			read, core.RegionKind)
	case write:
		// update write statistics
		regionWrite := cluster.RegionWriteStats()
		h.stLoadInfos[writeLeader] = summaryStoresLoad(
			h.stInfos,
			storesLoads,
			regionWrite,
			isTraceRegionFlow,
			write, core.LeaderKind)
		h.stLoadInfos[writePeer] = summaryStoresLoad(
			h.stInfos,
			storesLoads,
			regionWrite,
			isTraceRegionFlow,
			write, core.RegionKind)
	}
}

// summaryPendingInfluence calculate the summary of pending Influence for each store
// and clean the region from regionInfluence if they have ended operator.
// It makes each dim rate or count become `weight` times to the origin value.
func (h *hotScheduler) summaryPendingInfluence() {
	for id, p := range h.regionPendings {
		from := h.stInfos[p.from]
		to := h.stInfos[p.to]
		maxZombieDur := p.maxZombieDuration
		weight, needGC := h.calcPendingInfluence(p.op, maxZombieDur)

		if needGC {
			delete(h.regionPendings, id)
			schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Dec()
			log.Debug("gc pending influence in hot region scheduler",
				zap.Uint64("region-id", id),
				zap.Time("create", p.op.GetCreateTime()),
				zap.Time("now", time.Now()),
				zap.Duration("zombie", maxZombieDur))
			continue
		}

		if from != nil && weight > 0 {
			from.addInfluence(&p.origin, -weight)
		}
		if to != nil && weight > 0 {
			to.addInfluence(&p.origin, weight)
		}
	}
}

func (h *hotScheduler) tryAddPendingInfluence(op *operator.Operator, srcStore, dstStore uint64, infl Influence, maxZombieDur time.Duration) bool {
	regionID := op.RegionID()
	_, ok := h.regionPendings[regionID]
	if ok {
		schedulerStatus.WithLabelValues(h.GetName(), "pending_op_fails").Inc()
		return false
	}

	influence := newPendingInfluence(op, srcStore, dstStore, infl, maxZombieDur)
	h.regionPendings[regionID] = influence

	schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Inc()
	return true
}

func (h *hotScheduler) balanceHotReadRegions(cluster opt.Cluster) []*operator.Operator {
	leaderSolver := newBalanceSolver(h, cluster, read, transferLeader)
	leaderOps := leaderSolver.solve()
	peerSolver := newBalanceSolver(h, cluster, read, movePeer)
	peerOps := peerSolver.solve()
	if len(leaderOps) == 0 && len(peerOps) == 0 {
		schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
		return nil
	}
	if len(leaderOps) == 0 {
		if peerSolver.tryAddPendingInfluence() {
			return peerOps
		}
		schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
		return nil
	}
	if len(peerOps) == 0 {
		if leaderSolver.tryAddPendingInfluence() {
			return leaderOps
		}
		schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
		return nil
	}
	leaderSolver.cur = leaderSolver.best
	if leaderSolver.betterThan(peerSolver.best) {
		if leaderSolver.tryAddPendingInfluence() {
			return leaderOps
		}
		if peerSolver.tryAddPendingInfluence() {
			return peerOps
		}
	} else {
		if peerSolver.tryAddPendingInfluence() {
			return peerOps
		}
		if leaderSolver.tryAddPendingInfluence() {
			return leaderOps
		}
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
		if len(ops) > 0 && peerSolver.tryAddPendingInfluence() {
			return ops
		}
	default:
	}

	leaderSolver := newBalanceSolver(h, cluster, write, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 && leaderSolver.tryAddPendingInfluence() {
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

	cur  *solution
	best *solution
	ops  []*operator.Operator
	infl Influence

	maxSrc   *storeLoad
	minDst   *storeLoad
	rankStep *storeLoad

	// firstPriority and secondPriority indicate priority of hot schedule
	// they may be byte(0), key(1), query(2), and always less than dimLen
	firstPriority  int
	secondPriority int

	firstPriorityIsBetter  bool
	secondPriorityIsBetter bool
}

type solution struct {
	srcDetail   *storeLoadDetail
	srcPeerStat *statistics.HotPeerStat
	region      *core.RegionInfo
	dstDetail   *storeLoadDetail

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

	bs.firstPriority, bs.secondPriority = prioritiesToDim(bs.getPriorities())
}

func (bs *balanceSolver) isSelectedDim(dim int) bool {
	return dim == bs.firstPriority || dim == bs.secondPriority
}

func (bs *balanceSolver) getPriorities() []string {
	querySupport := bs.sche.conf.checkQuerySupport(bs.cluster)
	// For read, transfer-leader and move-peer have the same priority config
	// For write, they are different
	switch bs.rwTy {
	case read:
		return adjustConfig(querySupport, bs.sche.conf.GetReadPriorities(), getReadPriorities)
	case write:
		switch bs.opTy {
		case transferLeader:
			return adjustConfig(querySupport, bs.sche.conf.GetWriteLeaderPriorities(), getWriteLeaderPriorities)
		case movePeer:
			return adjustConfig(querySupport, bs.sche.conf.GetWritePeerPriorities(), getWritePeerPriorities)
		}
	}
	log.Error("illegal type or illegal operator while getting the priority", zap.String("type", bs.rwTy.String()), zap.String("operator", bs.opTy.String()))
	return []string{}
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

	for _, srcDetail := range bs.filterSrcStores() {
		bs.cur.srcDetail = srcDetail

		for _, srcPeerStat := range bs.filterHotPeers() {
			bs.cur.srcPeerStat = srcPeerStat
			bs.cur.region = bs.getRegion()
			if bs.cur.region == nil {
				continue
			}
			for _, dstDetail := range bs.filterDstStores() {
				bs.cur.dstDetail = dstDetail
				bs.calcProgressiveRank()
				if bs.cur.progressiveRank < 0 && bs.betterThan(bs.best) {
					if newOp, newInfl := bs.buildOperator(); newOp != nil {
						bs.ops = []*operator.Operator{newOp}
						bs.infl = *newInfl
						clone := *bs.cur
						bs.best = &clone
					}
				}
			}
		}
	}
	return bs.ops
}

func (bs *balanceSolver) tryAddPendingInfluence() bool {
	if bs.best == nil || len(bs.ops) == 0 {
		return false
	}
	if bs.best.srcDetail.Info.IsTiFlash != bs.best.dstDetail.Info.IsTiFlash {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "not-same-engine").Inc()
		return false
	}
	// Depending on the source of the statistics used, a different ZombieDuration will be used.
	// If the statistics are from the sum of Regions, there will be a longer ZombieDuration.
	var maxZombieDur time.Duration
	switch {
	case bs.isForWriteLeader():
		maxZombieDur = bs.sche.conf.GetRegionsStatZombieDuration()
	case bs.isForWritePeer():
		if bs.best.srcDetail.Info.IsTiFlash {
			maxZombieDur = bs.sche.conf.GetRegionsStatZombieDuration()
		} else {
			maxZombieDur = bs.sche.conf.GetStoreStatZombieDuration()
		}
	default:
		maxZombieDur = bs.sche.conf.GetStoreStatZombieDuration()
	}
	return bs.sche.tryAddPendingInfluence(bs.ops[0], bs.best.srcDetail.getID(), bs.best.dstDetail.getID(), bs.infl, maxZombieDur)
}

func (bs *balanceSolver) isForWriteLeader() bool {
	return bs.rwTy == write && bs.opTy == transferLeader
}

func (bs *balanceSolver) isForWritePeer() bool {
	return bs.rwTy == write && bs.opTy == movePeer
}

// filterSrcStores compare the min rate and the ratio * expectation rate, if two dim rate is greater than
// its expectation * ratio, the store would be selected as hot source store
func (bs *balanceSolver) filterSrcStores() map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail)
	confSrcToleranceRatio := bs.sche.conf.GetSrcToleranceRatio()
	confEnableForTiFlash := bs.sche.conf.GetEnableForTiFlash()
	for id, detail := range bs.stLoadDetail {
		srcToleranceRatio := confSrcToleranceRatio
		if detail.Info.IsTiFlash {
			if !confEnableForTiFlash {
				continue
			}
			if bs.rwTy != write || bs.opTy != movePeer {
				continue
			}
			srcToleranceRatio += tiflashToleranceRatioCorrection
		}
		if len(detail.HotPeers) == 0 {
			continue
		}

		if bs.checkSrcByDimPriorityAndTolerance(detail.LoadPred.min(), &detail.LoadPred.Expect, srcToleranceRatio) {
			ret[id] = detail
			hotSchedulerResultCounter.WithLabelValues("src-store-succ", strconv.FormatUint(id, 10)).Inc()
		} else {
			hotSchedulerResultCounter.WithLabelValues("src-store-failed", strconv.FormatUint(id, 10)).Inc()
		}
	}
	return ret
}

func (bs *balanceSolver) checkSrcByDimPriorityAndTolerance(minLoad, expectLoad *storeLoad, toleranceRatio float64) bool {
	if bs.sche.conf.IsStrictPickingStoreEnabled() {
		return slice.AllOf(minLoad.Loads, func(i int) bool {
			if bs.isSelectedDim(i) {
				return minLoad.Loads[i] > toleranceRatio*expectLoad.Loads[i]
			}
			return true
		})
	}
	return minLoad.Loads[bs.firstPriority] > toleranceRatio*expectLoad.Loads[bs.firstPriority]
}

// filterHotPeers filtered hot peers from statistics.HotPeerStat and deleted the peer if its region is in pending status.
// The returned hotPeer count in controlled by `max-peer-number`.
func (bs *balanceSolver) filterHotPeers() []*statistics.HotPeerStat {
	ret := bs.cur.srcDetail.HotPeers
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

	if influence, ok := bs.sche.regionPendings[region.GetID()]; ok {
		if bs.opTy == transferLeader {
			return false
		}
		op := influence.op
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
		srcPeer := region.GetStorePeer(bs.cur.srcDetail.getID())
		if srcPeer == nil {
			log.Debug("region does not have a peer on source store, maybe stat out of date", zap.Uint64("region-id", bs.cur.srcPeerStat.ID()))
			return nil
		}
	case transferLeader:
		if region.GetLeader().GetStoreId() != bs.cur.srcDetail.getID() {
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
	srcStore := bs.cur.srcDetail.Info.Store
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
	confDstToleranceRatio := bs.sche.conf.GetDstToleranceRatio()
	confEnableForTiFlash := bs.sche.conf.GetEnableForTiFlash()
	for _, detail := range candidates {
		store := detail.Info.Store
		dstToleranceRatio := confDstToleranceRatio
		if detail.Info.IsTiFlash {
			if !confEnableForTiFlash {
				continue
			}
			if bs.rwTy != write || bs.opTy != movePeer {
				continue
			}
			dstToleranceRatio += tiflashToleranceRatioCorrection
		}
		if filter.Target(bs.cluster.GetOpts(), store, filters) {
			id := store.GetID()
			if bs.checkDstByPriorityAndTolerance(detail.LoadPred.max(), &detail.LoadPred.Expect, dstToleranceRatio) {
				ret[id] = detail
				hotSchedulerResultCounter.WithLabelValues("dst-store-succ", strconv.FormatUint(id, 10)).Inc()
			} else {
				hotSchedulerResultCounter.WithLabelValues("dst-store-failed", strconv.FormatUint(id, 10)).Inc()
			}
		}
	}
	return ret
}

func (bs *balanceSolver) checkDstByPriorityAndTolerance(maxLoad, expect *storeLoad, toleranceRatio float64) bool {
	if bs.sche.conf.IsStrictPickingStoreEnabled() {
		return slice.AllOf(maxLoad.Loads, func(i int) bool {
			if bs.isSelectedDim(i) {
				return maxLoad.Loads[i]*toleranceRatio < expect.Loads[i]
			}
			return true
		})
	}
	return maxLoad.Loads[bs.firstPriority]*toleranceRatio < expect.Loads[bs.firstPriority]
}

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
func (bs *balanceSolver) calcProgressiveRank() {
	src := bs.cur.srcDetail
	dst := bs.cur.dstDetail
	srcLd := src.LoadPred.min()
	dstLd := dst.LoadPred.max()
	bs.cur.progressiveRank = 0
	peer := bs.cur.srcPeerStat

	if bs.isForWriteLeader() {
		if !bs.isTolerance(src, dst, bs.firstPriority) {
			return
		}
		srcRate := srcLd.Loads[bs.firstPriority]
		dstRate := dstLd.Loads[bs.firstPriority]
		peerRate := peer.GetLoad(getRegionStatKind(bs.rwTy, bs.firstPriority))
		if srcRate-peerRate >= dstRate+peerRate {
			bs.cur.progressiveRank = -1
		}
	} else {
		firstPriorityDimHot, firstPriorityDecRatio, secondPriorityDimHot, secondPriorityDecRatio := bs.getHotDecRatioByPriorities(srcLd, dstLd, peer)
		greatDecRatio, minorDecRatio := bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorGreatDecRatio()
		switch {
		case firstPriorityDimHot && firstPriorityDecRatio <= greatDecRatio && secondPriorityDimHot && secondPriorityDecRatio <= greatDecRatio:
			// If belong to the case, two dim will be more balanced, the best choice.
			if !bs.isTolerance(src, dst, bs.firstPriority) || !bs.isTolerance(src, dst, bs.secondPriority) {
				return
			}
			bs.cur.progressiveRank = -3
			bs.firstPriorityIsBetter = true
			bs.secondPriorityIsBetter = true
		case firstPriorityDecRatio <= minorDecRatio && secondPriorityDimHot && secondPriorityDecRatio <= greatDecRatio:
			// If belong to the case, first priority dim will be not worsened, second priority dim will be more balanced.
			if !bs.isTolerance(src, dst, bs.secondPriority) {
				return
			}
			bs.cur.progressiveRank = -2
			bs.secondPriorityIsBetter = true
		case firstPriorityDimHot && firstPriorityDecRatio <= greatDecRatio:
			// If belong to the case, first priority dim will be more balanced, ignore the second priority dim.
			if !bs.isTolerance(src, dst, bs.firstPriority) {
				return
			}
			bs.cur.progressiveRank = -1
			bs.firstPriorityIsBetter = true
		}
	}
}

// isTolerance checks source store and target store by checking the difference value with pendingAmpFactor * pendingPeer.
// This will make the hot region scheduling slow even serializely running when each 2 store's pending influence is close.
func (bs *balanceSolver) isTolerance(src, dst *storeLoadDetail, dim int) bool {
	srcRate := src.LoadPred.Current.Loads[dim]
	dstRate := dst.LoadPred.Current.Loads[dim]
	if srcRate <= dstRate {
		return false
	}
	pendingAmp := (1 + pendingAmpFactor*srcRate/(srcRate-dstRate))
	srcPending := src.LoadPred.pending().Loads[dim]
	dstPending := dst.LoadPred.pending().Loads[dim]
	hotPendingStatus.WithLabelValues(bs.rwTy.String(), strconv.FormatUint(src.getID(), 10), strconv.FormatUint(dst.getID(), 10)).Set(pendingAmp)
	return srcRate-pendingAmp*srcPending > dstRate+pendingAmp*dstPending
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

	if r := bs.compareSrcStore(bs.cur.srcDetail, old.srcDetail); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r := bs.compareDstStore(bs.cur.dstDetail, old.dstDetail); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if bs.cur.srcPeerStat != old.srcPeerStat {
		// compare region

		if bs.isForWriteLeader() {
			kind := getRegionStatKind(write, bs.firstPriority)
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
func (bs *balanceSolver) compareSrcStore(detail1, detail2 *storeLoadDetail) int {
	if detail1 != detail2 {
		// compare source store
		var lpCmp storeLPCmp
		if bs.isForWriteLeader() {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(bs.maxSrc.Loads[bs.firstPriority], bs.rankStep.Loads[bs.firstPriority])),
					stLdRankCmp(stLdRate(bs.secondPriority), stepRank(bs.maxSrc.Loads[bs.secondPriority], bs.rankStep.Loads[bs.secondPriority])),
				))),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(0, bs.rankStep.Loads[bs.firstPriority])),
					stLdRankCmp(stLdRate(bs.secondPriority), stepRank(0, bs.rankStep.Loads[bs.secondPriority])),
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
		return lpCmp(detail1.LoadPred, detail2.LoadPred)
	}
	return 0
}

// smaller is better
func (bs *balanceSolver) compareDstStore(detail1, detail2 *storeLoadDetail) int {
	if detail1 != detail2 {
		// compare destination store
		var lpCmp storeLPCmp
		if bs.isForWriteLeader() {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(bs.minDst.Loads[bs.firstPriority], bs.rankStep.Loads[bs.firstPriority])),
					stLdRankCmp(stLdRate(bs.secondPriority), stepRank(bs.minDst.Loads[bs.secondPriority], bs.rankStep.Loads[bs.secondPriority])),
				)),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.Count)),
					stLdRankCmp(stLdRate(bs.firstPriority), stepRank(0, bs.rankStep.Loads[bs.firstPriority])),
					stLdRankCmp(stLdRate(bs.secondPriority), stepRank(0, bs.rankStep.Loads[bs.secondPriority])),
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
		return lpCmp(detail1.LoadPred, detail2.LoadPred)
	}
	return 0
}

func stepRank(rk0 float64, step float64) func(float64) int64 {
	return func(rate float64) int64 {
		return int64((rate - rk0) / step)
	}
}

func (bs *balanceSolver) isReadyToBuild() bool {
	if bs.cur.srcDetail == nil || bs.cur.dstDetail == nil ||
		bs.cur.srcPeerStat == nil || bs.cur.region == nil {
		return false
	}
	if bs.cur.srcDetail.getID() != bs.cur.srcPeerStat.StoreID ||
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

	srcStoreID := bs.cur.srcDetail.getID()
	dstStoreID := bs.cur.dstDetail.getID()
	switch bs.opTy {
	case movePeer:
		srcPeer := bs.cur.region.GetStorePeer(srcStoreID) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: dstStoreID, Role: srcPeer.Role}
		sourceLabel = strconv.FormatUint(srcStoreID, 10)
		targetLabel = strconv.FormatUint(dstPeer.GetStoreId(), 10)

		if bs.rwTy == read && bs.cur.region.GetLeader().StoreId == srcStoreID { // move read leader
			op, err = operator.CreateMoveLeaderOperator(
				"move-hot-read-leader",
				bs.cluster,
				bs.cur.region,
				operator.OpHotRegion,
				srcStoreID,
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
				srcStoreID,
				dstPeer)
		}
	case transferLeader:
		if bs.cur.region.GetStoreVoter(dstStoreID) == nil {
			return nil, nil
		}
		desc := "transfer-hot-" + bs.rwTy.String() + "-leader"
		typ = "transfer-leader"
		sourceLabel = strconv.FormatUint(srcStoreID, 10)
		targetLabel = strconv.FormatUint(dstStoreID, 10)
		op, err = operator.CreateTransferLeaderOperator(
			desc,
			bs.cluster,
			bs.cur.region,
			srcStoreID,
			dstStoreID,
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
	ret := make(map[uint64]*Influence, len(h.stInfos))
	for id, info := range h.stInfos {
		if info.PendingSum != nil {
			ret[id] = info.PendingSum
		}
	}
	return ret
}

// calcPendingInfluence return the calculate weight of one Operator, the value will between [0,1]
func (h *hotScheduler) calcPendingInfluence(op *operator.Operator, maxZombieDur time.Duration) (weight float64, needGC bool) {
	status := op.CheckAndGetStatus()
	if !operator.IsEndStatus(status) {
		return 1, false
	}

	// TODO: use store statistics update time to make a more accurate estimation
	zombieDur := time.Since(op.GetReachTimeOf(status))
	if zombieDur >= maxZombieDur {
		weight = 0
	} else {
		weight = 1
	}

	needGC = weight == 0
	if status != operator.SUCCESS {
		// CANCELED, REPLACED, TIMEOUT, EXPIRED, etc.
		// The actual weight is 0, but there is still a delay in GC.
		weight = 0
	}
	return
}

func (h *hotScheduler) clearPendingInfluence() {
	h.regionPendings = make(map[uint64]*pendingInfluence)
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

func prioritiesToDim(priorities []string) (firstPriority int, secondPriority int) {
	return stringToDim(priorities[0]), stringToDim(priorities[1])
}
