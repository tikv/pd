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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const (
	// HotRegionName is balance hot region scheduler name.
	HotRegionName = "balance-hot-region-scheduler"
	// HotRegionType is balance hot region scheduler type.
	HotRegionType          = "hot-region"
	splitHotReadBuckets    = "split-hot-read-region"
	splitHotWriteBuckets   = "split-hot-write-region"
	splitProgressiveRank   = int64(-5)
	minHotScheduleInterval = time.Second
	maxHotScheduleInterval = 20 * time.Second
)

var (
	// schedulePeerPr the probability of schedule the hot peer.
	schedulePeerPr = 0.66
	// pendingAmpFactor will amplify the impact of pending influence, making scheduling slower or even serial when two stores are close together
	pendingAmpFactor = 2.0
	// If the distribution of a dimension is below the corresponding stddev threshold, then scheduling will no longer be based on this dimension,
	// as it implies that this dimension is sufficiently uniform.
	stddevThreshold = 0.1
	// topnPosition is the position of the topn peer in the hot peer list.
	// We use it to judge whether to schedule the hot peer in some cases.
	topnPosition = 10
	// statisticsInterval is the interval to update statistics information.
	statisticsInterval = time.Second
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	hotSchedulerCounter                     = schedulerCounter.WithLabelValues(HotRegionName, "schedule")
	hotSchedulerSkipCounter                 = schedulerCounter.WithLabelValues(HotRegionName, "skip")
	hotSchedulerSearchRevertRegionsCounter  = schedulerCounter.WithLabelValues(HotRegionName, "search_revert_regions")
	hotSchedulerNotSameEngineCounter        = schedulerCounter.WithLabelValues(HotRegionName, "not_same_engine")
	hotSchedulerNoRegionCounter             = schedulerCounter.WithLabelValues(HotRegionName, "no_region")
	hotSchedulerUnhealthyReplicaCounter     = schedulerCounter.WithLabelValues(HotRegionName, "unhealthy_replica")
	hotSchedulerAbnormalReplicaCounter      = schedulerCounter.WithLabelValues(HotRegionName, "abnormal_replica")
	hotSchedulerCreateOperatorFailedCounter = schedulerCounter.WithLabelValues(HotRegionName, "create_operator_failed")
	hotSchedulerNewOperatorCounter          = schedulerCounter.WithLabelValues(HotRegionName, "new_operator")
	hotSchedulerSnapshotSenderLimitCounter  = schedulerCounter.WithLabelValues(HotRegionName, "snapshot_sender_limit")

	// counter related with the split region
	hotSchedulerNotFoundSplitKeysCounter          = schedulerCounter.WithLabelValues(HotRegionName, "not_found_split_keys")
	hotSchedulerRegionBucketsNotHotCounter        = schedulerCounter.WithLabelValues(HotRegionName, "region_buckets_not_hot")
	hotSchedulerOnlyOneBucketsHotCounter          = schedulerCounter.WithLabelValues(HotRegionName, "only_one_buckets_hot")
	hotSchedulerHotBucketNotValidCounter          = schedulerCounter.WithLabelValues(HotRegionName, "hot_buckets_not_valid")
	hotSchedulerRegionBucketsSingleHotSpotCounter = schedulerCounter.WithLabelValues(HotRegionName, "region_buckets_single_hot_spot")
	hotSchedulerSplitSuccessCounter               = schedulerCounter.WithLabelValues(HotRegionName, "split_success")
	hotSchedulerNeedSplitBeforeScheduleCounter    = schedulerCounter.WithLabelValues(HotRegionName, "need_split_before_move_peer")
	hotSchedulerRegionTooHotNeedSplitCounter      = schedulerCounter.WithLabelValues(HotRegionName, "region_is_too_hot_need_split")

	hotSchedulerMoveLeaderCounter     = schedulerCounter.WithLabelValues(HotRegionName, moveLeader.String())
	hotSchedulerMovePeerCounter       = schedulerCounter.WithLabelValues(HotRegionName, movePeer.String())
	hotSchedulerTransferLeaderCounter = schedulerCounter.WithLabelValues(HotRegionName, transferLeader.String())

	readSkipAllDimUniformStoreCounter    = schedulerCounter.WithLabelValues(HotRegionName, "read-skip-all-dim-uniform-store")
	writeSkipAllDimUniformStoreCounter   = schedulerCounter.WithLabelValues(HotRegionName, "write-skip-all-dim-uniform-store")
	readSkipByteDimUniformStoreCounter   = schedulerCounter.WithLabelValues(HotRegionName, "read-skip-byte-uniform-store")
	writeSkipByteDimUniformStoreCounter  = schedulerCounter.WithLabelValues(HotRegionName, "write-skip-byte-uniform-store")
	readSkipKeyDimUniformStoreCounter    = schedulerCounter.WithLabelValues(HotRegionName, "read-skip-key-uniform-store")
	writeSkipKeyDimUniformStoreCounter   = schedulerCounter.WithLabelValues(HotRegionName, "write-skip-key-uniform-store")
	readSkipQueryDimUniformStoreCounter  = schedulerCounter.WithLabelValues(HotRegionName, "read-skip-query-uniform-store")
	writeSkipQueryDimUniformStoreCounter = schedulerCounter.WithLabelValues(HotRegionName, "write-skip-query-uniform-store")
	pendingOpFailsStoreCounter           = schedulerCounter.WithLabelValues(HotRegionName, "pending-op-fails")
)

type baseHotScheduler struct {
	*BaseScheduler
	// stLoadInfos contain store statistics information by resource type.
	// stLoadInfos is temporary states but exported to API or metrics.
	// Every time `Schedule()` will recalculate it.
	stLoadInfos [resourceTypeLen]map[uint64]*statistics.StoreLoadDetail
	// stHistoryLoads stores the history `stLoadInfos`
	// Every time `Schedule()` will rolling update it.
	stHistoryLoads *statistics.StoreHistoryLoads
	// regionPendings stores regionID -> pendingInfluence,
	// this records regionID which have pending Operator by operation type. During filterHotPeers, the hot peers won't
	// be selected if its owner region is tracked in this attribute.
	regionPendings  map[uint64]*pendingInfluence
	types           []utils.RWType
	r               *rand.Rand
	updateReadTime  time.Time
	updateWriteTime time.Time
}

func newBaseHotScheduler(opController *operator.Controller) *baseHotScheduler {
	base := NewBaseScheduler(opController)
	ret := &baseHotScheduler{
		BaseScheduler:  base,
		types:          []utils.RWType{utils.Write, utils.Read},
		regionPendings: make(map[uint64]*pendingInfluence),
		stHistoryLoads: statistics.NewStoreHistoryLoads(utils.DimLen),
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.stLoadInfos[ty] = map[uint64]*statistics.StoreLoadDetail{}
	}
	return ret
}

// prepareForBalance calculate the summary of pending Influence for each store and prepare the load detail for
// each store, only update read or write load detail
func (h *baseHotScheduler) prepareForBalance(rw utils.RWType, cluster sche.SchedulerCluster) {
	storeInfos := statistics.SummaryStoreInfos(cluster.GetStores())
	h.summaryPendingInfluence(storeInfos)
	storesLoads := cluster.GetStoresLoads()
	isTraceRegionFlow := cluster.GetSchedulerConfig().IsTraceRegionFlow()

	prepare := func(regionStats map[uint64][]*statistics.HotPeerStat, resource constant.ResourceKind) {
		ty := buildResourceType(rw, resource)
		h.stLoadInfos[ty] = statistics.SummaryStoresLoad(
			storeInfos,
			storesLoads,
			h.stHistoryLoads,
			regionStats,
			isTraceRegionFlow,
			rw, resource)
	}
	switch rw {
	case utils.Read:
		// update read statistics
		if time.Since(h.updateReadTime) >= statisticsInterval {
			regionRead := cluster.RegionReadStats()
			prepare(regionRead, constant.LeaderKind)
			prepare(regionRead, constant.RegionKind)
			h.updateReadTime = time.Now()
		}
	case utils.Write:
		// update write statistics
		if time.Since(h.updateWriteTime) >= statisticsInterval {
			regionWrite := cluster.RegionWriteStats()
			prepare(regionWrite, constant.LeaderKind)
			prepare(regionWrite, constant.RegionKind)
			h.updateWriteTime = time.Now()
		}
	}
}

// summaryPendingInfluence calculate the summary of pending Influence for each store
// and clean the region from regionInfluence if they have ended operator.
// It makes each dim rate or count become `weight` times to the origin value.
func (h *baseHotScheduler) summaryPendingInfluence(storeInfos map[uint64]*statistics.StoreSummaryInfo) {
	for id, p := range h.regionPendings {
		for _, from := range p.froms {
			from := storeInfos[from]
			to := storeInfos[p.to]
			maxZombieDur := p.maxZombieDuration
			weight, needGC := calcPendingInfluence(p.op, maxZombieDur)

			if needGC {
				delete(h.regionPendings, id)
				continue
			}

			if from != nil && weight > 0 {
				from.AddInfluence(&p.origin, -weight)
			}
			if to != nil && weight > 0 {
				to.AddInfluence(&p.origin, weight)
			}
		}
	}
	for storeID, info := range storeInfos {
		storeLabel := strconv.FormatUint(storeID, 10)
		if infl := info.PendingSum; infl != nil && len(infl.Loads) != 0 {
			utils.ForeachRegionStats(func(rwTy utils.RWType, dim int, kind utils.RegionStatKind) {
				setHotPendingInfluenceMetrics(storeLabel, rwTy.String(), utils.DimToString(dim), infl.Loads[kind])
			})
		}
	}
}

// setHotPendingInfluenceMetrics sets pending influence in hot scheduler.
func setHotPendingInfluenceMetrics(storeLabel, rwTy, dim string, load float64) {
	HotPendingSum.WithLabelValues(storeLabel, rwTy, dim).Set(load)
}

func (h *baseHotScheduler) randomRWType() utils.RWType {
	return h.types[h.r.Int()%len(h.types)]
}

type hotScheduler struct {
	name string
	*baseHotScheduler
	syncutil.RWMutex
	// config of hot scheduler
	conf                *hotRegionSchedulerConfig
	searchRevertRegions [resourceTypeLen]bool // Whether to search revert regions.
}

func newHotScheduler(opController *operator.Controller, conf *hotRegionSchedulerConfig) *hotScheduler {
	base := newBaseHotScheduler(opController)
	ret := &hotScheduler{
		name:             HotRegionName,
		baseHotScheduler: base,
		conf:             conf,
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.searchRevertRegions[ty] = false
	}
	return ret
}

func (h *hotScheduler) GetName() string {
	return h.name
}

func (h *hotScheduler) GetType() string {
	return HotRegionType
}

func (h *hotScheduler) EncodeConfig() ([]byte, error) {
	return h.conf.EncodeConfig()
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

func (h *hotScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetSchedulerConfig().GetHotRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(h.GetType(), operator.OpHotRegion.String()).Inc()
	}
	return allowed
}

func (h *hotScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	hotSchedulerCounter.Inc()
	rw := h.randomRWType()
	return h.dispatch(rw, cluster), nil
}

func (h *hotScheduler) dispatch(typ utils.RWType, cluster sche.SchedulerCluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()
	h.prepareForBalance(typ, cluster)
	// it can not move earlier to support to use api and metrics.
	if h.conf.IsForbidRWType(typ) {
		return nil
	}
	switch typ {
	case utils.Read:
		return h.balanceHotReadRegions(cluster)
	case utils.Write:
		return h.balanceHotWriteRegions(cluster)
	}
	return nil
}

func (h *hotScheduler) tryAddPendingInfluence(op *operator.Operator, srcStore []uint64, dstStore uint64, infl statistics.Influence, maxZombieDur time.Duration) bool {
	regionID := op.RegionID()
	_, ok := h.regionPendings[regionID]
	if ok {
		pendingOpFailsStoreCounter.Inc()
		return false
	}

	influence := newPendingInfluence(op, srcStore, dstStore, infl, maxZombieDur)
	h.regionPendings[regionID] = influence

	utils.ForeachRegionStats(func(rwTy utils.RWType, dim int, kind utils.RegionStatKind) {
		hotPeerHist.WithLabelValues(h.GetName(), rwTy.String(), utils.DimToString(dim)).Observe(infl.Loads[kind])
	})
	return true
}

func (h *hotScheduler) balanceHotReadRegions(cluster sche.SchedulerCluster) []*operator.Operator {
	leaderSolver := newBalanceSolver(h, cluster, utils.Read, transferLeader)
	leaderOps := leaderSolver.solve()
	peerSolver := newBalanceSolver(h, cluster, utils.Read, movePeer)
	peerOps := peerSolver.solve()
	if len(leaderOps) == 0 && len(peerOps) == 0 {
		hotSchedulerSkipCounter.Inc()
		return nil
	}
	if len(leaderOps) == 0 {
		if peerSolver.tryAddPendingInfluence() {
			return peerOps
		}
		hotSchedulerSkipCounter.Inc()
		return nil
	}
	if len(peerOps) == 0 {
		if leaderSolver.tryAddPendingInfluence() {
			return leaderOps
		}
		hotSchedulerSkipCounter.Inc()
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
	hotSchedulerSkipCounter.Inc()
	return nil
}

func (h *hotScheduler) balanceHotWriteRegions(cluster sche.SchedulerCluster) []*operator.Operator {
	// prefer to balance by peer
	s := h.r.Intn(100)
	switch {
	case s < int(schedulePeerPr*100):
		peerSolver := newBalanceSolver(h, cluster, utils.Write, movePeer)
		ops := peerSolver.solve()
		if len(ops) > 0 && peerSolver.tryAddPendingInfluence() {
			return ops
		}
	default:
	}

	leaderSolver := newBalanceSolver(h, cluster, utils.Write, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 && leaderSolver.tryAddPendingInfluence() {
		return ops
	}

	hotSchedulerSkipCounter.Inc()
	return nil
}

type solution struct {
	srcStore     *statistics.StoreLoadDetail
	region       *core.RegionInfo // The region of the main balance effect. Relate mainPeerStat. srcStore -> dstStore
	mainPeerStat *statistics.HotPeerStat

	dstStore       *statistics.StoreLoadDetail
	revertRegion   *core.RegionInfo // The regions to hedge back effects. Relate revertPeerStat. dstStore -> srcStore
	revertPeerStat *statistics.HotPeerStat

	cachedPeersRate []float64

	// progressiveRank measures the contribution for balance.
	// The smaller the rank, the better this solution is.
	// If progressiveRank <= 0, this solution makes thing better.
	// 0 indicates that this is a solution that cannot be used directly, but can be optimized.
	// 1 indicates that this is a non-optimizable solution.
	// See `calcProgressiveRank` for more about progressive rank.
	progressiveRank int64
	// only for rank v2
	firstScore  int
	secondScore int
}

// getExtremeLoad returns the closest load in the selected src and dst statistics.
// in other word, the min load of the src store and the max load of the dst store.
// If peersRate is negative, the direction is reversed.
func (s *solution) getExtremeLoad(dim int) (src float64, dst float64) {
	if s.getPeersRateFromCache(dim) >= 0 {
		return s.srcStore.LoadPred.Min().Loads[dim], s.dstStore.LoadPred.Max().Loads[dim]
	}
	return s.srcStore.LoadPred.Max().Loads[dim], s.dstStore.LoadPred.Min().Loads[dim]
}

// getCurrentLoad returns the current load of the src store and the dst store.
func (s *solution) getCurrentLoad(dim int) (src float64, dst float64) {
	return s.srcStore.LoadPred.Current.Loads[dim], s.dstStore.LoadPred.Current.Loads[dim]
}

// getPendingLoad returns the pending load of the src store and the dst store.
func (s *solution) getPendingLoad(dim int) (src float64, dst float64) {
	return s.srcStore.LoadPred.Pending().Loads[dim], s.dstStore.LoadPred.Pending().Loads[dim]
}

// calcPeersRate precomputes the peer rate and stores it in cachedPeersRate.
func (s *solution) calcPeersRate(dims ...int) {
	s.cachedPeersRate = make([]float64, utils.DimLen)
	for _, dim := range dims {
		peersRate := s.mainPeerStat.GetLoad(dim)
		if s.revertPeerStat != nil {
			peersRate -= s.revertPeerStat.GetLoad(dim)
		}
		s.cachedPeersRate[dim] = peersRate
	}
}

// getPeersRateFromCache returns the load of the peer. Need to calcPeersRate first.
func (s *solution) getPeersRateFromCache(dim int) float64 {
	return s.cachedPeersRate[dim]
}

// isAvailable returns the solution is available.
// The solution should have no revertRegion and progressiveRank < 0.
func isAvailableV1(s *solution) bool {
	return s.progressiveRank < 0
}

type balanceSolver struct {
	sche.SchedulerCluster
	sche             *hotScheduler
	stLoadDetail     map[uint64]*statistics.StoreLoadDetail
	filteredHotPeers map[uint64][]*statistics.HotPeerStat // storeID -> hotPeers(filtered)
	nthHotPeer       map[uint64][]*statistics.HotPeerStat // storeID -> [dimLen]hotPeers
	rwTy             utils.RWType
	opTy             opType
	resourceTy       resourceType

	cur *solution

	best *solution
	ops  []*operator.Operator

	maxSrc   *statistics.StoreLoad
	minDst   *statistics.StoreLoad
	rankStep *statistics.StoreLoad

	// firstPriority and secondPriority indicate priority of hot schedule
	// they may be byte(0), key(1), query(2), and always less than dimLen
	firstPriority  int
	secondPriority int

	greatDecRatio float64
	minorDecRatio float64
	maxPeerNum    int
	minHotDegree  int

	firstPriorityV2Ratios  *rankV2Ratios
	secondPriorityV2Ratios *rankV2Ratios

	// The rank correlation function used according to the version
	isAvailable                 func(*solution) bool
	filterUniformStore          func() (string, bool)
	needSearchRevertRegions     func() bool
	setSearchRevertRegions      func()
	calcProgressiveRank         func()
	betterThan                  func(*solution) bool
	rankToDimString             func() string
	checkByPriorityAndTolerance func(loads []float64, f func(int) bool) bool
	checkHistoryLoadsByPriority func(loads [][]float64, f func(int) bool) bool
}

func (bs *balanceSolver) init() {
	// Load the configuration items of the scheduler.
	bs.resourceTy = toResourceType(bs.rwTy, bs.opTy)
	bs.maxPeerNum = bs.sche.conf.GetMaxPeerNumber()
	bs.minHotDegree = bs.GetSchedulerConfig().GetHotRegionCacheHitsThreshold()
	bs.firstPriority, bs.secondPriority = prioritiesToDim(bs.getPriorities())
	bs.greatDecRatio, bs.minorDecRatio = bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorDecRatio()
	switch bs.sche.conf.GetRankFormulaVersion() {
	case "v1":
		bs.initRankV1()
	default:
		bs.initRankV2()
	}

	// Init store load detail according to the type.
	bs.stLoadDetail = bs.sche.stLoadInfos[bs.resourceTy]

	bs.maxSrc = &statistics.StoreLoad{Loads: make([]float64, utils.DimLen)}
	bs.minDst = &statistics.StoreLoad{
		Loads: make([]float64, utils.DimLen),
		Count: math.MaxFloat64,
	}
	for i := range bs.minDst.Loads {
		bs.minDst.Loads[i] = math.MaxFloat64
	}
	maxCur := &statistics.StoreLoad{Loads: make([]float64, utils.DimLen)}

	bs.filteredHotPeers = make(map[uint64][]*statistics.HotPeerStat)
	bs.nthHotPeer = make(map[uint64][]*statistics.HotPeerStat)
	for _, detail := range bs.stLoadDetail {
		bs.maxSrc = statistics.MaxLoad(bs.maxSrc, detail.LoadPred.Min())
		bs.minDst = statistics.MinLoad(bs.minDst, detail.LoadPred.Max())
		maxCur = statistics.MaxLoad(maxCur, &detail.LoadPred.Current)
		bs.nthHotPeer[detail.GetID()] = make([]*statistics.HotPeerStat, utils.DimLen)
		bs.filteredHotPeers[detail.GetID()] = bs.filterHotPeers(detail)
	}

	rankStepRatios := []float64{
		utils.ByteDim:  bs.sche.conf.GetByteRankStepRatio(),
		utils.KeyDim:   bs.sche.conf.GetKeyRankStepRatio(),
		utils.QueryDim: bs.sche.conf.GetQueryRateRankStepRatio()}
	stepLoads := make([]float64, utils.DimLen)
	for i := range stepLoads {
		stepLoads[i] = maxCur.Loads[i] * rankStepRatios[i]
	}
	bs.rankStep = &statistics.StoreLoad{
		Loads: stepLoads,
		Count: maxCur.Count * bs.sche.conf.GetCountRankStepRatio(),
	}
}

func (bs *balanceSolver) initRankV1() {
	bs.isAvailable = isAvailableV1
	bs.filterUniformStore = bs.filterUniformStoreV1
	bs.needSearchRevertRegions = func() bool { return false }
	bs.setSearchRevertRegions = func() {}
	bs.calcProgressiveRank = bs.calcProgressiveRankV1
	bs.betterThan = bs.betterThanV1
	bs.rankToDimString = bs.rankToDimStringV1
	bs.pickCheckPolicyV1()
}

func (bs *balanceSolver) pickCheckPolicyV1() {
	switch {
	case bs.resourceTy == writeLeader:
		bs.checkByPriorityAndTolerance = bs.checkByPriorityAndToleranceFirstOnly
		bs.checkHistoryLoadsByPriority = bs.checkHistoryLoadsByPriorityAndToleranceFirstOnly
	case bs.sche.conf.IsStrictPickingStoreEnabled():
		bs.checkByPriorityAndTolerance = bs.checkByPriorityAndToleranceAllOf
		bs.checkHistoryLoadsByPriority = bs.checkHistoryLoadsByPriorityAndToleranceAllOf
	default:
		bs.checkByPriorityAndTolerance = bs.checkByPriorityAndToleranceFirstOnly
		bs.checkHistoryLoadsByPriority = bs.checkHistoryLoadsByPriorityAndToleranceFirstOnly
	}
}

func (bs *balanceSolver) isSelectedDim(dim int) bool {
	return dim == bs.firstPriority || dim == bs.secondPriority
}

func (bs *balanceSolver) getPriorities() []string {
	querySupport := bs.sche.conf.checkQuerySupport(bs.SchedulerCluster)
	// For read, transfer-leader and move-peer have the same priority config
	// For write, they are different
	switch bs.resourceTy {
	case readLeader, readPeer:
		return adjustPrioritiesConfig(querySupport, bs.sche.conf.GetReadPriorities(), getReadPriorities)
	case writeLeader:
		return adjustPrioritiesConfig(querySupport, bs.sche.conf.GetWriteLeaderPriorities(), getWriteLeaderPriorities)
	case writePeer:
		return adjustPrioritiesConfig(querySupport, bs.sche.conf.GetWritePeerPriorities(), getWritePeerPriorities)
	}
	log.Error("illegal type or illegal operator while getting the priority", zap.String("type", bs.rwTy.String()), zap.String("operator", bs.opTy.String()))
	return []string{}
}

func newBalanceSolver(sche *hotScheduler, cluster sche.SchedulerCluster, rwTy utils.RWType, opTy opType) *balanceSolver {
	bs := &balanceSolver{
		SchedulerCluster: cluster,
		sche:             sche,
		rwTy:             rwTy,
		opTy:             opTy,
	}
	bs.init()
	return bs
}

func (bs *balanceSolver) isValid() bool {
	if bs.SchedulerCluster == nil || bs.sche == nil || bs.stLoadDetail == nil {
		return false
	}
	return true
}

func (bs *balanceSolver) filterUniformStoreV1() (string, bool) {
	if !bs.enableExpectation() {
		return "", false
	}
	// Because region is available for src and dst, so stddev is the same for both, only need to calculate one.
	isUniformFirstPriority, isUniformSecondPriority := bs.isUniformFirstPriority(bs.cur.srcStore), bs.isUniformSecondPriority(bs.cur.srcStore)
	if isUniformFirstPriority && isUniformSecondPriority {
		// If both dims are enough uniform, any schedule is unnecessary.
		return "all-dim", true
	}
	if isUniformFirstPriority && (bs.cur.progressiveRank == -1 || bs.cur.progressiveRank == -3) {
		// If first priority dim is enough uniform, -1 is unnecessary and maybe lead to worse balance for second priority dim
		return dimToString(bs.firstPriority), true
	}
	if isUniformSecondPriority && bs.cur.progressiveRank == -2 {
		// If second priority dim is enough uniform, -2 is unnecessary and maybe lead to worse balance for first priority dim
		return dimToString(bs.secondPriority), true
	}
	return "", false
}

// solve travels all the src stores, hot peers, dst stores and select each one of them to make a best scheduling solution.
// The comparing between solutions is based on calcProgressiveRank.
func (bs *balanceSolver) solve() []*operator.Operator {
	if !bs.isValid() {
		return nil
	}
	bs.cur = &solution{}
	tryUpdateBestSolution := func() {
		if label, ok := bs.filterUniformStore(); ok {
			bs.skipCounter(label).Inc()
			return
		}
		if bs.isAvailable(bs.cur) && bs.betterThan(bs.best) {
			if newOps := bs.buildOperators(); len(newOps) > 0 {
				bs.ops = newOps
				clone := *bs.cur
				bs.best = &clone
			}
		}
	}

	// Whether to allow move region peer from dstStore to srcStore
	var allowRevertRegion func(region *core.RegionInfo, srcStoreID uint64) bool
	if bs.opTy == transferLeader {
		allowRevertRegion = func(region *core.RegionInfo, srcStoreID uint64) bool {
			return region.GetStorePeer(srcStoreID) != nil
		}
	} else {
		allowRevertRegion = func(region *core.RegionInfo, srcStoreID uint64) bool {
			return region.GetStorePeer(srcStoreID) == nil
		}
	}
	snapshotFilter := filter.NewSnapshotSendFilter(bs.GetStores(), constant.Medium)
	splitThresholds := bs.sche.conf.getSplitThresholds()
	for _, srcStore := range bs.filterSrcStores() {
		bs.cur.srcStore = srcStore
		srcStoreID := srcStore.GetID()
		for _, mainPeerStat := range bs.filteredHotPeers[srcStoreID] {
			if bs.cur.region = bs.getRegion(mainPeerStat, srcStoreID); bs.cur.region == nil {
				continue
			} else if bs.opTy == movePeer {
				if !snapshotFilter.Select(bs.cur.region).IsOK() {
					hotSchedulerSnapshotSenderLimitCounter.Inc()
					continue
				}
			}
			bs.cur.mainPeerStat = mainPeerStat
			if bs.GetStoreConfig().IsEnableRegionBucket() && bs.tooHotNeedSplit(srcStore, mainPeerStat, splitThresholds) {
				hotSchedulerRegionTooHotNeedSplitCounter.Inc()
				ops := bs.createSplitOperator([]*core.RegionInfo{bs.cur.region}, byLoad)
				if len(ops) > 0 {
					bs.ops = ops
					bs.cur.calcPeersRate(bs.firstPriority, bs.secondPriority)
					bs.best = bs.cur
					return ops
				}
			}

			for _, dstStore := range bs.filterDstStores() {
				bs.cur.dstStore = dstStore
				bs.calcProgressiveRank()
				tryUpdateBestSolution()
				if bs.needSearchRevertRegions() {
					hotSchedulerSearchRevertRegionsCounter.Inc()
					dstStoreID := dstStore.GetID()
					for _, revertPeerStat := range bs.filteredHotPeers[dstStoreID] {
						revertRegion := bs.getRegion(revertPeerStat, dstStoreID)
						if revertRegion == nil || revertRegion.GetID() == bs.cur.region.GetID() ||
							!allowRevertRegion(revertRegion, srcStoreID) {
							continue
						}
						bs.cur.revertPeerStat = revertPeerStat
						bs.cur.revertRegion = revertRegion
						bs.calcProgressiveRank()
						tryUpdateBestSolution()
					}
					bs.cur.revertPeerStat = nil
					bs.cur.revertRegion = nil
				}
			}
		}
	}

	bs.setSearchRevertRegions()
	return bs.ops
}

func (bs *balanceSolver) skipCounter(label string) prometheus.Counter {
	if bs.rwTy == utils.Read {
		switch label {
		case "byte":
			return readSkipByteDimUniformStoreCounter
		case "key":
			return readSkipKeyDimUniformStoreCounter
		case "query":
			return readSkipQueryDimUniformStoreCounter
		default:
			return readSkipAllDimUniformStoreCounter
		}
	}
	switch label {
	case "byte":
		return writeSkipByteDimUniformStoreCounter
	case "key":
		return writeSkipKeyDimUniformStoreCounter
	case "query":
		return writeSkipQueryDimUniformStoreCounter
	default:
		return writeSkipAllDimUniformStoreCounter
	}
}

func (bs *balanceSolver) tryAddPendingInfluence() bool {
	if bs.best == nil || len(bs.ops) == 0 {
		return false
	}
	isSplit := bs.ops[0].Kind() == operator.OpSplit
	if !isSplit && bs.best.srcStore.IsTiFlash() != bs.best.dstStore.IsTiFlash() {
		hotSchedulerNotSameEngineCounter.Inc()
		return false
	}
	maxZombieDur := bs.calcMaxZombieDur()

	// TODO: Process operators atomically.
	// main peer

	srcStoreIDs := make([]uint64, 0)
	dstStoreID := uint64(0)
	if isSplit {
		region := bs.GetRegion(bs.ops[0].RegionID())
		if region == nil {
			return false
		}
		for id := range region.GetStoreIDs() {
			srcStoreIDs = append(srcStoreIDs, id)
		}
	} else {
		srcStoreIDs = append(srcStoreIDs, bs.best.srcStore.GetID())
		dstStoreID = bs.best.dstStore.GetID()
	}
	infl := bs.collectPendingInfluence(bs.best.mainPeerStat)
	if !bs.sche.tryAddPendingInfluence(bs.ops[0], srcStoreIDs, dstStoreID, infl, maxZombieDur) {
		return false
	}
	if isSplit {
		return true
	}
	// revert peers
	if bs.best.revertPeerStat != nil && len(bs.ops) > 1 {
		infl := bs.collectPendingInfluence(bs.best.revertPeerStat)
		if !bs.sche.tryAddPendingInfluence(bs.ops[1], srcStoreIDs, dstStoreID, infl, maxZombieDur) {
			return false
		}
	}
	bs.logBestSolution()
	return true
}

func (bs *balanceSolver) collectPendingInfluence(peer *statistics.HotPeerStat) statistics.Influence {
	infl := statistics.Influence{Loads: make([]float64, utils.RegionStatCount), Count: 1}
	bs.rwTy.SetFullLoadRates(infl.Loads, peer.GetLoads())
	inverse := bs.rwTy.Inverse()
	another := bs.GetHotPeerStat(inverse, peer.RegionID, peer.StoreID)
	if another != nil {
		inverse.SetFullLoadRates(infl.Loads, another.GetLoads())
	}
	return infl
}

// Depending on the source of the statistics used, a different ZombieDuration will be used.
// If the statistics are from the sum of Regions, there will be a longer ZombieDuration.
func (bs *balanceSolver) calcMaxZombieDur() time.Duration {
	switch bs.resourceTy {
	case writeLeader:
		if bs.firstPriority == utils.QueryDim {
			// We use store query info rather than total of hot write leader to guide hot write leader scheduler
			// when its first priority is `QueryDim`, because `Write-peer` does not have `QueryDim`.
			// The reason is the same with `tikvCollector.GetLoads`.
			return bs.sche.conf.GetStoreStatZombieDuration()
		}
		return bs.sche.conf.GetRegionsStatZombieDuration()
	case writePeer:
		if bs.best.srcStore.IsTiFlash() {
			return bs.sche.conf.GetRegionsStatZombieDuration()
		}
		return bs.sche.conf.GetStoreStatZombieDuration()
	default:
		return bs.sche.conf.GetStoreStatZombieDuration()
	}
}

// filterSrcStores compare the min rate and the ratio * expectation rate, if two dim rate is greater than
// its expectation * ratio, the store would be selected as hot source store
func (bs *balanceSolver) filterSrcStores() map[uint64]*statistics.StoreLoadDetail {
	ret := make(map[uint64]*statistics.StoreLoadDetail)
	confSrcToleranceRatio := bs.sche.conf.GetSrcToleranceRatio()
	confEnableForTiFlash := bs.sche.conf.GetEnableForTiFlash()
	for id, detail := range bs.stLoadDetail {
		srcToleranceRatio := confSrcToleranceRatio
		if detail.IsTiFlash() {
			if !confEnableForTiFlash {
				continue
			}
			if bs.rwTy != utils.Write || bs.opTy != movePeer {
				continue
			}
			srcToleranceRatio += tiflashToleranceRatioCorrection
		}
		if len(detail.HotPeers) == 0 {
			continue
		}

		if !bs.checkSrcByPriorityAndTolerance(detail.LoadPred.Min(), &detail.LoadPred.Expect, srcToleranceRatio) {
			hotSchedulerResultCounter.WithLabelValues("src-store-failed-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
			continue
		}
		if !bs.checkSrcHistoryLoadsByPriorityAndTolerance(&detail.LoadPred.Current, &detail.LoadPred.Expect, srcToleranceRatio) {
			hotSchedulerResultCounter.WithLabelValues("src-store-history-loads-failed-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
			continue
		}

		ret[id] = detail
		hotSchedulerResultCounter.WithLabelValues("src-store-succ-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
	}
	return ret
}

func (bs *balanceSolver) checkSrcByPriorityAndTolerance(minLoad, expectLoad *statistics.StoreLoad, toleranceRatio float64) bool {
	return bs.checkByPriorityAndTolerance(minLoad.Loads, func(i int) bool {
		return minLoad.Loads[i] > toleranceRatio*expectLoad.Loads[i]
	})
}

func (bs *balanceSolver) checkSrcHistoryLoadsByPriorityAndTolerance(current, expectLoad *statistics.StoreLoad, toleranceRatio float64) bool {
	if len(current.HistoryLoads) == 0 {
		return true
	}
	return bs.checkHistoryLoadsByPriority(current.HistoryLoads, func(i int) bool {
		return slice.AllOf(current.HistoryLoads[i], func(j int) bool {
			return current.HistoryLoads[i][j] > toleranceRatio*expectLoad.HistoryLoads[i][j]
		})
	})
}

// filterHotPeers filtered hot peers from statistics.HotPeerStat and deleted the peer if its region is in pending status.
// The returned hotPeer count in controlled by `max-peer-number`.
func (bs *balanceSolver) filterHotPeers(storeLoad *statistics.StoreLoadDetail) []*statistics.HotPeerStat {
	hotPeers := storeLoad.HotPeers
	ret := make([]*statistics.HotPeerStat, 0, len(hotPeers))
	appendItem := func(item *statistics.HotPeerStat) {
		if _, ok := bs.sche.regionPendings[item.ID()]; !ok && !item.IsNeedCoolDownTransferLeader(bs.minHotDegree, bs.rwTy) {
			// no in pending operator and no need cool down after transfer leader
			ret = append(ret, item)
		}
	}

	var firstSort, secondSort []*statistics.HotPeerStat
	if len(hotPeers) >= topnPosition || len(hotPeers) > bs.maxPeerNum {
		firstSort = make([]*statistics.HotPeerStat, len(hotPeers))
		copy(firstSort, hotPeers)
		sort.Slice(firstSort, func(i, j int) bool {
			return firstSort[i].GetLoad(bs.firstPriority) > firstSort[j].GetLoad(bs.firstPriority)
		})
		secondSort = make([]*statistics.HotPeerStat, len(hotPeers))
		copy(secondSort, hotPeers)
		sort.Slice(secondSort, func(i, j int) bool {
			return secondSort[i].GetLoad(bs.secondPriority) > secondSort[j].GetLoad(bs.secondPriority)
		})
	}
	if len(hotPeers) >= topnPosition {
		storeID := storeLoad.GetID()
		bs.nthHotPeer[storeID][bs.firstPriority] = firstSort[topnPosition-1]
		bs.nthHotPeer[storeID][bs.secondPriority] = secondSort[topnPosition-1]
	}
	if len(hotPeers) > bs.maxPeerNum {
		union := bs.sortHotPeers(firstSort, secondSort)
		ret = make([]*statistics.HotPeerStat, 0, len(union))
		for peer := range union {
			appendItem(peer)
		}
		return ret
	}

	for _, peer := range hotPeers {
		appendItem(peer)
	}
	return ret
}

func (bs *balanceSolver) sortHotPeers(firstSort, secondSort []*statistics.HotPeerStat) map[*statistics.HotPeerStat]struct{} {
	union := make(map[*statistics.HotPeerStat]struct{}, bs.maxPeerNum)
	// At most MaxPeerNum peers, to prevent balanceSolver.solve() too slow.
	for len(union) < bs.maxPeerNum {
		for len(firstSort) > 0 {
			peer := firstSort[0]
			firstSort = firstSort[1:]
			if _, ok := union[peer]; !ok {
				union[peer] = struct{}{}
				break
			}
		}
		for len(union) < bs.maxPeerNum && len(secondSort) > 0 {
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
		hotSchedulerNoRegionCounter.Inc()
		return false
	}

	if !filter.IsRegionHealthyAllowPending(region) {
		hotSchedulerUnhealthyReplicaCounter.Inc()
		return false
	}

	if !filter.IsRegionReplicated(bs.SchedulerCluster, region) {
		log.Debug("region has abnormal replica count", zap.String("scheduler", bs.sche.GetName()), zap.Uint64("region-id", region.GetID()))
		hotSchedulerAbnormalReplicaCounter.Inc()
		return false
	}

	return true
}

func (bs *balanceSolver) getRegion(peerStat *statistics.HotPeerStat, storeID uint64) *core.RegionInfo {
	region := bs.GetRegion(peerStat.ID())
	if !bs.isRegionAvailable(region) {
		return nil
	}

	switch bs.opTy {
	case movePeer:
		srcPeer := region.GetStorePeer(storeID)
		if srcPeer == nil {
			log.Debug("region does not have a peer on source store, maybe stat out of date",
				zap.Uint64("region-id", peerStat.ID()),
				zap.Uint64("leader-store-id", storeID))
			return nil
		}
	case transferLeader:
		if region.GetLeader().GetStoreId() != storeID {
			log.Debug("region leader is not on source store, maybe stat out of date",
				zap.Uint64("region-id", peerStat.ID()),
				zap.Uint64("leader-store-id", storeID))
			return nil
		}
	default:
		return nil
	}

	return region
}

// filterDstStores select the candidate store by filters
func (bs *balanceSolver) filterDstStores() map[uint64]*statistics.StoreLoadDetail {
	var (
		filters    []filter.Filter
		candidates []*statistics.StoreLoadDetail
	)
	srcStore := bs.cur.srcStore.StoreInfo
	switch bs.opTy {
	case movePeer:
		if bs.rwTy == utils.Read && bs.cur.mainPeerStat.IsLeader() { // for hot-read scheduler, only move peer
			return nil
		}
		filters = []filter.Filter{
			&filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true, OperatorLevel: constant.High},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.region.GetStoreIDs(), bs.cur.region.GetStoreIDs()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
			filter.NewPlacementSafeguard(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(), bs.GetRuleManager(), bs.cur.region, srcStore, nil),
		}
		for _, detail := range bs.stLoadDetail {
			candidates = append(candidates, detail)
		}

	case transferLeader:
		if !bs.cur.mainPeerStat.IsLeader() { // source peer must be leader whether it is move leader or transfer leader
			return nil
		}
		filters = []filter.Filter{
			&filter.StoreStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true, OperatorLevel: constant.High},
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
		}
		if bs.rwTy == utils.Read {
			peers := bs.cur.region.GetPeers()
			moveLeaderFilters := []filter.Filter{&filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true, OperatorLevel: constant.High}}
			if leaderFilter := filter.NewPlacementLeaderSafeguard(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(), bs.GetRuleManager(), bs.cur.region, srcStore, true /*allowMoveLeader*/); leaderFilter != nil {
				filters = append(filters, leaderFilter)
			}
			for storeID, detail := range bs.stLoadDetail {
				if storeID == bs.cur.mainPeerStat.StoreID {
					continue
				}
				// transfer leader
				if slice.AnyOf(peers, func(i int) bool {
					return peers[i].GetStoreId() == storeID
				}) {
					candidates = append(candidates, detail)
					continue
				}
				// move leader
				if filter.Target(bs.GetSchedulerConfig(), detail.StoreInfo, moveLeaderFilters) {
					candidates = append(candidates, detail)
				}
			}
		} else {
			if leaderFilter := filter.NewPlacementLeaderSafeguard(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(), bs.GetRuleManager(), bs.cur.region, srcStore, false /*allowMoveLeader*/); leaderFilter != nil {
				filters = append(filters, leaderFilter)
			}
			for _, peer := range bs.cur.region.GetFollowers() {
				if detail, ok := bs.stLoadDetail[peer.GetStoreId()]; ok {
					candidates = append(candidates, detail)
				}
			}
		}

	default:
		return nil
	}
	return bs.pickDstStores(filters, candidates)
}

func (bs *balanceSolver) pickDstStores(filters []filter.Filter, candidates []*statistics.StoreLoadDetail) map[uint64]*statistics.StoreLoadDetail {
	ret := make(map[uint64]*statistics.StoreLoadDetail, len(candidates))
	confDstToleranceRatio := bs.sche.conf.GetDstToleranceRatio()
	confEnableForTiFlash := bs.sche.conf.GetEnableForTiFlash()
	for _, detail := range candidates {
		store := detail.StoreInfo
		dstToleranceRatio := confDstToleranceRatio
		if detail.IsTiFlash() {
			if !confEnableForTiFlash {
				continue
			}
			if bs.rwTy != utils.Write || bs.opTy != movePeer {
				continue
			}
			dstToleranceRatio += tiflashToleranceRatioCorrection
		}
		if filter.Target(bs.GetSchedulerConfig(), store, filters) {
			id := store.GetID()
			if !bs.checkDstByPriorityAndTolerance(detail.LoadPred.Max(), &detail.LoadPred.Expect, dstToleranceRatio) {
				hotSchedulerResultCounter.WithLabelValues("dst-store-failed-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
				continue
			}
			if !bs.checkDstHistoryLoadsByPriorityAndTolerance(&detail.LoadPred.Current, &detail.LoadPred.Expect, dstToleranceRatio) {
				hotSchedulerResultCounter.WithLabelValues("dst-store-history-loads-failed-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
				continue
			}

			hotSchedulerResultCounter.WithLabelValues("dst-store-succ-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
			ret[id] = detail
		}
	}
	return ret
}

func (bs *balanceSolver) checkDstByPriorityAndTolerance(maxLoad, expect *statistics.StoreLoad, toleranceRatio float64) bool {
	return bs.checkByPriorityAndTolerance(maxLoad.Loads, func(i int) bool {
		return maxLoad.Loads[i]*toleranceRatio < expect.Loads[i]
	})
}

func (bs *balanceSolver) checkDstHistoryLoadsByPriorityAndTolerance(current, expect *statistics.StoreLoad, toleranceRatio float64) bool {
	if len(current.HistoryLoads) == 0 {
		return true
	}
	return bs.checkHistoryLoadsByPriority(current.HistoryLoads, func(i int) bool {
		return slice.AllOf(current.HistoryLoads[i], func(j int) bool {
			return current.HistoryLoads[i][j]*toleranceRatio < expect.HistoryLoads[i][j]
		})
	})
}

func (bs *balanceSolver) checkByPriorityAndToleranceAllOf(loads []float64, f func(int) bool) bool {
	return slice.AllOf(loads, func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return true
	})
}

func (bs *balanceSolver) checkHistoryLoadsByPriorityAndToleranceAllOf(loads [][]float64, f func(int) bool) bool {
	return slice.AllOf(loads, func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return true
	})
}

func (bs *balanceSolver) checkByPriorityAndToleranceAnyOf(loads []float64, f func(int) bool) bool {
	return slice.AnyOf(loads, func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return false
	})
}

func (bs *balanceSolver) checkHistoryByPriorityAndToleranceAnyOf(loads [][]float64, f func(int) bool) bool {
	return slice.AnyOf(loads, func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return false
	})
}

func (bs *balanceSolver) checkByPriorityAndToleranceFirstOnly(loads []float64, f func(int) bool) bool {
	return f(bs.firstPriority)
}

func (bs *balanceSolver) checkHistoryLoadsByPriorityAndToleranceFirstOnly(_ [][]float64, f func(int) bool) bool {
	return f(bs.firstPriority)
}

func (bs *balanceSolver) enableExpectation() bool {
	return bs.sche.conf.GetDstToleranceRatio() > 0 && bs.sche.conf.GetSrcToleranceRatio() > 0
}

func (bs *balanceSolver) isUniformFirstPriority(store *statistics.StoreLoadDetail) bool {
	// first priority should be more uniform than second priority
	return store.IsUniform(bs.firstPriority, stddevThreshold*0.5)
}

func (bs *balanceSolver) isUniformSecondPriority(store *statistics.StoreLoadDetail) bool {
	return store.IsUniform(bs.secondPriority, stddevThreshold)
}

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
// | ↓ firstPriority \ secondPriority → | isBetter | isNotWorsened | Worsened |
// |   isBetter                         | -4       | -3            | -1 / 0   |
// |   isNotWorsened                    | -2       | 1             | 1        |
// |   Worsened                         | 0        | 1             | 1        |
func (bs *balanceSolver) calcProgressiveRankV1() {
	bs.cur.progressiveRank = 1
	bs.cur.calcPeersRate(bs.firstPriority, bs.secondPriority)
	if bs.cur.getPeersRateFromCache(bs.firstPriority) < bs.getMinRate(bs.firstPriority) &&
		bs.cur.getPeersRateFromCache(bs.secondPriority) < bs.getMinRate(bs.secondPriority) {
		return
	}

	if bs.resourceTy == writeLeader {
		// For write leader, only compare the first priority.
		// If the first priority is better, the progressiveRank is -3.
		// Because it is not a solution that needs to be optimized.
		if bs.isBetterForWriteLeader() {
			bs.cur.progressiveRank = -3
		}
		return
	}

	isFirstBetter, isSecondBetter := bs.isBetter(bs.firstPriority), bs.isBetter(bs.secondPriority)
	isFirstNotWorsened := isFirstBetter || bs.isNotWorsened(bs.firstPriority)
	isSecondNotWorsened := isSecondBetter || bs.isNotWorsened(bs.secondPriority)
	switch {
	case isFirstBetter && isSecondBetter:
		// If belonging to the case, all two dim will be more balanced, the best choice.
		bs.cur.progressiveRank = -4
	case isFirstBetter && isSecondNotWorsened:
		// If belonging to the case, the first priority dim will be more balanced, the second priority dim will be not worsened.
		bs.cur.progressiveRank = -3
	case isFirstNotWorsened && isSecondBetter:
		// If belonging to the case, the first priority dim will be not worsened, the second priority dim will be more balanced.
		bs.cur.progressiveRank = -2
	case isFirstBetter:
		// If belonging to the case, the first priority dim will be more balanced, ignore the second priority dim.
		bs.cur.progressiveRank = -1
	case isSecondBetter:
		// If belonging to the case, the second priority dim will be more balanced, ignore the first priority dim.
		// It's a solution that cannot be used directly, but can be optimized.
		bs.cur.progressiveRank = 0
	}
}

// isTolerance checks source store and target store by checking the difference value with pendingAmpFactor * pendingPeer.
// This will make the hot region scheduling slow even serialize running when each 2 store's pending influence is close.
func (bs *balanceSolver) isTolerance(dim int, reverse bool) bool {
	srcStoreID := bs.cur.srcStore.GetID()
	dstStoreID := bs.cur.dstStore.GetID()
	srcRate, dstRate := bs.cur.getCurrentLoad(dim)
	srcPending, dstPending := bs.cur.getPendingLoad(dim)
	if reverse {
		srcStoreID, dstStoreID = dstStoreID, srcStoreID
		srcRate, dstRate = dstRate, srcRate
		srcPending, dstPending = dstPending, srcPending
	}

	if srcRate <= dstRate {
		return false
	}
	pendingAmp := 1 + pendingAmpFactor*srcRate/(srcRate-dstRate)
	hotPendingStatus.WithLabelValues(bs.rwTy.String(), strconv.FormatUint(srcStoreID, 10), strconv.FormatUint(dstStoreID, 10)).Set(pendingAmp)
	return srcRate-pendingAmp*srcPending > dstRate+pendingAmp*dstPending
}

func (bs *balanceSolver) getHotDecRatioByPriorities(dim int) (isHot bool, decRatio float64) {
	// we use DecRatio(Decline Ratio) to expect that the dst store's rate should still be less
	// than the src store's rate after scheduling one peer.
	srcRate, dstRate := bs.cur.getExtremeLoad(dim)
	peersRate := bs.cur.getPeersRateFromCache(dim)
	// Rate may be negative after adding revertRegion, which should be regarded as moving from dst to src.
	if peersRate >= 0 {
		isHot = peersRate >= bs.getMinRate(dim)
		decRatio = (dstRate + peersRate) / math.Max(srcRate-peersRate, 1)
	} else {
		isHot = -peersRate >= bs.getMinRate(dim)
		decRatio = (srcRate - peersRate) / math.Max(dstRate+peersRate, 1)
	}
	return
}

func (bs *balanceSolver) isBetterForWriteLeader() bool {
	srcRate, dstRate := bs.cur.getExtremeLoad(bs.firstPriority)
	peersRate := bs.cur.getPeersRateFromCache(bs.firstPriority)
	return srcRate-peersRate >= dstRate+peersRate && bs.isTolerance(bs.firstPriority, false)
}

func (bs *balanceSolver) isBetter(dim int) bool {
	isHot, decRatio := bs.getHotDecRatioByPriorities(dim)
	return isHot && decRatio <= bs.greatDecRatio && bs.isTolerance(dim, false)
}

// isNotWorsened must be true if isBetter is true.
func (bs *balanceSolver) isNotWorsened(dim int) bool {
	isHot, decRatio := bs.getHotDecRatioByPriorities(dim)
	return !isHot || decRatio <= bs.minorDecRatio
}

func (bs *balanceSolver) getMinRate(dim int) float64 {
	switch dim {
	case utils.KeyDim:
		return bs.sche.conf.GetMinHotKeyRate()
	case utils.ByteDim:
		return bs.sche.conf.GetMinHotByteRate()
	case utils.QueryDim:
		return bs.sche.conf.GetMinHotQueryRate()
	}
	return -1
}

// betterThan checks if `bs.cur` is a better solution than `old`.
func (bs *balanceSolver) betterThanV1(old *solution) bool {
	if old == nil || bs.cur.progressiveRank <= splitProgressiveRank {
		return true
	}
	if bs.cur.progressiveRank != old.progressiveRank {
		// Smaller rank is better.
		return bs.cur.progressiveRank < old.progressiveRank
	}
	if (bs.cur.revertRegion == nil) != (old.revertRegion == nil) {
		// Fewer revertRegions are better.
		return bs.cur.revertRegion == nil
	}

	if r := bs.compareSrcStore(bs.cur.srcStore, old.srcStore); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r := bs.compareDstStore(bs.cur.dstStore, old.dstStore); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if bs.cur.mainPeerStat != old.mainPeerStat {
		// compare region
		if bs.resourceTy == writeLeader {
			return bs.cur.getPeersRateFromCache(bs.firstPriority) > old.getPeersRateFromCache(bs.firstPriority)
		}

		// We will firstly consider ensuring converge faster, secondly reduce oscillation
		firstCmp, secondCmp := bs.getRkCmpPrioritiesV1(old)
		switch bs.cur.progressiveRank {
		case -4: // isBetter(firstPriority) && isBetter(secondPriority)
			if firstCmp != 0 {
				return firstCmp > 0
			}
			return secondCmp > 0
		case -3: // isBetter(firstPriority) && isNotWorsened(secondPriority)
			if firstCmp != 0 {
				return firstCmp > 0
			}
			// prefer smaller second priority rate, to reduce oscillation
			return secondCmp < 0
		case -2: // isNotWorsened(firstPriority) && isBetter(secondPriority)
			if secondCmp != 0 {
				return secondCmp > 0
			}
			// prefer smaller first priority rate, to reduce oscillation
			return firstCmp < 0
		case -1: // isBetter(firstPriority)
			return firstCmp > 0
			// TODO: The smaller the difference between the value and the expectation, the better.
		}
	}

	return false
}

var dimToStep = [utils.DimLen]float64{
	utils.ByteDim:  100,
	utils.KeyDim:   10,
	utils.QueryDim: 10,
}

func (bs *balanceSolver) getRkCmpPrioritiesV1(old *solution) (firstCmp int, secondCmp int) {
	firstCmp = rankCmp(bs.cur.getPeersRateFromCache(bs.firstPriority), old.getPeersRateFromCache(bs.firstPriority), stepRank(0, dimToStep[bs.firstPriority]))
	secondCmp = rankCmp(bs.cur.getPeersRateFromCache(bs.secondPriority), old.getPeersRateFromCache(bs.secondPriority), stepRank(0, dimToStep[bs.secondPriority]))
	return
}

// compareSrcStore compares the source store of detail1, detail2, the result is:
// 1. if detail1 is better than detail2, return -1
// 2. if detail1 is worse than detail2, return 1
// 3. if detail1 is equal to detail2, return 0
// The comparison is based on the following principles:
// 1. select the min load of store in current and future, because we want to select the store as source store;
// 2. compare detail1 and detail2 by first priority and second priority, we pick the larger one to speed up the convergence;
// 3. if the first priority and second priority are equal, we pick the store with the smaller difference between current and future to minimize oscillations.
func (bs *balanceSolver) compareSrcStore(detail1, detail2 *statistics.StoreLoadDetail) int {
	if detail1 != detail2 {
		var lpCmp storeLPCmp
		if bs.resourceTy == writeLeader {
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

// compareDstStore compares the destination store of detail1, detail2, the result is:
// 1. if detail1 is better than detail2, return -1
// 2. if detail1 is worse than detail2, return 1
// 3. if detail1 is equal to detail2, return 0
// The comparison is based on the following principles:
// 1. select the max load of store in current and future, because we want to select the store as destination store;
// 2. compare detail1 and detail2 by first priority and second priority, we pick the smaller one to speed up the convergence;
// 3. if the first priority and second priority are equal, we pick the store with the smaller difference between current and future to minimize oscillations.
func (bs *balanceSolver) compareDstStore(detail1, detail2 *statistics.StoreLoadDetail) int {
	if detail1 != detail2 {
		// compare destination store
		var lpCmp storeLPCmp
		if bs.resourceTy == writeLeader {
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

// stepRank returns a function can calculate the discretized data,
// where `rate` will be discretized by `step`.
// `rate` is the speed of the dim, `step` is the step size of the discretized data.
func stepRank(rk0 float64, step float64) func(float64) int64 {
	return func(rate float64) int64 {
		return int64((rate - rk0) / step)
	}
}

// Once we are ready to build the operator, we must ensure the following things:
// 1. the source store and destination store in the current solution are not nil
// 2. the peer we choose as a source in the current solution is not nil, and it belongs to the source store
// 3. the region which owns the peer in the current solution is not nil, and its ID should equal to the peer's region ID
func (bs *balanceSolver) isReadyToBuild() bool {
	if !(bs.cur.srcStore != nil && bs.cur.dstStore != nil &&
		bs.cur.mainPeerStat != nil && bs.cur.mainPeerStat.StoreID == bs.cur.srcStore.GetID() &&
		bs.cur.region != nil && bs.cur.region.GetID() == bs.cur.mainPeerStat.ID()) {
		return false
	}
	if bs.cur.revertPeerStat == nil {
		return bs.cur.revertRegion == nil
	}
	return bs.cur.revertPeerStat.StoreID == bs.cur.dstStore.GetID() &&
		bs.cur.revertRegion != nil && bs.cur.revertRegion.GetID() == bs.cur.revertPeerStat.ID()
}

func (bs *balanceSolver) rankToDimStringV1() string {
	switch bs.cur.progressiveRank {
	case -4:
		return "all"
	case -3:
		return dimToString(bs.firstPriority)
	case -2:
		return dimToString(bs.secondPriority)
	case -1:
		return dimToString(bs.firstPriority) + "-only"
	default:
		return "none"
	}
}

func (bs *balanceSolver) buildOperators() (ops []*operator.Operator) {
	if !bs.isReadyToBuild() {
		return nil
	}

	splitRegions := make([]*core.RegionInfo, 0)
	if bs.opTy == movePeer {
		for _, region := range []*core.RegionInfo{bs.cur.region, bs.cur.revertRegion} {
			if region == nil {
				continue
			}
			if region.GetApproximateSize() > bs.GetSchedulerConfig().GetMaxMovableHotPeerSize() {
				hotSchedulerNeedSplitBeforeScheduleCounter.Inc()
				splitRegions = append(splitRegions, region)
			}
		}
	}
	if len(splitRegions) > 0 {
		return bs.createSplitOperator(splitRegions, bySize)
	}

	srcStoreID := bs.cur.srcStore.GetID()
	dstStoreID := bs.cur.dstStore.GetID()
	sourceLabel := strconv.FormatUint(srcStoreID, 10)
	targetLabel := strconv.FormatUint(dstStoreID, 10)
	dim := bs.rankToDimString()

	var createOperator func(region *core.RegionInfo, srcStoreID, dstStoreID uint64) (op *operator.Operator, typ string, err error)
	switch bs.rwTy {
	case utils.Read:
		createOperator = bs.createReadOperator
	case utils.Write:
		createOperator = bs.createWriteOperator
	}

	currentOp, typ, err := createOperator(bs.cur.region, srcStoreID, dstStoreID)
	if err == nil {
		bs.decorateOperator(currentOp, false, sourceLabel, targetLabel, typ, dim)
		ops = []*operator.Operator{currentOp}
		if bs.cur.revertRegion != nil {
			currentOp, typ, err = createOperator(bs.cur.revertRegion, dstStoreID, srcStoreID)
			if err == nil {
				bs.decorateOperator(currentOp, true, targetLabel, sourceLabel, typ, dim)
				ops = append(ops, currentOp)
			}
		}
	}

	if err != nil {
		log.Debug("fail to create operator", zap.Stringer("rw-type", bs.rwTy), zap.Stringer("op-type", bs.opTy), errs.ZapError(err))
		hotSchedulerCreateOperatorFailedCounter.Inc()
		return nil
	}

	return
}

// bucketFirstStat returns the first priority statistics of the bucket.
// if the first priority is query rate, it will return the second priority .
func (bs *balanceSolver) bucketFirstStat() utils.RegionStatKind {
	base := utils.RegionReadBytes
	if bs.rwTy == utils.Write {
		base = utils.RegionWriteBytes
	}
	offset := bs.firstPriority
	// todo: remove it if bucket's qps has been supported.
	if bs.firstPriority == utils.QueryDim {
		offset = bs.secondPriority
	}
	return base + utils.RegionStatKind(offset)
}

func (bs *balanceSolver) splitBucketsOperator(region *core.RegionInfo, keys [][]byte) *operator.Operator {
	splitKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		// make sure that this split key is in the region
		if keyutil.Between(region.GetStartKey(), region.GetEndKey(), key) {
			splitKeys = append(splitKeys, key)
		}
	}
	if len(splitKeys) == 0 {
		hotSchedulerNotFoundSplitKeysCounter.Inc()
		return nil
	}
	desc := splitHotReadBuckets
	if bs.rwTy == utils.Write {
		desc = splitHotWriteBuckets
	}

	op, err := operator.CreateSplitRegionOperator(desc, region, operator.OpSplit, pdpb.CheckPolicy_USEKEY, splitKeys)
	if err != nil {
		log.Error("fail to create split operator",
			zap.Stringer("resource-type", bs.resourceTy),
			errs.ZapError(err))
		return nil
	}
	hotSchedulerSplitSuccessCounter.Inc()
	return op
}

func (bs *balanceSolver) splitBucketsByLoad(region *core.RegionInfo, bucketStats []*buckets.BucketStat) *operator.Operator {
	// bucket key range maybe not match the region key range, so we should filter the invalid buckets.
	// filter some buckets key range not match the region start key and end key.
	stats := make([]*buckets.BucketStat, 0, len(bucketStats))
	startKey, endKey := region.GetStartKey(), region.GetEndKey()
	for _, stat := range bucketStats {
		if keyutil.Between(startKey, endKey, stat.StartKey) || keyutil.Between(startKey, endKey, stat.EndKey) {
			stats = append(stats, stat)
		}
	}
	if len(stats) == 0 {
		hotSchedulerHotBucketNotValidCounter.Inc()
		return nil
	}

	// if this region has only one buckets, we can't split it into two hot region, so skip it.
	if len(stats) == 1 {
		hotSchedulerOnlyOneBucketsHotCounter.Inc()
		return nil
	}
	totalLoads := uint64(0)
	dim := bs.bucketFirstStat()
	for _, stat := range stats {
		totalLoads += stat.Loads[dim]
	}

	// find the half point of the total loads.
	acc, splitIdx := uint64(0), 0
	for ; acc < totalLoads/2 && splitIdx < len(stats); splitIdx++ {
		acc += stats[splitIdx].Loads[dim]
	}
	if splitIdx <= 0 {
		hotSchedulerRegionBucketsSingleHotSpotCounter.Inc()
		return nil
	}
	splitKey := stats[splitIdx-1].EndKey
	// if the split key is not in the region, we should use the start key of the bucket.
	if !keyutil.Between(region.GetStartKey(), region.GetEndKey(), splitKey) {
		splitKey = stats[splitIdx-1].StartKey
	}
	op := bs.splitBucketsOperator(region, [][]byte{splitKey})
	if op != nil {
		op.AdditionalInfos["accLoads"] = strconv.FormatUint(acc-stats[splitIdx-1].Loads[dim], 10)
		op.AdditionalInfos["totalLoads"] = strconv.FormatUint(totalLoads, 10)
	}
	return op
}

// splitBucketBySize splits the region order by bucket count if the region is too big.
func (bs *balanceSolver) splitBucketBySize(region *core.RegionInfo) *operator.Operator {
	splitKeys := make([][]byte, 0)
	for _, key := range region.GetBuckets().GetKeys() {
		if keyutil.Between(region.GetStartKey(), region.GetEndKey(), key) {
			splitKeys = append(splitKeys, key)
		}
	}
	if len(splitKeys) == 0 {
		return nil
	}
	splitKey := splitKeys[len(splitKeys)/2]
	return bs.splitBucketsOperator(region, [][]byte{splitKey})
}

// createSplitOperator creates split operators for the given regions.
func (bs *balanceSolver) createSplitOperator(regions []*core.RegionInfo, strategy splitStrategy) []*operator.Operator {
	if len(regions) == 0 {
		return nil
	}
	ids := make([]uint64, len(regions))
	for i, region := range regions {
		ids[i] = region.GetID()
	}
	operators := make([]*operator.Operator, 0)
	var hotBuckets map[uint64][]*buckets.BucketStat

	createFunc := func(region *core.RegionInfo) {
		switch strategy {
		case bySize:
			if op := bs.splitBucketBySize(region); op != nil {
				operators = append(operators, op)
			}
		case byLoad:
			if hotBuckets == nil {
				hotBuckets = bs.SchedulerCluster.BucketsStats(bs.minHotDegree, ids...)
			}
			stats, ok := hotBuckets[region.GetID()]
			if !ok {
				hotSchedulerRegionBucketsNotHotCounter.Inc()
				return
			}
			if op := bs.splitBucketsByLoad(region, stats); op != nil {
				operators = append(operators, op)
			}
		}
	}

	for _, region := range regions {
		createFunc(region)
	}
	// the split bucket's priority is highest
	if len(operators) > 0 {
		bs.cur.progressiveRank = splitProgressiveRank
	}
	return operators
}

func (bs *balanceSolver) createReadOperator(region *core.RegionInfo, srcStoreID, dstStoreID uint64) (op *operator.Operator, typ string, err error) {
	if region.GetStorePeer(dstStoreID) != nil {
		typ = "transfer-leader"
		op, err = operator.CreateTransferLeaderOperator(
			"transfer-hot-read-leader",
			bs,
			region,
			srcStoreID,
			dstStoreID,
			[]uint64{},
			operator.OpHotRegion)
	} else {
		srcPeer := region.GetStorePeer(srcStoreID) // checked in `filterHotPeers`
		dstPeer := &metapb.Peer{StoreId: dstStoreID, Role: srcPeer.Role}
		if region.GetLeader().GetStoreId() == srcStoreID {
			typ = "move-leader"
			op, err = operator.CreateMoveLeaderOperator(
				"move-hot-read-leader",
				bs,
				region,
				operator.OpHotRegion,
				srcStoreID,
				dstPeer)
		} else {
			typ = "move-peer"
			op, err = operator.CreateMovePeerOperator(
				"move-hot-read-peer",
				bs,
				region,
				operator.OpHotRegion,
				srcStoreID,
				dstPeer)
		}
	}
	return
}

func (bs *balanceSolver) createWriteOperator(region *core.RegionInfo, srcStoreID, dstStoreID uint64) (op *operator.Operator, typ string, err error) {
	if region.GetStorePeer(dstStoreID) != nil {
		typ = "transfer-leader"
		op, err = operator.CreateTransferLeaderOperator(
			"transfer-hot-write-leader",
			bs,
			region,
			srcStoreID,
			dstStoreID,
			[]uint64{},
			operator.OpHotRegion)
	} else {
		srcPeer := region.GetStorePeer(srcStoreID) // checked in `filterHotPeers`
		dstPeer := &metapb.Peer{StoreId: dstStoreID, Role: srcPeer.Role}
		typ = "move-peer"
		op, err = operator.CreateMovePeerOperator(
			"move-hot-write-peer",
			bs,
			region,
			operator.OpHotRegion,
			srcStoreID,
			dstPeer)
	}
	return
}

func (bs *balanceSolver) decorateOperator(op *operator.Operator, isRevert bool, sourceLabel, targetLabel, typ, dim string) {
	op.SetPriorityLevel(constant.High)
	op.FinishedCounters = append(op.FinishedCounters,
		hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), sourceLabel, "out", dim),
		hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), targetLabel, "in", dim),
		balanceDirectionCounter.WithLabelValues(bs.sche.GetName(), sourceLabel, targetLabel))
	op.Counters = append(op.Counters,
		hotSchedulerNewOperatorCounter,
		opCounter(typ))
	if isRevert {
		op.FinishedCounters = append(op.FinishedCounters,
			hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), sourceLabel, "out-for-revert", dim),
			hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), targetLabel, "in-for-revert", dim))
	}
}

func opCounter(typ string) prometheus.Counter {
	switch typ {
	case "move-leader":
		return hotSchedulerMoveLeaderCounter
	case "move-peer":
		return hotSchedulerMovePeerCounter
	default: // transfer-leader
		return hotSchedulerTransferLeaderCounter
	}
}

func (bs *balanceSolver) logBestSolution() {
	best := bs.best
	if best == nil {
		return
	}

	if best.revertRegion != nil {
		// Log more information on solutions containing revertRegion
		srcFirstRate, dstFirstRate := best.getExtremeLoad(bs.firstPriority)
		srcSecondRate, dstSecondRate := best.getExtremeLoad(bs.secondPriority)
		mainFirstRate := best.mainPeerStat.GetLoad(bs.firstPriority)
		mainSecondRate := best.mainPeerStat.GetLoad(bs.secondPriority)
		log.Info("use solution with revert regions",
			zap.Uint64("src-store", best.srcStore.GetID()),
			zap.Float64("src-first-rate", srcFirstRate),
			zap.Float64("src-second-rate", srcSecondRate),
			zap.Uint64("dst-store", best.dstStore.GetID()),
			zap.Float64("dst-first-rate", dstFirstRate),
			zap.Float64("dst-second-rate", dstSecondRate),
			zap.Uint64("main-region", best.region.GetID()),
			zap.Float64("main-first-rate", mainFirstRate),
			zap.Float64("main-second-rate", mainSecondRate),
			zap.Uint64("revert-regions", best.revertRegion.GetID()),
			zap.Float64("peers-first-rate", best.getPeersRateFromCache(bs.firstPriority)),
			zap.Float64("peers-second-rate", best.getPeersRateFromCache(bs.secondPriority)))
	}
}

// calcPendingInfluence return the calculate weight of one Operator, the value will between [0,1]
func calcPendingInfluence(op *operator.Operator, maxZombieDur time.Duration) (weight float64, needGC bool) {
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

type opType int

const (
	movePeer opType = iota
	transferLeader
	moveLeader
)

func (ty opType) String() string {
	switch ty {
	case movePeer:
		return "move-peer"
	case moveLeader:
		return "move-leader"
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

// String implements fmt.Stringer interface.
func (ty resourceType) String() string {
	switch ty {
	case writePeer:
		return "write-peer"
	case writeLeader:
		return "write-leader"
	case readPeer:
		return "read-peer"
	case readLeader:
		return "read-leader"
	default:
		return ""
	}
}

func toResourceType(rwTy utils.RWType, opTy opType) resourceType {
	switch rwTy {
	case utils.Write:
		switch opTy {
		case movePeer:
			return writePeer
		case transferLeader:
			return writeLeader
		}
	case utils.Read:
		switch opTy {
		case movePeer:
			return readPeer
		case transferLeader:
			return readLeader
		}
	}
	panic(fmt.Sprintf("invalid arguments for toResourceType: rwTy = %v, opTy = %v", rwTy, opTy))
}

func buildResourceType(rwTy utils.RWType, ty constant.ResourceKind) resourceType {
	switch rwTy {
	case utils.Write:
		switch ty {
		case constant.RegionKind:
			return writePeer
		case constant.LeaderKind:
			return writeLeader
		}
	case utils.Read:
		switch ty {
		case constant.RegionKind:
			return readPeer
		case constant.LeaderKind:
			return readLeader
		}
	}
	panic(fmt.Sprintf("invalid arguments for buildResourceType: rwTy = %v, ty = %v", rwTy, ty))
}

func stringToDim(name string) int {
	switch name {
	case utils.BytePriority:
		return utils.ByteDim
	case utils.KeyPriority:
		return utils.KeyDim
	case utils.QueryPriority:
		return utils.QueryDim
	}
	return utils.ByteDim
}

func dimToString(dim int) string {
	switch dim {
	case utils.ByteDim:
		return utils.BytePriority
	case utils.KeyDim:
		return utils.KeyPriority
	case utils.QueryDim:
		return utils.QueryPriority
	default:
		return ""
	}
}

func prioritiesToDim(priorities []string) (firstPriority int, secondPriority int) {
	return stringToDim(priorities[0]), stringToDim(priorities[1])
}

// tooHotNeedSplit returns true if any dim of the hot region is greater than the store threshold.
func (bs *balanceSolver) tooHotNeedSplit(store *statistics.StoreLoadDetail, region *statistics.HotPeerStat, splitThresholds float64) bool {
	return bs.checkByPriorityAndTolerance(store.LoadPred.Current.Loads, func(i int) bool {
		return region.Loads[i] > store.LoadPred.Current.Loads[i]*splitThresholds
	})
}

type splitStrategy int

const (
	byLoad splitStrategy = iota
	bySize
)
