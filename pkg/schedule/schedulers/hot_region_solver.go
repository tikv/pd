// Copyright 2025 TiKV Project Authors.
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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

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
	// curFit is the placement-rule fit for the current region.
	curFit *placement.RegionFit
	// curScope and revertScope contain placement-scoped load statistics. A nil
	// scope uses the existing engine-wide statistics.
	curScope             *placementLoadScope
	revertScope          *placementLoadScope
	placementScopeCache  map[placementScopeKey]*placementLoadScope
	placementV2Enabled   bool
	placementCanRestrict [2]bool

	best *solution
	ops  []*operator.Operator

	// maxSrc and minDst are used to calculate the rank.
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

	rank
}

type placementScopeKey struct {
	rule      *placement.Rule
	ruleScope string
	version   uint64
	isTiKV    bool
}

type placementLoadScope struct {
	expect statistics.StoreLoad
	stddev statistics.StoreLoad
}

type placementLoadState struct {
	enabled     bool
	canRestrict [2]bool
}

func (bs *balanceSolver) init(placementState *placementLoadState) {
	// Load the configuration items of the scheduler.
	bs.resourceTy = toResourceType(bs.rwTy, bs.opTy)
	bs.maxPeerNum = bs.sche.conf.getMaxPeerNumber()
	bs.minHotDegree = bs.GetSchedulerConfig().GetHotRegionCacheHitsThreshold()
	bs.firstPriority, bs.secondPriority = prioritiesToDim(bs.getPriorities())
	bs.greatDecRatio, bs.minorDecRatio = bs.sche.conf.getGreatDecRatio(), bs.sche.conf.getMinorDecRatio()
	rankFormulaVersion := bs.sche.conf.getRankFormulaVersion()
	switch rankFormulaVersion {
	case "v1":
		bs.rank = initRankV1(bs)
	default:
		bs.rank = initRankV2(bs)
	}

	// Init store load detail according to the type.
	bs.stLoadDetail = bs.sche.stLoadInfos[bs.resourceTy]
	if placementState == nil {
		bs.placementV2Enabled = bs.GetSchedulerConfig().IsPlacementRulesEnabled() && rankFormulaVersion == "v2"
		if bs.placementV2Enabled {
			bs.placementCanRestrict[0] = bs.GetRuleManager().MayRestrictStoreLoad(true)
			bs.placementCanRestrict[1] = bs.GetRuleManager().MayRestrictStoreLoad(false)
		}
	} else {
		bs.placementV2Enabled = placementState.enabled
		bs.placementCanRestrict = placementState.canRestrict
	}

	bs.maxSrc = &statistics.StoreLoad{}
	bs.minDst = &statistics.StoreLoad{HotPeerCount: math.MaxFloat64}
	for i := range bs.minDst.Loads {
		bs.minDst.Loads[i] = math.MaxFloat64
	}
	maxCur := &statistics.StoreLoad{}

	bs.filteredHotPeers = make(map[uint64][]*statistics.HotPeerStat)
	bs.nthHotPeer = make(map[uint64][]*statistics.HotPeerStat)
	checkPlacementRestriction := placementState == nil && bs.placementV2Enabled
	for _, detail := range bs.stLoadDetail {
		if checkPlacementRestriction {
			bs.recordPlacementRestriction(detail)
		}
		bs.maxSrc = statistics.MaxLoad(bs.maxSrc, detail.LoadPred.Min())
		bs.minDst = statistics.MinLoad(bs.minDst, detail.LoadPred.Max())
		maxCur = statistics.MaxLoad(maxCur, &detail.LoadPred.Current)
		bs.nthHotPeer[detail.GetID()] = make([]*statistics.HotPeerStat, utils.DimLen)
		bs.filteredHotPeers[detail.GetID()] = bs.filterHotPeers(detail)
	}

	rankStepRatios := []float64{
		utils.ByteDim:  bs.sche.conf.getByteRankStepRatio(),
		utils.KeyDim:   bs.sche.conf.getKeyRankStepRatio(),
		utils.QueryDim: bs.sche.conf.getQueryRateRankStepRatio(),
		utils.CPUDim:   bs.sche.conf.getCPURateRankStepRatio(),
	}
	var stepLoads statistics.Loads
	for i := range stepLoads {
		stepLoads[i] = maxCur.Loads[i] * rankStepRatios[i]
	}
	bs.rankStep = &statistics.StoreLoad{
		Loads:        stepLoads,
		HotPeerCount: maxCur.HotPeerCount * bs.sche.conf.getCountRankStepRatio(),
	}
}

func (bs *balanceSolver) isSelectedDim(dim int) bool {
	return dim == bs.firstPriority || dim == bs.secondPriority
}

func (bs *balanceSolver) getPriorities() []string {
	querySupport := bs.sche.conf.checkQuerySupport(bs.SchedulerCluster)
	cpuSupport := bs.sche.conf.checkCPUSupport(bs.SchedulerCluster)
	// For read, transfer-leader and move-peer have the same priority config
	// For write, they are different
	switch bs.resourceTy {
	case readLeader, readPeer:
		return adjustPrioritiesConfig(querySupport, cpuSupport, bs.sche.conf.getReadPriorities(), getReadPriorities)
	case writeLeader:
		return adjustPrioritiesConfig(querySupport, cpuSupport, bs.sche.conf.getWriteLeaderPriorities(), getWriteLeaderPriorities)
	case writePeer:
		return adjustPrioritiesConfig(querySupport, cpuSupport, bs.sche.conf.getWritePeerPriorities(), getWritePeerPriorities)
	}
	log.Error("illegal type or illegal operator while getting the priority", zap.String("type", bs.rwTy.String()), zap.String("operator", bs.opTy.String()))
	return []string{}
}

func newBalanceSolver(sche *hotScheduler, cluster sche.SchedulerCluster, rwTy utils.RWType, opTy opType) *balanceSolver {
	return newBalanceSolverWithPlacementState(sche, cluster, rwTy, opTy, nil)
}

func newBalanceSolverWithPlacementState(
	sche *hotScheduler,
	cluster sche.SchedulerCluster,
	rwTy utils.RWType,
	opTy opType,
	placementState *placementLoadState,
) *balanceSolver {
	bs := &balanceSolver{
		SchedulerCluster: cluster,
		sche:             sche,
		rwTy:             rwTy,
		opTy:             opTy,
	}
	bs.init(placementState)
	return bs
}

func (bs *balanceSolver) isValid() bool {
	if bs.SchedulerCluster == nil || bs.sche == nil || bs.stLoadDetail == nil {
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
	tryUpdateBestSolution := func() {
		if label, ok := bs.filterUniformStore(); ok {
			bs.skipCounter(label).Inc()
			return
		}
		if bs.isAvailable(bs.cur) && bs.betterThan(bs.best) {
			if !bs.isReadyToBuild() {
				return
			}
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
	affinityFilter := filter.NewAffinityFilter(bs.SchedulerCluster)
	splitThresholds := bs.sche.conf.getSplitThresholds()
	for _, srcStore := range bs.filterSrcStores() {
		bs.cur.srcStore = srcStore
		srcStoreID := srcStore.GetID()
		usePlacementScope := bs.mayUsePlacementScope(srcStore.IsTiKV())
		sourceResultRecorded := !usePlacementScope
		var globalSourceFailure, sourceFailure string
		if usePlacementScope {
			globalSourceFailure = bs.sourceStoreFailure(srcStore, nil)
			sourceFailure = globalSourceFailure
		}
		checkedRegion := false
		for _, mainPeerStat := range bs.filteredHotPeers[srcStoreID] {
			region, fit := bs.getRegion(mainPeerStat, srcStoreID)
			if region == nil {
				continue
			}
			bs.cur.region, bs.curFit = region, fit
			checkedRegion = true
			bs.curScope = nil
			if usePlacementScope {
				bs.curScope = bs.prepareForRegion(bs.cur.region, bs.curFit, srcStore)
				sourceFailure = globalSourceFailure
				if bs.curScope != nil {
					sourceFailure = bs.sourceStoreFailure(srcStore, bs.curScope)
				}
				if sourceFailure != "" {
					continue
				}
			}
			if !sourceResultRecorded {
				bs.recordSourceStoreResult("src-store-succ", srcStoreID)
				sourceResultRecorded = true
			}
			if bs.opTy == movePeer && !snapshotFilter.Select(bs.cur.region).IsOK() {
				hotSchedulerSnapshotSenderLimitCounter.Inc()
				continue
			}
			if !affinityFilter.Select(bs.cur.region).IsOK() {
				hotSchedulerIgnoredAffinity.Inc()
				continue
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
						revertRegion, revertFit := bs.getRegion(revertPeerStat, dstStoreID)
						if revertRegion == nil || revertRegion.GetID() == bs.cur.region.GetID() ||
							!allowRevertRegion(revertRegion, srcStoreID) ||
							bs.placementV2Enabled && !bs.isRevertRegionValid(revertRegion, revertFit) {
							continue
						}
						bs.revertScope = nil
						if bs.mayUsePlacementScope(dstStore.IsTiKV()) {
							bs.revertScope = bs.prepareForRegion(revertRegion, revertFit, dstStore)
						}
						if bs.revertScope != nil && (bs.sourceStoreFailure(dstStore, bs.revertScope) != "" ||
							bs.destinationStoreFailure(srcStore, bs.revertScope, bs.dstToleranceRatio(srcStore)) != "") {
							continue
						}
						bs.cur.revertPeerStat = revertPeerStat
						bs.cur.revertRegion = revertRegion
						bs.calcProgressiveRank()
						tryUpdateBestSolution()
					}
					bs.cur.revertPeerStat = nil
					bs.cur.revertRegion = nil
					bs.revertScope = nil
				}
			}
		}
		if !sourceResultRecorded {
			if !checkedRegion {
				sourceFailure = globalSourceFailure
			}
			bs.recordSourceStoreResult(sourceFailureOrSuccess(sourceFailure), srcStoreID)
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
		case "cpu":
			return readSkipCPUDimUniformStoreCounter
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
	// Ensure source and destination stores have the exact same engine type.
	// We need to distinguish between TiKV, TiFlash Write, and TiFlash Compute nodes.
	if !isSplit {
		srcIsTiFlashWrite := bs.best.srcStore.IsTiFlashWrite()
		dstIsTiFlashWrite := bs.best.dstStore.IsTiFlashWrite()
		srcIsTiFlashCompute := bs.best.srcStore.IsTiFlashCompute()
		dstIsTiFlashCompute := bs.best.dstStore.IsTiFlashCompute()

		// Check if engine types match exactly
		if srcIsTiFlashWrite != dstIsTiFlashWrite || srcIsTiFlashCompute != dstIsTiFlashCompute {
			hotSchedulerNotSameEngineCounter.Inc()
			return false
		}
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
	infl := statistics.Influence{Loads: make([]float64, utils.RegionStatCount), HotPeerCount: 1}
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
			return bs.sche.conf.getStoreStatZombieDuration()
		}
		return bs.sche.conf.getRegionsStatZombieDuration()
	case writePeer:
		// TiFlash Write nodes (TiFlash Compute nodes are filtered out in filterSrcStores)
		// use RegionsStatZombieDuration, while TiKV uses StoreStatZombieDuration.
		if bs.best.srcStore.IsTiFlashWrite() {
			return bs.sche.conf.getRegionsStatZombieDuration()
		}
		return bs.sche.conf.getStoreStatZombieDuration()
	default:
		return bs.sche.conf.getStoreStatZombieDuration()
	}
}

// filterSrcStores selects stores with hot peers that support the resource type.
// The global load safeguards are deferred only when placement may narrow the
// store population for the engine.
func (bs *balanceSolver) filterSrcStores() map[uint64]*statistics.StoreLoadDetail {
	ret := make(map[uint64]*statistics.StoreLoadDetail)
	confEnableForTiFlash := bs.sche.conf.getEnableForTiFlash()
	for id, detail := range bs.stLoadDetail {
		if !detail.IsTiKV() {
			if !confEnableForTiFlash || detail.IsTiFlashCompute() {
				continue
			}
			if bs.rwTy != utils.Write || bs.opTy != movePeer {
				continue
			}
		}
		if len(detail.HotPeers) == 0 {
			continue
		}
		if !bs.mayUsePlacementScope(detail.IsTiKV()) {
			failure := bs.sourceStoreFailure(detail, nil)
			bs.recordSourceStoreResult(sourceFailureOrSuccess(failure), id)
			if failure != "" {
				continue
			}
		}
		ret[id] = detail
	}
	return ret
}

func (bs *balanceSolver) sourceStoreFailure(detail *statistics.StoreLoadDetail, scope *placementLoadScope) string {
	srcToleranceRatio := bs.sche.conf.getSrcToleranceRatio()
	if !detail.IsTiKV() {
		srcToleranceRatio += tiflashToleranceRatioCorrection
	}
	expect := bs.expectLoad(detail, scope)
	if !bs.checkSrcByPriorityAndTolerance(detail.LoadPred.Min(), expect, srcToleranceRatio) {
		return "src-store-failed"
	}
	if !bs.checkSrcHistoryLoadsByPriorityAndTolerance(&detail.LoadPred.Current, expect, srcToleranceRatio) {
		return "src-store-history-loads-failed"
	}
	return ""
}

func sourceFailureOrSuccess(failure string) string {
	if failure == "" {
		return "src-store-succ"
	}
	return failure
}

func (*balanceSolver) expectLoad(detail *statistics.StoreLoadDetail, scope *placementLoadScope) *statistics.StoreLoad {
	if scope != nil {
		return &scope.expect
	}
	return &detail.LoadPred.Expect
}

func (bs *balanceSolver) recordSourceStoreResult(result string, storeID uint64) {
	hotSchedulerResultCounter.WithLabelValues(result+"-"+bs.resourceTy.String(), strconv.FormatUint(storeID, 10)).Inc()
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
		union := sortHotPeers(firstSort, secondSort, bs.maxPeerNum)
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

func sortHotPeers[T any](firstSort, secondSort []*T, maxPeerNum int) map[*T]struct{} {
	union := make(map[*T]struct{}, maxPeerNum)
	// At most MaxPeerNum peers, to prevent balanceSolver.solve() too slow.
	for len(union) < maxPeerNum {
		if len(firstSort) == 0 && len(secondSort) == 0 {
			break
		}
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

// isRegionAvailable checks whether the given region is available to schedule
// and returns its placement fit when placement rules are enabled.
func (bs *balanceSolver) isRegionAvailable(region *core.RegionInfo) (*placement.RegionFit, bool) {
	if region == nil {
		hotSchedulerNoRegionCounter.Inc()
		return nil, false
	}

	if !filter.IsRegionHealthyAllowPending(region) {
		hotSchedulerUnhealthyReplicaCounter.Inc()
		return nil, false
	}

	var fit *placement.RegionFit
	replicated := false
	if bs.GetSchedulerConfig().IsPlacementRulesEnabled() {
		fit = bs.GetRuleManager().FitRegion(bs.GetBasicCluster(), region)
		replicated = fit.IsSatisfied()
	} else {
		replicated = filter.IsRegionReplicated(bs.SchedulerCluster, region)
	}
	if !replicated {
		log.Debug("region has abnormal replica count", zap.String("scheduler", bs.sche.GetName()), zap.Uint64("region-id", region.GetID()))
		hotSchedulerAbnormalReplicaCounter.Inc()
		return nil, false
	}

	return fit, true
}

func (bs *balanceSolver) getRegion(peerStat *statistics.HotPeerStat, storeID uint64) (*core.RegionInfo, *placement.RegionFit) {
	region := bs.GetRegion(peerStat.ID())
	fit, available := bs.isRegionAvailable(region)
	if !available {
		return nil, nil
	}

	switch bs.opTy {
	case movePeer:
		srcPeer := region.GetStorePeer(storeID)
		if srcPeer == nil {
			log.Debug("region does not have a peer on source store, maybe stat out of date",
				zap.Uint64("region-id", peerStat.ID()),
				zap.Uint64("leader-store-id", storeID))
			return nil, nil
		}
	case transferLeader:
		if region.GetLeader().GetStoreId() != storeID {
			log.Debug("region leader is not on source store, maybe stat out of date",
				zap.Uint64("region-id", peerStat.ID()),
				zap.Uint64("leader-store-id", storeID))
			return nil, nil
		}
	default:
		return nil, nil
	}

	return region, fit
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
			filter.NewHotRegionEvictedTargetFilter(bs.sche.GetName()),
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.region.GetStoreIDs(), bs.cur.region.GetStoreIDs()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
			filter.NewPlacementSafeguard(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(), bs.GetRuleManager(), bs.cur.region, srcStore, bs.curFit),
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
			moveLeaderFilters := []filter.Filter{
				&filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true, OperatorLevel: constant.High},
				filter.NewHotRegionEvictedTargetFilter(bs.sche.GetName()),
			}
			if leaderFilter := filter.NewPlacementLeaderSafeguardWithFit(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(), bs.GetRuleManager(), bs.cur.region, srcStore, bs.curFit, true /*allowMoveLeader*/); leaderFilter != nil {
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
			if leaderFilter := filter.NewPlacementLeaderSafeguardWithFit(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(), bs.GetRuleManager(), bs.cur.region, srcStore, bs.curFit, false /*allowMoveLeader*/); leaderFilter != nil {
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

func (bs *balanceSolver) isRevertRegionValid(region *core.RegionInfo, fit *placement.RegionFit) bool {
	source, target := bs.cur.dstStore.StoreInfo, bs.cur.srcStore.StoreInfo
	var safeguard filter.Filter
	switch bs.opTy {
	case movePeer:
		safeguard = filter.NewPlacementSafeguard(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(),
			bs.GetRuleManager(), region, source, fit)
	case transferLeader:
		safeguard = filter.NewPlacementLeaderSafeguardWithFit(bs.sche.GetName(), bs.GetSchedulerConfig(), bs.GetBasicCluster(),
			bs.GetRuleManager(), region, source, fit, bs.rwTy == utils.Read)
	}
	return safeguard == nil || safeguard.Target(bs.GetSchedulerConfig(), target).IsOK()
}

func (bs *balanceSolver) pickDstStores(filters []filter.Filter, candidates []*statistics.StoreLoadDetail) map[uint64]*statistics.StoreLoadDetail {
	ret := make(map[uint64]*statistics.StoreLoadDetail, len(candidates))
	confEnableForTiFlash := bs.sche.conf.getEnableForTiFlash()
	for _, detail := range candidates {
		store := detail.StoreInfo
		if !detail.IsTiKV() {
			if !confEnableForTiFlash || detail.IsTiFlashCompute() {
				continue
			}
			if bs.rwTy != utils.Write || bs.opTy != movePeer {
				continue
			}
		}
		if filter.Target(bs.GetSchedulerConfig(), store, filters) {
			id := store.GetID()
			if failure := bs.destinationStoreFailure(detail, bs.curScope, bs.dstToleranceRatio(detail)); failure != "" {
				hotSchedulerResultCounter.WithLabelValues(failure+"-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
				continue
			}

			hotSchedulerResultCounter.WithLabelValues("dst-store-succ-"+bs.resourceTy.String(), strconv.FormatUint(id, 10)).Inc()
			ret[id] = detail
		}
	}
	return ret
}

func (bs *balanceSolver) dstToleranceRatio(detail *statistics.StoreLoadDetail) float64 {
	ratio := bs.sche.conf.getDstToleranceRatio()
	if !detail.IsTiKV() {
		ratio += tiflashToleranceRatioCorrection
	}
	return ratio
}

func (bs *balanceSolver) destinationStoreFailure(detail *statistics.StoreLoadDetail, scope *placementLoadScope, toleranceRatio float64) string {
	expect := bs.expectLoad(detail, scope)
	if !bs.checkDstByPriorityAndTolerance(detail.LoadPred.Max(), expect, toleranceRatio) {
		return "dst-store-failed"
	}
	if !bs.checkDstHistoryLoadsByPriorityAndTolerance(&detail.LoadPred.Current, expect, toleranceRatio) {
		return "dst-store-history-loads-failed"
	}
	return ""
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

func (bs *balanceSolver) checkByPriorityAndToleranceAllOf(loads statistics.Loads, f func(int) bool) bool {
	return slice.AllOf(loads[:], func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return true
	})
}

func (bs *balanceSolver) checkHistoryLoadsByPriorityAndToleranceAllOf(loads statistics.HistoryLoads, f func(int) bool) bool {
	return slice.AllOf(loads[:], func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return true
	})
}

func (bs *balanceSolver) checkByPriorityAndToleranceAnyOf(loads statistics.Loads, f func(int) bool) bool {
	return slice.AnyOf(loads[:], func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return false
	})
}

func (bs *balanceSolver) checkHistoryByPriorityAndToleranceAnyOf(loads statistics.HistoryLoads, f func(int) bool) bool {
	return slice.AnyOf(loads[:], func(i int) bool {
		if bs.isSelectedDim(i) {
			return f(i)
		}
		return false
	})
}

func (bs *balanceSolver) checkByPriorityAndToleranceFirstOnly(_ statistics.Loads, f func(int) bool) bool {
	return f(bs.firstPriority)
}

func (bs *balanceSolver) checkHistoryLoadsByPriorityAndToleranceFirstOnly(_ statistics.HistoryLoads, f func(int) bool) bool {
	return f(bs.firstPriority)
}

func (bs *balanceSolver) enableExpectation() bool {
	return bs.sche.conf.getDstToleranceRatio() > 0 && bs.sche.conf.getSrcToleranceRatio() > 0
}

// prepareForRegion derives the stores that can legally carry the current load.
// Leaders can move between different RuleFits with the same placement role.
func (bs *balanceSolver) prepareForRegion(region *core.RegionInfo, fit *placement.RegionFit, source *statistics.StoreLoadDetail) *placementLoadScope {
	if fit == nil {
		return nil
	}
	sourcePeer := region.GetStorePeer(source.GetID())
	if sourcePeer == nil {
		return nil
	}
	sourceFit := fit.GetRuleFit(sourcePeer.GetId())
	if sourceFit == nil {
		return nil
	}

	rules := []*placement.Rule{sourceFit.Rule}
	if bs.opTy == transferLeader {
		rules = rules[:0]
		for _, ruleFit := range fit.RuleFits {
			if ruleFit.Rule.Role == sourceFit.Rule.Role {
				rules = append(rules, ruleFit.Rule)
			}
		}
	}
	return bs.getPlacementLoadScope(rules, source.IsTiKV())
}

func (bs *balanceSolver) recordPlacementRestriction(detail *statistics.StoreLoadDetail) {
	isTiKV := detail.IsTiKV()
	engine := 0
	if !isTiKV {
		engine = 1
	}
	if bs.placementCanRestrict[engine] {
		return
	}
	var constraints []placement.LabelConstraint
	if !isTiKV {
		constraints = []placement.LabelConstraint{{Key: core.EngineKey, Op: placement.In, Values: []string{core.EngineTiFlash}}}
	}
	if !placement.MatchLabelConstraints(detail.StoreInfo, constraints) {
		bs.placementCanRestrict[engine] = true
	}
}

func (bs *balanceSolver) mayUsePlacementScope(isTiKV bool) bool {
	if !bs.placementV2Enabled {
		return false
	}
	if isTiKV {
		return bs.placementCanRestrict[0]
	}
	return bs.placementCanRestrict[1]
}

// getPlacementLoadScope calculates local safeguards only when the placement
// population is narrower than the existing engine-wide population.
func (bs *balanceSolver) getPlacementLoadScope(rules []*placement.Rule, isTiKV bool) *placementLoadScope {
	if len(rules) == 0 {
		return nil
	}

	var ruleKey placementScopeKey
	if len(rules) == 1 {
		ruleKey = placementScopeKey{rule: rules[0], version: rules[0].Version, isTiKV: isTiKV}
		if scope, ok := bs.placementScopeCache[ruleKey]; ok {
			return scope
		}
	}

	var builder strings.Builder
	builder.WriteString(strconv.Itoa(len(rules)))
	builder.WriteByte(':')
	for _, rule := range rules {
		builder.WriteString(strconv.Itoa(len(rule.LabelConstraints)))
		builder.WriteByte(':')
		for _, constraint := range rule.LabelConstraints {
			appendPlacementScopeKeyPart(&builder, constraint.Key)
			appendPlacementScopeKeyPart(&builder, string(constraint.Op))
			builder.WriteString(strconv.Itoa(len(constraint.Values)))
			builder.WriteByte(':')
			for _, value := range constraint.Values {
				appendPlacementScopeKeyPart(&builder, value)
			}
		}
	}
	key := placementScopeKey{ruleScope: builder.String(), isTiKV: isTiKV}
	scope, ok := bs.placementScopeCache[key]

	if !ok {
		allCount := 0
		var summary statistics.StoreLoadSummary
		var details []*statistics.StoreLoadDetail
		for _, detail := range bs.stLoadDetail {
			if detail.IsTiKV() != isTiKV {
				continue
			}
			allCount++
			if !slice.AnyOf(rules, func(i int) bool {
				return placement.MatchLabelConstraints(detail.StoreInfo, rules[i].LabelConstraints)
			}) {
				continue
			}
			details = append(details, detail)
			summary.Add(&detail.LoadPred.Current)
		}
		if len(details) < allCount {
			expect, stddev := summary.Result(details)
			scope = &placementLoadScope{expect: expect, stddev: stddev}
		}
		if bs.placementScopeCache == nil {
			bs.placementScopeCache = make(map[placementScopeKey]*placementLoadScope)
		}
		bs.placementScopeCache[key] = scope
	}
	if len(rules) == 1 {
		bs.placementScopeCache[ruleKey] = scope
	}
	return scope
}

func appendPlacementScopeKeyPart(builder *strings.Builder, value string) {
	builder.WriteString(strconv.Itoa(len(value)))
	builder.WriteByte(':')
	builder.WriteString(value)
}

func (bs *balanceSolver) isUniformFirstPriority(store *statistics.StoreLoadDetail) bool {
	// first priority should be more uniform than second priority
	return bs.isUniform(store, bs.firstPriority, stddevThreshold*0.5)
}

func (bs *balanceSolver) isUniformSecondPriority(store *statistics.StoreLoadDetail) bool {
	return bs.isUniform(store, bs.secondPriority, stddevThreshold)
}

func (bs *balanceSolver) isUniform(store *statistics.StoreLoadDetail, dim int, threshold float64) bool {
	uniform := func(scope *placementLoadScope) bool {
		if scope != nil {
			return scope.stddev.Loads[dim] < threshold
		}
		return store.IsUniform(dim, threshold)
	}
	return uniform(bs.curScope) || bs.cur.revertRegion != nil && uniform(bs.revertScope)
}

// isTolerance checks source store and target store by checking the difference value with pendingAmpFactor * pendingPeer.
// This will make the hot region scheduling slow even serialize running when each 2 store's pending influence is close.
func (bs *balanceSolver) isTolerance(dim int, reverse bool) bool {
	srcRate, dstRate := bs.cur.getCurrentLoad(dim)
	srcPending, dstPending := bs.cur.getPendingLoad(dim)
	if reverse {
		srcRate, dstRate = dstRate, srcRate
		srcPending, dstPending = dstPending, srcPending
	}

	if srcRate <= dstRate {
		return false
	}
	pendingAmp := 1 + pendingAmpFactor*srcRate/(srcRate-dstRate)
	return srcRate-pendingAmp*srcPending > dstRate+pendingAmp*dstPending
}

func (bs *balanceSolver) getMinRate(dim int) float64 {
	switch dim {
	case utils.KeyDim:
		return bs.sche.conf.getMinHotKeyRate()
	case utils.ByteDim:
		return bs.sche.conf.getMinHotByteRate()
	case utils.QueryDim:
		return bs.sche.conf.getMinHotQueryRate()
	case utils.CPUDim:
		return bs.sche.conf.getMinHotCPURate()
	}
	return -1
}

var dimToStep = [utils.DimLen]float64{
	utils.ByteDim:  100,
	utils.KeyDim:   10,
	utils.QueryDim: 10,
	utils.CPUDim:   10,
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
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.HotPeerCount)),
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
					stLdRankCmp(stLdCount, stepRank(0, bs.rankStep.HotPeerCount)),
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
	if bs.cur.srcStore == nil || bs.cur.dstStore == nil ||
		bs.cur.mainPeerStat == nil || bs.cur.mainPeerStat.StoreID != bs.cur.srcStore.GetID() ||
		bs.cur.region == nil || bs.cur.region.GetID() != bs.cur.mainPeerStat.ID() {
		return false
	}
	if bs.cur.revertPeerStat == nil {
		return bs.cur.revertRegion == nil
	}
	return bs.cur.revertPeerStat.StoreID == bs.cur.dstStore.GetID() &&
		bs.cur.revertRegion != nil && bs.cur.revertRegion.GetID() == bs.cur.revertPeerStat.ID()
}

func (bs *balanceSolver) buildOperators() (ops []*operator.Operator) {
	enabledBucket := bs.GetStoreConfig().IsEnableRegionBucket()
	splitRegions := make([]*core.RegionInfo, 0)

	if bs.opTy == movePeer && enabledBucket {
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

	currentOp, typ, err := bs.createOperator(bs.cur.region, srcStoreID, dstStoreID)
	if err == nil {
		bs.decorateOperator(currentOp, false, sourceLabel, targetLabel, typ, dim)
		ops = []*operator.Operator{currentOp}
		if bs.cur.revertRegion != nil {
			currentOp, typ, err = bs.createOperator(bs.cur.revertRegion, dstStoreID, srcStoreID)
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
// If the first priority is a dimension that buckets do not report, it falls
// back to another bucket-supported priority.
func (bs *balanceSolver) bucketFirstStat() utils.RegionStatKind {
	dim := bs.firstPriority
	if !isBucketLoadDimSupported(dim) {
		dim = bs.secondPriority
	}
	if !isBucketLoadDimSupported(dim) {
		dim = utils.ByteDim
	}
	return bs.rwTy.RegionStats()[dim]
}

func isBucketLoadDimSupported(dim int) bool {
	return dim == utils.ByteDim || dim == utils.KeyDim || dim == utils.QueryDim
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
		log.Debug("fail to create split operator",
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
		op.SetAdditionalInfo("accLoads", strconv.FormatUint(acc-stats[splitIdx-1].Loads[dim], 10))
		op.SetAdditionalInfo("totalLoads", strconv.FormatUint(totalLoads, 10))
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
				hotBuckets = bs.BucketsStats(bs.minHotDegree, ids...)
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

func (bs *balanceSolver) createOperator(region *core.RegionInfo, srcStoreID, dstStoreID uint64) (op *operator.Operator, typ string, err error) {
	if region.GetStorePeer(dstStoreID) != nil {
		typ = "transfer-leader"
		op, err = operator.CreateTransferLeaderOperator(
			"transfer-hot-"+bs.rwTy.String()+"-leader",
			bs,
			region,
			dstStoreID,
			[]uint64{},
			operator.OpHotRegion)
	} else {
		srcPeer := region.GetStorePeer(srcStoreID) // checked in `filterHotPeers`
		dstPeer := &metapb.Peer{StoreId: dstStoreID, Role: srcPeer.Role}
		if region.GetLeader().GetStoreId() == srcStoreID {
			typ = "move-leader"
			op, err = operator.CreateMoveLeaderOperator(
				"move-hot-"+bs.rwTy.String()+"-leader",
				bs,
				region,
				operator.OpHotRegion,
				srcStoreID,
				dstPeer)
		} else {
			typ = "move-peer"
			op, err = operator.CreateMovePeerOperator(
				"move-hot-"+bs.rwTy.String()+"-peer",
				bs,
				region,
				operator.OpHotRegion,
				srcStoreID,
				dstPeer)
		}
	}
	return
}

func (bs *balanceSolver) decorateOperator(op *operator.Operator, isRevert bool, sourceLabel, targetLabel, typ, dim string) {
	op.SetPriorityLevel(constant.High)
	op.FinishedCounters = append(op.FinishedCounters,
		hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), sourceLabel, "out", dim),
		hotDirectionCounter.WithLabelValues(typ, bs.rwTy.String(), targetLabel, "in", dim),
	)
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
