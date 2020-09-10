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
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/statistics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(MultipleDimensionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler(MultipleDimensionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := initHotRegionScheduleConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newMultiDimensionScheduler(opController, conf), nil
	})
}

const (
	// MultipleDimensionName is balance hot region scheduler name.
	MultipleDimensionName = "balance-multiple-dimension-scheduler"
	// MultipleDimensionType is balance hot region scheduler type.
	MultipleDimensionType = "multiple-dimension-scheduler"

	minHotScheduleIntervalHR = time.Second
	maxHotScheduleIntervalHR = 20 * time.Second

	divisorHR                   = float64(1000) * 2
	hotWriteRegionMinFlowRateHR = 16 * 1024
	hotReadRegionMinFlowRateHR  = 128 * 1024
	hotWriteRegionMinKeyRateHR  = 256
	hotReadRegionMinKeyRateHR   = 512
	batchOperationLimit         = 10
	operationRetryLimit         = 10
)

type opRecord struct {
	pendingOp *operator.Operator
	region    *regionInfo
	retry     uint64
	isFinish  bool
}

// schedulePeerPrHR the probability of schedule the hot peer.
var schedulePeerPrHR = 0.66

type multiDimensionScheduler struct {
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

	isScheduled    bool
	regionOpRecord map[uint64]*opRecord
}

func newMultiDimensionScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *multiDimensionScheduler {
	base := NewBaseScheduler(opController)
	ret := &multiDimensionScheduler{
		name:           MultipleDimensionName,
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		types:          []rwType{write, read},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		regionPendings: make(map[uint64][2]*operator.Operator),
		conf:           conf,
		regionOpRecord: make(map[uint64]*opRecord),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.pendings[ty] = map[*pendingInfluence]struct{}{}
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func (h *multiDimensionScheduler) GetName() string {
	return h.name
}

func (h *multiDimensionScheduler) GetType() string {
	return MultipleDimensionType
}

func (h *multiDimensionScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.conf.ServeHTTP(w, r)
}

func (h *multiDimensionScheduler) GetMinInterval() time.Duration {
	return minHotScheduleIntervalHR
}
func (h *multiDimensionScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(h.GetMinInterval(), maxHotScheduleIntervalHR, exponentialGrowth)
}

func (h *multiDimensionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return h.allowBalanceLeader(cluster) || h.allowBalanceRegion(cluster)
}

func (h *multiDimensionScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetHotRegionScheduleLimit() &&
		h.OpController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (h *multiDimensionScheduler) allowBalanceRegion(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetHotRegionScheduleLimit()
}

func (h *multiDimensionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[1], cluster)
}

func (h *multiDimensionScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()

	// log.Info("execute multiDimensionScheduler")
	h.prepareForBalance(cluster)
	bs := newBalanceSolverHR(h, cluster, read, transferLeader)

	pendingOps := h.processPendingOps(bs)
	if len(pendingOps) > 0 || h.isScheduled {
		return pendingOps
	}

	bs.greedyTwoDimension()
	h.isScheduled = true
	// ops := bs.genScheduleRequestFromFile()

	return h.processPendingOps(bs)
}

func (h *multiDimensionScheduler) processPendingOps(bs *balanceSolverHR) []*operator.Operator {
	var pendingOps []*operator.Operator

	for regionID, record := range h.regionOpRecord {
		if record.isFinish {
		} else if record.pendingOp == nil {
			if len(pendingOps) < batchOperationLimit {
				op := bs.generateOperator(record.region)
				if op != nil {
					record.pendingOp = op
					pendingOps = append(pendingOps, record.pendingOp)
				} else {
					record.isFinish = true
				}
			}
		} else if record.pendingOp.CheckSuccess() {
			record.isFinish = true
		} else if record.pendingOp.Status() > operator.SUCCESS {
			if record.retry >= operationRetryLimit {
				log.Info("cancel failed operation",
					zap.Uint64("regionID", regionID),
					zap.String("desc", record.pendingOp.Desc()))
				record.isFinish = true
			} else {
				log.Info("retry to process failed operation",
					zap.Uint64("regionID", regionID),
					zap.String("status", operator.OpStatusToString(record.pendingOp.Status())),
					zap.String("opType", record.pendingOp.Kind().String()))
				record.retry++
				op := bs.generateOperator(record.region)
				if op != nil {
					record.pendingOp = op
					pendingOps = append(pendingOps, record.pendingOp)
					if len(pendingOps) == batchOperationLimit {
						break
					}
				} else {
					record.isFinish = true
				}
			}
		}
	}
	if len(pendingOps) > 0 {
		log.Info("process pending operations", zap.Int("count", len(pendingOps)))
	}
	return pendingOps
}

func (h *multiDimensionScheduler) prepareForBalance(cluster opt.Cluster) {
	h.summaryPendingInfluence()

	storesStat := cluster.GetStoresStats()

	minHotDegree := cluster.GetHotRegionCacheHitsThreshold()
	{ // update read statistics
		regionRead := cluster.RegionReadStats()
		storeByte := storesStat.GetStoresBytesReadStat()
		storeKey := storesStat.GetStoresKeysReadStat()
		hotRegionThreshold := getHotRegionThresholdHR(storesStat, read)
		h.stLoadInfos[readLeader] = summaryStoresLoadHR(
			storeByte,
			storeKey,
			h.pendingSums[readLeader],
			regionRead,
			minHotDegree,
			hotRegionThreshold,
			read, core.LeaderKind, mixed)
	}

	{ // update write statistics
		regionWrite := cluster.RegionWriteStats()
		storeByte := storesStat.GetStoresBytesWriteStat()
		storeKey := storesStat.GetStoresKeysWriteStat()
		hotRegionThreshold := getHotRegionThresholdHR(storesStat, write)
		h.stLoadInfos[writeLeader] = summaryStoresLoadHR(
			storeByte,
			storeKey,
			h.pendingSums[writeLeader],
			regionWrite,
			minHotDegree,
			hotRegionThreshold,
			write, core.LeaderKind, mixed)

		h.stLoadInfos[writePeer] = summaryStoresLoadHR(
			storeByte,
			storeKey,
			h.pendingSums[writePeer],
			regionWrite,
			minHotDegree,
			hotRegionThreshold,
			write, core.RegionKind, mixed)
	}
}

func getHotRegionThresholdHR(stats *statistics.StoresStats, typ rwType) [2]uint64 {
	var hotRegionThreshold [2]uint64
	switch typ {
	case write:
		hotRegionThreshold[0] = uint64(stats.TotalBytesWriteRate() / divisorHR)
		if hotRegionThreshold[0] < hotWriteRegionMinFlowRateHR {
			hotRegionThreshold[0] = hotWriteRegionMinFlowRateHR
		}
		hotRegionThreshold[1] = uint64(stats.TotalKeysWriteRate() / divisorHR)
		if hotRegionThreshold[1] < hotWriteRegionMinKeyRateHR {
			hotRegionThreshold[1] = hotWriteRegionMinKeyRateHR
		}
		return hotRegionThreshold
	case read:
		hotRegionThreshold[0] = uint64(stats.TotalBytesReadRate() / divisorHR)
		if hotRegionThreshold[0] < hotReadRegionMinFlowRateHR {
			hotRegionThreshold[0] = hotReadRegionMinFlowRateHR
		}
		hotRegionThreshold[1] = uint64(stats.TotalKeysWriteRate() / divisorHR)
		if hotRegionThreshold[1] < hotReadRegionMinKeyRateHR {
			hotRegionThreshold[1] = hotReadRegionMinKeyRateHR
		}
		return hotRegionThreshold
	default:
		panic("invalid type: " + typ.String())
	}
}

func (h *multiDimensionScheduler) summaryPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendingSums[ty] = summaryPendingInfluence(h.pendings[ty], h.calcPendingWeight)
	}
	h.gcRegionPendings()
}

func (h *multiDimensionScheduler) gcRegionPendings() {
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
func summaryStoresLoadHR(
	storeByteRate map[uint64]float64,
	storeKeyRate map[uint64]float64,
	pendings map[uint64]Influence,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	minHotDegree int,
	hotRegionThreshold [2]uint64,
	rwTy rwType,
	kind core.ResourceKind,
	hotPeerFilterTy hotPeerFilterType,
) map[uint64]*storeLoadDetail {
	loadDetail := make(map[uint64]*storeLoadDetail, len(storeByteRate))
	allByteSum := 0.0
	allKeySum := 0.0
	allCount := 0.0

	// Stores without byte rate statistics is not available to schedule.
	for id, byteRate := range storeByteRate {
		keyRate := storeKeyRate[id]

		// Find all hot peers first
		hotPeers := make([]*statistics.HotPeerStat, 0)
		{
			byteSum := 0.0
			keySum := 0.0
			for _, peer := range filterHotPeersHR(kind, minHotDegree, hotRegionThreshold, storeHotPeers[id], hotPeerFilterTy) {
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
		allByteSum += byteRate
		allKeySum += keyRate
		allCount += float64(len(hotPeers))

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
	storeLen := float64(len(storeByteRate))

	for id, detail := range loadDetail {
		byteExp := allByteSum / storeLen
		keyExp := allKeySum / storeLen
		countExp := allCount / storeLen
		detail.LoadPred.Future.ExpByteRate = byteExp
		detail.LoadPred.Future.ExpKeyRate = keyExp
		detail.LoadPred.Future.ExpCount = countExp
		// Debug
		{
			ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(byteExp)
		}
		{
			ty := "exp-key-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(keyExp)
		}
		{
			ty := "exp-count-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(countExp)
		}
	}
	return loadDetail
}

func filterHotPeersHR(
	kind core.ResourceKind,
	minHotDegree int,
	hotRegionThreshold [2]uint64,
	peers []*statistics.HotPeerStat,
	hotPeerFilterTy hotPeerFilterType,
) []*statistics.HotPeerStat {
	ret := make([]*statistics.HotPeerStat, 0)
	for _, peer := range peers {
		if (kind == core.LeaderKind && !peer.IsLeader()) ||
			peer.HotDegree < minHotDegree ||
			isHotPeerFilteredHR(peer, hotRegionThreshold, hotPeerFilterTy) {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}

func isHotPeerFilteredHR(peer *statistics.HotPeerStat, hotRegionThreshold [2]uint64, hotPeerFilterTy hotPeerFilterType) bool {
	var isFiltered bool
	switch hotPeerFilterTy {
	case high:
		if peer.GetByteRate() < float64(hotRegionThreshold[0]) && peer.GetKeyRate() < float64(hotRegionThreshold[1]) {
			isFiltered = true
		}
	case low:
		if peer.GetByteRate() >= float64(hotRegionThreshold[0]) || peer.GetKeyRate() >= float64(hotRegionThreshold[1]) {
			isFiltered = true
		}
	case mixed:
	}
	return isFiltered
}

func (h *multiDimensionScheduler) addPendingInfluence(op *operator.Operator, srcStore, dstStore uint64, infl Influence, rwTy rwType, opTy opType) bool {
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

func (h *multiDimensionScheduler) balanceHotReadRegions(cluster opt.Cluster) []*operator.Operator {
	// prefer to balance by leader
	leaderSolver := newBalanceSolverHR(h, cluster, read, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	peerSolver := newBalanceSolverHR(h, cluster, read, movePeer)
	ops = peerSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *multiDimensionScheduler) balanceHotWriteRegions(cluster opt.Cluster) []*operator.Operator {
	// prefer to balance by peer
	s := h.r.Intn(100)
	switch {
	case s < int(schedulePeerPrHR*100):
		peerSolver := newBalanceSolverHR(h, cluster, write, movePeer)
		ops := peerSolver.solve()
		if len(ops) > 0 {
			return ops
		}
	default:
	}

	leaderSolver := newBalanceSolverHR(h, cluster, write, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 {
		return ops
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

type balanceSolverHR struct {
	sche         *multiDimensionScheduler
	cluster      opt.Cluster
	stLoadDetail map[uint64]*storeLoadDetail
	rwTy         rwType
	opTy         opType

	cur *solutionHR

	maxSrc   *storeLoad
	minDst   *storeLoad
	rankStep *storeLoad
}

type solutionHR struct {
	srcStoreID  uint64
	srcPeerStat *statistics.HotPeerStat
	region      *core.RegionInfo
	dstStoreID  uint64

	// progressiveRank measures the contribution for balance.
	// The smaller the rank, the better this solutionHR is.
	// If rank < 0, this solutionHR makes thing better.
	progressiveRank int64
}

func (bs *balanceSolverHR) init() {
	switch toResourceType(bs.rwTy, bs.opTy) {
	case writePeer:
		bs.stLoadDetail = bs.sche.stLoadInfos[writePeer]
	case writeLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[writeLeader]
	case readLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[readLeader]
	}
	for _, id := range getUnhealthyStoresHR(bs.cluster) {
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

type scheduleCmd struct {
	regionID   uint64
	srcStoreID uint64
	dstStoreID uint64
}

func loadScheduleRequest() []scheduleCmd {
	ret := make([]scheduleCmd, 0)
	schFileName := "schedule.txt"
	file, err := os.Open(schFileName)
	log.Info(fmt.Sprintf("Open schedule file: %s", schFileName))
	if err != nil {
		log.Error(fmt.Sprintf("Open schedule file %s error", schFileName))
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		log.Debug("read schedule rule: " + line)
		strs := strings.Split(line, " ")
		regionID, _ := strconv.ParseUint(strs[0], 10, 64)
		srcStoreID, _ := strconv.ParseUint(strs[1], 10, 64)
		dstStoreID, _ := strconv.ParseUint(strs[2], 10, 64)
		schCmd := scheduleCmd{
			regionID:   regionID,
			srcStoreID: srcStoreID,
			dstStoreID: dstStoreID,
		}
		ret = append(ret, schCmd)
	}

	return ret
}

func (bs *balanceSolverHR) genScheduleRequestFromFile() []*operator.Operator {
	ret := make([]*operator.Operator, 0)
	schCmds := loadScheduleRequest()
	for _, cmd := range schCmds {
		log.Info(fmt.Sprintf("parse schedule cmd: %v", cmd))
		ri := &regionInfo{
			id:         cmd.regionID,
			srcStoreID: cmd.srcStoreID,
			dstStoreID: cmd.dstStoreID,
		}
		op := bs.generateOperator(ri)
		if op != nil {
			ret = append(ret, op)
		}
	}

	return ret
}

func (bs *balanceSolverHR) generateOperator(ri *regionInfo) *operator.Operator {
	var (
		op       *operator.Operator
		counters []prometheus.Counter
		err      error
		opTy     opType
		rwTy     rwType = bs.rwTy
	)

	regionID := ri.id
	srcStoreID := ri.srcStoreID
	dstStoreID := ri.dstStoreID

	region := bs.cluster.GetRegion(regionID)
	if !bs.isRegionAvailable(region) {
		log.Error("region is not avaiable", zap.Uint64("regionID", regionID))
		return nil
	}

	if ri.NeedSplit() {
		opts := []float64{float64(ri.splitDimID), ri.splitRatio}
		op := operator.CreateSplitRegionOperator("hotspot-split-region", bs.cluster.GetRegion(ri.id), operator.OpAdmin, pdpb.CheckPolicy_RATIO, nil, opts)
		return op
	}

	if region.GetStoreVoter(dstStoreID) == nil { // move peer
		opTy = movePeer
		srcPeer := region.GetStorePeer(srcStoreID) // checked in getRegionAndSrcPeer
		if srcPeer == nil {
			log.Error("GetStorePeer return null", zap.Uint64("storeID", srcStoreID), zap.Uint64("regionID", regionID))
			return nil
		}
		dstPeer := &metapb.Peer{StoreId: dstStoreID, IsLearner: srcPeer.IsLearner}
		op, err = operator.CreateMovePeerOperator(
			"move-hot-"+rwTy.String()+"-region",
			bs.cluster,
			region,
			operator.OpHotRegion,
			srcStoreID,
			dstPeer)

		counters = append(counters,
			balanceHotRegionCounter.WithLabelValues("move-peer", strconv.FormatUint(srcStoreID, 10)+"-out"),
			balanceHotRegionCounter.WithLabelValues("move-peer", strconv.FormatUint(dstPeer.GetStoreId(), 10)+"-in"))
	} else { // transfer leader
		if region.GetLeader().GetStoreId() == dstStoreID {
			log.Info("leader is already transferred", zap.Uint64("regionID", regionID), zap.Uint64("leaderStoreID", dstStoreID))
			return nil
		}
		opTy = transferLeader
		op, err = operator.CreateTransferLeaderOperator(
			"transfer-hot-"+rwTy.String()+"-leader",
			bs.cluster,
			region,
			srcStoreID,
			dstStoreID,
			operator.OpHotRegion)
		counters = append(counters,
			balanceHotRegionCounter.WithLabelValues("move-leader", strconv.FormatUint(srcStoreID, 10)+"-out"),
			balanceHotRegionCounter.WithLabelValues("move-leader", strconv.FormatUint(dstStoreID, 10)+"-in"))
	}

	if err != nil {
		log.Info("fail to create operator", zap.Error(err), zap.Stringer("rwType", rwTy), zap.Stringer("opType", opTy))
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "create-operator-fail").Inc()
		return nil
	}

	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, counters...)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(bs.sche.GetName(), opTy.String()))

	return op
}

func (bs *balanceSolverHR) greedyTwoDimension() {
	storeInfos := make([]*storeInfo, 0)
	// create RegionInfo and StoreInfo to normalize flow data
	for id, loadDetail := range bs.stLoadDetail {
		hotRegions := make(map[uint64]*regionInfo)
		for _, peer := range loadDetail.HotPeers {
			hotRegions[peer.RegionID] = newRegionInfo(peer.RegionID, id,
				peer.GetByteRate()/loadDetail.LoadPred.Future.ExpByteRate,
				peer.GetKeyRate()/loadDetail.LoadPred.Future.ExpKeyRate)
		}
		si := newStoreInfo(id,
			[]float64{loadDetail.LoadPred.Current.ByteRate / loadDetail.LoadPred.Future.ExpByteRate,
				loadDetail.LoadPred.Current.KeyRate / loadDetail.LoadPred.Future.ExpKeyRate},
			hotRegions)
		storeInfos = append(storeInfos, si)
	}

	pendingRegions, needSplit := greedyBalance(storeInfos, 0.07)

	// pendingRegions, needSplit := greedySingle(storeInfos, 0.07, 0)
	// pendingRegions1, needSplit := greedySingle(storeInfos, 0.07, 1)
	// pendingRegions = append(pendingRegions, pendingRegions1...)

	for _, ri := range pendingRegions {
		if needSplit {
			log.Info("split", zap.String("regionInfo", fmt.Sprintf("%+v", ri)))
		} else {
			log.Info("migrate", zap.String("regionInfo", fmt.Sprintf("%+v", ri)))
		}
		bs.sche.regionOpRecord[ri.id] = &opRecord{
			region: ri,
		}
	}
}

func getUnhealthyStoresHR(cluster opt.Cluster) []uint64 {
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

func newBalanceSolverHR(sche *multiDimensionScheduler, cluster opt.Cluster, rwTy rwType, opTy opType) *balanceSolverHR {
	solver := &balanceSolverHR{
		sche:    sche,
		cluster: cluster,
		rwTy:    rwTy,
		opTy:    opTy,
	}
	solver.init()
	return solver
}

func (bs *balanceSolverHR) isValid() bool {
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

func (bs *balanceSolverHR) solve() []*operator.Operator {
	if !bs.isValid() || !bs.allowBalance() {
		return nil
	}
	bs.cur = &solutionHR{}
	var (
		best  *solutionHR
		ops   []*operator.Operator
		infls []Influence
	)

	for srcStoreID := range bs.filterSrcStores() {
		bs.cur.srcStoreID = srcStoreID

		for _, srcPeerStat := range bs.filterHotPeersHR() {
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

func (bs *balanceSolverHR) allowBalance() bool {
	switch bs.opTy {
	case movePeer:
		return bs.sche.allowBalanceRegion(bs.cluster)
	case transferLeader:
		return bs.sche.allowBalanceLeader(bs.cluster)
	default:
		return false
	}
}

func (bs *balanceSolverHR) filterSrcStores() map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail)
	for id, detail := range bs.stLoadDetail {
		if bs.cluster.GetStore(id) == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", id))
			continue
		}
		if len(detail.HotPeers) == 0 {
			continue
		}
		if detail.LoadPred.min().ByteRate > bs.sche.conf.GetSrcToleranceRatio()*detail.LoadPred.Future.ExpByteRate &&
			detail.LoadPred.min().KeyRate > bs.sche.conf.GetSrcToleranceRatio()*detail.LoadPred.Future.ExpKeyRate {
			ret[id] = detail
			balanceHotRegionCounter.WithLabelValues("src-store-succ", strconv.FormatUint(id, 10)).Inc()
		}
		balanceHotRegionCounter.WithLabelValues("src-store-failed", strconv.FormatUint(id, 10)).Inc()
	}
	return ret
}

func (bs *balanceSolverHR) filterHotPeersHR() []*statistics.HotPeerStat {
	ret := bs.stLoadDetail[bs.cur.srcStoreID].HotPeers
	// Return at most MaxPeerNum peers, to prevent balanceSolverHR.solve() too slow.
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
func (bs *balanceSolverHR) isRegionAvailable(region *core.RegionInfo) bool {
	if region == nil {
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "no-region").Inc()
		return false
	}

	// if pendings, ok := bs.sche.regionPendings[region.GetID()]; ok {
	// 	if bs.opTy == transferLeader {
	// 		return false
	// 	}
	// 	if pendings[movePeer] != nil ||
	// 		(pendings[transferLeader] != nil && !pendings[transferLeader].IsEnd()) {
	// 		return false
	// 	}
	// }

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

func (bs *balanceSolverHR) getRegion() *core.RegionInfo {
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

func (bs *balanceSolverHR) filterDstStores() map[uint64]*storeLoadDetail {
	var (
		filters    []filter.Filter
		candidates []*core.StoreInfo
	)

	switch bs.opTy {
	case movePeer:
		var scoreGuard filter.Filter
		if bs.cluster.IsPlacementRulesEnabled() {
			return nil
			// scoreGuard = filter.NewRuleFitFilter(bs.sche.GetName(), bs.cluster, bs.cur.region, bs.cur.srcStoreID)
		} else {
			srcStore := bs.cluster.GetStore(bs.cur.srcStoreID)
			if srcStore == nil {
				return nil
			}
			// scoreGuard = filter.NewDistinctScoreFilter(bs.sche.GetName(), bs.cluster.GetLocationLabels(), bs.cluster.GetRegionStores(bs.cur.region), srcStore)
			return nil
		}

		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.region.GetStoreIds(), bs.cur.region.GetStoreIds()),
			// filter.NewHealthFilter(bs.sche.GetName()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
			scoreGuard,
		}

		candidates = bs.cluster.GetStores()

	case transferLeader:
		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true},
			// filter.NewHealthFilterNewHealthFilter(bs.sche.GetName()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
		}

		candidates = bs.cluster.GetFollowerStores(bs.cur.region)

	default:
		return nil
	}

	ret := make(map[uint64]*storeLoadDetail, len(candidates))
	for _, store := range candidates {
		if filter.Target(bs.cluster, store, filters) {
			detail := bs.stLoadDetail[store.GetID()]
			if detail.LoadPred.max().ByteRate*bs.sche.conf.GetDstToleranceRatio() < detail.LoadPred.Future.ExpByteRate &&
				detail.LoadPred.max().KeyRate*bs.sche.conf.GetDstToleranceRatio() < detail.LoadPred.Future.ExpKeyRate {
				ret[store.GetID()] = bs.stLoadDetail[store.GetID()]
				balanceHotRegionCounter.WithLabelValues("dst-store-succ", strconv.FormatUint(store.GetID(), 10)).Inc()
			}
			balanceHotRegionCounter.WithLabelValues("dst-store-fail", strconv.FormatUint(store.GetID(), 10)).Inc()
			ret[store.GetID()] = bs.stLoadDetail[store.GetID()]
		}
	}
	return ret
}

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solutionHR.progressiveRank` for more about progressive rank.
func (bs *balanceSolverHR) calcProgressiveRank() {
	srcLd := bs.stLoadDetail[bs.cur.srcStoreID].LoadPred.min()
	dstLd := bs.stLoadDetail[bs.cur.dstStoreID].LoadPred.max()
	peer := bs.cur.srcPeerStat
	rank := int64(0)
	if bs.rwTy == write && bs.opTy == transferLeader {
		// In this condition, CPU usage is the matter.
		// Only consider about key rate.
		if srcLd.KeyRate >= dstLd.KeyRate+peer.GetKeyRate() {
			rank = -1
		}
	} else {
		getSrcDecRate := func(a, b float64) float64 {
			if a-b <= 0 {
				return 1
			}
			return a - b
		}
		keyDecRatio := (dstLd.KeyRate + peer.GetKeyRate()) / getSrcDecRate(srcLd.KeyRate, peer.GetKeyRate())
		keyHot := peer.GetKeyRate() >= bs.sche.conf.GetMinHotKeyRate()
		byteDecRatio := (dstLd.ByteRate + peer.GetByteRate()) / getSrcDecRate(srcLd.ByteRate, peer.GetByteRate())
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

// betterThan checks if `bs.cur` is a better solutionHR than `old`.
func (bs *balanceSolverHR) betterThan(old *solutionHR) bool {
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
			byteRkCmp := rankCmp(bs.cur.srcPeerStat.GetByteRate(), old.srcPeerStat.GetByteRate(), stepRankHR(0, 100))
			keyRkCmp := rankCmp(bs.cur.srcPeerStat.GetKeyRate(), old.srcPeerStat.GetKeyRate(), stepRankHR(0, 10))

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
func (bs *balanceSolverHR) compareSrcStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare source store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdKeyRate, stepRankHR(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRankHR(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
				))),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRankHR(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRankHR(0, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRankHR(0, bs.rankStep.ByteRate)),
				)),
			)
		} else {
			lpCmp = sliceLPCmp(
				minLPCmp(negLoadCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRankHR(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
					stLdRankCmp(stLdKeyRate, stepRankHR(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
				))),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRankHR(0, bs.rankStep.ByteRate)),
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
func (bs *balanceSolverHR) compareDstStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare destination store
		var lpCmp storeLPCmp
		if bs.rwTy == write && bs.opTy == transferLeader {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdKeyRate, stepRankHR(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRankHR(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
				)),
				diffCmp(sliceLoadCmp(
					stLdRankCmp(stLdCount, stepRankHR(0, bs.rankStep.Count)),
					stLdRankCmp(stLdKeyRate, stepRankHR(0, bs.rankStep.KeyRate)),
					stLdRankCmp(stLdByteRate, stepRankHR(0, bs.rankStep.ByteRate)),
				)))
		} else {
			lpCmp = sliceLPCmp(
				maxLPCmp(sliceLoadCmp(
					stLdRankCmp(stLdByteRate, stepRankHR(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
					stLdRankCmp(stLdKeyRate, stepRankHR(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
				)),
				diffCmp(
					stLdRankCmp(stLdByteRate, stepRankHR(0, bs.rankStep.ByteRate)),
				),
			)
		}

		lp1 := bs.stLoadDetail[st1].LoadPred
		lp2 := bs.stLoadDetail[st2].LoadPred
		return lpCmp(lp1, lp2)
	}
	return 0
}

func stepRankHR(rk0 float64, step float64) func(float64) int64 {
	return func(rate float64) int64 {
		return int64((rate - rk0) / step)
	}
}

func (bs *balanceSolverHR) isReadyToBuild() bool {
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

func (bs *balanceSolverHR) buildOperators() ([]*operator.Operator, []Influence) {
	if !bs.isReadyToBuild() {
		return nil, nil
	}
	var (
		op       *operator.Operator
		counters []prometheus.Counter
		err      error
	)

	switch bs.opTy {
	case movePeer:
		srcPeer := bs.cur.region.GetStorePeer(bs.cur.srcStoreID) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: bs.cur.dstStoreID, IsLearner: srcPeer.IsLearner}
		op, err = operator.CreateMovePeerOperator(
			"move-hot-"+bs.rwTy.String()+"-region",
			bs.cluster,
			bs.cur.region,
			operator.OpHotRegion,
			bs.cur.srcStoreID,
			dstPeer)

		counters = append(counters,
			balanceHotRegionCounter.WithLabelValues("move-peer", strconv.FormatUint(bs.cur.srcStoreID, 10)+"-out"),
			balanceHotRegionCounter.WithLabelValues("move-peer", strconv.FormatUint(dstPeer.GetStoreId(), 10)+"-in"))
	case transferLeader:
		if bs.cur.region.GetStoreVoter(bs.cur.dstStoreID) == nil {
			return nil, nil
		}
		op, err = operator.CreateTransferLeaderOperator(
			"transfer-hot-"+bs.rwTy.String()+"-leader",
			bs.cluster,
			bs.cur.region,
			bs.cur.srcStoreID,
			bs.cur.dstStoreID,
			operator.OpHotRegion)
		counters = append(counters,
			balanceHotRegionCounter.WithLabelValues("move-leader", strconv.FormatUint(bs.cur.srcStoreID, 10)+"-out"),
			balanceHotRegionCounter.WithLabelValues("move-leader", strconv.FormatUint(bs.cur.dstStoreID, 10)+"-in"))
	}

	if err != nil {
		log.Debug("fail to create operator", zap.Error(err), zap.Stringer("rwType", bs.rwTy), zap.Stringer("opType", bs.opTy))
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "create-operator-fail").Inc()
		return nil, nil
	}

	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, counters...)
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

func (h *multiDimensionScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
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

func (h *multiDimensionScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
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

func (h *multiDimensionScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(writePeer)
}

func (h *multiDimensionScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(readLeader)
}

func (h *multiDimensionScheduler) copyPendingInfluence(ty resourceType) map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	pendingSum := h.pendingSums[ty]
	ret := make(map[uint64]Influence, len(pendingSum))
	for id, infl := range pendingSum {
		ret[id] = infl
	}
	return ret
}

func (h *multiDimensionScheduler) calcPendingWeight(op *operator.Operator) float64 {
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

func (h *multiDimensionScheduler) clearPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendings[ty] = map[*pendingInfluence]struct{}{}
		h.pendingSums[ty] = nil
	}
	h.regionPendings = make(map[uint64][2]*operator.Operator)
}
