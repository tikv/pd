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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
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

	batchOperationLimit = 4
	operationRetryLimit = 3
	// wait for the new splitted regions to be identified as hot
	waitSplitInfoStableInterval = 10 * time.Second

	storeMinExpQPSRate  = 512
	storeMinExpFlowRate = 128 * 1024
)

type scheduleStatus uint32

const (
	scheduleInit scheduleStatus = iota
	scheduleSplit
	scheduleSplitProcess
	scheduleMigration
	scheduleMigrationProcess
	scheduleEnd
)

var statusString [scheduleEnd]string = [scheduleEnd]string{
	scheduleInit:             "Init",
	scheduleSplit:            "Split",
	scheduleSplitProcess:     "SplitProcess",
	scheduleMigration:        "Migration",
	scheduleMigrationProcess: "MigrationProcess",
}

type opRecord struct {
	pendingOp *operator.Operator
	region    *regionInfo
	retry     uint64
	isFinish  bool
}

type multiDimensionScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []rwType
	r           *rand.Rand

	// config of hot scheduler
	conf *hotRegionSchedulerConfig

	hotSched          *hotScheduler
	schStatus         scheduleStatus
	lastCheckLoadTime time.Time
	opCompleteTime    time.Time
	storeInfos        []*storeInfo
	candidateRegions  *regionContainer
	regionOpRecord    map[uint64]*opRecord

	storeFlowRateHistroy     *stableAnalysis
	storeQPSHistroy          *stableAnalysis
	hotRegionLoadRateHistory []*stableAnalysis
	mode                     int
	balanceRatio			 float64
}

func newMultiDimensionScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *multiDimensionScheduler {
	base := NewBaseScheduler(opController)
	ret := &multiDimensionScheduler{
		name:          MultipleDimensionName,
		BaseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		types:         []rwType{write, read},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		conf:          conf,
		hotSched:      newHotScheduler(opController, conf),
		schStatus:     scheduleInit,

		storeFlowRateHistroy:     newStableAnalysis(5, 0.15),
		storeQPSHistroy:          newStableAnalysis(5, 0.15),
		hotRegionLoadRateHistory: []*stableAnalysis{newStableAnalysis(5, 0.15), newStableAnalysis(5, 0.15)},
		balanceRatio: 			balanceRatioConst,
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
	return h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit() &&
		h.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (h *multiDimensionScheduler) allowBalanceRegion(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit()
}

func (h *multiDimensionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[1], cluster)
}

func (h *multiDimensionScheduler) dispatch(typ rwType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()

	h.hotSched.prepareForBalance(cluster)
	bs := newBalanceSolver(h.hotSched, cluster, write, movePeer)
	h.mode = cluster.GetOpts().GetHotSchedulerMode()
	h.balanceRatio = cluster.GetOpts().GetHotBalanceRatio()

	for {
		switch h.schStatus {
		case scheduleInit:
			if h.checkStoreLoad(bs) {
				h.schStatus++
			} else {
				return nil
			}
		case scheduleMigration: // wait the flow data of splitted region to be stable (to be improved)
			if time.Since(h.opCompleteTime) < waitSplitInfoStableInterval {
				log.Info("wait for schedule", zap.Float64("duration", time.Since(h.opCompleteTime).Seconds()),
					zap.Float64("maxTime", waitSplitInfoStableInterval.Seconds()))
				return nil
			}
			splitRegionInfos := cluster.GetSplitRegionInfos()
			h.updateSplitInfos(bs, splitRegionInfos)
			fallthrough
		case scheduleSplit:
			log.Info("run schedule algorithm", zap.String("status", statusString[h.schStatus]))
			h.greedyTwoDimension(bs)
			h.schStatus++
		case scheduleSplitProcess, scheduleMigrationProcess:
			pendingOps, done := h.processPendingOps(bs, read)
			if !done {
				return pendingOps
			}
			h.opCompleteTime = time.Now()
			h.schStatus++
		case scheduleEnd:
			if time.Since(h.opCompleteTime) > 60*time.Second {
				h.schStatus = scheduleInit
				h.lastCheckLoadTime = time.Now()
				h.storeFlowRateHistroy.reset()
				h.storeQPSHistroy.reset()
				h.hotRegionLoadRateHistory[0].reset()
				h.hotRegionLoadRateHistory[1].reset()
			} else {
				return nil
			}
		default:
			return nil
		}
	}

	// ops := bs.genScheduleRequestFromFile(bs) 	// for test
	// return ops
}

func (h *multiDimensionScheduler) isLoadBalanced(bs *balanceSolver) bool {
	var storeLoads [DimensionCount][]float64
	for dimID := uint64(0); dimID < DimensionCount; dimID++ {
		for _, loadDetail := range bs.stLoadDetail {
			if dimID == 0 {
				storeLoads[dimID] = append(storeLoads[dimID], loadDetail.LoadPred.Current.ByteRate/loadDetail.LoadPred.Future.ExpByteRate)
			} else {
				storeLoads[dimID] = append(storeLoads[dimID], loadDetail.LoadPred.Current.KeyRate/loadDetail.LoadPred.Future.ExpKeyRate)
			}
		}
	}
	return loadBalanced(storeLoads[0], h.balanceRatio) && loadBalanced(storeLoads[1], h.balanceRatio)
}

func (h *multiDimensionScheduler) checkStoreLoad(bs *balanceSolver) bool {
	var expByteRate, expKeyRate float64

	for _, loadDetail := range bs.stLoadDetail {
		expByteRate = loadDetail.LoadPred.Future.ExpByteRate
		expKeyRate = loadDetail.LoadPred.Future.ExpKeyRate
		break
	}

	if time.Since(h.lastCheckLoadTime) > 10*time.Second {
		h.lastCheckLoadTime = time.Now()
		if expByteRate < storeMinExpFlowRate && expKeyRate < storeMinExpQPSRate {
			log.Info("the load is too low", zap.Float64("curExpByteRate", expByteRate), zap.Float64("curExpKeyRate", expKeyRate))
			return false
		}

		if h.isLoadBalanced(bs) {
			log.Info("System has reached load balance")
			h.storeFlowRateHistroy.reset()
			h.storeQPSHistroy.reset()
			h.hotRegionLoadRateHistory[0].reset()
			h.hotRegionLoadRateHistory[1].reset()
			return false
		}

		h.storeFlowRateHistroy.add(expByteRate)
		h.storeQPSHistroy.add(expKeyRate)

		h.initLoadInfo(bs)

		// unstable store flow
		if !h.storeFlowRateHistroy.isStable() || !h.storeQPSHistroy.isStable() {
			log.Info("loads of stores are not stable",
				zap.String("FlowRateHistroy", h.storeFlowRateHistroy.toString()),
				zap.String("QPSHistroy", h.storeQPSHistroy.toString()),
			)
			return false
		}
		// unstable region flow
		if !h.hotRegionLoadRateHistory[0].isStable() || !h.hotRegionLoadRateHistory[1].isStable() ||
			h.hotRegionLoadRateHistory[0].last() > 1.2 || h.hotRegionLoadRateHistory[1].last() > 1.2 {
			log.Info("loads of hot regions are not stable",
				zap.String("FlowRateHistroy", h.hotRegionLoadRateHistory[0].toString()),
				zap.String("QPSHistroy", h.hotRegionLoadRateHistory[1].toString()),
			)
			return false
		}
		return true
	}

	return false
}

func (h *multiDimensionScheduler) initLoadInfo(bs *balanceSolver) {
	var totalHotRegionFlowRate, totalHotRegionQPS float64

	h.storeInfos = make([]*storeInfo, 0)
	h.candidateRegions = newRegionContainer()

	peerID := uint64(1)
	// create RegionInfo and StoreInfo to normalize flow data
	for id, loadDetail := range bs.stLoadDetail {
		hotRegions := make(map[uint64]*regionInfo)
		for _, peer := range loadDetail.HotPeers {
			hotRegions[peerID] = newRegionInfo(peerID, peer.RegionID, id,
				peer.GetByteRate()/loadDetail.LoadPred.Future.ExpByteRate,
				peer.GetKeyRate()/loadDetail.LoadPred.Future.ExpKeyRate)
			peerID++
			totalHotRegionFlowRate += peer.GetByteRate()
			totalHotRegionQPS += peer.GetKeyRate()
		}
		si := newStoreInfo(id,
			[]float64{loadDetail.LoadPred.Current.ByteRate / loadDetail.LoadPred.Future.ExpByteRate,
				loadDetail.LoadPred.Current.KeyRate / loadDetail.LoadPred.Future.ExpKeyRate},
			hotRegions)
		h.storeInfos = append(h.storeInfos, si)
	}

	h.hotRegionLoadRateHistory[0].add(totalHotRegionFlowRate / h.storeFlowRateHistroy.last() / float64(len(bs.stLoadDetail)))
	h.hotRegionLoadRateHistory[1].add(totalHotRegionQPS / h.storeQPSHistroy.last() / float64(len(bs.stLoadDetail)))
}

func (h *multiDimensionScheduler) greedyTwoDimension(bs *balanceSolver) {
	var pendingRegions []*regionInfo
	if h.schStatus == scheduleSplit {
		// pendingRegions = splitProcedure(h.storeInfos, h.candidateRegions, h.balanceRatio)
	} else {
		// pendingRegions = migrationProcedure(h.storeInfos, h.candidateRegions, h.balanceRatio)

		pendingRegions = greedySingle(h.storeInfos, h.balanceRatio, uint64(h.mode%10), nil)
		// pendingRegions1 := greedySingle(h.storeInfos, h.balanceRatio, 1)
		// pendingRegions = append(pendingRegions, pendingRegions1...)
	}

	h.regionOpRecord = make(map[uint64]*opRecord, len(pendingRegions))
	for _, ri := range pendingRegions {
		log.Info("schedule region", zap.String("regionInfo", fmt.Sprintf("%+v", ri)))
		h.regionOpRecord[ri.id] = &opRecord{
			region: ri,
		}
	}
}

func (h *multiDimensionScheduler) processPendingOps(bs *balanceSolver, rw rwType) ([]*operator.Operator, bool) {
	var pendingOps []*operator.Operator
	var runningOps []*operator.Operator

	hotRegions := make(map[uint64]*regionInfo)
	for id, loadDetail := range bs.stLoadDetail {
		for _, peer := range loadDetail.HotPeers {
			hotRegions[peer.RegionID] = newRegionInfo(peer.RegionID, peer.RegionID, id,
				peer.GetByteRate()/loadDetail.LoadPred.Future.ExpByteRate,
				peer.GetKeyRate()/loadDetail.LoadPred.Future.ExpKeyRate)
		}
	}
	// for test
	reportFlow := func(regionID uint64, tag string) {
		ri, ok := hotRegions[regionID]
		if !ok {
			log.Info("flow report", zap.String("tag", "not hot region"), zap.Uint64("regionID", regionID))
		} else {
			log.Info("flow report", zap.String("tag", tag), zap.Uint64("regionID", regionID), zap.Float64("load0", ri.loads[0]), zap.Float64("load1", ri.loads[1]))
		}
	}

	for regionID, record := range h.regionOpRecord {
		if record.isFinish || record.pendingOp == nil {
		} else if record.pendingOp.CheckSuccess() {
			reportFlow(regionID, "finish")
			record.isFinish = true
		} else if record.pendingOp.Status() > operator.SUCCESS { // some operators may be canceled or expired, try to reschedule them
			if record.retry >= operationRetryLimit {
				log.Info("cancel failed operation",
					zap.Uint64("regionID", regionID),
					zap.String("desc", record.pendingOp.Desc()))
				record.isFinish = true
			} else {
				log.Info("try to reschedule failed operation",
					zap.Uint64("regionID", regionID),
					zap.String("status", operator.OpStatusToString(record.pendingOp.Status())),
					zap.String("opType", record.pendingOp.Kind().String()))
				record.retry++
				op := h.generateOperator(bs, record.region, rw)
				if op != nil {
					record.pendingOp = op
					pendingOps = append(pendingOps, record.pendingOp)
				} else {
					record.isFinish = true
				}
			}
			reportFlow(regionID, "failed")
		} else {
			runningOps = append(runningOps, record.pendingOp)
			reportFlow(regionID, "running")
		}
	}
	for regionID, record := range h.regionOpRecord {
		if !record.isFinish && record.pendingOp == nil && len(pendingOps)+len(runningOps) < batchOperationLimit { // operators not yet to be created
			op := h.generateOperator(bs, record.region, rw)
			if op != nil {
				record.pendingOp = op
				pendingOps = append(pendingOps, record.pendingOp)
			} else {
				record.isFinish = true
			}
			reportFlow(regionID, "created")
		}
	}
	if len(pendingOps) > 0 {
		log.Info("process pending operations", zap.Int("count", len(pendingOps)))
	}

	done := len(pendingOps)+len(runningOps) == 0
	return pendingOps, done
}

// to do: peer split
func (h *multiDimensionScheduler) updateSplitInfos(bs *balanceSolver, splitRegionInfos map[uint64][]uint64) {
	for regionID, splittedIDs := range splitRegionInfos {
		log.Info("update split info", zap.Uint64("regionID", regionID))
		if record, ok := h.regionOpRecord[regionID]; ok {
			region := record.region
			region.splittedIDs = splittedIDs
			region.splittedRegions = make(map[uint64]*regionInfo, len(splittedIDs))

			// estimate load of splitted region
			for _, id := range splittedIDs {
				splitRegion := newRegionInfo(id, id, region.srcStoreID, // id
					region.loads[0]*region.splitRatio,
					region.loads[1]*region.splitRatio)
				region.splittedRegions[id] = splitRegion
				h.candidateRegions.push(region.splitDimID, splitRegion)
			}
			restRatio := 1.0 - math.Floor(1.0/region.splitRatio)*region.splitRatio
			region.loads[0] *= restRatio
			region.loads[1] *= restRatio
			region.splitRatio = 0

			// idSet := make(map[uint64]struct{}, len(splittedIDs))
			// for _, id := range splittedIDs {
			// 	idSet[id] = struct{}{}
			// }
			// // update actual load of splitted regions
			// if loadDetail, ok := bs.stLoadDetail[region.srcStoreID]; ok {
			// 	for _, peer := range loadDetail.HotPeers {
			// 		if _, exist := idSet[peer.RegionID]; exist { // insert new region info
			// 			splitRegion := region.splittedRegions[peer.RegionID]
			// 			expectLoads := []float64{splitRegion.loads[0], splitRegion.loads[1]}

			// 			splitRegion.loads[0] = peer.GetByteRate() / loadDetail.LoadPred.Future.ExpByteRate
			// 			splitRegion.loads[1] = peer.GetKeyRate() / loadDetail.LoadPred.Future.ExpKeyRate
			// 			splitRegion.diffLoad = math.Abs(splitRegion.loads[0] - splitRegion.loads[1])

			// 			log.Info("splitInfo",
			// 				zap.String("splitRegionInfo", fmt.Sprintf("%+v", splitRegion)),
			// 				zap.String("expectLoads", fmt.Sprintf("%+v", expectLoads)),
			// 			)
			// 		} else if peer.RegionID == region.id { // use splittedLoads to store the flow data of current region
			// 			expectLoads := []float64{region.loads[0], region.loads[1]}
			// 			region.loads[0] = peer.GetByteRate() / loadDetail.LoadPred.Future.ExpByteRate
			// 			region.loads[1] = peer.GetKeyRate() / loadDetail.LoadPred.Future.ExpKeyRate
			// 			log.Info("splitInfoRest",
			// 				zap.String("splitRegionInfo", fmt.Sprintf("%+v", region)),
			// 				zap.String("expectLoads", fmt.Sprintf("%+v", expectLoads)),
			// 			)
			// 		}
			// 	}
			// }
		}
	}
}

func (h *multiDimensionScheduler) generateOperator(bs *balanceSolver, ri *regionInfo, rwTy rwType) *operator.Operator {
	var (
		op       *operator.Operator
		counters []prometheus.Counter
		err      error
		opTy     opType
	)

	regionID := ri.regionID
	srcStoreID := ri.srcStoreID
	dstStoreID := ri.dstStoreID

	region := bs.cluster.GetRegion(regionID)
	if !bs.isRegionAvailable(region) {
		log.Error("region is not avaiable", zap.Uint64("regionID", regionID))
		return nil
	}

	if ri.NeedSplit() {
		opts := []float64{float64(ri.splitDimID), ri.splitRatio}
		op := operator.CreateSplitRegionOperator("hotspot-split-region", region, operator.OpAdmin, pdpb.CheckPolicy_RATIO, nil, opts)
		return op
	}

	if region.GetStoreVoter(dstStoreID) == nil { // move peer
		opTy = movePeer
		srcPeer := region.GetStorePeer(srcStoreID) // checked in getRegionAndSrcPeer
		if srcPeer == nil {
			log.Error("GetStorePeer return null", zap.Uint64("storeID", srcStoreID), zap.Uint64("regionID", regionID))
			return nil
		}
		dstPeer := &metapb.Peer{StoreId: dstStoreID, Role: srcPeer.Role}
		desc := "move-hot-" + rwTy.String() + "-peer"
		op, err = operator.CreateMovePeerOperator(
			desc,
			bs.cluster,
			region,
			operator.OpHotRegion,
			srcStoreID,
			dstPeer)

		counters = append(counters,
			hotDirectionCounter.WithLabelValues("move-peer", rwTy.String(), strconv.FormatUint(srcStoreID, 10), "out"),
			hotDirectionCounter.WithLabelValues("move-peer", rwTy.String(), strconv.FormatUint(dstPeer.GetStoreId(), 10), "in"))
	} else { // transfer leader
		opTy = transferLeader
		if region.GetLeader().GetStoreId() == dstStoreID || region.GetLeader().GetStoreId() != srcStoreID {
			log.Info("leader is already transferred", zap.Uint64("regionID", regionID), zap.Uint64("leaderStoreID", dstStoreID))
			return nil
		}
		desc := "transfer-hot-" + rwTy.String() + "-leader"
		op, err = operator.CreateTransferLeaderOperator(
			desc,
			bs.cluster,
			region,
			srcStoreID,
			dstStoreID,
			operator.OpHotRegion)
		counters = append(counters,
			hotDirectionCounter.WithLabelValues("transfer-leader", rwTy.String(), strconv.FormatUint(srcStoreID, 10), "out"),
			hotDirectionCounter.WithLabelValues("transfer-leader", rwTy.String(), strconv.FormatUint(dstStoreID, 10), "in"))
	}

	if err != nil {
		log.Info("fail to create operator", zap.Stringer("rwType", rwTy), zap.Stringer("opType", opTy), errs.ZapError(err))
		schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
		return nil
	}

	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, counters...)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(h.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(h.GetName(), opTy.String()))

	return op
}

type scheduleCmd struct {
	regionID   uint64
	srcStoreID uint64
	dstStoreID uint64
}

// function for test
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

// function for test
func (h *multiDimensionScheduler) genScheduleRequestFromFile(bs *balanceSolver) []*operator.Operator {
	ret := make([]*operator.Operator, 0)
	schCmds := loadScheduleRequest()
	for _, cmd := range schCmds {
		log.Info(fmt.Sprintf("parse schedule cmd: %v", cmd))
		ri := &regionInfo{
			regionID:   cmd.regionID,
			srcStoreID: cmd.srcStoreID,
			dstStoreID: cmd.dstStoreID,
		}
		op := h.generateOperator(bs, ri, read)
		if op != nil {
			ret = append(ret, op)
		}
	}

	return ret
}

func (h *multiDimensionScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
	return h.hotSched.GetHotReadStatus()
}

func (h *multiDimensionScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
	return h.hotSched.GetHotWriteStatus()
}

func (h *multiDimensionScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.hotSched.GetWritePendingInfluence()
}

func (h *multiDimensionScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.hotSched.GetReadPendingInfluence()
}
