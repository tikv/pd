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

	balanceRatio        = 0.07
	batchOperationLimit = 5
	operationRetryLimit = 10
)

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

	hotSched       *hotScheduler
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
		conf:           conf,
		hotSched:       newHotScheduler(opController, conf),
		regionOpRecord: make(map[uint64]*opRecord),
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
	bs := newBalanceSolver(h.hotSched, cluster, read, transferLeader)

	// ops := bs.genScheduleRequestFromFile(bs) 	// for test
	// return ops

	pendingOps := h.processPendingOps(bs, read)
	if len(pendingOps) > 0 || h.isScheduled {
		return pendingOps
	}

	h.greedyTwoDimension(bs)
	h.isScheduled = true

	return h.processPendingOps(bs, read)
}

func (h *multiDimensionScheduler) processPendingOps(bs *balanceSolver, rw rwType) []*operator.Operator {
	var pendingOps []*operator.Operator

	for regionID, record := range h.regionOpRecord {
		if record.isFinish {
		} else if record.pendingOp == nil {
			if len(pendingOps) < batchOperationLimit {
				op := h.generateOperator(bs, record.region, rw)
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
				op := h.generateOperator(bs, record.region, rw)
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
			id:         cmd.regionID,
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

func (h *multiDimensionScheduler) generateOperator(bs *balanceSolver, ri *regionInfo, rwTy rwType) *operator.Operator {
	var (
		op       *operator.Operator
		counters []prometheus.Counter
		err      error
		opTy     opType
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
		if region.GetLeader().GetStoreId() == dstStoreID {
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

func (h *multiDimensionScheduler) greedyTwoDimension(bs *balanceSolver) {
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

	pendingRegions, needSplit := greedyBalance(storeInfos, balanceRatio)

	// pendingRegions, needSplit := greedySingle(storeInfos, balanceRatio, 0)
	// pendingRegions1, needSplit := greedySingle(storeInfos, balanceRatio, 1)
	// pendingRegions = append(pendingRegions, pendingRegions1...)

	for _, ri := range pendingRegions {
		if needSplit {
			log.Info("split", zap.String("regionInfo", fmt.Sprintf("%+v", ri)))
		} else {
			log.Info("migrate", zap.String("regionInfo", fmt.Sprintf("%+v", ri)))
		}
		h.regionOpRecord[ri.id] = &opRecord{
			region: ri,
		}
	}
}
