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
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core/constant"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitHotReadBuckets     = "split-hot-read-region"
	splitHotWriteBuckets    = "split-hot-write-region"
	splitProgressiveRank    = 5
	minHotScheduleInterval  = time.Second
	maxHotScheduleInterval  = 20 * time.Second
	defaultPendingAmpFactor = 2.0
	defaultStddevThreshold  = 0.1
	defaultTopnPosition     = 10
)

var (
	// pendingAmpFactor will amplify the impact of pending influence, making scheduling slower or even serial when two stores are close together
	pendingAmpFactor = defaultPendingAmpFactor
	// If the distribution of a dimension is below the corresponding stddev threshold, then scheduling will no longer be based on this dimension,
	// as it implies that this dimension is sufficiently uniform.
	stddevThreshold = defaultStddevThreshold
	// topnPosition is the position of the topn peer in the hot peer list.
	// We use it to judge whether to schedule the hot peer in some cases.
	topnPosition = defaultTopnPosition
	// statisticsInterval is the interval to update statistics information.
	statisticsInterval = time.Second
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
	regionPendings map[uint64]*pendingInfluence
	// types is the resource types that the scheduler considers.
	types           []resourceType
	r               *rand.Rand
	updateReadTime  time.Time
	updateWriteTime time.Time
}

func newBaseHotScheduler(
	opController *operator.Controller,
	sampleDuration, sampleInterval time.Duration,
	schedulerConfig schedulerConfig,
) *baseHotScheduler {
	base := NewBaseScheduler(opController, types.BalanceHotRegionScheduler, schedulerConfig)
	ret := &baseHotScheduler{
		BaseScheduler:  base,
		regionPendings: make(map[uint64]*pendingInfluence),
		stHistoryLoads: statistics.NewStoreHistoryLoads(utils.DimLen, sampleDuration, sampleInterval),
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.types = append(ret.types, ty)
		ret.stLoadInfos[ty] = map[uint64]*statistics.StoreLoadDetail{}
	}
	return ret
}

// prepareForBalance calculate the summary of pending Influence for each store and prepare the load detail for
// each store, only update read or write load detail
func (s *baseHotScheduler) prepareForBalance(typ resourceType, cluster sche.SchedulerCluster) {
	storeInfos := statistics.SummaryStoreInfos(cluster.GetStores())
	s.summaryPendingInfluence(storeInfos)
	storesLoads := cluster.GetStoresLoads()
	isTraceRegionFlow := cluster.GetSchedulerConfig().IsTraceRegionFlow()

	prepare := func(regionStats map[uint64][]*statistics.HotPeerStat, rw utils.RWType, resource constant.ResourceKind) {
		ty := buildResourceType(rw, resource)
		s.stLoadInfos[ty] = statistics.SummaryStoresLoad(
			storeInfos,
			storesLoads,
			s.stHistoryLoads,
			regionStats,
			isTraceRegionFlow,
			rw, resource)
	}
	switch typ {
	case readLeader, readPeer:
		// update read statistics
		// avoid to update read statistics frequently
		if time.Since(s.updateReadTime) >= statisticsInterval {
			regionRead := cluster.GetHotPeerStats(utils.Read)
			prepare(regionRead, utils.Read, constant.LeaderKind)
			prepare(regionRead, utils.Read, constant.RegionKind)
			s.updateReadTime = time.Now()
		}
	case writeLeader, writePeer:
		// update write statistics
		// avoid to update write statistics frequently
		if time.Since(s.updateWriteTime) >= statisticsInterval {
			regionWrite := cluster.GetHotPeerStats(utils.Write)
			prepare(regionWrite, utils.Write, constant.LeaderKind)
			prepare(regionWrite, utils.Write, constant.RegionKind)
			s.updateWriteTime = time.Now()
		}
	default:
		log.Error("invalid resource type", zap.String("type", typ.String()))
	}
}

func (s *baseHotScheduler) updateHistoryLoadConfig(sampleDuration, sampleInterval time.Duration) {
	s.stHistoryLoads = s.stHistoryLoads.UpdateConfig(sampleDuration, sampleInterval)
}

// summaryPendingInfluence calculate the summary of pending Influence for each store
// and clean the region from regionInfluence if they have ended operator.
// It makes each dim rate or count become `weight` times to the origin value.
func (s *baseHotScheduler) summaryPendingInfluence(storeInfos map[uint64]*statistics.StoreSummaryInfo) {
	for id, p := range s.regionPendings {
		for _, from := range p.froms {
			from := storeInfos[from]
			to := storeInfos[p.to]
			maxZombieDur := p.maxZombieDuration
			weight, needGC := calcPendingInfluence(p.op, maxZombieDur)

			if needGC {
				delete(s.regionPendings, id)
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
	// for metrics
	for storeID, info := range storeInfos {
		storeLabel := strconv.FormatUint(storeID, 10)
		if infl := info.PendingSum; infl != nil && len(infl.Loads) != 0 {
			utils.ForeachRegionStats(func(rwTy utils.RWType, dim int, kind utils.RegionStatKind) {
				HotPendingSum.WithLabelValues(storeLabel, rwTy.String(), utils.DimToString(dim)).Set(infl.Loads[kind])
			})
		}
	}
}

func (s *baseHotScheduler) randomType() resourceType {
	return s.types[s.r.Int()%len(s.types)]
}

type hotScheduler struct {
	*baseHotScheduler
	syncutil.RWMutex
	// config of hot scheduler
	conf                *hotRegionSchedulerConfig
	searchRevertRegions [resourceTypeLen]bool // Whether to search revert regions.
}

func newHotScheduler(opController *operator.Controller, conf *hotRegionSchedulerConfig) *hotScheduler {
	base := newBaseHotScheduler(opController, conf.getHistorySampleDuration(),
		conf.getHistorySampleInterval(), conf)
	ret := &hotScheduler{
		baseHotScheduler: base,
		conf:             conf,
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.searchRevertRegions[ty] = false
	}
	return ret
}

// EncodeConfig implements the Scheduler interface.
func (s *hotScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.encodeConfig()
}

// ReloadConfig impl
func (s *hotScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &hotRegionSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.MinHotByteRate = newCfg.MinHotByteRate
	s.conf.MinHotKeyRate = newCfg.MinHotKeyRate
	s.conf.MinHotQueryRate = newCfg.MinHotQueryRate
	s.conf.MaxZombieRounds = newCfg.MaxZombieRounds
	s.conf.MaxPeerNum = newCfg.MaxPeerNum
	s.conf.ByteRateRankStepRatio = newCfg.ByteRateRankStepRatio
	s.conf.KeyRateRankStepRatio = newCfg.KeyRateRankStepRatio
	s.conf.QueryRateRankStepRatio = newCfg.QueryRateRankStepRatio
	s.conf.CountRankStepRatio = newCfg.CountRankStepRatio
	s.conf.GreatDecRatio = newCfg.GreatDecRatio
	s.conf.MinorDecRatio = newCfg.MinorDecRatio
	s.conf.SrcToleranceRatio = newCfg.SrcToleranceRatio
	s.conf.DstToleranceRatio = newCfg.DstToleranceRatio
	s.conf.WriteLeaderPriorities = newCfg.WriteLeaderPriorities
	s.conf.WritePeerPriorities = newCfg.WritePeerPriorities
	s.conf.ReadPriorities = newCfg.ReadPriorities
	s.conf.StrictPickingStore = newCfg.StrictPickingStore
	s.conf.EnableForTiFlash = newCfg.EnableForTiFlash
	s.conf.RankFormulaVersion = newCfg.RankFormulaVersion
	s.conf.ForbidRWType = newCfg.ForbidRWType
	s.conf.SplitThresholds = newCfg.SplitThresholds
	s.conf.HistorySampleDuration = newCfg.HistorySampleDuration
	s.conf.HistorySampleInterval = newCfg.HistorySampleInterval
	return nil
}

// ServeHTTP implements the http.Handler interface.
func (s *hotScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.conf.ServeHTTP(w, r)
}

// GetMinInterval implements the Scheduler interface.
func (*hotScheduler) GetMinInterval() time.Duration {
	return minHotScheduleInterval
}

// GetNextInterval implements the Scheduler interface.
func (s *hotScheduler) GetNextInterval(time.Duration) time.Duration {
	return intervalGrow(s.GetMinInterval(), maxHotScheduleInterval, exponentialGrowth)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *hotScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetSchedulerConfig().GetHotRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpHotRegion)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *hotScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	hotSchedulerCounter.Inc()
	typ := s.randomType()
	return s.dispatch(typ, cluster), nil
}

func (s *hotScheduler) dispatch(typ resourceType, cluster sche.SchedulerCluster) (ops []*operator.Operator) {
	s.Lock()
	defer s.Unlock()
	s.updateHistoryLoadConfig(s.conf.getHistorySampleDuration(), s.conf.getHistorySampleInterval())
	s.prepareForBalance(typ, cluster)
	// isForbidRWType can not be move earlier to support to use api and metrics.
	switch typ {
	case readLeader, readPeer:
		if s.conf.isForbidRWType(utils.Read) {
			return nil
		}
		ops = s.balanceHotReadRegions(cluster)
	case writePeer:
		if s.conf.isForbidRWType(utils.Write) {
			return nil
		}
		ops = s.balanceHotWritePeers(cluster)
	case writeLeader:
		if s.conf.isForbidRWType(utils.Write) {
			return nil
		}
		ops = s.balanceHotWriteLeaders(cluster)
	}
	if len(ops) == 0 {
		hotSchedulerSkipCounter.Inc()
	}
	return ops
}

func (s *hotScheduler) tryAddPendingInfluence(op *operator.Operator, srcStore []uint64, dstStore uint64, infl statistics.Influence, maxZombieDur time.Duration) bool {
	regionID := op.RegionID()
	_, ok := s.regionPendings[regionID]
	if ok {
		pendingOpFailsStoreCounter.Inc()
		return false
	}

	influence := newPendingInfluence(op, srcStore, dstStore, infl, maxZombieDur)
	s.regionPendings[regionID] = influence

	utils.ForeachRegionStats(func(rwTy utils.RWType, dim int, kind utils.RegionStatKind) {
		hotPeerHist.WithLabelValues(s.GetName(), rwTy.String(), utils.DimToString(dim)).Observe(infl.Loads[kind])
	})
	return true
}

func (s *hotScheduler) balanceHotReadRegions(cluster sche.SchedulerCluster) []*operator.Operator {
	leaderSolver := newBalanceSolver(s, cluster, utils.Read, transferLeader)
	leaderOps := leaderSolver.solve()
	peerSolver := newBalanceSolver(s, cluster, utils.Read, movePeer)
	peerOps := peerSolver.solve()
	if len(leaderOps) == 0 && len(peerOps) == 0 {
		return nil
	}
	if len(leaderOps) == 0 {
		if peerSolver.tryAddPendingInfluence() {
			return peerOps
		}
		return nil
	}
	if len(peerOps) == 0 {
		if leaderSolver.tryAddPendingInfluence() {
			return leaderOps
		}
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
	return nil
}

func (s *hotScheduler) balanceHotWritePeers(cluster sche.SchedulerCluster) []*operator.Operator {
	peerSolver := newBalanceSolver(s, cluster, utils.Write, movePeer)
	ops := peerSolver.solve()
	if len(ops) > 0 && peerSolver.tryAddPendingInfluence() {
		return ops
	}
	return nil
}

func (s *hotScheduler) balanceHotWriteLeaders(cluster sche.SchedulerCluster) []*operator.Operator {
	leaderSolver := newBalanceSolver(s, cluster, utils.Write, transferLeader)
	ops := leaderSolver.solve()
	if len(ops) > 0 && leaderSolver.tryAddPendingInfluence() {
		return ops
	}

	return nil
}

type rank interface {
	isAvailable(*solution) bool
	filterUniformStore() (string, bool)
	needSearchRevertRegions() bool
	setSearchRevertRegions()
	calcProgressiveRank()
	betterThan(*solution) bool
	rankToDimString() string
	checkByPriorityAndTolerance(loads []float64, f func(int) bool) bool
	checkHistoryLoadsByPriority(loads [][]float64, f func(int) bool) bool
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

func prioritiesToDim(priorities []string) (firstPriority int, secondPriority int) {
	return utils.StringToDim(priorities[0]), utils.StringToDim(priorities[1])
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
