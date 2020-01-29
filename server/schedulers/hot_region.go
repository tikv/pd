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
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(HotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler(HotRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotScheduler(opController), nil
	})
	// FIXME: remove this two schedule after the balance test move in schedulers package
	schedule.RegisterScheduler(HotWriteRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotWriteScheduler(opController), nil
	})
	schedule.RegisterScheduler(HotReadRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotReadScheduler(opController), nil
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

	hotRegionLimitFactor    = 0.75
	storeHotPeersDefaultLen = 100
	hotRegionScheduleFactor = 0.95

	minFlowBytes               = 128 * 1024
	maxZombieDur time.Duration = statistics.StoreHeartBeatReportInterval * time.Second

	minRegionScheduleInterval time.Duration = statistics.StoreHeartBeatReportInterval * time.Second
)

// rwType : the perspective of balance
type rwType int

const (
	write rwType = iota
	read
)

type opType int

const (
	movePeer opType = iota
	transferLeader
)

type storeStatistics struct {
	readStatAsLeader  statistics.StoreHotPeersStat
	writeStatAsPeer   statistics.StoreHotPeersStat
	writeStatAsLeader statistics.StoreHotPeersStat
}

func newStoreStatistics() *storeStatistics {
	return &storeStatistics{
		readStatAsLeader:  make(statistics.StoreHotPeersStat),
		writeStatAsLeader: make(statistics.StoreHotPeersStat),
		writeStatAsPeer:   make(statistics.StoreHotPeersStat),
	}
}

type storeLoadPreds struct {
	readLeaders  map[uint64]*storeLoadPred
	writeLeaders map[uint64]*storeLoadPred
	writePeers   map[uint64]*storeLoadPred
}

func newStoreLoadPreds() *storeLoadPreds {
	return &storeLoadPreds{
		readLeaders:  make(map[uint64]*storeLoadPred),
		writeLeaders: make(map[uint64]*storeLoadPred),
		writePeers:   make(map[uint64]*storeLoadPred),
	}
}

type hotScheduler struct {
	name string
	*BaseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []rwType
	r           *rand.Rand

	// states across multiple `Schedule` calls
	readPendings   map[*pendingInfluence]struct{}
	writePendings  map[*pendingInfluence]struct{}
	regionPendings map[uint64][2]*operator.Operator

	// temporary states but exported to API or metrics
	stats           *storeStatistics // store id -> hot regions statistics
	readPendingSum  map[uint64]Influence
	writePendingSum map[uint64]Influence
	readScores      *ScoreInfos
	writeScores     *ScoreInfos

	// temporary states
	stLoadPreds *storeLoadPreds
}

func newHotScheduler(opController *schedule.OperatorController) *hotScheduler {
	base := NewBaseScheduler(opController)
	return &hotScheduler{
		name:           HotRegionName,
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		stats:          newStoreStatistics(),
		types:          []rwType{write, read},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		readScores:     NewScoreInfos(),
		writeScores:    NewScoreInfos(),
		readPendings:   map[*pendingInfluence]struct{}{},
		writePendings:  map[*pendingInfluence]struct{}{},
		regionPendings: make(map[uint64][2]*operator.Operator),

		stLoadPreds: newStoreLoadPreds(),
	}
}

func newHotReadScheduler(opController *schedule.OperatorController) *hotScheduler {
	ret := newHotScheduler(opController)
	ret.name = ""
	ret.types = []rwType{read}
	return ret
}

func newHotWriteScheduler(opController *schedule.OperatorController) *hotScheduler {
	ret := newHotScheduler(opController)
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

	h.readScores = h.analyzeStoreLoad(cluster, read)
	h.writeScores = h.analyzeStoreLoad(cluster, write)

	storesStat := cluster.GetStoresStats()

	{ // update read statistics
		regionRead := cluster.RegionReadStats()
		storeRead := storesStat.GetStoresBytesReadStat()

		leadersLoad, asLeader := calcScore(regionRead, storeRead, cluster, core.LeaderKind)
		h.stats.readStatAsLeader = asLeader
		h.stLoadPreds.readLeaders = calcPendingInfluence(leadersLoad, h.readPendingSum)
	}

	{ // update write statistics
		regionWrite := cluster.RegionWriteStats()
		storeWrite := storesStat.GetStoresBytesWriteStat()

		leadersLoad, asLeader := calcScore(regionWrite, storeWrite, cluster, core.LeaderKind)
		h.stats.writeStatAsLeader = asLeader
		h.stLoadPreds.writeLeaders = calcPendingInfluence(leadersLoad, h.writePendingSum)

		peersLoad, asPeer := calcScore(regionWrite, storeWrite, cluster, core.RegionKind)
		h.stats.writeStatAsPeer = asPeer
		h.stLoadPreds.writePeers = calcPendingInfluence(peersLoad, h.writePendingSum)
	}
}

func calcPendingInfluence(load map[uint64]storeLoad, pending map[uint64]Influence) map[uint64]*storeLoadPred {
	loadPred := make(map[uint64]*storeLoadPred, len(load))
	for id, current := range load {
		loadPred[id] = current.ToLoadPred(pending[id])
	}
	return loadPred
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

func filterUnhealthyStore(cluster opt.Cluster, storeStatsMap map[uint64]float64) {
	for _, id := range getUnhealthyStores(cluster) {
		delete(storeStatsMap, id)
	}
}

func (h *hotScheduler) updateStatsByPendingOpInfo(storeStatsMap map[uint64]float64, balanceType rwType) {
	for storeID := range storeStatsMap {
		if balanceType == read {
			storeStatsMap[storeID] += h.readPendingSum[storeID].ByteRate
		} else {
			storeStatsMap[storeID] += h.writePendingSum[storeID].ByteRate
		}
	}
}

func (h *hotScheduler) gcRegionPendings() {
	for regionID, pendings := range h.regionPendings {
		empty := true
		for ty, op := range pendings {
			if op != nil && op.IsEnd() {
				if time.Now().After(op.GetCreateTime().Add(minRegionScheduleInterval)) {
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

func (h *hotScheduler) analyzeStoreLoad(cluster opt.Cluster, balanceType rwType) *ScoreInfos {
	storesStats := cluster.GetStoresStats()
	var storeStatsMap map[uint64]float64
	if balanceType == read {
		storeStatsMap = storesStats.GetStoresBytesReadStat()
	} else {
		storeStatsMap = storesStats.GetStoresBytesWriteStat()
	}
	flowMean := MeanStoresStats(storeStatsMap)
	if flowMean <= minFlowBytes {
		for id := range storeStatsMap {
			storeStatsMap[id] = 0
		}
	} else {
		filterUnhealthyStore(cluster, storeStatsMap)
		h.updateStatsByPendingOpInfo(storeStatsMap, balanceType)
	}
	return ConvertStoresStats(storeStatsMap)
}

func (h *hotScheduler) addPendingInfluence(op *operator.Operator, srcStore, dstStore uint64, infl Influence, balanceType rwType, ty opType) {
	influence := newPendingInfluence(op, srcStore, dstStore, infl)
	regionID := op.RegionID()
	if balanceType == read {
		h.readPendings[influence] = struct{}{}
	} else {
		h.writePendings[influence] = struct{}{}
	}

	if _, ok := h.regionPendings[regionID]; !ok {
		h.regionPendings[regionID] = [2]*operator.Operator{nil, nil}
	}
	{ // h.pendingOpInfos[regionID][ty] = influence
		tmp := h.regionPendings[regionID]
		tmp[ty] = op
		h.regionPendings[regionID] = tmp
	}

	schedulerStatus.WithLabelValues(h.GetName(), "pending_op_infos").Inc()
}

func calcScore(storeHotPeers map[uint64][]*statistics.HotPeerStat, storeBytesStat map[uint64]float64, cluster opt.Cluster, kind core.ResourceKind) (map[uint64]storeLoad, statistics.StoreHotPeersStat) {
	load := make(map[uint64]storeLoad, len(storeHotPeers))
	stat := make(statistics.StoreHotPeersStat)

	for storeID, items := range storeHotPeers {
		hotPeers, ok := stat[storeID]
		if !ok {
			hotPeers = &statistics.HotPeersStat{
				Stats: make([]statistics.HotPeerStat, 0, storeHotPeersDefaultLen),
			}
			stat[storeID] = hotPeers
		}

		cacheHitsThreshold := cluster.GetHotRegionCacheHitsThreshold()
		for _, peerStat := range items {
			if (kind == core.LeaderKind && !peerStat.IsLeader()) ||
				peerStat.HotDegree < cacheHitsThreshold ||
				cluster.GetRegion(peerStat.RegionID) == nil {
				continue
			}
			hotPeers.TotalBytesRate += peerStat.GetBytesRate()
			hotPeers.Stats = append(hotPeers.Stats, peerStat.Clone())
		}
		hotPeers.Count = len(hotPeers.Stats)
	}

	for id, rate := range storeBytesStat {
		stLoad := storeLoad{
			byteRate: rate,
		}
		if hotPeers, ok := stat[id]; ok {
			stLoad.count = hotPeers.Count
		}
		load[id] = stLoad
	}

	return load, stat
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

// balanceHotRetryLimit is the limit to retry schedule for selected balance strategy.
const balanceHotRetryLimit = 5

func (h *hotScheduler) balanceHotWriteRegions(cluster opt.Cluster) []*operator.Operator {
	for i := 0; i < balanceHotRetryLimit; i++ {
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
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

type balanceSolver struct {
	sche       *hotScheduler
	cluster    opt.Cluster
	stLoadPred map[uint64]*storeLoadPred
	storesStat statistics.StoreHotPeersStat
	rwTy       rwType
	opTy       opType

	// temporary states
	srcStoreID  uint64
	srcPeerStat *statistics.HotPeerStat
	region      *core.RegionInfo
	dstStoreID  uint64
}

func (bs *balanceSolver) init() {
	switch bs.rwTy {
	case read:
		bs.stLoadPred = bs.sche.stLoadPreds.readLeaders
		bs.storesStat = bs.sche.stats.readStatAsLeader
	case write:
		switch bs.opTy {
		case movePeer:
			bs.stLoadPred = bs.sche.stLoadPreds.writePeers
			bs.storesStat = bs.sche.stats.writeStatAsPeer
		case transferLeader:
			bs.stLoadPred = bs.sche.stLoadPreds.writeLeaders
			bs.storesStat = bs.sche.stats.writeStatAsLeader
		}
	}
	for _, id := range getUnhealthyStores(bs.cluster) {
		delete(bs.stLoadPred, id)
		delete(bs.storesStat, id)
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
	if bs.cluster == nil || bs.sche == nil || bs.storesStat == nil {
		return false
	}
	switch bs.rwTy {
	case read, write:
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
	bs.srcStoreID = bs.selectSrcStoreID()
	if bs.srcStoreID == 0 {
		return nil
	}

	for _, srcPeerStat := range bs.getPeerList() {
		bs.srcPeerStat = &srcPeerStat
		bs.region = bs.getRegion()
		if bs.region == nil {
			continue
		}
		dstCandidates := bs.getDstCandidateIDs()
		if len(dstCandidates) <= 0 {
			continue
		}
		bs.dstStoreID = bs.selectDstStoreID(dstCandidates)
		ops := bs.buildOperators()
		if len(ops) > 0 {
			return ops
		}
	}
	return nil
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

func (bs *balanceSolver) selectSrcStoreID() uint64 {
	var id uint64
	switch bs.opTy {
	case movePeer:
		id = selectSrcStoreByByteRate(bs.stLoadPred)
	case transferLeader:
		if bs.rwTy == write {
			id = selectSrcStoreByCount(bs.stLoadPred)
		} else {
			id = selectSrcStoreByByteRate(bs.stLoadPred)
		}
	}
	if id != 0 && bs.cluster.GetStore(id) == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", id))
	}
	return id
}

func (bs *balanceSolver) getPeerList() []statistics.HotPeerStat {
	ret := bs.storesStat[bs.srcStoreID].Stats
	bs.sche.r.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
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
	region := bs.cluster.GetRegion(bs.srcPeerStat.ID())
	if !bs.isRegionAvailable(region) {
		return nil
	}

	switch bs.opTy {
	case movePeer:
		srcPeer := region.GetStorePeer(bs.srcStoreID)
		if srcPeer == nil {
			log.Debug("region does not have a peer on source store, maybe stat out of date", zap.Uint64("region-id", bs.srcPeerStat.ID()))
			return nil
		}
	case transferLeader:
		if region.GetLeader().GetStoreId() != bs.srcStoreID {
			log.Debug("region leader is not on source store, maybe stat out of date", zap.Uint64("region-id", bs.srcPeerStat.ID()))
			return nil
		}
	default:
		return nil
	}

	return region
}

func (bs *balanceSolver) getDstCandidateIDs() map[uint64]struct{} {
	var (
		filters    []filter.Filter
		candidates []*core.StoreInfo
	)

	switch bs.opTy {
	case movePeer:
		var scoreGuard filter.Filter
		if bs.cluster.IsPlacementRulesEnabled() {
			scoreGuard = filter.NewRuleFitFilter(bs.sche.GetName(), bs.cluster, bs.region, bs.srcStoreID)
		} else {
			srcStore := bs.cluster.GetStore(bs.srcStoreID)
			if srcStore == nil {
				return nil
			}
			scoreGuard = filter.NewDistinctScoreFilter(bs.sche.GetName(), bs.cluster.GetLocationLabels(), bs.cluster.GetRegionStores(bs.region), srcStore)
		}

		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.region.GetStoreIds(), bs.region.GetStoreIds()),
			filter.NewHealthFilter(bs.sche.GetName()),
			scoreGuard,
		}

		candidates = bs.cluster.GetStores()

	case transferLeader:
		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true},
			filter.NewHealthFilter(bs.sche.GetName()),
		}

		candidates = bs.cluster.GetFollowerStores(bs.region)

	default:
		return nil
	}

	ret := make(map[uint64]struct{}, len(candidates))
	for _, store := range candidates {
		if !filter.Target(bs.cluster, store, filters) {
			ret[store.GetID()] = struct{}{}
		}
	}
	return ret
}

func (bs *balanceSolver) selectDstStoreID(candidateIDs map[uint64]struct{}) uint64 {
	candidateLoadPred := make(map[uint64]*storeLoadPred, len(candidateIDs))
	for id := range candidateIDs {
		candidateLoadPred[id] = bs.stLoadPred[id]
	}
	switch bs.opTy {
	case movePeer:
		return selectDstStoreByByteRate(candidateLoadPred, bs.srcPeerStat.GetBytesRate(), bs.stLoadPred[bs.srcStoreID])
	case transferLeader:
		if bs.rwTy == write {
			return selectDstStoreByCount(candidateLoadPred, bs.srcPeerStat.GetBytesRate(), bs.stLoadPred[bs.srcStoreID])
		}
		return selectDstStoreByByteRate(candidateLoadPred, bs.srcPeerStat.GetBytesRate(), bs.stLoadPred[bs.srcStoreID])
	default:
		return 0
	}
}

func (bs *balanceSolver) isReadyToBuild() bool {
	if bs.srcStoreID == 0 || bs.dstStoreID == 0 ||
		bs.srcPeerStat == nil || bs.region == nil {
		return false
	}
	if bs.srcStoreID != bs.srcPeerStat.StoreID ||
		bs.region.GetID() != bs.srcPeerStat.ID() {
		return false
	}
	return true
}

func (bs *balanceSolver) buildOperators() []*operator.Operator {
	if !bs.isReadyToBuild() {
		return nil
	}
	var (
		op  *operator.Operator
		err error
	)

	switch bs.opTy {
	case movePeer:
		srcPeer := bs.region.GetStorePeer(bs.srcStoreID) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: bs.dstStoreID, IsLearner: srcPeer.IsLearner}
		bs.sche.peerLimit = bs.sche.adjustBalanceLimit(bs.srcStoreID, bs.storesStat)
		op, err = operator.CreateMovePeerOperator("move-hot-"+bs.rwTy.String()+"-region", bs.cluster, bs.region, operator.OpHotRegion, bs.srcStoreID, dstPeer)
	case transferLeader:
		if bs.region.GetStoreVoter(bs.dstStoreID) == nil {
			return nil
		}
		bs.sche.leaderLimit = bs.sche.adjustBalanceLimit(bs.srcStoreID, bs.storesStat)
		op, err = operator.CreateTransferLeaderOperator("transfer-hot-"+bs.rwTy.String()+"-leader", bs.cluster, bs.region, bs.srcStoreID, bs.dstStoreID, operator.OpHotRegion)
	}

	if err != nil {
		log.Debug("fail to create operator", zap.Error(err), zap.Stringer("opType", bs.opTy), zap.Stringer("rwType", bs.rwTy))
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "create-operator-fail").Inc()
		return nil
	}

	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(bs.sche.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(bs.sche.GetName(), bs.opTy.String()))

	infl := Influence{ByteRate: bs.srcPeerStat.GetBytesRate()}
	if bs.opTy == transferLeader && bs.rwTy == write {
		infl.ByteRate = 0
	}
	bs.sche.addPendingInfluence(op, bs.srcStoreID, bs.dstStoreID, infl, bs.rwTy, bs.opTy)

	return []*operator.Operator{op}
}

// Sort stores according to their load prediction.
func sortStores(loadPred map[uint64]*storeLoadPred, better func(lp1, lp2 *storeLoadPred) bool) []uint64 {
	ids := make([]uint64, 0, len(loadPred))
	for id := range loadPred {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		id1, id2 := ids[i], ids[j]
		return better(loadPred[id1], loadPred[id2])
	})
	return ids
}

// Prefer store with larger `count`.
func selectSrcStoreByCount(loadPred map[uint64]*storeLoadPred) uint64 {
	stores := sortStores(loadPred, func(lp1, lp2 *storeLoadPred) bool {
		ld1, ld2 := lp1.min(), lp2.min()
		if ld1.count > ld2.count ||
			(ld1.count == ld2.count && ld1.byteRate > ld2.byteRate) {
			return true
		}
		return false
	})

	if len(stores) > 0 && loadPred[stores[0]].current.count > 1 {
		return stores[0]
	}
	return 0
}

// Prefer store with larger `byteRate`.
func selectSrcStoreByByteRate(loadPred map[uint64]*storeLoadPred) uint64 {
	stores := sortStores(loadPred, func(lp1, lp2 *storeLoadPred) bool {
		ld1, ld2 := lp1.min(), lp2.min()
		if ld1.byteRate > ld2.byteRate ||
			(ld1.byteRate == ld2.byteRate && ld1.count > ld2.count) {
			return true
		}
		return false
	})

	for _, id := range stores {
		if loadPred[id].current.count > 1 {
			return id
		}
	}
	return 0
}

// Prefer store with smaller `count`.
func selectDstStoreByCount(candidates map[uint64]*storeLoadPred, regionBytesRate float64, srcLoadPred *storeLoadPred) uint64 {
	stores := sortStores(candidates, func(lp1, lp2 *storeLoadPred) bool {
		ld1, ld2 := lp1.max(), lp2.max()
		if ld1.count < ld2.count ||
			(ld1.count == ld2.count && ld1.byteRate < ld2.byteRate) {
			return true
		}
		return false
	})

	srcLoad := srcLoadPred.min()
	for _, id := range stores {
		dstLoad := candidates[id].max()
		if srcLoad.count-1 >= dstLoad.count+1 &&
			srcLoad.byteRate*hotRegionScheduleFactor > dstLoad.byteRate+regionBytesRate {
			return id
		}
	}
	return 0
}

// Prefer store with smaller `byteRate`.
func selectDstStoreByByteRate(candidates map[uint64]*storeLoadPred, regionBytesRate float64, srcLoadPred *storeLoadPred) uint64 {
	stores := sortStores(candidates, func(lp1, lp2 *storeLoadPred) bool {
		ld1, ld2 := lp1.max(), lp2.max()
		if ld1.byteRate < ld2.byteRate ||
			(ld1.byteRate == ld2.byteRate && ld1.count < ld2.count) {
			return true
		}
		return false
	})

	srcLoad := srcLoadPred.min()
	for _, id := range stores {
		dstLoad := candidates[id].max()
		if srcLoad.byteRate*hotRegionScheduleFactor > dstLoad.byteRate+regionBytesRate {
			return id
		}
	}
	return 0
}

func (h *hotScheduler) adjustBalanceLimit(storeID uint64, storesStat statistics.StoreHotPeersStat) uint64 {
	srcStoreStatistics := storesStat[storeID]

	var hotRegionTotalCount int
	for _, m := range storesStat {
		hotRegionTotalCount += len(m.Stats)
	}

	avgRegionCount := float64(hotRegionTotalCount) / float64(len(storesStat))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(len(srcStoreStatistics.Stats)) - avgRegionCount) * hotRegionLimitFactor)
	return maxUint64(limit, 1)
}

func (h *hotScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stats.readStatAsLeader))
	for id, stat := range h.stats.readStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
	}
}

func (h *hotScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stats.writeStatAsLeader))
	asPeer := make(statistics.StoreHotPeersStat, len(h.stats.writeStatAsPeer))
	for id, stat := range h.stats.writeStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	for id, stat := range h.stats.writeStatAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

func (h *hotScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(write)
}

func (h *hotScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(read)
}

func (h *hotScheduler) copyPendingInfluence(typ rwType) map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	var pendingSum map[uint64]Influence
	if typ == read {
		pendingSum = h.readPendingSum
	} else {
		pendingSum = h.writePendingSum
	}
	ret := make(map[uint64]Influence, len(pendingSum))
	for id, infl := range pendingSum {
		ret[id] = infl
	}
	return ret
}

func (h *hotScheduler) GetReadScores() map[uint64]float64 {
	return h.GetStoresScore(read)
}

func (h *hotScheduler) GetWriteScores() map[uint64]float64 {
	return h.GetStoresScore(write)
}

func (h *hotScheduler) GetStoresScore(typ rwType) map[uint64]float64 {
	h.RLock()
	defer h.RUnlock()
	storesScore := make(map[uint64]float64)
	var infos []*ScoreInfo
	if typ == read {
		infos = h.readScores.ToSlice()
	} else {
		infos = h.writeScores.ToSlice()
	}
	for _, info := range infos {
		storesScore[info.GetStoreID()] = info.GetScore()
	}
	return storesScore
}

func calcPendingWeight(op *operator.Operator) float64 {
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
		if zombieDur >= maxZombieDur {
			return 0
		}
		// TODO: use store statistics update time to make a more accurate estimation
		return float64(maxZombieDur-zombieDur) / float64(maxZombieDur)
	default:
		return 0
	}
}

func (h *hotScheduler) summaryPendingInfluence() {
	h.readPendingSum = summaryPendingInfluence(h.readPendings, calcPendingWeight)
	h.writePendingSum = summaryPendingInfluence(h.writePendings, calcPendingWeight)
	h.gcRegionPendings()
}

func (h *hotScheduler) clearPendingInfluence() {
	h.readPendings = map[*pendingInfluence]struct{}{}
	h.writePendings = map[*pendingInfluence]struct{}{}
	h.readPendingSum = nil
	h.writePendingSum = nil
	h.regionPendings = make(map[uint64][2]*operator.Operator)
}

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
