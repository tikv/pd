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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
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

	// FIXME: remove this two schedule after the balance test move in schedulers package
	{
		schedule.RegisterScheduler(HotWriteRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
			return newHotWriteScheduler(opController, initHotRegionScheduleConfig()), nil
		})
		schedule.RegisterScheduler(HotReadRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
			return newHotReadScheduler(opController, initHotRegionScheduleConfig()), nil
		})

	}
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

	divisor                   = float64(1000) * 2
	hotWriteRegionMinFlowRate = 16 * 1024
	hotReadRegionMinFlowRate  = 128 * 1024
	hotWriteRegionMinKeyRate  = 256
	hotReadRegionMinKeyRate   = 512
)

// schedulePeerPr the probability of schedule the hot peer.
var schedulePeerPr = 0.66

type rankStatus struct {
	rank  int64
	dims  [3]dimStatus // byte,key,qps
	order [3]uint64
}

func newRankStatus(rs *rankSet, qpsStatus, byteStatus, keyStatus dimStatus) *rankStatus { // bs.conf
	rank := rs.getRank(qpsStatus, keyStatus, byteStatus)
	return &rankStatus{
		rank: rank,
		dims: [3]dimStatus{
			statistics.ByteDim: byteStatus,
			statistics.KeyDim:  keyStatus,
			statistics.QPSDim:  qpsStatus,
		},
		order: rs.getPerm(),
	}
}

func (r *rankStatus) statusNum(s dimStatus) uint64 {
	var num uint64
	for _, dim := range r.dims {
		if dim == s {
			num++
		}
	}
	return num
}

func (r *rankStatus) getDim(s dimStatus) int {
	for i, dim := range r.dims {
		if dim == s {
			return i
		}
	}
	return statistics.DimLen
}

func (r *rankStatus) cmp(dimRkCmps []int) bool {
	than := func(dimRkCmp int, status dimStatus) (bool, bool) {
		if dimRkCmp != 0 {
			if status == noWorse {
				// prefer smaller rate, to reduce oscillation
				return dimRkCmp < 0, true
			}
			if status == better {
				// prefer region with larger rate, to converge faster
				return dimRkCmp > 0, true
			}
		}
		return false, false
	}

	thanByOrder := func(status dimStatus) (bool, bool) {
		for _, o := range r.order {
			dimRkCmp := dimRkCmps[o]
			if result, ok := than(dimRkCmp, status); ok {
				return result, true
			}
		}
		return false, false
	}

	switch r.statusNum(better) {
	case 0:
		if result, ok := thanByOrder(noWorse); ok {
			return result
		}
	case 3:
		if result, ok := thanByOrder(better); ok {
			return result
		}
	case 2:
		// first to compare only noWorse dim
		if result, ok := than(dimRkCmps[r.getDim(noWorse)], noWorse); ok {
			return result
		}
		// second to compare better dim
		if result, ok := thanByOrder(better); ok {
			return result
		}
	case 1:
		// first to compare only better dim
		if result, ok := than(dimRkCmps[r.getDim(better)], better); ok {
			return result
		}
		// second to compare noWorse dim
		if result, ok := thanByOrder(noWorse); ok {
			return result
		}
	default:
		return false
	}
	return false
}

type rankSet struct {
	conf *hotRegionSchedulerConfig
	num  uint64
	set  map[uint64]int64
}

func newRankSet(conf *hotRegionSchedulerConfig) *rankSet {
	return &rankSet{
		conf: conf,
		num:  0,
		set:  nil,
	}
}

// qps better, key better, bytes better =>7
// qps better, key better, bytes noWorse =>6
// qps better, key noWorse, bytes better =>5
// qps better, key noWorse, bytes noWorse =>4
// qps noWorse, key better, bytes better =>3
// qps noWorse, key better, bytes noWorse =>2
// qps noWorse, key noWorse, bytes better =>1
// 7,(6,5,3),(4,2,1),0
var (
	p = [6][3]uint64{
		{0, 1, 2}, // ByteDim, KeyDim, QPSDim
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}
)

func perm(n uint64, ranks []uint64) []uint64 {
	if n > 5 {
		log.Info("TestError")
		return nil
	}
	var ret []uint64
	for _, num := range p[n] {
		ret = append(ret, ranks[num])
	}
	return ret
}

func genSet(num uint64) map[uint64]int64 {
	set := make(map[uint64]int64)
	twoBetter := []uint64{3, 5, 6} // key+byte , qps+byte , qps+key
	oneBetter := []uint64{1, 2, 4} // byte , key , qps
	twoBetterPerm := perm(num, twoBetter)
	oneBetterPerm := perm(num, oneBetter)
	rank := int64(-2)
	perm := append(oneBetterPerm, twoBetterPerm...)
	for _, ord := range perm {
		set[ord] = rank
		rank--
	}
	return set
}

func (rs *rankSet) getRank(qpsStatus, keyStatus, bytesStatus dimStatus) int64 {
	if qpsStatus == better && keyStatus == better && bytesStatus == better {
		return -7
	}

	if qpsStatus == noWorse && keyStatus == noWorse && bytesStatus == noWorse {
		return -1
	}

	if qpsStatus == worse || keyStatus == worse || bytesStatus == worse {
		return 0
	}

	ord := uint64((qpsStatus-1)*4 + (keyStatus-1)*2 + bytesStatus - 1)
	if rs.num != rs.conf.GetRank() || rs.set == nil {
		rs.num = rs.conf.GetRank()
		rs.set = genSet(rs.num)
	}

	if rank, ok := rs.set[ord]; ok {
		return rank
	}
	log.Info("TestError")
	return 0
}

func (rs *rankSet) getPerm() [3]uint64 {
	return p[rs.num]
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
	pendings [resourceTypeLen]map[*pendingInfluence]struct{}
	// regionPendings stores regionID -> [opType]Operator
	// this records regionID which have pending Operator by operation type. During filterHotPeers, the hot peers won't
	// be selected if its owner region is tracked in this attribute.
	regionPendings map[uint64][2]*operator.Operator

	// temporary states but exported to API or metrics
	stLoadInfos [resourceTypeLen]map[uint64]*storeLoadDetail
	// pendingSums indicates the [resourceType] storeID -> pending Influence
	// This stores the pending Influence for each store by resource type.
	pendingSums [resourceTypeLen]map[uint64]Influence
	// config of hot scheduler
	conf *hotRegionSchedulerConfig
	rs   *rankSet
}

func newHotScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	base := NewBaseScheduler(opController)
	ret := &hotScheduler{
		name:           HotRegionName,
		BaseScheduler:  base,
		leaderLimit:    1,
		peerLimit:      1,
		types:          []rwType{write, read},
		r:              rand.New(rand.NewSource(time.Now().UnixNano())),
		regionPendings: make(map[uint64][2]*operator.Operator),
		conf:           conf,
		rs:             newRankSet(conf),
	}
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		ret.pendings[ty] = map[*pendingInfluence]struct{}{}
		ret.stLoadInfos[ty] = map[uint64]*storeLoadDetail{}
	}
	return ret
}

func newHotReadScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []rwType{read}
	return ret
}

func newHotWriteScheduler(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
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
	return h.allowBalanceLeader(cluster) || h.allowBalanceRegion(cluster)
}

func (h *hotScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit() &&
		h.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (h *hotScheduler) allowBalanceRegion(cluster opt.Cluster) bool {
	return h.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit()
}

func (h *hotScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	rankMetricsStatus.WithLabelValues("hot").Set(float64(h.conf.GetRank()))
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

	storesStat := cluster.GetStoresStats()

	minHotDegree := cluster.GetOpts().GetHotRegionCacheHitsThreshold()
	{ // update read statistics
		regionRead := cluster.RegionReadStats()
		storeByte := storesStat.GetStoresBytesReadStat()
		storeKey := storesStat.GetStoresKeysReadStat()
		storeQPS := storesStat.GetStoresQPSReadStat()
		hotRegionThreshold := getHotRegionThreshold(storesStat, read)
		h.stLoadInfos[readLeader] = summaryStoresLoad(
			storeByte,
			storeKey,
			storeQPS,
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
		storeQPS := storesStat.GetStoresQPSWriteStat()
		hotRegionThreshold := getHotRegionThreshold(storesStat, write)
		h.stLoadInfos[writeLeader] = summaryStoresLoad(
			storeByte,
			storeKey,
			storeQPS,
			h.pendingSums[writeLeader],
			regionWrite,
			minHotDegree,
			hotRegionThreshold,
			write, core.LeaderKind, mixed)

		h.stLoadInfos[writePeer] = summaryStoresLoad(
			storeByte,
			storeKey,
			storeQPS,
			h.pendingSums[writePeer],
			regionWrite,
			minHotDegree,
			hotRegionThreshold,
			write, core.RegionKind, mixed)
	}
}

// getHotRegionThreshold return the min rate for the rw(read/write) rate and key rate
func getHotRegionThreshold(stats *statistics.StoresStats, typ rwType) [2]uint64 {
	var hotRegionThreshold [2]uint64
	switch typ {
	case write:
		hotRegionThreshold[0] = uint64(stats.TotalBytesWriteRate() / divisor)
		if hotRegionThreshold[0] < hotWriteRegionMinFlowRate {
			hotRegionThreshold[0] = hotWriteRegionMinFlowRate
		}
		hotRegionThreshold[1] = uint64(stats.TotalKeysWriteRate() / divisor)
		if hotRegionThreshold[1] < hotWriteRegionMinKeyRate {
			hotRegionThreshold[1] = hotWriteRegionMinKeyRate
		}
		return hotRegionThreshold
	case read:
		hotRegionThreshold[0] = uint64(stats.TotalBytesReadRate() / divisor)
		if hotRegionThreshold[0] < hotReadRegionMinFlowRate {
			hotRegionThreshold[0] = hotReadRegionMinFlowRate
		}
		hotRegionThreshold[1] = uint64(stats.TotalKeysWriteRate() / divisor)
		if hotRegionThreshold[1] < hotReadRegionMinKeyRate {
			hotRegionThreshold[1] = hotReadRegionMinKeyRate
		}
		return hotRegionThreshold
	default:
		panic("invalid type: " + typ.String())
	}
}

// summaryPendingInfluence calculate the summary of pending Influence for each store
// and clean the region from regionInfluence if they have ended operator.
func (h *hotScheduler) summaryPendingInfluence() {
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendingSums[ty] = summaryPendingInfluence(h.pendings[ty], h.calcPendingWeight)
	}
	h.gcRegionPendings()
}

// gcRegionPendings check the region whether it need to be deleted from regionPendings depended on whether it have
// ended operator
func (h *hotScheduler) gcRegionPendings() {
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

// summaryStoresLoad Load information of all available stores.
// it will filtered the hot peer and calculate the current and future stat(byte/key rate,count) for each store
func summaryStoresLoad(
	storeByteRate map[uint64]float64,
	storeKeyRate map[uint64]float64,
	storeQPS map[uint64]float64,
	storePendings map[uint64]Influence,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	minHotDegree int,
	hotRegionThreshold [2]uint64,
	rwTy rwType,
	kind core.ResourceKind,
	hotPeerFilterTy hotPeerFilterType,
) map[uint64]*storeLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(key/byte rate,count)
	loadDetail := make(map[uint64]*storeLoadDetail, len(storeByteRate))
	// Stores without byte rate statistics is not available to schedule.
	for id, byteRate := range storeByteRate {
		keyRate := storeKeyRate[id]
		QPS := storeQPS[id]

		// Find all hot peers first
		hotPeers := make([]*statistics.HotPeerStat, 0)
		{
			byteSum := 0.0
			keySum := 0.0
			QPSSum := 0.0
			for _, peer := range filterHotPeers(kind, minHotDegree, hotRegionThreshold, storeHotPeers[id], hotPeerFilterTy) {
				byteSum += peer.GetByteRate()
				keySum += peer.GetKeyRate()
				QPSSum += peer.GetQPS()
				hotPeers = append(hotPeers, peer.Clone())
			}
			// Use sum of hot peers to estimate leader-only byte rate.
			if kind == core.LeaderKind && rwTy == write {
				byteRate = byteSum
				keyRate = keySum
				QPS = QPSSum
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
			{
				ty := "QPS-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(QPSSum)
			}
		}
		// Build store load prediction from current load and pending influence.
		stLoadPred := (&storeLoad{
			ByteRate: byteRate,
			KeyRate:  keyRate,
			QPS:      QPS,
			Count:    float64(len(hotPeers)),
		}).ToLoadPred(storePendings[id])

		// Construct store load info.
		loadDetail[id] = &storeLoadDetail{
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
		}
	}
	return loadDetail
}

// filterHotPeers filter the peer whose hot degree is less than minHotDegress
func filterHotPeers(
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
			// As hotPeerFilterTy here is always mix currently, isHotPeerFiltered will directly return false here.
			isHotPeerFiltered(peer, hotRegionThreshold, hotPeerFilterTy) {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}

// isHotPeerFiltered compare whether the target peer would be filtered depended on its stat and hot rate threshold
// In case mixed, we directly return false
func isHotPeerFiltered(peer *statistics.HotPeerStat, hotRegionThreshold [2]uint64, hotPeerFilterTy hotPeerFilterType) bool {
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

func (h *hotScheduler) addPendingInfluence(op *operator.Operator, srcStore, dstStore uint64, infl Influence, rwTy rwType, opTy opType) bool {
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
}

type solution struct {
	srcStoreID  uint64
	srcPeerStat *statistics.HotPeerStat
	region      *core.RegionInfo
	dstStoreID  uint64

	// progressiveRank measures the contribution for balance.
	// The smaller the rank, the better this solution is.
	// If rank < 0, this solution makes thing better.
	progressiveRank *rankStatus
}

func (bs *balanceSolver) init() {
	switch toResourceType(bs.rwTy, bs.opTy) {
	case writePeer:
		bs.stLoadDetail = bs.sche.stLoadInfos[writePeer]
	case writeLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[writeLeader]
	case readLeader:
		bs.stLoadDetail = bs.sche.stLoadInfos[readLeader]
	}
	// And it will be unnecessary to filter unhealthy store, because it has been solved in process heartbeat

	bs.maxSrc = &storeLoad{}
	bs.minDst = &storeLoad{
		ByteRate: math.MaxFloat64,
		KeyRate:  math.MaxFloat64,
		QPS:      math.MaxFloat64,
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
		QPS:      maxCur.QPS * bs.sche.conf.GetKeyRankStepRatio(),
		Count:    maxCur.Count * bs.sche.conf.GetCountRankStepRatio(),
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
	if !bs.isValid() || !bs.allowBalance() {
		return nil
	}
	bs.cur = &solution{}
	var (
		best  *solution
		ops   []*operator.Operator
		infls []Influence
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
				if bs.cur.progressiveRank.rank < 0 && bs.betterThan(best) {
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

// allowBalance check whether the operator count have exceed the hot region limit by type
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

// filterSrcStores compare the min rate and the ratio * expectation rate, if both key and byte rate is greater than
// its expectation * ratio, the store would be selected as hot source store
func (bs *balanceSolver) filterSrcStores() map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail)
	for id, detail := range bs.stLoadDetail {
		if bs.cluster.GetStore(id) == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", id), errs.ZapError(errs.ErrGetSourceStore))
			continue
		}
		if len(detail.HotPeers) == 0 {
			continue
		}
		ret[id] = detail
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

	QPSSort := make([]*statistics.HotPeerStat, len(ret))
	copy(QPSSort, ret)
	sort.Slice(QPSSort, func(i, j int) bool {
		return QPSSort[i].GetQPS() > QPSSort[j].GetQPS()
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
		for len(QPSSort) > 0 {
			peer := QPSSort[0]
			QPSSort = QPSSort[1:]
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
		candidates []*core.StoreInfo
	)
	srcStore := bs.cluster.GetStore(bs.cur.srcStoreID)
	if srcStore == nil {
		return nil
	}

	switch bs.opTy {
	case movePeer:
		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(bs.sche.GetName(), bs.cur.region.GetStoreIds(), bs.cur.region.GetStoreIds()),
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
			filter.NewPlacementSafeguard(bs.sche.GetName(), bs.cluster, bs.cur.region, srcStore),
		}

		candidates = bs.cluster.GetStores()

	case transferLeader:
		filters = []filter.Filter{
			filter.StoreStateFilter{ActionScope: bs.sche.GetName(), TransferLeader: true},
			filter.NewSpecialUseFilter(bs.sche.GetName(), filter.SpecialUseHotRegion),
		}
		if leaderFilter := filter.NewPlacementLeaderSafeguard(bs.sche.GetName(), bs.cluster, bs.cur.region, srcStore); leaderFilter != nil {
			filters = append(filters, leaderFilter)
		}

		candidates = bs.cluster.GetFollowerStores(bs.cur.region)

	default:
		return nil
	}
	return bs.pickDstStores(filters, candidates)
}

func (bs *balanceSolver) pickDstStores(filters []filter.Filter, candidates []*core.StoreInfo) map[uint64]*storeLoadDetail {
	ret := make(map[uint64]*storeLoadDetail, len(candidates))
	for _, store := range candidates {
		if filter.Target(bs.cluster.GetOpts(), store, filters) {
			ret[store.GetID()] = bs.stLoadDetail[store.GetID()]
		}
	}
	return ret
}

// dimStatus distinguishes different kinds of rank
type dimStatus int

const (
	worse dimStatus = iota
	noWorse
	better
)

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
func (bs *balanceSolver) calcProgressiveRank() {
	srcLd := bs.stLoadDetail[bs.cur.srcStoreID].LoadPred.min()
	dstLd := bs.stLoadDetail[bs.cur.dstStoreID].LoadPred.max()
	peer := bs.cur.srcPeerStat

	greatDecRatio, minorDecRatio := bs.sche.conf.GetGreatDecRatio(), bs.sche.conf.GetMinorGreatDecRatio()

	getSrcDecRate := func(a, b float64) float64 {
		if a-b <= 0 {
			return 1
		}
		return a - b
	}
	isHot := func(a, b float64) bool {
		if a == 0 {
			return false
		}
		return a >= b
	}

	status := func(isHot bool, ratio float64) dimStatus {
		if isHot && ratio <= greatDecRatio {
			return better
		} else if ratio <= minorDecRatio {
			return noWorse
		}
		return worse
	}

	keyDecRatio := (dstLd.KeyRate + peer.GetKeyRate()) / getSrcDecRate(srcLd.KeyRate, peer.GetKeyRate())
	keyHot := isHot(peer.GetKeyRate(), bs.sche.conf.GetMinHotKeyRate())

	byteDecRatio := (dstLd.ByteRate + peer.GetByteRate()) / getSrcDecRate(srcLd.ByteRate, peer.GetByteRate())
	byteHot := isHot(peer.GetByteRate(), bs.sche.conf.GetMinHotByteRate())

	qpsDecRatio := (dstLd.QPS + peer.GetQPS()) / getSrcDecRate(srcLd.QPS, peer.GetQPS())
	qpsHot := isHot(peer.GetQPS(), bs.sche.conf.GetMinHotQPS())

	qpsStatus := status(qpsHot, qpsDecRatio)
	keyStatus := status(keyHot, keyDecRatio)
	byteStatus := status(byteHot, byteDecRatio)

	rank := newRankStatus(bs.sche.rs, qpsStatus, byteStatus, keyStatus)
	bs.cur.progressiveRank = rank
	rankScheCounter.WithLabelValues(bs.rwTy.String(),
		fmt.Sprintf("%v", rank.statusNum(better)),
		fmt.Sprintf("%v", bs.cur.srcStoreID),
		fmt.Sprintf("%v", bs.cur.dstStoreID))
}

// betterThan checks if `bs.cur` is a better solution than `old`.
func (bs *balanceSolver) betterThan(old *solution) bool {
	if old == nil {
		return true
	}

	switch {
	case bs.cur.progressiveRank.rank < old.progressiveRank.rank:
		return true
	case bs.cur.progressiveRank.rank > old.progressiveRank.rank:
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
		byteRkCmp := rankCmp(bs.cur.srcPeerStat.GetByteRate(), old.srcPeerStat.GetByteRate(), stepRank(0, 100))
		keyRkCmp := rankCmp(bs.cur.srcPeerStat.GetKeyRate(), old.srcPeerStat.GetKeyRate(), stepRank(0, 10))
		qpsRkCmp := rankCmp(bs.cur.srcPeerStat.GetQPS(), old.srcPeerStat.GetQPS(), stepRank(0, 10))

		return bs.cur.progressiveRank.cmp([]int{
			statistics.ByteDim: byteRkCmp,
			statistics.KeyDim:  keyRkCmp,
			statistics.QPSDim:  qpsRkCmp,
		})
	}

	return false
}

func (bs *balanceSolver) getCmps(isSrc bool) []storeLoadCmp {
	p := bs.sche.rs.getPerm()
	var cmps []storeLoadCmp
	if isSrc {
		cmps = []storeLoadCmp{
			statistics.ByteDim: stLdRankCmp(stLdByteRate, stepRank(bs.minDst.ByteRate, bs.rankStep.ByteRate)),
			statistics.KeyDim:  stLdRankCmp(stLdKeyRate, stepRank(bs.minDst.KeyRate, bs.rankStep.KeyRate)),
			statistics.QPSDim:  stLdRankCmp(stLdQPS, stepRank(bs.minDst.QPS, bs.rankStep.QPS)),
		}
	} else {
		cmps = []storeLoadCmp{
			statistics.ByteDim: stLdRankCmp(stLdByteRate, stepRank(bs.maxSrc.ByteRate, bs.rankStep.ByteRate)),
			statistics.KeyDim:  stLdRankCmp(stLdKeyRate, stepRank(bs.maxSrc.KeyRate, bs.rankStep.KeyRate)),
			statistics.QPSDim:  stLdRankCmp(stLdQPS, stepRank(bs.maxSrc.QPS, bs.rankStep.QPS)),
		}
	}

	var ret []storeLoadCmp
	for _, num := range p {
		ret = append(ret, cmps[num])
	}
	return ret
}

// smaller is better
func (bs *balanceSolver) compareSrcStore(st1, st2 uint64) int {
	if st1 != st2 {
		// compare source store
		lpCmp := sliceLPCmp(
			minLPCmp(negLoadCmp(sliceLoadCmp(
				bs.getCmps(true)...,
			))),
			diffCmp(
				stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)), // todo
			),
		)

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
		lpCmp := sliceLPCmp(
			maxLPCmp(sliceLoadCmp(
				bs.getCmps(false)...,
			)),
			diffCmp(
				stLdRankCmp(stLdByteRate, stepRank(0, bs.rankStep.ByteRate)),
			),
		)
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

func (bs *balanceSolver) buildOperators() ([]*operator.Operator, []Influence) {
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
		dstPeer := &metapb.Peer{StoreId: bs.cur.dstStoreID, Role: srcPeer.Role}
		desc := "move-hot-" + bs.rwTy.String() + "-peer"
		op, err = operator.CreateMovePeerOperator(
			desc,
			bs.cluster,
			bs.cur.region,
			operator.OpHotRegion,
			bs.cur.srcStoreID,
			dstPeer)

		counters = append(counters,
			hotDirectionCounter.WithLabelValues("move-peer", bs.rwTy.String(), strconv.FormatUint(bs.cur.srcStoreID, 10), "out"),
			hotDirectionCounter.WithLabelValues("move-peer", bs.rwTy.String(), strconv.FormatUint(dstPeer.GetStoreId(), 10), "in"))
	case transferLeader:
		if bs.cur.region.GetStoreVoter(bs.cur.dstStoreID) == nil {
			return nil, nil
		}
		desc := "transfer-hot-" + bs.rwTy.String() + "-leader"
		op, err = operator.CreateTransferLeaderOperator(
			desc,
			bs.cluster,
			bs.cur.region,
			bs.cur.srcStoreID,
			bs.cur.dstStoreID,
			operator.OpHotRegion)
		counters = append(counters,
			hotDirectionCounter.WithLabelValues("transfer-leader", bs.rwTy.String(), strconv.FormatUint(bs.cur.srcStoreID, 10), "out"),
			hotDirectionCounter.WithLabelValues("transfer-leader", bs.rwTy.String(), strconv.FormatUint(bs.cur.dstStoreID, 10), "in"))
	}

	if err != nil {
		log.Debug("fail to create operator", zap.Stringer("rwType", bs.rwTy), zap.Stringer("opType", bs.opTy), errs.ZapError(err))
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
		QPS:      bs.cur.srcPeerStat.GetQPS(),
		Count:    1,
	}

	return []*operator.Operator{op}, []Influence{infl}
}

func (h *hotScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
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

func (h *hotScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
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

func (h *hotScheduler) GetWritePendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(writePeer)
}

func (h *hotScheduler) GetReadPendingInfluence() map[uint64]Influence {
	return h.copyPendingInfluence(readLeader)
}

func (h *hotScheduler) copyPendingInfluence(ty resourceType) map[uint64]Influence {
	h.RLock()
	defer h.RUnlock()
	pendingSum := h.pendingSums[ty]
	ret := make(map[uint64]Influence, len(pendingSum))
	for id, infl := range pendingSum {
		ret[id] = infl
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
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		h.pendings[ty] = map[*pendingInfluence]struct{}{}
		h.pendingSums[ty] = nil
	}
	h.regionPendings = make(map[uint64][2]*operator.Operator)
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

type hotPeerFilterType int

const (
	high hotPeerFilterType = iota
	low
	mixed
)

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
		return readLeader
	}
	panic(fmt.Sprintf("invalid arguments for toResourceType: rwTy = %v, opTy = %v", rwTy, opTy))
}
