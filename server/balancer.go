// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/montanaflynn/stats"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	storeCacheInterval    = 30 * time.Second
	bootstrapBalanceCount = 10
	bootstrapBalanceDiff  = 2
)

type HotRegionType int

const (
	minor HotRegionType = iota
	major HotRegionType = iota
)

// minBalanceDiff returns the minimal diff to do balance. The formula is based
// on experience to let the diff increase alone with the count slowly.
func minBalanceDiff(count uint64) float64 {
	if count < bootstrapBalanceCount {
		return bootstrapBalanceDiff
	}
	return math.Sqrt(float64(count))
}

// shouldBalance returns true if we should balance the source and target store.
// The min balance diff provides a buffer to make the cluster stable, so that we
// don't need to schedule very frequently.
func shouldBalance(source, target *storeInfo, kind ResourceKind) bool {
	sourceCount := source.resourceCount(kind)
	sourceScore := source.resourceScore(kind)
	targetScore := target.resourceScore(kind)
	if targetScore >= sourceScore {
		return false
	}
	diffRatio := 1 - targetScore/sourceScore
	diffCount := diffRatio * float64(sourceCount)
	return diffCount >= minBalanceDiff(sourceCount)
}

func adjustBalanceLimit(cluster *clusterInfo, kind ResourceKind) uint64 {
	stores := cluster.getStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.isUp() {
			counts = append(counts, float64(s.resourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

type balanceLeaderScheduler struct {
	opt      *scheduleOption
	limit    uint64
	selector Selector
}

func newBalanceLeaderScheduler(opt *scheduleOption) *balanceLeaderScheduler {
	var filters []Filter
	filters = append(filters, newBlockFilter())
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newHealthFilter(opt))

	return &balanceLeaderScheduler{
		opt:      opt,
		limit:    1,
		selector: newBalanceSelector(leaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetLeaderScheduleLimit())
}

func (l *balanceLeaderScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (l *balanceLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	region, newLeader := scheduleTransferLeader(cluster, l.selector)
	if region == nil {
		return nil
	}

	source := cluster.getStore(region.Leader.GetStoreId())
	target := cluster.getStore(newLeader.GetStoreId())
	if !shouldBalance(source, target, l.GetResourceKind()) {
		return nil
	}
	l.limit = adjustBalanceLimit(cluster, l.GetResourceKind())

	return newTransferLeader(region, newLeader)
}

type balanceRegionScheduler struct {
	opt      *scheduleOption
	rep      *Replication
	cache    *idCache
	limit    uint64
	selector Selector
}

func newBalanceRegionScheduler(opt *scheduleOption) *balanceRegionScheduler {
	cache := newIDCache(storeCacheInterval, 4*storeCacheInterval)

	var filters []Filter
	filters = append(filters, newCacheFilter(cache))
	filters = append(filters, newStateFilter(opt))
	filters = append(filters, newHealthFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))
	filters = append(filters, newStorageThresholdFilter(opt))

	return &balanceRegionScheduler{
		opt:      opt,
		rep:      opt.GetReplication(),
		cache:    cache,
		limit:    1,
		selector: newBalanceSelector(regionKind, filters),
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) GetResourceKind() ResourceKind {
	return regionKind
}

func (s *balanceRegionScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.opt.GetRegionScheduleLimit())
}

func (s *balanceRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (s *balanceRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (s *balanceRegionScheduler) Schedule(cluster *clusterInfo) Operator {
	// Select a peer from the store with most regions.
	region, oldPeer := scheduleRemovePeer(cluster, s.selector)
	if region == nil {
		return nil
	}

	// We don't schedule region with abnormal number of replicas.
	if len(region.GetPeers()) != s.rep.GetMaxReplicas() {
		return nil
	}

	op := s.transferPeer(cluster, region, oldPeer)
	if op == nil {
		// We can't transfer peer from this store now, so we add it to the cache
		// and skip it for a while.
		s.cache.set(oldPeer.GetStoreId())
	}
	return op
}

func (s *balanceRegionScheduler) transferPeer(cluster *clusterInfo, region *RegionInfo, oldPeer *metapb.Peer) Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.getRegionStores(region)
	source := cluster.getStore(oldPeer.GetStoreId())
	scoreGuard := newDistinctScoreFilter(s.rep, stores, source)

	checker := newReplicaChecker(s.opt, cluster)
	newPeer, _ := checker.selectBestPeer(region, scoreGuard)
	if newPeer == nil {
		return nil
	}

	target := cluster.getStore(newPeer.GetStoreId())
	if !shouldBalance(source, target, s.GetResourceKind()) {
		return nil
	}
	s.limit = adjustBalanceLimit(cluster, s.GetResourceKind())

	return newTransferPeer(region, oldPeer, newPeer)
}

// replicaChecker ensures region has the best replicas.
type replicaChecker struct {
	opt     *scheduleOption
	rep     *Replication
	cluster *clusterInfo
	filters []Filter
}

func newReplicaChecker(opt *scheduleOption, cluster *clusterInfo) *replicaChecker {
	var filters []Filter
	filters = append(filters, newHealthFilter(opt))
	filters = append(filters, newSnapshotCountFilter(opt))

	return &replicaChecker{
		opt:     opt,
		rep:     opt.GetReplication(),
		cluster: cluster,
		filters: filters,
	}
}

func (r *replicaChecker) Check(region *RegionInfo) Operator {
	if op := r.checkDownPeer(region); op != nil {
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		return op
	}

	if len(region.GetPeers()) < r.rep.GetMaxReplicas() {
		newPeer, _ := r.selectBestPeer(region, r.filters...)
		if newPeer == nil {
			return nil
		}
		return newAddPeer(region, newPeer)
	}

	if len(region.GetPeers()) > r.rep.GetMaxReplicas() {
		oldPeer, _ := r.selectWorstPeer(region)
		if oldPeer == nil {
			return nil
		}
		return newRemovePeer(region, oldPeer)
	}

	return r.checkBestReplacement(region)
}

// selectBestPeer returns the best peer in other stores.
func (r *replicaChecker) selectBestPeer(region *RegionInfo, filters ...Filter) (*metapb.Peer, float64) {
	// Add some must have filters.
	filters = append(filters, newStateFilter(r.opt))
	filters = append(filters, newStorageThresholdFilter(r.opt))
	filters = append(filters, newExcludedFilter(nil, region.GetStoreIds()))

	var (
		bestStore *storeInfo
		bestScore float64
	)

	// Select the store with best distinct score.
	// If the scores are the same, select the store with minimal region score.
	stores := r.cluster.getRegionStores(region)
	for _, store := range r.cluster.getStores() {
		if filterTarget(store, filters) {
			continue
		}
		score := r.rep.GetDistinctScore(stores, store)
		if bestStore == nil || compareStoreScore(store, score, bestStore, bestScore) > 0 {
			bestStore = store
			bestScore = score
		}
	}

	if bestStore == nil || filterTarget(bestStore, r.filters) {
		return nil, 0
	}

	newPeer, err := r.cluster.allocPeer(bestStore.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil, 0
	}
	return newPeer, bestScore
}

// selectWorstPeer returns the worst peer in the region.
func (r *replicaChecker) selectWorstPeer(region *RegionInfo, filters ...Filter) (*metapb.Peer, float64) {
	var (
		worstStore *storeInfo
		worstScore float64
	)

	// Select the store with lowest distinct score.
	// If the scores are the same, select the store with maximal region score.
	stores := r.cluster.getRegionStores(region)
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		score := r.rep.GetDistinctScore(stores, store)
		if worstStore == nil || compareStoreScore(store, score, worstStore, worstScore) < 0 {
			worstStore = store
			worstScore = score
		}
	}

	if worstStore == nil || filterSource(worstStore, r.filters) {
		return nil, 0
	}
	return region.GetStorePeer(worstStore.GetId()), worstScore
}

// selectBestReplacement returns the best peer to replace the region peer.
func (r *replicaChecker) selectBestReplacement(region *RegionInfo, peer *metapb.Peer) (*metapb.Peer, float64) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	return r.selectBestPeer(newRegion, newExcludedFilter(nil, region.GetStoreIds()))
}

func (r *replicaChecker) checkDownPeer(region *RegionInfo) Operator {
	for _, stats := range region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := r.cluster.getStore(peer.GetStoreId())
		if store.downTime() < r.opt.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.opt.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return newRemovePeer(region, peer)
	}
	return nil
}

func (r *replicaChecker) checkOfflinePeer(region *RegionInfo) Operator {
	for _, peer := range region.GetPeers() {
		store := r.cluster.getStore(peer.GetStoreId())
		if store.isUp() {
			continue
		}
		newPeer, _ := r.selectBestPeer(region)
		if newPeer == nil {
			return nil
		}
		return newTransferPeer(region, peer, newPeer)
	}

	return nil
}

func (r *replicaChecker) checkBestReplacement(region *RegionInfo) Operator {
	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		return nil
	}
	newPeer, newScore := r.selectBestReplacement(region, oldPeer)
	if newPeer == nil {
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}
	return newTransferPeer(region, oldPeer, newPeer)
}

// RegionStat records each hot region's statistics
type RegionStat struct {
	RegionID     uint64 `json:"region_id"`
	WrittenBytes uint64 `json:"written_bytes"`
	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	StoreID        uint64    `json:"-"`
	// antiCount used to eliminate some noise when remove region in cache
	antiCount int
	// version used to check the region split times
	version uint64
}

// RegionsStat is a list of a group region state type
type RegionsStat []RegionStat

func (m RegionsStat) Len() int           { return len(m) }
func (m RegionsStat) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m RegionsStat) Less(i, j int) bool { return m[i].WrittenBytes < m[j].WrittenBytes }

// StoreHotRegions records all hot regions in one store with sequence
type StoreHotRegions struct {
	// Hot regions in this store as the role of Leader
	WrittenBytesAsLeader uint64      `json:"total_written_bytes_as_leader"`
	RegionsCountAsLeader int         `json:"regions_count_as_leader"`
	RegionsStatAsLeader  RegionsStat

	// Hot regions in this store as the role of Follower or Follower
	WrittenBytesAsPeer uint64      `json:"total_written_bytes_as_peer"`
	RegionsCountAsPeer int         `json:"regions_count_as_peer"`
	RegionsStatAsPeer  RegionsStat
}

type balanceHotRegionScheduler struct {
	sync.RWMutex
	opt              *scheduleOption
	limit            uint64
	majorScoreStatus map[uint64]*StoreHotRegions // store id -> regions status in this store
	minorScoreStatus map[uint64]*StoreHotRegions // store id -> regions status in this store
	r                *rand.Rand
}

func newBalanceHotRegionScheduler(opt *scheduleOption) *balanceHotRegionScheduler {
	return &balanceHotRegionScheduler{
		opt:              opt,
		limit:            1,
		majorScoreStatus: make(map[uint64]*StoreHotRegions),
		minorScoreStatus: make(map[uint64]*StoreHotRegions),
		r:                rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *balanceHotRegionScheduler) GetName() string {
	return "balance-hot-region-scheduler"
}

func (h *balanceHotRegionScheduler) GetResourceKind() ResourceKind {
	return priorityKind
}

func (h *balanceHotRegionScheduler) GetResourceLimit() uint64 {
	return h.limit
}

func (h *balanceHotRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (h *balanceHotRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (h *balanceHotRegionScheduler) Schedule(cluster *clusterInfo) Operator {
	h.calculateScores(cluster)

	// balance major hot regions
	srcRegionMajor, srcPeerMajor, destPeerMajor := h.balanceByPeer(cluster, major)
	if srcRegionMajor != nil {
		return newPriorityTransferPeer(srcRegionMajor, srcPeerMajor, destPeerMajor)
	}

	srcRegionMajor, newLeaderMajor := h.balanceByLeader(cluster, major)
	if srcRegionMajor != nil {
		return newPriorityTransferLeader(srcRegionMajor, newLeaderMajor)
	}

	//// balance minor hot regions
	//srcRegionMinor, srcPeerMinor, destPeerMinor := h.balanceByPeer(cluster, minor)
	//if srcRegionMinor != nil {
	//	return newPriorityTransferPeer(srcRegionMinor, srcPeerMinor, destPeerMinor)
	//}
	//
	srcRegionMinor, newLeaderMinor := h.balanceByLeader(cluster, minor)
	if srcRegionMinor != nil {
		return newPriorityTransferLeader(srcRegionMinor, newLeaderMinor)
	}

	return nil
}

func (h *balanceHotRegionScheduler) calculateScores(cluster *clusterInfo) {
	h.Lock()
	defer h.Unlock()

	h.calculateScoresImpl(cluster, major, majorHotRegionDegreeLowThreshold)
	h.calculateScoresImpl(cluster, minor, minorHotRegionDegreeLowThreshold)
}

func (h *balanceHotRegionScheduler) calculateScoresImpl(cluster *clusterInfo, t HotRegionType, degreeThreshold int) {
	scoreStatus := make(map[uint64]*StoreHotRegions)
	var items []*cacheItem
	switch t {
	case major:
		items = cluster.majorWriteStatistics.elems()
	case minor:
		items = cluster.minorWriteStatistics.elems()
	default:
		panic("Not supportted hot region type")
	}

	for _, item := range items {
		r, ok := item.value.(*RegionStat)
		if !ok {
			continue
		}
		if r.HotDegree < degreeThreshold {
			continue
		}

		regionInfo := cluster.getRegion(r.RegionID)
		LeaderStoreId := regionInfo.Leader.GetStoreId()
		StoreIds := regionInfo.GetStoreIds()
		for storeId := range StoreIds {
			statistics, ok := scoreStatus[storeId]
			if !ok {
				statistics = &StoreHotRegions{
					RegionsStatAsLeader: make(RegionsStat, 0, storeHotRegionsDefaultLen),
					RegionsStatAsPeer:   make(RegionsStat, 0, storeHotRegionsDefaultLen),
				}
				scoreStatus[storeId] = statistics
			}

			stat := RegionStat{
				RegionID:       r.RegionID,
				WrittenBytes:   r.WrittenBytes,
				HotDegree:      r.HotDegree,
				LastUpdateTime: r.LastUpdateTime,
				StoreID:        storeId,
				antiCount:      r.antiCount,
				version:        r.version,
			}
			statistics.WrittenBytesAsPeer += r.WrittenBytes
			statistics.RegionsCountAsPeer++
			statistics.RegionsStatAsPeer = append(statistics.RegionsStatAsPeer, stat)

			if storeId == LeaderStoreId {
				statistics.WrittenBytesAsLeader += r.WrittenBytes
				statistics.RegionsCountAsLeader++
				statistics.RegionsStatAsLeader = append(statistics.RegionsStatAsLeader, stat)
			}
		}
	}

	switch t {
	case major:
		h.majorScoreStatus = scoreStatus
	case minor:
		h.minorScoreStatus = scoreStatus
	default:
		panic("Not supportted hot region type")
	}
}

func (h *balanceHotRegionScheduler) balanceByPeer(cluster *clusterInfo, t HotRegionType) (*RegionInfo, *metapb.Peer, *metapb.Peer) {
	var (
		maxWrittenBytes        uint64
		srcStoreId             uint64
		maxHotStoreRegionCount int
		scoreStatus            map[uint64]*StoreHotRegions
	)

	// get the srcStoreId
	switch t {
	case major:
		scoreStatus = h.majorScoreStatus
	case minor:
		scoreStatus = h.minorScoreStatus
	default:
		panic("Not supportted hot region type")
	}
	for storeId, statistics := range scoreStatus {
		if statistics.RegionsStatAsPeer.Len() < 2 {
			continue
		}

		if maxHotStoreRegionCount < statistics.RegionsStatAsPeer.Len() {
			maxHotStoreRegionCount = statistics.RegionsStatAsPeer.Len()
			maxWrittenBytes = statistics.WrittenBytesAsPeer
			srcStoreId = storeId
			continue
		}

		if maxHotStoreRegionCount == statistics.RegionsStatAsPeer.Len() && maxWrittenBytes < statistics.WrittenBytesAsPeer {
			maxWrittenBytes = statistics.WrittenBytesAsPeer
			srcStoreId = storeId
		}
	}
	if srcStoreId == 0 {
		return nil, nil, nil
	}

	stores := cluster.getStores()
	var destStoreId uint64
	for _, i := range h.r.Perm(scoreStatus[srcStoreId].RegionsStatAsPeer.Len()) {
		rs := scoreStatus[srcStoreId].RegionsStatAsPeer[i]
		srcRegion := cluster.getRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		var filters []Filter
		filters = append(filters, newExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()))
		filters = append(filters, newDistinctScoreFilter(h.opt.GetReplication(), stores, cluster.getLeaderStore(srcRegion)))
		filters = append(filters, newStateFilter(h.opt))
		filters = append(filters, newStorageThresholdFilter(h.opt))
		destStoreIds := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if filterTarget(store, filters) {
				continue
			}
			destStoreIds = append(destStoreIds, store.GetId())
		}

		destStoreId = h.selectDestStoreByPeer(destStoreIds, srcRegion, srcStoreId, t)
		if destStoreId != 0 {
			srcRegion.WrittenBytes = rs.WrittenBytes
			h.adjustBalanceLimitByPeer(srcStoreId)

			var srcPeer *metapb.Peer
			for _, peer := range srcRegion.GetPeers() {
				if peer.GetStoreId() == srcStoreId {
					srcPeer = peer
					break
				}
			}

			if srcPeer == nil {
				// Todo: should panic here
				return nil, nil, nil
			}

			destPeer, err := cluster.allocPeer(destStoreId)
			if err != nil {
				log.Errorf("failed to allocate peer: %v", err)
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByPeer(candidateStoreIds []uint64, srcRegion *RegionInfo, srcStoreId uint64, t HotRegionType) uint64 {
	var scoreStatus map[uint64]*StoreHotRegions
	switch t {
	case major:
		scoreStatus = h.majorScoreStatus
	case minor:
		scoreStatus = h.minorScoreStatus
	default:
		panic("Not supportted hot region type")
	}
	sr := scoreStatus[srcStoreId]
	srcWrittenBytes := sr.WrittenBytesAsPeer
	srcHotRegionsCount := sr.RegionsStatAsPeer.Len()

	var (
		destStoreId     uint64
		minWrittenBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for _, storeId := range candidateStoreIds {
		if s, ok := scoreStatus[storeId]; ok {
			if srcHotRegionsCount-s.RegionsStatAsPeer.Len() > 1 && minRegionsCount > s.RegionsStatAsLeader.Len() {
				destStoreId = storeId
				minWrittenBytes = s.WrittenBytesAsPeer
				minRegionsCount = s.RegionsStatAsPeer.Len()
				continue
			}
			if minRegionsCount == s.RegionsStatAsPeer.Len() && minWrittenBytes > s.WrittenBytesAsPeer &&
				uint64(float64(srcWrittenBytes)*hotRegionScheduleFactor) > s.WrittenBytesAsPeer+2*srcRegion.WrittenBytes {
				minWrittenBytes = s.WrittenBytesAsPeer
				destStoreId = storeId
			}
		} else {
			destStoreId = storeId
			break
		}
	}
	return destStoreId
}

func (h *balanceHotRegionScheduler) adjustBalanceLimitByPeer(storeID uint64) {
	s := h.majorScoreStatus[storeID]
	var hotRegionTotalCount float64
	for _, m := range h.majorScoreStatus {
		hotRegionTotalCount += float64(m.RegionsStatAsPeer.Len())
	}

	avgRegionCount := hotRegionTotalCount / float64(len(h.majorScoreStatus))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(s.RegionsStatAsPeer.Len()) - avgRegionCount) * hotRegionLimitFactor)
	h.limit = maxUint64(1, limit)
}

func (h *balanceHotRegionScheduler) balanceByLeader(cluster *clusterInfo, t HotRegionType) (*RegionInfo, *metapb.Peer) {
	var (
		maxWrittenBytes        uint64
		srcStoreId             uint64
		maxHotStoreRegionCount int
		scoreStatus            map[uint64]*StoreHotRegions
	)
	switch t {
	case major:
		scoreStatus = h.majorScoreStatus
	case minor:
		scoreStatus = h.minorScoreStatus
	default:
		panic("Not supportted hot region type")
	}

	// select srcStoreId by leader
	for storeId, statistics := range scoreStatus {
		if statistics.RegionsStatAsLeader.Len() < 2 {
			continue
		}

		if maxHotStoreRegionCount < statistics.RegionsStatAsLeader.Len() {
			maxHotStoreRegionCount = statistics.RegionsStatAsLeader.Len()
			maxWrittenBytes = statistics.WrittenBytesAsLeader
			srcStoreId = storeId
			continue
		}

		if maxHotStoreRegionCount == statistics.RegionsStatAsLeader.Len() && maxWrittenBytes < statistics.WrittenBytesAsLeader {
			maxWrittenBytes = statistics.WrittenBytesAsLeader
			srcStoreId = storeId
		}
	}
	if srcStoreId == 0 {
		return nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(scoreStatus[srcStoreId].RegionsStatAsLeader.Len()) {
		rs := scoreStatus[srcStoreId].RegionsStatAsLeader[i]
		srcRegion := cluster.getRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		destPeer := h.selectDestStoreByLeader(srcRegion, t)
		if destPeer != nil {
			return srcRegion, destPeer
		}
	}
	return nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByLeader(srcRegion *RegionInfo, t HotRegionType) *metapb.Peer {
	var scoreStatus map[uint64]*StoreHotRegions
	switch t {
	case major:
		scoreStatus = h.majorScoreStatus
	case minor:
		scoreStatus = h.minorScoreStatus
	default:
		panic("Not supportted hot region type")
	}

	sr := scoreStatus[srcRegion.Leader.GetStoreId()]
	srcWrittenBytes := sr.WrittenBytesAsLeader
	srcHotRegionsCount := sr.RegionsStatAsLeader.Len()

	var (
		destPeer        *metapb.Peer
		minWrittenBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for storeId, peer := range srcRegion.GetFollowers() {
		if s, ok := scoreStatus[storeId]; ok {
			if srcHotRegionsCount-s.RegionsStatAsLeader.Len() > 1 && minRegionsCount > s.RegionsStatAsLeader.Len() {
				destPeer = peer
				minWrittenBytes = s.WrittenBytesAsLeader
				minRegionsCount = s.RegionsStatAsLeader.Len()
				continue
			}
			if minRegionsCount == s.RegionsStatAsLeader.Len() && minWrittenBytes > s.WrittenBytesAsLeader &&
				uint64(float64(srcWrittenBytes)*hotRegionScheduleFactor) > s.WrittenBytesAsLeader+2*srcRegion.WrittenBytes {
				minWrittenBytes = s.WrittenBytesAsLeader
				destPeer = peer
			}
		} else {
			destPeer = peer
			break
		}
	}
	return destPeer
}

func (h *balanceHotRegionScheduler) GetStatus() map[uint64]*StoreHotRegions {
	h.RLock()
	defer h.RUnlock()
	status := make(map[uint64]*StoreHotRegions)
	for id, stat := range h.majorScoreStatus {
		clone := *stat
		status[id] = &clone
	}
	return status
}
