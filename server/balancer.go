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

// BalanceType : the perspective of balance
type BalanceType int

const (
	byPeer BalanceType = iota
	byLeader
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
	filters := []Filter{
		newBlockFilter(),
		newStateFilter(opt),
		newHealthFilter(opt),
	}
	return &balanceLeaderScheduler{
		opt:      opt,
		limit:    1,
		selector: newBalanceSelector(LeaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetResourceKind() ResourceKind {
	return LeaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetLeaderScheduleLimit())
}

func (l *balanceLeaderScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (l *balanceLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	region, newLeader := scheduleTransferLeader(cluster, l.GetName(), l.selector)
	if region == nil {
		return nil
	}

	source := cluster.getStore(region.Leader.GetStoreId())
	target := cluster.getStore(newLeader.GetStoreId())
	if !shouldBalance(source, target, l.GetResourceKind()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}
	l.limit = adjustBalanceLimit(cluster, l.GetResourceKind())
	schedulerCounter.WithLabelValues(l.GetName(), "new_opeartor").Inc()
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
	filters := []Filter{
		newCacheFilter(cache),
		newStateFilter(opt),
		newHealthFilter(opt),
		newSnapshotCountFilter(opt),
		newStorageThresholdFilter(opt),
	}

	return &balanceRegionScheduler{
		opt:      opt,
		rep:      opt.GetReplication(),
		cache:    cache,
		limit:    1,
		selector: newBalanceSelector(RegionKind, filters),
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) GetResourceKind() ResourceKind {
	return RegionKind
}

func (s *balanceRegionScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.opt.GetRegionScheduleLimit())
}

func (s *balanceRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (s *balanceRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (s *balanceRegionScheduler) Schedule(cluster *clusterInfo) Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	// Select a peer from the store with most regions.
	region, oldPeer := scheduleRemovePeer(cluster, s.GetName(), s.selector)
	if region == nil {
		return nil
	}

	// We don't schedule region with abnormal number of replicas.
	if len(region.GetPeers()) != s.rep.GetMaxReplicas() {
		schedulerCounter.WithLabelValues(s.GetName(), "abnormal_replica").Inc()
		return nil
	}

	op := s.transferPeer(cluster, region, oldPeer)
	if op == nil {
		// We can't transfer peer from this store now, so we add it to the cache
		// and skip it for a while.
		s.cache.set(oldPeer.GetStoreId())
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return op
}

func (s *balanceRegionScheduler) transferPeer(cluster *clusterInfo, region *RegionInfo, oldPeer *metapb.Peer) Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.getRegionStores(region)
	source := cluster.getStore(oldPeer.GetStoreId())
	scoreGuard := newDistinctScoreFilter(s.rep, stores, source)

	checker := newReplicaChecker(s.opt, cluster)
	newPeer := checker.SelectBestPeerToAddReplica(region, scoreGuard)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}

	target := cluster.getStore(newPeer.GetStoreId())
	if !shouldBalance(source, target, s.GetResourceKind()) {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	s.limit = adjustBalanceLimit(cluster, s.GetResourceKind())

	return newTransferPeer(region, RegionKind, oldPeer, newPeer)
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
		newPeer := r.SelectBestPeerToAddReplica(region, r.filters...)
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

// SelectBestPeerToAddReplica returns a new peer that to be used to add a replica.
func (r *replicaChecker) SelectBestPeerToAddReplica(region *RegionInfo, filters ...Filter) *metapb.Peer {
	storeID, _ := r.SelectBestStoreToAddReplica(region, filters...)
	if storeID == 0 {
		return nil
	}
	newPeer, err := r.cluster.allocPeer(storeID)
	if err != nil {
		return nil
	}
	return newPeer
}

// SelectBestStoreToAddReplica returns the store to add a replica.
func (r *replicaChecker) SelectBestStoreToAddReplica(region *RegionInfo, filters ...Filter) (uint64, float64) {
	// Add some must have filters.
	newFilters := []Filter{
		newStateFilter(r.opt),
		newStorageThresholdFilter(r.opt),
		newExcludedFilter(nil, region.GetStoreIds()),
	}
	filters = append(filters, newFilters...)

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
		return 0, 0
	}

	return bestStore.GetId(), bestScore
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

// selectBestReplacement returns the best store to replace the region peer.
func (r *replicaChecker) selectBestReplacement(region *RegionInfo, peer *metapb.Peer) (uint64, float64) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	return r.SelectBestStoreToAddReplica(newRegion, newExcludedFilter(nil, region.GetStoreIds()))
}

func (r *replicaChecker) checkDownPeer(region *RegionInfo) Operator {
	for _, stats := range region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := r.cluster.getStore(peer.GetStoreId())
		if store == nil {
			log.Infof("lost the store %d,maybe you are recovering the PD cluster.", peer.GetStoreId())
			return nil
		}
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
		if store == nil {
			log.Infof("lost the store %d,maybe you are recovering the PD cluster.", peer.GetStoreId())
			return nil
		}
		if store.isUp() {
			continue
		}

		// check the number of replicas firstly
		if len(region.GetPeers()) > r.opt.GetMaxReplicas() {
			return newRemovePeer(region, peer)
		}

		newPeer := r.SelectBestPeerToAddReplica(region)
		if newPeer == nil {
			return nil
		}
		return newTransferPeer(region, RegionKind, peer, newPeer)
	}

	return nil
}

func (r *replicaChecker) checkBestReplacement(region *RegionInfo) Operator {
	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		return nil
	}
	storeID, newScore := r.selectBestReplacement(region, oldPeer)
	if storeID == 0 {
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}
	newPeer, err := r.cluster.allocPeer(storeID)
	if err != nil {
		return nil
	}
	return newTransferPeer(region, RegionKind, oldPeer, newPeer)
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

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	WrittenBytes uint64      `json:"total_written_bytes"`
	RegionsCount int         `json:"regions_count"`
	RegionsStat  RegionsStat `json:"statistics"`
}

type balanceHotRegionScheduler struct {
	sync.RWMutex
	opt   *scheduleOption
	limit uint64

	// store id -> hot regions statistics as the role of replica
	statisticsAsPeer map[uint64]*HotRegionsStat
	// store id -> hot regions statistics as the role of leader
	statisticsAsLeader map[uint64]*HotRegionsStat
	r                  *rand.Rand
}

func newBalanceHotRegionScheduler(opt *scheduleOption) *balanceHotRegionScheduler {
	return &balanceHotRegionScheduler{
		opt:                opt,
		limit:              1,
		statisticsAsPeer:   make(map[uint64]*HotRegionsStat),
		statisticsAsLeader: make(map[uint64]*HotRegionsStat),
		r:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *balanceHotRegionScheduler) GetName() string {
	return "balance-hot-region-scheduler"
}

func (h *balanceHotRegionScheduler) GetResourceKind() ResourceKind {
	return PriorityKind
}

func (h *balanceHotRegionScheduler) GetResourceLimit() uint64 {
	return h.limit
}

func (h *balanceHotRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (h *balanceHotRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (h *balanceHotRegionScheduler) Schedule(cluster *clusterInfo) Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	h.calcScore(cluster)

	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()
		return newTransferPeer(srcRegion, PriorityKind, srcPeer, destPeer)
	}

	// balance by leader
	srcRegion, newLeader := h.balanceByLeader(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()
		return newPriorityTransferLeader(srcRegion, newLeader)
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *balanceHotRegionScheduler) calcScore(cluster *clusterInfo) {
	h.Lock()
	defer h.Unlock()

	h.statisticsAsPeer = make(map[uint64]*HotRegionsStat)
	h.statisticsAsLeader = make(map[uint64]*HotRegionsStat)
	items := cluster.writeStatistics.elems()
	for _, item := range items {
		r, ok := item.value.(*RegionStat)
		if !ok {
			continue
		}
		if r.HotDegree < hotRegionLowThreshold {
			continue
		}

		regionInfo := cluster.getRegion(r.RegionID)
		leaderStoreID := regionInfo.Leader.GetStoreId()
		storeIDs := regionInfo.GetStoreIds()
		for storeID := range storeIDs {
			peerStat, ok := h.statisticsAsPeer[storeID]
			if !ok {
				peerStat = &HotRegionsStat{
					RegionsStat: make(RegionsStat, 0, storeHotRegionsDefaultLen),
				}
				h.statisticsAsPeer[storeID] = peerStat
			}
			leaderStat, ok := h.statisticsAsLeader[storeID]
			if !ok {
				leaderStat = &HotRegionsStat{
					RegionsStat: make(RegionsStat, 0, storeHotRegionsDefaultLen),
				}
				h.statisticsAsLeader[storeID] = leaderStat
			}

			stat := RegionStat{
				RegionID:       r.RegionID,
				WrittenBytes:   r.WrittenBytes,
				HotDegree:      r.HotDegree,
				LastUpdateTime: r.LastUpdateTime,
				StoreID:        storeID,
				antiCount:      r.antiCount,
				version:        r.version,
			}
			peerStat.WrittenBytes += r.WrittenBytes
			peerStat.RegionsCount++
			peerStat.RegionsStat = append(peerStat.RegionsStat, stat)

			if storeID == leaderStoreID {
				leaderStat.WrittenBytes += r.WrittenBytes
				leaderStat.RegionsCount++
				leaderStat.RegionsStat = append(leaderStat.RegionsStat, stat)
			}
		}
	}
}

func (h *balanceHotRegionScheduler) balanceByPeer(cluster *clusterInfo) (*RegionInfo, *metapb.Peer, *metapb.Peer) {
	var (
		maxWrittenBytes        uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// get the srcStoreId
	for storeID, statistics := range h.statisticsAsPeer {
		count, writtenBytes := statistics.RegionsStat.Len(), statistics.WrittenBytes
		if count >= 2 && (count > maxHotStoreRegionCount || (count == maxHotStoreRegionCount && writtenBytes > maxWrittenBytes)) {
			maxHotStoreRegionCount = count
			maxWrittenBytes = writtenBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil, nil
	}

	stores := cluster.getStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(h.statisticsAsPeer[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsPeer[srcStoreID].RegionsStat[i]
		srcRegion := cluster.getRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		filters := []Filter{
			newExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			newDistinctScoreFilter(h.opt.GetReplication(), stores, cluster.getLeaderStore(srcRegion)),
			newStateFilter(h.opt),
			newStorageThresholdFilter(h.opt),
		}
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if filterTarget(store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetId())
		}

		destStoreID = h.selectDestStoreByPeer(destStoreIDs, srcRegion, srcStoreID)
		if destStoreID != 0 {
			srcRegion.WrittenBytes = rs.WrittenBytes
			h.adjustBalanceLimit(srcStoreID, byPeer)

			var srcPeer *metapb.Peer
			for _, peer := range srcRegion.GetPeers() {
				if peer.GetStoreId() == srcStoreID {
					srcPeer = peer
					break
				}
			}

			if srcPeer == nil {
				return nil, nil, nil
			}

			destPeer, err := cluster.allocPeer(destStoreID)
			if err != nil {
				log.Errorf("failed to allocate peer: %v", err)
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByPeer(candidateStoreIDs []uint64, srcRegion *RegionInfo, srcStoreID uint64) uint64 {
	sr := h.statisticsAsPeer[srcStoreID]
	srcWrittenBytes := sr.WrittenBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destStoreID     uint64
		minWrittenBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for _, storeID := range candidateStoreIDs {
		if s, ok := h.statisticsAsPeer[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destStoreID = storeID
				minWrittenBytes = s.WrittenBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minWrittenBytes > s.WrittenBytes &&
				uint64(float64(srcWrittenBytes)*hotRegionScheduleFactor) > s.WrittenBytes+2*srcRegion.WrittenBytes {
				minWrittenBytes = s.WrittenBytes
				destStoreID = storeID
			}
		} else {
			destStoreID = storeID
			break
		}
	}
	return destStoreID
}

func (h *balanceHotRegionScheduler) adjustBalanceLimit(storeID uint64, t BalanceType) {
	var srcStatistics *HotRegionsStat
	var allStatistics map[uint64]*HotRegionsStat
	switch t {
	case byPeer:
		srcStatistics = h.statisticsAsPeer[storeID]
		allStatistics = h.statisticsAsPeer
	case byLeader:
		srcStatistics = h.statisticsAsLeader[storeID]
		allStatistics = h.statisticsAsLeader
	}

	var hotRegionTotalCount float64
	for _, m := range allStatistics {
		hotRegionTotalCount += float64(m.RegionsStat.Len())
	}

	avgRegionCount := hotRegionTotalCount / float64(len(allStatistics))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(srcStatistics.RegionsStat.Len()) - avgRegionCount) * hotRegionLimitFactor)
	h.limit = maxUint64(1, limit)
}

func (h *balanceHotRegionScheduler) balanceByLeader(cluster *clusterInfo) (*RegionInfo, *metapb.Peer) {
	var (
		maxWrittenBytes        uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// select srcStoreId by leader
	for storeID, statistics := range h.statisticsAsLeader {
		if statistics.RegionsStat.Len() < 2 {
			continue
		}

		if maxHotStoreRegionCount < statistics.RegionsStat.Len() {
			maxHotStoreRegionCount = statistics.RegionsStat.Len()
			maxWrittenBytes = statistics.WrittenBytes
			srcStoreID = storeID
			continue
		}

		if maxHotStoreRegionCount == statistics.RegionsStat.Len() && maxWrittenBytes < statistics.WrittenBytes {
			maxWrittenBytes = statistics.WrittenBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(h.statisticsAsLeader[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsLeader[srcStoreID].RegionsStat[i]
		srcRegion := cluster.getRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		destPeer := h.selectDestStoreByLeader(srcRegion)
		if destPeer != nil {
			h.adjustBalanceLimit(srcStoreID, byLeader)
			return srcRegion, destPeer
		}
	}
	return nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByLeader(srcRegion *RegionInfo) *metapb.Peer {
	sr := h.statisticsAsLeader[srcRegion.Leader.GetStoreId()]
	srcWrittenBytes := sr.WrittenBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destPeer        *metapb.Peer
		minWrittenBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for storeID, peer := range srcRegion.GetFollowers() {
		if s, ok := h.statisticsAsLeader[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destPeer = peer
				minWrittenBytes = s.WrittenBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minWrittenBytes > s.WrittenBytes &&
				uint64(float64(srcWrittenBytes)*hotRegionScheduleFactor) > s.WrittenBytes+2*srcRegion.WrittenBytes {
				minWrittenBytes = s.WrittenBytes
				destPeer = peer
			}
		} else {
			destPeer = peer
			break
		}
	}
	return destPeer
}

// StoreHotRegionInfos : used to get human readable description for hot regions.
type StoreHotRegionInfos struct {
	AsPeer   map[uint64]*HotRegionsStat `json:"as_peer"`
	AsLeader map[uint64]*HotRegionsStat `json:"as_leader"`
}

func (h *balanceHotRegionScheduler) GetStatus() *StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asPeer := make(map[uint64]*HotRegionsStat, len(h.statisticsAsPeer))
	for id, stat := range h.statisticsAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	asLeader := make(map[uint64]*HotRegionsStat, len(h.statisticsAsLeader))
	for id, stat := range h.statisticsAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &StoreHotRegionInfos{
		AsPeer:   asPeer,
		AsLeader: asLeader,
	}
}
