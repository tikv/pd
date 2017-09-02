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
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
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
func shouldBalance(source, target *core.StoreInfo, kind core.ResourceKind) bool {
	sourceCount := source.ResourceCount(kind)
	sourceScore := source.ResourceScore(kind)
	targetScore := target.ResourceScore(kind)
	if targetScore >= sourceScore {
		return false
	}
	diffRatio := 1 - targetScore/sourceScore
	diffCount := diffRatio * float64(sourceCount)
	return diffCount >= minBalanceDiff(sourceCount)
}

func adjustBalanceLimit(cluster *clusterInfo, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

type balanceLeaderScheduler struct {
	opt      *scheduleOption
	limit    uint64
	selector schedule.Selector
}

func newBalanceLeaderScheduler(opt *scheduleOption) *balanceLeaderScheduler {
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}
	return &balanceLeaderScheduler{
		opt:      opt,
		limit:    1,
		selector: schedule.NewBalanceSelector(core.LeaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetLeaderScheduleLimit())
}

func (l *balanceLeaderScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (l *balanceLeaderScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	region, newLeader := scheduleTransferLeader(cluster, l.GetName(), l.selector)
	if region == nil {
		return nil
	}

	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(l.GetName(), "region_hot").Inc()
		return nil
	}

	source := cluster.GetStore(region.Leader.GetStoreId())
	target := cluster.GetStore(newLeader.GetStoreId())
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
	cache    *cache.TTLUint64
	limit    uint64
	selector schedule.Selector
}

func newBalanceRegionScheduler(opt *scheduleOption) *balanceRegionScheduler {
	cache := cache.NewIDTTL(storeCacheInterval, 4*storeCacheInterval)
	filters := []schedule.Filter{
		schedule.NewCacheFilter(cache),
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
		schedule.NewSnapshotCountFilter(opt),
		schedule.NewStorageThresholdFilter(opt),
	}

	return &balanceRegionScheduler{
		opt:      opt,
		rep:      opt.GetReplication(),
		cache:    cache,
		limit:    1,
		selector: schedule.NewBalanceSelector(core.RegionKind, filters),
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.RegionKind
}

func (s *balanceRegionScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.opt.GetRegionScheduleLimit())
}

func (s *balanceRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (s *balanceRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (s *balanceRegionScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
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

	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(s.GetName(), "region_hot").Inc()
		return nil
	}

	op := s.transferPeer(cluster, region, oldPeer)
	if op == nil {
		// We can't transfer peer from this store now, so we add it to the cache
		// and skip it for a while.
		s.cache.Put(oldPeer.GetStoreId())
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return op
}

func (s *balanceRegionScheduler) transferPeer(cluster *clusterInfo, region *core.RegionInfo, oldPeer *metapb.Peer) schedule.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(oldPeer.GetStoreId())
	scoreGuard := schedule.NewDistinctScoreFilter(s.rep.GetLocationLabels(), stores, source)

	checker := schedule.NewReplicaChecker(s.opt, cluster)
	newPeer := checker.SelectBestPeerToAddReplica(region, scoreGuard)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}

	target := cluster.GetStore(newPeer.GetStoreId())
	if !shouldBalance(source, target, s.GetResourceKind()) {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	s.limit = adjustBalanceLimit(cluster, s.GetResourceKind())

	return schedule.CreateMovePeerOperator(region, core.RegionKind, oldPeer, newPeer)
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

func (h *balanceHotRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.PriorityKind
}

func (h *balanceHotRegionScheduler) GetResourceLimit() uint64 {
	return h.limit
}

func (h *balanceHotRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (h *balanceHotRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (h *balanceHotRegionScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	h.calcScore(cluster)

	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()
		return schedule.CreateMovePeerOperator(srcRegion, core.PriorityKind, srcPeer, destPeer)
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
	items := cluster.writeStatistics.Elems()
	for _, item := range items {
		r, ok := item.Value.(*RegionStat)
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

func (h *balanceHotRegionScheduler) balanceByPeer(cluster *clusterInfo) (*core.RegionInfo, *metapb.Peer, *metapb.Peer) {
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

	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(h.statisticsAsPeer[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsPeer[srcStoreID].RegionsStat[i]
		srcRegion := cluster.getRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		filters := []schedule.Filter{
			schedule.NewExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			schedule.NewDistinctScoreFilter(h.opt.GetReplication().GetLocationLabels(), stores, cluster.GetLeaderStore(srcRegion)),
			schedule.NewStateFilter(h.opt),
			schedule.NewStorageThresholdFilter(h.opt),
		}
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if schedule.FilterTarget(store, filters) {
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

			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Errorf("failed to allocate peer: %v", err)
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByPeer(candidateStoreIDs []uint64, srcRegion *core.RegionInfo, srcStoreID uint64) uint64 {
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

func (h *balanceHotRegionScheduler) balanceByLeader(cluster *clusterInfo) (*core.RegionInfo, *metapb.Peer) {
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

func (h *balanceHotRegionScheduler) selectDestStoreByLeader(srcRegion *core.RegionInfo) *metapb.Peer {
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
