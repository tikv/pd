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
	"sort"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	storeCacheInterval    = 30 * time.Second
	bootstrapBalanceCount = 10
	bootstrapBalanceDiff  = 2
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

type RegionStateValue struct {
	RegionID       uint64    `json:"region_id"`
	WriteBytes     uint64    `json:"write_bytes"`
	UpdateTimes    int       `json:"update_times"`
	LastUpdateTime time.Time `json:"last_update_time"`
	StoreID        uint64    `json: "-"`
	antiTimes      int       `json:"-"`
	version        uint64    `json:"-"`
}
type MetaRegionStatus []RegionStateValue

func (m MetaRegionStatus) Len() int           { return len(m) }
func (m MetaRegionStatus) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MetaRegionStatus) Less(i, j int) bool { return m[i].WriteBytes > m[j].WriteBytes }

type RegionHotStatus struct {
	TotalWriteBytes uint64           `json:"total_write"`
	MetaStatus      MetaRegionStatus `json:"status"`
}

type balanceHotRegionScheduler struct {
	opt         *scheduleOption
	limit       uint64
	scoreStatus map[uint64]RegionHotStatus // store id -> regions status in this store
	r           *rand.Rand
}

func newBalanceHotRegionScheduler(opt *scheduleOption) *balanceHotRegionScheduler {
	return &balanceHotRegionScheduler{
		opt:         opt,
		limit:       1,
		scoreStatus: make(map[uint64]RegionHotStatus),
		r:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (l *balanceHotRegionScheduler) GetName() string {
	return "balance-hot-region-scheduler"
}

func (l *balanceHotRegionScheduler) GetResourceKind() ResourceKind {
	return priorityKind
}

func (l *balanceHotRegionScheduler) GetResourceLimit() uint64 {
	return l.limit
}

func (l *balanceHotRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (l *balanceHotRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (l *balanceHotRegionScheduler) Schedule(cluster *clusterInfo) Operator {
	l.CalculateScore(cluster)
	region, leader := l.SelectTransferLeader(cluster)
	if region == nil {
		return nil
	}
	if leader != nil {
		return newPriorityTransferLeader(region, leader)
	}
	peer := l.SelectTransferPeer(cluster, region)
	return newPriorityTransferPeer(region, region.Leader, peer)
}

func (l *balanceHotRegionScheduler) clearScore() {
	for sid, status := range l.scoreStatus {
		status.TotalWriteBytes = 0
		if status.MetaStatus.Len() != 0 {
			status.MetaStatus = status.MetaStatus[0:0]
		}
		l.scoreStatus[sid] = status
	}
}
func (l *balanceHotRegionScheduler) CalculateScore(cluster *clusterInfo) {
	l.clearScore()
	items := cluster.writeStatus.GetList()
	for _, item := range items {
		r := item.(RegionStateValue)
		if r.UpdateTimes < 5 {
			continue
		}

		id := r.RegionID
		regionInfo := cluster.getRegion(id)
		if regionInfo.WriteBytes == 0 {
			log.Infof("Debug error meet 0 write_bytes %d \n", regionInfo.GetId())
			continue
		}
		storeID := regionInfo.Leader.GetStoreId()
		status, ok := l.scoreStatus[storeID]
		if !ok {
			status = RegionHotStatus{
				MetaStatus: make(MetaRegionStatus, 0, 100),
			}
			l.scoreStatus[storeID] = status
		}
		status.TotalWriteBytes += regionInfo.WriteBytes
		status.MetaStatus = append(status.MetaStatus, RegionStateValue{id, regionInfo.WriteBytes, r.UpdateTimes, r.LastUpdateTime, storeID, r.antiTimes, r.version})
		l.scoreStatus[storeID] = status
	}

	for sid, rs := range l.scoreStatus {
		sort.Sort(rs.MetaStatus)
		l.scoreStatus[sid] = rs
	}
}
func (l *balanceHotRegionScheduler) SelectSourceRegion(cluster *clusterInfo) *RegionInfo {
	var (
		maxWrite    uint64
		sourceStore uint64
	)
	for sid, s := range l.scoreStatus {
		if s.MetaStatus.Len() < 2 {
			continue
		}
		if maxWrite < s.TotalWriteBytes {
			maxWrite = s.TotalWriteBytes
			sourceStore = sid
		}
	}
	if sourceStore == 0 {
		return nil
	}

	length := l.scoreStatus[sourceStore].MetaStatus.Len()
	for i := 0; i < 10; i++ {
		rr := l.r.Int31n(int32((length+1)/2)) + int32(length/2)
		rid := l.scoreStatus[sourceStore].MetaStatus[rr].RegionID
		region := cluster.getRegion(rid)
		if len(region.DownPeers) != 0 || len(region.PendingPeers) != 0 {
			log.Info("Debug not select peer", region.DownPeers, region.PendingPeers)
			continue
		}
		return region
	}
	return nil
}

func (l *balanceHotRegionScheduler) GetStatus() map[uint64]RegionHotStatus {
	return l.scoreStatus
}

func (l *balanceHotRegionScheduler) SelectTransferLeader(cluster *clusterInfo) (*RegionInfo, *metapb.Peer) {
	sourceRegion := l.SelectSourceRegion(cluster)
	if sourceRegion == nil {
		log.Info("Debug not select source region")
		return nil, nil
	}
	sourceStoreWriteBytes := l.scoreStatus[sourceRegion.Leader.GetStoreId()].TotalWriteBytes
	var targetPeer *metapb.Peer

	//select follow to transfer leader
	var least uint64 = math.MaxUint64
	for _, peer := range sourceRegion.GetFollowers() {
		if s, ok := l.scoreStatus[peer.GetStoreId()]; ok {
			if least >= s.TotalWriteBytes && uint64(float64(sourceStoreWriteBytes)*hotRegionScheduleFactor) > s.TotalWriteBytes+2*sourceRegion.WriteBytes {
				targetPeer = peer
				least = s.TotalWriteBytes
			}
		} else {
			targetPeer = peer
			least = 0
		}
	}
	if targetPeer != nil {
		log.Infof("Debug Transfer Leader Source:%d Target:%d Write:%d,RegionID:%d Key:%s\n", sourceRegion.Leader.GetStoreId(), targetPeer.GetStoreId(), sourceRegion.WriteBytes, sourceRegion.GetId(), sourceRegion.GetEndKey())
	}
	return sourceRegion, targetPeer
}

func (l *balanceHotRegionScheduler) SelectTransferPeer(cluster *clusterInfo, region *RegionInfo) *metapb.Peer {
	filter := newExcludedFilter(region.GetStoreIds(), region.GetStoreIds())
	sourceStoreWriteBytes := l.scoreStatus[region.Leader.GetStoreId()].TotalWriteBytes
	stores := cluster.getStores()
	var bestStore *storeInfo
	var least uint64 = math.MaxUint64
	for _, store := range stores {
		if filter.FilterSource(store) {
			continue
		}
		if s, ok := l.scoreStatus[store.GetId()]; ok {
			if least >= s.TotalWriteBytes && uint64(float64(sourceStoreWriteBytes)*hotRegionScheduleFactor) > s.TotalWriteBytes+2*region.WriteBytes {
				least = s.TotalWriteBytes
				bestStore = store
			}
		} else {
			least = 0
			bestStore = store
			break
		}
	}
	if bestStore == nil {
		return nil
	}
	newPeer, err := cluster.allocPeer(bestStore.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}
	if newPeer != nil {
		log.Infof("Debug Transfer Peer Source:%d Target:%d Write:%d RegionID:%d Key:%s \n", region.Leader.GetStoreId(), newPeer.GetStoreId(), region.WriteBytes, region.GetId(), region.GetEndKey())
	}

	return newPeer
}
