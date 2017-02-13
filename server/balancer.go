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
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var storeCacheInterval = 30 * time.Second

func minBalanceDiff(count uint64) float64 {
	return math.Pow(2, math.Log10(float64(count)))
}

func adjustBalanceSpeed(sourceCount uint64, sourceRatio, targetRatio float64) (uint64, bool) {
	if targetRatio >= sourceRatio {
		return 0, false
	}
	diffRatio := 1 - targetRatio/sourceRatio
	diffCount := diffRatio * float64(sourceCount)
	return uint64(diffCount) / 4, diffCount > minBalanceDiff(sourceCount)
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
	return l.limit
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
	if !l.adjustSchedule(source, target) {
		return nil
	}

	return newTransferLeader(region, newLeader)
}

// adjustSchedule returns true if the schedule is allowed.
func (l *balanceLeaderScheduler) adjustSchedule(source, target *storeInfo) bool {
	sourceCount := source.leaderCount()
	sourceRatio := source.leaderRatio()
	targetRatio := target.leaderRatio()
	limit, ok := adjustBalanceSpeed(sourceCount, sourceRatio, targetRatio)
	l.limit = roundUint64(limit, 1, l.opt.GetLeaderScheduleLimit())
	return ok
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
	return s.limit
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

func (s *balanceRegionScheduler) transferPeer(cluster *clusterInfo, region *regionInfo, oldPeer *metapb.Peer) Operator {
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
	if !s.adjustSchedule(source, target) {
		return nil
	}

	return newTransferPeer(region, oldPeer, newPeer)
}

// adjustSchedule returns true if the schedule is allowed.
func (s *balanceRegionScheduler) adjustSchedule(source, target *storeInfo) bool {
	sourceCount := source.regionCount()
	sourceRatio := source.regionRatio()
	targetRatio := target.regionRatio()
	limit, ok := adjustBalanceSpeed(sourceCount, sourceRatio, targetRatio)
	s.limit = roundUint64(limit, 1, s.opt.GetRegionScheduleLimit())
	return ok
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

func (r *replicaChecker) Check(region *regionInfo) Operator {
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
func (r *replicaChecker) selectBestPeer(region *regionInfo, filters ...Filter) (*metapb.Peer, float64) {
	// Add some must have filters.
	filters = append(filters, newStateFilter(r.opt))
	filters = append(filters, newStorageThresholdFilter(r.opt))
	filters = append(filters, newExcludedFilter(nil, region.GetStoreIds()))

	var (
		bestStore *storeInfo
		bestScore float64
	)

	// Select the store with best distinct score.
	// If the scores are the same, select the store with minimal region ratio.
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
func (r *replicaChecker) selectWorstPeer(region *regionInfo, filters ...Filter) (*metapb.Peer, float64) {
	var (
		worstStore *storeInfo
		worstScore float64
	)

	// Select the store with lowest distinct score.
	// If the scores are the same, select the store with maximal region ratio.
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
func (r *replicaChecker) selectBestReplacement(region *regionInfo, peer *metapb.Peer) (*metapb.Peer, float64) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	return r.selectBestPeer(newRegion, newExcludedFilter(nil, region.GetStoreIds()))
}

func (r *replicaChecker) checkDownPeer(region *regionInfo) Operator {
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

func (r *replicaChecker) checkOfflinePeer(region *regionInfo) Operator {
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

func (r *replicaChecker) checkBestReplacement(region *regionInfo) Operator {
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
