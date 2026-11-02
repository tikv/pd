// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"context"

	"github.com/smallnest/chanx"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const chanMaxLength = 6000000

// HotCache is a cache hold hot regions.
type HotCache struct {
	ctx        context.Context
	writeCache *HotPeerCache
	readCache  *HotPeerCache
	// Region metadata is coalesced separately so read sampling pressure cannot
	// hold up region publication on the heartbeat path.
	regionMetaMu      syncutil.Mutex
	regionMetaUpdates map[uint64]regionMetadataUpdate
	regionMetaReady   chan struct{}
}

type regionMetadataUpdate struct {
	hotRegionInfo
	regionID      uint64
	retainedPeers []*metapb.Peer
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context, cluster *core.BasicCluster) *HotCache {
	w := &HotCache{
		ctx:               ctx,
		writeCache:        NewHotPeerCache(ctx, cluster, utils.Write),
		readCache:         NewHotPeerCache(ctx, cluster, utils.Read),
		regionMetaUpdates: make(map[uint64]regionMetadataUpdate),
		regionMetaReady:   make(chan struct{}, 1),
	}
	go w.updateReadItems(w.readCache.taskQueue)
	go w.updateItems(w.writeCache.taskQueue, w.runWriteTask)
	return w
}

// UpdateRegion validates and publishes a region, then queues its read metadata.
// Publication and coalescing share a lock so concurrent heartbeats preserve
// intermediate transfers and removals without blocking on the read task queue.
func (w *HotCache) UpdateRegion(ctx *core.MetaProcessContext, region *core.RegionInfo) ([]*core.RegionInfo, error) {
	w.regionMetaMu.Lock()
	cluster := w.readCache.cluster
	origin, overlaps, err := cluster.CheckAndPutRootTreeWithOrigin(ctx, region)
	changed := false
	if err == nil {
		for _, old := range overlaps {
			changed = w.enqueueReadRegionMetadataLocked(old, nil) || changed
		}
		changed = w.enqueueReadRegionMetadataLocked(origin, region) || changed
	}
	if changed {
		// Notify before another publication can acquire the lock, so a transfer
		// away and back cannot leave matching snapshots without a pending wakeup.
		select {
		case w.regionMetaReady <- struct{}{}:
		default:
		}
	}
	w.regionMetaMu.Unlock()
	return overlaps, err
}

// CheckWriteAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckWriteAsync(task func(cache *HotPeerCache)) bool {
	if w.writeCache.taskQueue.Len() > chanMaxLength {
		return false
	}
	select {
	case w.writeCache.taskQueue.In <- task:
		return true
	default:
		return false
	}
}

// CheckReadAsync puts the flowItem into queue, and check it asynchronously
func (w *HotCache) CheckReadAsync(task func(cache *HotPeerCache)) bool {
	if w.readCache.taskQueue.Len() > chanMaxLength {
		return false
	}
	select {
	case w.readCache.taskQueue.In <- task:
		return true
	default:
		return false
	}
}

// CheckRegionFlowAsync checks expired write items and write flow asynchronously.
// Read metadata follows successful root publication.
func (w *HotCache) CheckRegionFlowAsync(region *core.RegionInfo) {
	w.CheckRegionFlowWithOriginAsync(region, w.writeCache.cluster.GetRegion(region.GetID()))
}

// CheckRegionFlowWithOriginAsync uses the previous region snapshot from heartbeat
// validation. The snapshot must precede publication of the new region metadata.
func (w *HotCache) CheckRegionFlowWithOriginAsync(region, origin *core.RegionInfo) {
	checkExpiredTask, checkWritePeerTask := newRegionFlowTasks(region, origin)
	w.CheckWriteAsync(checkExpiredTask)
	w.CheckWriteAsync(checkWritePeerTask)
}

func (w *HotCache) enqueueReadRegionMetadataLocked(origin, region *core.RegionInfo) bool {
	if w.ctx.Err() != nil || origin != nil && region != nil &&
		origin.GetLeader().GetStoreId() == region.GetLeader().GetStoreId() &&
		core.SortedPeersEqual(origin.GetPeers(), region.GetPeers()) {
		return false
	}
	meta := region.GetMeta()
	regionID, leaderStoreID, previousLeader := meta.GetId(), uint64(0), uint64(0)
	if region != nil {
		leaderStoreID = region.GetLeader().GetStoreId()
	} else {
		regionID = origin.GetID()
	}
	if origin != nil {
		previousLeader = origin.GetLeader().GetStoreId()
	}
	update, exists := w.regionMetaUpdates[regionID]
	peers := meta.GetPeers()
	if !exists {
		update.retainedPeers = peers
		if origin != nil {
			update.retainedPeers = intersectReadPeers(origin.GetPeers(), peers)
		}
	} else {
		update.retainedPeers = intersectReadPeers(update.retainedPeers, peers)
	}
	update.regionID = regionID
	update.meta = meta
	update.leaderStoreID = leaderStoreID
	update.leaderTransferred = update.leaderTransferred || origin != nil && previousLeader != leaderStoreID
	w.regionMetaUpdates[regionID] = update
	return true
}

// The intersection bounds pending state by the first update's peer count and
// remembers removals even if a peer is readded before the read worker runs.
func intersectReadPeers(previous, current []*metapb.Peer) []*metapb.Peer {
	if core.SortedPeersEqual(previous, current) {
		return current
	}
	peers := make([]*metapb.Peer, 0, min(len(previous), len(current)))
	for _, old := range previous {
		for _, peer := range current {
			if old.GetId() == peer.GetId() && old.GetStoreId() == peer.GetStoreId() {
				peers = append(peers, old)
				break
			}
		}
	}
	return peers
}

func newRegionFlowTasks(region, origin *core.RegionInfo) (checkExpiredTask, checkWritePeerTask func(*HotPeerCache)) {
	regionInfo := hotRegionInfo{
		meta:          region.GetMeta(),
		leaderStoreID: region.GetLeader().GetStoreId(),
	}
	if origin != nil {
		previousLeader := origin.GetLeader().GetStoreId()
		// A leader can leave and return between heartbeats observed by PD.
		termIncreased := origin.GetTerm() != 0 && region.GetTerm() > origin.GetTerm()
		regionInfo.leaderTransferred = previousLeader != 0 && regionInfo.leaderStoreID != 0 &&
			(previousLeader != regionInfo.leaderStoreID || termIncreased)
	}
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	writtenBytes := region.GetBytesWritten()
	writtenKeys := region.GetKeysWritten()
	writeQueryNum := region.GetWriteQueryNum()
	return newExpiredRegionTask(regionInfo.meta), newWriteRegionTask(
		regionInfo,
		interval,
		writtenBytes,
		writtenKeys,
		writeQueryNum,
	)
}

func newExpiredRegionTask(meta *metapb.Region) func(*HotPeerCache) {
	return func(cache *HotPeerCache) {
		regionInfo := hotRegionInfo{meta: meta}
		for _, stat := range cache.collectExpiredItemsForRegion(&regionInfo) {
			cache.UpdateStat(stat)
		}
	}
}

func (u regionMetadataUpdate) apply(cache *HotPeerCache) {
	peers := u.retainedPeers
	if previous := cache.readMetadata[u.regionID]; previous != nil {
		// Direct root changes can precede this heartbeat's origin snapshot.
		// Invalidate old physical peers before replacing their cached metadata.
		peers = intersectReadPeers(previous.meta.GetPeers(), peers)
	}
	retained := hotRegionInfo{meta: &metapb.Region{Id: u.regionID, Peers: peers}}
	for _, stat := range cache.collectExpiredItemsForRegion(&retained) {
		cache.UpdateStat(stat)
	}
	if u.meta == nil || len(cache.storesOfRegion[u.regionID]) == 0 {
		delete(cache.readMetadata, u.regionID)
		return
	}
	cache.updateReadLeader(&u.hotRegionInfo)
}

func newWriteRegionTask(regionInfo hotRegionInfo, interval, writtenBytes, writtenKeys, writeQueryNum uint64) func(*HotPeerCache) {
	return func(cache *HotPeerCache) {
		// Keep the captured snapshot by value to avoid an allocation per heartbeat.
		region := regionInfo
		var loads [utils.RegionStatCount]float64
		loads[utils.RegionWriteBytes] = float64(writtenBytes)
		loads[utils.RegionWriteKeys] = float64(writtenKeys)
		loads[utils.RegionWriteQueryNum] = float64(writeQueryNum)
		stats := cache.checkPeerFlowForRegion(&region, nil, loads[:], interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
}

// CheckReadPeerAsync checks the read flow for one peer asynchronously without
// retaining the complete RegionInfo or Peer in the pending task.
func (w *HotCache) CheckReadPeerAsync(region *core.RegionInfo, peer *metapb.Peer, loads []float64, interval uint64) bool {
	regionID, storeID, peerID := region.GetID(), peer.GetStoreId(), peer.GetId()
	return w.CheckReadAsync(func(cache *HotPeerCache) {
		failpoint.InjectCall("beforeReadRegionMetadata")
		regionInfo := w.readRegionMetadata(regionID)
		if regionInfo == nil {
			return
		}
		validPeer := false
		for _, currentPeer := range regionInfo.meta.GetPeers() {
			if currentPeer.GetStoreId() == storeID && currentPeer.GetId() == peerID {
				validPeer = true
				break
			}
		}
		if validPeer {
			for _, stat := range cache.checkPeerFlowForRegion(regionInfo, []uint64{storeID}, loads, interval) {
				cache.UpdateStat(stat)
			}
		}
		if len(cache.storesOfRegion[regionID]) == 0 {
			delete(cache.readMetadata, regionID)
		} else {
			cache.readMetadata[regionID] = regionInfo
		}
	})
}

// Reconcile read samples with accepted metadata before checking their peers.
func (w *HotCache) readRegionMetadata(regionID uint64) *hotRegionInfo {
	previous := w.readCache.readMetadata[regionID]
	// Consumed notifications are applied by this worker before sampling. A
	// matching snapshot is reusable when no new notification is pending.
	if previous != nil {
		current := w.readCache.cluster.GetRegion(regionID)
		if current != nil && previous.leaderStoreID == current.GetLeader().GetStoreId() &&
			core.SortedPeersEqual(previous.meta.GetPeers(), current.GetPeers()) && len(w.regionMetaReady) == 0 {
			return previous
		}
	}
	w.regionMetaMu.Lock()
	current := w.readCache.cluster.GetRegion(regionID)
	update, pending := w.regionMetaUpdates[regionID]
	delete(w.regionMetaUpdates, regionID)
	w.regionMetaMu.Unlock()
	if pending {
		update.apply(w.readCache)
	}
	if current == nil {
		if w.readCache.readMetadata[regionID] != nil {
			regionMetadataUpdate{regionID: regionID}.apply(w.readCache)
		}
		return nil
	}
	previous = w.readCache.readMetadata[regionID]
	if previous != nil && previous.leaderStoreID == current.GetLeader().GetStoreId() &&
		core.SortedPeersEqual(previous.meta.GetPeers(), current.GetPeers()) {
		return previous
	}
	metadata := newHotRegionInfo(current)
	update = regionMetadataUpdate{hotRegionInfo: *metadata, regionID: regionID, retainedPeers: current.GetPeers()}
	if previous != nil {
		update.leaderTransferred = previous.leaderStoreID != metadata.leaderStoreID
	}
	update.apply(w.readCache)
	if len(w.readCache.storesOfRegion[regionID]) > 0 {
		w.readCache.readMetadata[regionID] = metadata
	}
	return metadata
}

// CheckColdPeerAsync checks peers missing from a store heartbeat
// asynchronously. The pending task only keeps the reported region IDs.
func (w *HotCache) CheckColdPeerAsync(storeID uint64, reportedRegions map[uint64]struct{}, interval uint64) bool {
	checkColdPeerTask := func(cache *HotPeerCache) {
		stats := cache.checkColdPeerByRegionIDs(storeID, reportedRegions, interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
	return w.CheckReadAsync(checkColdPeerTask)
}

// GetHotPeerStats returns hot peer stats for the specified kind (read/write).
// It returns a map where the keys are store IDs and the values are slices of HotPeerStat.
func (w *HotCache) GetHotPeerStats(kind utils.RWType, minHotDegree int) map[uint64][]*HotPeerStat {
	ret := make(chan map[uint64][]*HotPeerStat, 1)
	collectRegionStatsTask := func(cache *HotPeerCache) {
		ret <- cache.GetHotPeerStats(minHotDegree)
	}
	var succ bool
	switch kind {
	case utils.Write:
		succ = w.CheckWriteAsync(collectRegionStatsTask)
	case utils.Read:
		succ = w.CheckReadAsync(collectRegionStatsTask)
	}
	if !succ {
		return nil
	}
	select {
	case <-w.ctx.Done():
		return nil
	case r := <-ret:
		return r
	}
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, minHotDegree int) bool {
	retWrite := make(chan bool, 1)
	retRead := make(chan bool, 1)
	checkRegionHotWriteTask := func(cache *HotPeerCache) {
		retWrite <- cache.isRegionHotWithAnyPeers(region, minHotDegree)
	}
	checkRegionHotReadTask := func(cache *HotPeerCache) {
		retRead <- cache.isRegionHotWithAnyPeers(region, minHotDegree)
	}
	succ1 := w.CheckWriteAsync(checkRegionHotWriteTask)
	succ2 := w.CheckReadAsync(checkRegionHotReadTask)
	if succ1 && succ2 {
		return waitRet(w.ctx, retWrite) || waitRet(w.ctx, retRead)
	}
	return false
}

func waitRet(ctx context.Context, ret chan bool) bool {
	select {
	case <-ctx.Done():
		return false
	case r := <-ret:
		return r
	}
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (w *HotCache) GetHotPeerStat(kind utils.RWType, regionID, storeID uint64) *HotPeerStat {
	ret := make(chan *HotPeerStat, 1)
	getHotPeerStatTask := func(cache *HotPeerCache) {
		ret <- cache.getHotPeerStat(regionID, storeID)
	}

	var succ bool
	switch kind {
	case utils.Read:
		succ = w.CheckReadAsync(getHotPeerStatTask)
	case utils.Write:
		succ = w.CheckWriteAsync(getHotPeerStatTask)
	}
	if !succ {
		return nil
	}
	select {
	case <-w.ctx.Done():
		return nil
	case r := <-ret:
		return r
	}
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	w.CheckWriteAsync(func(cache *HotPeerCache) {
		cache.collectMetrics()
		// gc() is otherwise only triggered from UpdateStat, so a store removed while
		// the cluster is idle (or was the only store still receiving updates) would
		// never have its hotCacheStatusGauge series cleaned up. Piggyback on this
		// periodic, activity-independent tick instead; gc() already self-throttles
		// via topNTTL, so calling it every tick is cheap.
		cache.gc()
	})
	w.CheckReadAsync(func(cache *HotPeerCache) {
		cache.collectMetrics()
		cache.gc()
	})
}

// ResetHotCacheStatusMetrics resets the hot cache metrics.
func ResetHotCacheStatusMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) updateReadItems(queue *chanx.UnboundedChan[func(*HotPeerCache)]) {
	defer logutil.LogPanic()

	batch := make(map[uint64]regionMetadataUpdate)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.regionMetaReady:
			batch = w.applyReadRegionMetadata(batch)
		case task, ok := <-queue.Out:
			if !ok {
				return
			}
			// Metadata received before this sample, query or cleanup must be
			// applied first, even if its wakeup has not been selected yet.
			select {
			case <-w.regionMetaReady:
				batch = w.applyReadRegionMetadata(batch)
			default:
			}
			w.runReadTask(task)
		}
	}
}

func (w *HotCache) applyReadRegionMetadata(spare map[uint64]regionMetadataUpdate) map[uint64]regionMetadataUpdate {
	w.regionMetaMu.Lock()
	batch := w.regionMetaUpdates
	if len(batch) == 0 {
		w.regionMetaMu.Unlock()
		return spare
	}
	w.regionMetaUpdates = spare
	w.regionMetaMu.Unlock()
	failpoint.InjectCall("afterReadRegionMetadataBatchSwap")
	previousGC := w.readCache.lastGCTime
	for _, update := range batch {
		if w.ctx.Err() != nil {
			break
		}
		update.apply(w.readCache)
	}
	clear(batch)
	if w.readCache.lastGCTime != previousGC {
		w.gcReadMetadata()
	}
	return batch
}

func (w *HotCache) gcReadMetadata() {
	for regionID := range w.readCache.readMetadata {
		if len(w.readCache.storesOfRegion[regionID]) == 0 {
			delete(w.readCache.readMetadata, regionID)
		}
	}
}

func (w *HotCache) updateItems(queue *chanx.UnboundedChan[func(*HotPeerCache)], runTask func(task func(*HotPeerCache))) {
	defer logutil.LogPanic()

	for {
		select {
		case <-w.ctx.Done():
			return
		case task, ok := <-queue.Out:
			if !ok {
				return
			}
			runTask(task)
		}
	}
}

func (w *HotCache) runReadTask(task func(cache *HotPeerCache)) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		previousGC := w.readCache.lastGCTime
		task(w.readCache)
		if w.readCache.lastGCTime != previousGC {
			w.gcReadMetadata()
		}
	}
}

func (w *HotCache) runWriteTask(task func(cache *HotPeerCache)) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task(w.writeCache)
	}
}

// Update updates the cache.
// This is used for mockcluster, for test purpose.
func (w *HotCache) Update(item *HotPeerStat, kind utils.RWType) {
	runSyncCacheTask(w, kind, func(cache *HotPeerCache) bool {
		cache.UpdateStat(item)
		return true
	})
}

// Mock helpers share the same owner as asynchronous samples and root updates.
func runSyncCacheTask[T any](w *HotCache, kind utils.RWType, task func(*HotPeerCache) T) T {
	var zero T
	var peerCache *HotPeerCache
	switch kind {
	case utils.Write:
		peerCache = w.writeCache
	case utils.Read:
		peerCache = w.readCache
	default:
		return zero
	}
	result := make(chan T, 1)
	select {
	case peerCache.taskQueue.In <- func(cache *HotPeerCache) { result <- task(cache) }:
	case <-w.ctx.Done():
		return zero
	}
	select {
	case value := <-result:
		return value
	case <-w.ctx.Done():
		return zero
	}
}

// CheckWritePeerSync checks the write status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckWritePeerSync(region *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) []*HotPeerStat {
	return runSyncCacheTask(w, utils.Write, func(cache *HotPeerCache) []*HotPeerStat {
		return cache.CheckPeerFlow(region, peers, loads, interval)
	})
}

// CheckReadPeerSync checks the read status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckReadPeerSync(region *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) []*HotPeerStat {
	return runSyncCacheTask(w, utils.Read, func(cache *HotPeerCache) []*HotPeerStat {
		return cache.CheckPeerFlow(region, peers, loads, interval)
	})
}

// ExpiredReadItems returns the read items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredReadItems(region *core.RegionInfo) []*HotPeerStat {
	return runSyncCacheTask(w, utils.Read, func(cache *HotPeerCache) []*HotPeerStat {
		return cache.CollectExpiredItems(region)
	})
}

// ExpiredWriteItems returns the write items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return runSyncCacheTask(w, utils.Write, func(cache *HotPeerCache) []*HotPeerStat {
		return cache.CollectExpiredItems(region)
	})
}

// GetThresholds returns thresholds.
// This is used for test purpose.
func (w *HotCache) GetThresholds(kind utils.RWType, storeID uint64) []float64 {
	return runSyncCacheTask(w, kind, func(cache *HotPeerCache) []float64 {
		return cache.calcHotThresholds(storeID)
	})
}

// CleanCache cleans the cache.
// This is used for test purpose.
func (w *HotCache) CleanCache() {
	done := make(chan struct{}, 2)
	clean := func(cache *HotPeerCache) {
		cache.removeAllItem()
		done <- struct{}{}
	}
	// Cleanup must run after pending tasks on each cache's owning worker.
	select {
	case w.writeCache.taskQueue.In <- clean:
	case <-w.ctx.Done():
		return
	}
	select {
	case w.readCache.taskQueue.In <- clean:
	case <-w.ctx.Done():
		return
	}
	for range 2 {
		select {
		case <-done:
		case <-w.ctx.Done():
			return
		}
	}
}
