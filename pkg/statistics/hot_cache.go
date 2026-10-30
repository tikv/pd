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
	regionMetaMu       syncutil.Mutex
	regionMetaUpdates  map[uint64]regionMetadataUpdate
	regionMetaInFlight map[uint64]regionMetadataUpdate
	regionMetaReady    chan struct{}
}

type regionMetadataUpdate struct {
	readRegionMetadata
	previous []readRegionMetadata
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

// CheckRegionFlowAsync checks expired items, read peer leadership, and write
// flow for a region asynchronously while retaining only immutable metadata.
func (w *HotCache) CheckRegionFlowAsync(region *core.RegionInfo) {
	checkRegionTask, checkWritePeerTask := newRegionFlowTasks(region)
	w.CheckWriteAsync(checkRegionTask)
	w.enqueueReadRegionMetadata(region)
	w.CheckWriteAsync(checkWritePeerTask)
}

func (w *HotCache) enqueueReadRegionMetadata(region *core.RegionInfo) {
	update := regionMetadataUpdate{
		meta:    region.GetMeta(),
		storeID: region.GetLeader().GetStoreId(),
		term:    region.GetTerm(),
	}
	w.regionMetaMu.Lock()
	// Validate at admission: the root can legitimately advance before the read
	// worker replays an already accepted transfer or peer removal.
	if _, _, err := w.readCache.cluster.PreCheckPutRegion(region); err != nil {
		w.regionMetaMu.Unlock()
		return
	}
	previous, exists := w.regionMetaUpdates[region.GetID()]
	latest := previous
	if !exists {
		latest = w.regionMetaInFlight[region.GetID()]
	}
	// The detached batch still fences admission until replay finishes. The
	// root can catch up before replay, leaving no cold-cache version marker.
	if latest.meta == nil || !isReadRegionMetadataStale(update.meta, update.term, latest.meta, latest.term) {
		if exists {
			update.previous = previous.previous
			// Fold repeated metadata while retaining significant transitions.
			if previous.storeID != update.storeID || (previous.term == 0) != (update.term == 0) ||
				!sameReadRegionMetadata(previous.meta, update.meta) {
				update.previous = append(update.previous, previous.readRegionMetadata)
			}
		}
		w.regionMetaUpdates[region.GetID()] = update
	}
	w.regionMetaMu.Unlock()
	select {
	case w.regionMetaReady <- struct{}{}:
	default:
	}
}

func newRegionFlowTasks(region *core.RegionInfo) (checkRegionTask, checkWritePeerTask func(*HotPeerCache)) {
	regionInfo := hotRegionInfo{
		meta:          region.GetMeta(),
		leaderStoreID: region.GetLeader().GetStoreId(),
	}
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	writtenBytes := region.GetBytesWritten()
	writtenKeys := region.GetKeysWritten()
	writeQueryNum := region.GetWriteQueryNum()
	return newRegionMetadataTask(regionInfo.meta, regionInfo.leaderStoreID, region.GetTerm()), newWriteRegionTask(
		regionInfo.meta,
		regionInfo.leaderStoreID,
		interval,
		writtenBytes,
		writtenKeys,
		writeQueryNum,
	)
}

func newRegionMetadataTask(meta *metapb.Region, leaderStoreID, term uint64) func(*HotPeerCache) {
	return func(cache *HotPeerCache) {
		applyRegionMetadata(cache, meta, leaderStoreID, term)
	}
}

func applyRegionMetadata(cache *HotPeerCache, meta *metapb.Region, leaderStoreID, term uint64) {
	regionInfo := hotRegionInfo{meta: meta, leaderStoreID: leaderStoreID}
	if !cache.updateReadLeader(&regionInfo, term) {
		return
	}
	expiredStats := cache.collectExpiredItemsForRegion(&regionInfo)
	for _, stat := range expiredStats {
		cache.UpdateStat(stat)
	}
}

func (u regionMetadataUpdate) apply(ctx context.Context, cache *HotPeerCache) {
	for _, item := range u.previous {
		if ctx.Err() != nil {
			return
		}
		applyRegionMetadata(cache, item.meta, item.storeID, item.term)
	}
	applyRegionMetadata(cache, u.meta, u.storeID, u.term)
}

func newWriteRegionTask(meta *metapb.Region, leaderStoreID, interval, writtenBytes, writtenKeys, writeQueryNum uint64) func(*HotPeerCache) {
	return func(cache *HotPeerCache) {
		regionInfo := hotRegionInfo{meta: meta, leaderStoreID: leaderStoreID}
		var loads [utils.RegionStatCount]float64
		loads[utils.RegionWriteBytes] = float64(writtenBytes)
		loads[utils.RegionWriteKeys] = float64(writtenKeys)
		loads[utils.RegionWriteQueryNum] = float64(writeQueryNum)
		stats := cache.checkPeerFlowForRegion(&regionInfo, nil, loads[:], interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
}

// CheckReadPeerAsync checks the read flow for one peer asynchronously without
// retaining the complete RegionInfo or Peer in the pending task.
func (w *HotCache) CheckReadPeerAsync(region *core.RegionInfo, peer *metapb.Peer, loads []float64, interval uint64) bool {
	return w.CheckReadAsync(newReadPeerTask(region, peer, loads, interval))
}

func newReadPeerTask(region *core.RegionInfo, peer *metapb.Peer, loads []float64, interval uint64) func(*HotPeerCache) {
	meta := region.GetMeta()
	leaderStoreID := region.GetLeader().GetStoreId()
	storeID := peer.GetStoreId()
	peerID := peer.GetId()
	return func(cache *HotPeerCache) {
		regionInfo := hotRegionInfo{meta: meta, leaderStoreID: leaderStoreID}
		// Prioritized metadata may have removed this peer after the sample was
		// admitted, even if its newer root-tree entry has not been published.
		readLeader := cache.readMetadata[meta.GetId()]
		if readLeader != nil {
			regionInfo.meta = readLeader.meta
			regionInfo.leaderStoreID = readLeader.storeID
		} else if current := cache.cluster.GetRegion(meta.GetId()); current != nil {
			// All read samples may have expired since this snapshot was captured.
			regionInfo.meta = current.GetMeta()
			regionInfo.leaderStoreID = current.GetLeader().GetStoreId()
		}
		validPeer := false
		for _, currentPeer := range regionInfo.meta.GetPeers() {
			if currentPeer.GetStoreId() == storeID && currentPeer.GetId() == peerID {
				validPeer = true
				break
			}
		}
		if !validPeer {
			return
		}
		stats := cache.checkPeerFlowForRegion(&regionInfo, []uint64{storeID}, loads, interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
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
	w.regionMetaInFlight = batch
	w.regionMetaMu.Unlock()
	failpoint.InjectCall("afterReadRegionMetadataBatchSwap")
	previousGC := w.readCache.lastGCTime
	for _, update := range batch {
		if w.ctx.Err() != nil {
			break
		}
		update.apply(w.ctx, w.readCache)
	}
	w.regionMetaMu.Lock()
	w.regionMetaInFlight = nil
	w.regionMetaMu.Unlock()
	clear(batch)
	if w.readCache.lastGCTime != previousGC {
		w.gcReadMetadata()
	}
	return batch
}

func (w *HotCache) gcReadMetadata() {
	// Run only on the read owner, outside metadata replay. Admission and this
	// root check share the lock so queued older metadata cannot outlive the
	// processed term that rejects it.
	for regionID, readLeader := range w.readCache.readMetadata {
		if len(w.readCache.storesOfRegion[regionID]) > 0 {
			continue
		}
		w.regionMetaMu.Lock()
		if _, pending := w.regionMetaUpdates[regionID]; !pending {
			current := w.readCache.cluster.GetRegion(regionID)
			if current == nil || current.GetLeader().GetStoreId() == readLeader.storeID && current.GetTerm() >= readLeader.term &&
				sameReadRegionMetadata(current.GetMeta(), readLeader.meta) {
				delete(w.readCache.readMetadata, regionID)
			}
		}
		w.regionMetaMu.Unlock()
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
	var peerCache *HotPeerCache
	switch kind {
	case utils.Write:
		peerCache = w.writeCache
	case utils.Read:
		peerCache = w.readCache
	default:
		return
	}
	done := make(chan struct{})
	// Keep mock updates synchronous while preserving the worker's ownership.
	select {
	case peerCache.taskQueue.In <- func(cache *HotPeerCache) {
		cache.UpdateStat(item)
		close(done)
	}:
	case <-w.ctx.Done():
		return
	}
	select {
	case <-done:
	case <-w.ctx.Done():
	}
}

// CheckWritePeerSync checks the write status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckWritePeerSync(region *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) []*HotPeerStat {
	return w.writeCache.CheckPeerFlow(region, peers, loads, interval)
}

// CheckReadPeerSync checks the read status, returns update items.
// This is used for mockcluster, for test purpose.
func (w *HotCache) CheckReadPeerSync(region *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) []*HotPeerStat {
	return w.readCache.CheckPeerFlow(region, peers, loads, interval)
}

// ExpiredReadItems returns the read items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredReadItems(region *core.RegionInfo) []*HotPeerStat {
	return w.readCache.CollectExpiredItems(region)
}

// ExpiredWriteItems returns the write items which are already expired.
// This is used for mockcluster, for test purpose.
func (w *HotCache) ExpiredWriteItems(region *core.RegionInfo) []*HotPeerStat {
	return w.writeCache.CollectExpiredItems(region)
}

// GetThresholds returns thresholds.
// This is used for test purpose.
func (w *HotCache) GetThresholds(kind utils.RWType, storeID uint64) []float64 {
	switch kind {
	case utils.Write:
		return w.writeCache.calcHotThresholds(storeID)
	case utils.Read:
		return w.readCache.calcHotThresholds(storeID)
	}
	return nil
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
