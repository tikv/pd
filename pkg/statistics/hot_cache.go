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
	"sync/atomic"
	"time"

	"github.com/smallnest/chanx"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	chanMaxLength         = 6000000
	readMetadataQueueSize = 4096
)

type readRegionMetadata struct {
	id     uint64
	region *core.RegionInfo
}

type readMetadataUpdates struct {
	regions chan readRegionMetadata
	dirty   atomic.Bool
}

// notify runs under the root lock, so accepted snapshots enter the mailbox in
// publication order. Never wait for the owner; overflow requests reconciliation.
func (u *readMetadataUpdates) notify(regionID uint64, region *core.RegionInfo) {
	if regionID == 0 {
		u.dirty.Store(true)
		return
	}
	if u.dirty.Load() {
		return
	}
	select {
	case u.regions <- readRegionMetadata{id: regionID, region: region}:
	default:
		u.dirty.Store(true)
	}
}

// HotCache is a cache hold hot regions.
type HotCache struct {
	ctx        context.Context
	writeCache *HotPeerCache
	readCache  *HotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache(ctx context.Context, cluster *core.BasicCluster) *HotCache {
	w := &HotCache{
		ctx:        ctx,
		writeCache: NewHotPeerCache(ctx, cluster, utils.Write),
		readCache:  NewHotPeerCache(ctx, cluster, utils.Read),
	}
	updates := &readMetadataUpdates{regions: make(chan readRegionMetadata, readMetadataQueueSize)}
	w.readCache.readMetadata = updates
	unregister := cluster.AddRegionChangeListener(updates.notify)
	context.AfterFunc(ctx, unregister)
	go w.updateItems(w.readCache.taskQueue, w.runReadTask)
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
		expiredStats := cache.collectExpiredItemsForRegion(&regionInfo)
		for _, stat := range expiredStats {
			cache.UpdateStat(stat)
		}
	}
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
	return w.CheckReadAsync(newReadPeerTask(region, peer, loads, interval))
}

func newReadPeerTask(region *core.RegionInfo, peer *metapb.Peer, loads []float64, interval uint64) func(*HotPeerCache) {
	meta := region.GetMeta()
	leaderStoreID := region.GetLeader().GetStoreId()
	storeID := peer.GetStoreId()
	return func(cache *HotPeerCache) {
		regionInfo := hotRegionInfo{meta: meta, leaderStoreID: leaderStoreID}
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
		if kind == utils.Read {
			cache.refreshReadMetadata()
		}
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
	case stats := <-ret:
		return stats
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
		cache.refreshReadMetadata()
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
		if kind == utils.Read {
			cache.refreshReadMetadata()
		}
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
	case stat := <-ret:
		if kind == utils.Read && stat != nil && stat.readRole == readRoleInvalid {
			return nil
		}
		return stat
	}
}

// refreshReadMetadata runs only on the read-cache owner. Capturing the pending
// prefix bounds work when publications continue while a query is running.
func (f *HotPeerCache) refreshReadMetadata() {
	updates := f.readMetadata
	if updates == nil || len(updates.regions) == 0 && !updates.dirty.Load() {
		return
	}
	// Clear before reading the root so a concurrent overflow stays pending.
	refreshAll := updates.dirty.Swap(false)
	count := len(updates.regions)
	if !refreshAll && count == 0 {
		return
	}
	if !refreshAll {
		regions := make(map[uint64]*core.RegionInfo, count)
		for range count {
			update := <-updates.regions
			if len(f.storesOfRegion[update.id]) != 0 {
				regions[update.id] = update.region
			}
		}
		for regionID, region := range regions {
			f.refreshReadRegion(regionID, region)
		}
		return
	}
	// Discard the captured prefix before reading current roots. Any newer
	// snapshot observed by that lookup has already notified under the root lock,
	// so the next prefix (or a new overflow) cannot restore an older role.
	for range count {
		<-updates.regions
	}
	regionIDs := make([]uint64, 0, len(f.storesOfRegion))
	for regionID, stores := range f.storesOfRegion {
		if len(stores) != 0 {
			regionIDs = append(regionIDs, regionID)
		}
	}
	for i, region := range f.cluster.GetRegionsByIDs(regionIDs) {
		f.refreshReadRegion(regionIDs[i], region)
	}
}

func (f *HotPeerCache) refreshReadRegion(regionID uint64, region *core.RegionInfo) {
	for storeID := range f.storesOfRegion[regionID] {
		stat := f.getOldHotPeerStat(regionID, storeID)
		if stat == nil {
			continue
		}
		role := readRoleForRegion(stat.isLeader, stat.peerID, storeID, region)
		if stat.readRole != role {
			// Invalid entries retain physical provenance until replacement or
			// expiry, but are excluded from queries and cold-sample renewal.
			updated := *stat
			updated.readRole = role
			rankChanged := (stat.readRole == readRoleInvalid) != (role == readRoleInvalid)
			// A leader change preserves the load and rank. Reheapifying would
			// needlessly read rolling loads and acquire their locks.
			f.peersOfStore[storeID].Replace(&updated, rankChanged)
			if rankChanged {
				if thresholds := f.thresholdsOfStore[storeID]; thresholds != nil {
					thresholds.updatedTime = time.Time{}
				}
			}
		}
	}
}

func readRoleForRegion(sampledLeader bool, peerID, storeID uint64, region *core.RegionInfo) readPeerRole {
	if region == nil {
		return readRoleInvalid
	}
	peer := region.GetStorePeer(storeID)
	if peer == nil || peerID != 0 && peer.GetId() != peerID {
		return readRoleInvalid
	}
	isLeader := region.GetLeader().GetStoreId() == storeID
	if sampledLeader == isLeader {
		return readRoleSampled
	}
	if isLeader {
		return readRoleLeader
	}
	return readRoleFollower
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

func (w *HotCache) updateItems(queue *chanx.UnboundedChan[func(*HotPeerCache)], runTask func(task func(*HotPeerCache))) {
	defer logutil.LogPanic()

	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-queue.Out:
			runTask(task)
		}
	}
}

func (w *HotCache) runReadTask(task func(cache *HotPeerCache)) {
	if task != nil {
		// TODO: do we need a run-task timeout to protect the queue won't be stuck by a task?
		task(w.readCache)
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
	switch kind {
	case utils.Write:
		w.writeCache.UpdateStat(item)
	case utils.Read:
		w.readCache.UpdateStat(item)
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
	w.writeCache.removeAllItem()
	w.readCache.removeAllItem()
}
