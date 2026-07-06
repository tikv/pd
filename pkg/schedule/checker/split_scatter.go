// Copyright 2026 TiKV Project Authors.
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

package checker

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitScatterPendingLimit = 4096
	// The actual retry cadence is also bounded by the checker dispatch loop. This
	// is only the minimum interval to avoid retrying the same pending item too
	// frequently when checker ticks are fast.
	splitScatterRetryBackoff = time.Second
	// Keep the pending TTL aligned with PD's slow operator step threshold so a
	// scatter blocked by slow AddLearner/store limits still has time to retry.
	splitScatterPendingTTL = 10 * time.Minute
)

type splitScatterPendingItem struct {
	regionID          uint64
	group             string
	sourceRegionID    uint64
	sourceWaitVersion uint64
	retryAt           time.Time
	expireAt          time.Time
	// attempted means this item has been selected by the dispatcher at least once.
	attempted bool
}

type splitScatterController struct {
	cluster         sche.CheckerCluster
	opController    *operator.Controller
	regionScatterer *scatter.RegionScatterer

	pendingMu syncutil.RWMutex
	// pending maps a pending region ID to its latest split-scatter batch item.
	// The item keeps its batch group so stale snapshots cannot mutate a newer
	// pending entry for the same region.
	pending        map[uint64]splitScatterPendingItem
	nextDispatchAt time.Time
}

func newSplitScatterController(
	ctx context.Context,
	cluster sche.CheckerCluster,
	opController *operator.Controller,
	addPendingProcessedRegions func(needCheckLen bool, ids ...uint64),
) *splitScatterController {
	return &splitScatterController{
		cluster:         cluster,
		opController:    opController,
		regionScatterer: scatter.NewRegionScatterer(ctx, cluster, opController, addPendingProcessedRegions),
		pending:         make(map[uint64]splitScatterPendingItem),
	}
}

// splitScatterRangeHint is a derived key range for the current table/index
// group. When available, split-scatter seeds the scatterer's group
// distribution with the existing region count in this range before dispatch.
type splitScatterRangeHint struct {
	startKey     []byte
	endKey       []byte
	scatterGroup string
}

func (c *splitScatterController) hasSplitScatterTxnKeyspaceBounds(keyspaceID uint32) bool {
	regionBound := keyspace.MakeRegionBound(keyspaceID)
	return c.hasRegionStartKey(regionBound.TxnLeftBound) && c.hasRegionStartKey(regionBound.TxnRightBound)
}

func (c *splitScatterController) hasRegionStartKey(key []byte) bool {
	region := c.cluster.GetRegionByKey(key)
	return region != nil && bytes.Equal(region.GetStartKey(), key)
}

func (c *splitScatterController) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	if limit <= 0 {
		return nil
	}
	now := time.Now()
	c.pendingMu.RLock()

	// Keep pendingMu short: cluster reads below can be slower and do not need
	// to block pending updates. Stale snapshots are safe because candidates
	// are rechecked before dispatch and delay/delete recheck the pending identity.
	pendingSnapshot := make([]splitScatterPendingItem, 0, len(c.pending))
	expiredSnapshot := make([]splitScatterPendingItem, 0)
	for _, pending := range c.pending {
		if !pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
			expiredSnapshot = append(expiredSnapshot, pending)
			continue
		}
		pendingSnapshot = append(pendingSnapshot, pending)
	}
	c.pendingMu.RUnlock()

	candidates := make([]splitScatterPendingItem, 0, len(pendingSnapshot))
	missingSnapshot := make([]splitScatterPendingItem, 0)
	for _, pending := range pendingSnapshot {
		if !pending.retryAt.IsZero() && now.Before(pending.retryAt) {
			continue
		}
		regionID := pending.regionID
		region := c.cluster.GetRegion(regionID)
		if region == nil {
			missingSnapshot = append(missingSnapshot, pending)
			continue
		}
		sourceRegion := c.cluster.GetRegion(pending.sourceRegionID)
		if sourceRegion == nil {
			missingSnapshot = append(missingSnapshot, pending)
			continue
		}
		sourceVersion := uint64(0)
		if sourceRegion.GetRegionEpoch() != nil {
			sourceVersion = sourceRegion.GetRegionEpoch().GetVersion()
		}
		if pending.sourceWaitVersion > 0 && sourceVersion < pending.sourceWaitVersion {
			continue
		}
		candidates = append(candidates, pending)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].expireAt.Equal(candidates[j].expireAt) {
			return candidates[i].expireAt.Before(candidates[j].expireAt)
		}
		if candidates[i].group != candidates[j].group {
			return candidates[i].group < candidates[j].group
		}
		return candidates[i].regionID < candidates[j].regionID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	if len(candidates) > 0 {
		c.pendingMu.Lock()
		selected := candidates[:0]
		for _, candidate := range candidates {
			pending, ok := c.pending[candidate.regionID]
			if !ok || pending.group != candidate.group || !pending.expireAt.Equal(candidate.expireAt) {
				continue
			}
			pending.attempted = true
			c.pending[pending.regionID] = pending
			candidate.attempted = true
			selected = append(selected, candidate)
		}
		candidates = selected
		c.pendingMu.Unlock()
	}
	c.delayMissingPendingSplitScatter(missingSnapshot, now)

	if len(expiredSnapshot) > 0 {
		attemptedExpiredCount := 0
		unattemptedExpiredCount := 0
		c.pendingMu.Lock()
		for _, expired := range expiredSnapshot {
			pending, ok := c.pending[expired.regionID]
			if ok && pending.group == expired.group && pending.expireAt.Equal(expired.expireAt) &&
				!pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
				delete(c.pending, expired.regionID)
				if pending.attempted {
					attemptedExpiredCount++
				} else {
					unattemptedExpiredCount++
				}
			}
		}
		if attemptedExpiredCount+unattemptedExpiredCount > 0 {
			c.updatePendingGaugeLocked()
		}
		c.pendingMu.Unlock()
		observeSplitScatterPendingExpired(attemptedExpiredCount, unattemptedExpiredCount)
	}
	return candidates
}

func (c *splitScatterController) delayMissingPendingSplitScatter(missing []splitScatterPendingItem, now time.Time) {
	if len(missing) == 0 {
		return
	}
	missingCount := 0
	c.pendingMu.Lock()
	for _, expected := range missing {
		pending, ok := c.pending[expected.regionID]
		if !ok || pending.group != expected.group || !pending.expireAt.Equal(expected.expireAt) {
			continue
		}
		if !pending.retryAt.IsZero() && now.Before(pending.retryAt) {
			continue
		}
		pending.retryAt = now.Add(splitScatterRetryBackoff)
		c.pending[expected.regionID] = pending
		missingCount++
	}
	c.pendingMu.Unlock()
	if missingCount > 0 {
		splitScatterDispatchRegionMissingCounter.Add(float64(missingCount))
	}
}

func (c *splitScatterController) delayPendingSplitScatter(expected splitScatterPendingItem) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	pending, ok := c.pending[expected.regionID]
	if !ok || pending.group != expected.group || !pending.expireAt.Equal(expected.expireAt) {
		return
	}
	pending.retryAt = time.Now().Add(splitScatterRetryBackoff)
	c.pending[expected.regionID] = pending
}

func (c *splitScatterController) deletePendingSplitScatter(expected splitScatterPendingItem) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	pending, ok := c.pending[expected.regionID]
	if !ok || pending.group != expected.group || !pending.expireAt.Equal(expected.expireAt) {
		return
	}
	delete(c.pending, expected.regionID)
	c.updatePendingGaugeLocked()
}

func (c *splitScatterController) updatePendingGaugeLocked() {
	splitScatterPendingGauge.Set(float64(len(c.pending)))
}

func observeSplitScatterPendingExpired(attemptedCount, unattemptedCount int) {
	if attemptedCount > 0 {
		splitScatterPendingExpiredAttemptedCounter.Add(float64(attemptedCount))
	}
	if unattemptedCount > 0 {
		splitScatterPendingExpiredUnattemptedCounter.Add(float64(unattemptedCount))
	}
}

func (c *splitScatterController) removeExpiredPendingSplitScatterLocked() (attemptedCount, unattemptedCount int) {
	now := time.Now()
	for regionID, pending := range c.pending {
		if !pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
			delete(c.pending, regionID)
			if pending.attempted {
				attemptedCount++
			} else {
				unattemptedCount++
			}
		}
	}
	if attemptedCount+unattemptedCount > 0 {
		c.updatePendingGaugeLocked()
	}
	return attemptedCount, unattemptedCount
}

func (c *splitScatterController) cleanupExpiredPendingSplitScatter() int {
	c.pendingMu.Lock()
	attemptedExpiredCount, unattemptedExpiredCount := c.removeExpiredPendingSplitScatterLocked()
	pendingCount := len(c.pending)
	c.pendingMu.Unlock()
	observeSplitScatterPendingExpired(attemptedExpiredCount, unattemptedExpiredCount)
	return pendingCount
}

func (c *splitScatterController) clearPendingSplitScatter() {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.pending = make(map[uint64]splitScatterPendingItem)
	c.updatePendingGaugeLocked()
	c.nextDispatchAt = time.Time{}
}

func (c *splitScatterController) skipDispatchUntil(now time.Time) bool {
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()
	return !c.nextDispatchAt.IsZero() && now.Before(c.nextDispatchAt)
}

func (c *splitScatterController) delayNextDispatch(now time.Time) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.nextDispatchAt = now.Add(splitScatterRetryBackoff)
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

// RecordSplitScatterBatch records a newly split batch for later scatter.
func (c *Controller) RecordSplitScatterBatch(sourceRegionID, sourceWaitVersion uint64, newRegionIDs []uint64) {
	c.splitScatter.recordSplitScatterBatch(sourceRegionID, sourceWaitVersion, newRegionIDs)
}

func (c *splitScatterController) recordSplitScatterBatch(sourceRegionID, sourceWaitVersion uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	if c.cluster.GetCheckerConfig().GetSplitScatterScheduleLimit() == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	expireAt := time.Now().Add(splitScatterPendingTTL)
	if sourceWaitVersion == 0 {
		sourceWaitVersion = 1
	}
	if sourceRegion := c.cluster.GetRegion(sourceRegionID); sourceRegion != nil && sourceRegion.GetRegionEpoch() != nil {
		cachedWaitVersion := sourceRegion.GetRegionEpoch().GetVersion() + 1
		if cachedWaitVersion > sourceWaitVersion {
			sourceWaitVersion = cachedWaitVersion
		}
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	attemptedExpiredCount, unattemptedExpiredCount := c.removeExpiredPendingSplitScatterLocked()
	observeSplitScatterPendingExpired(attemptedExpiredCount, unattemptedExpiredCount)
	newPendingCount := 0
	if _, ok := c.pending[sourceRegionID]; !ok {
		newPendingCount++
	}
	for _, regionID := range newRegionIDs {
		if _, ok := c.pending[regionID]; !ok {
			newPendingCount++
		}
	}
	if len(c.pending)+newPendingCount > splitScatterPendingLimit {
		// Keep each split batch atomic. Recording only part of a source/child
		// batch would make the scatter group incomplete, so skip the whole batch
		// when the remaining capacity cannot fit it.
		splitScatterPendingDroppedCounter.Add(float64(newPendingCount))
		log.Info("skip recording split scatter batch due to pending limit",
			zap.Uint64("source-region-id", sourceRegionID),
			zap.Uint64s("new-region-ids", newRegionIDs),
			zap.Int("pending-count", len(c.pending)),
			zap.Int("new-pending-count", newPendingCount),
			zap.Int("pending-limit", splitScatterPendingLimit))
		return
	}
	for _, regionID := range newRegionIDs {
		c.pending[regionID] = splitScatterPendingItem{
			regionID:          regionID,
			group:             group,
			sourceRegionID:    sourceRegionID,
			sourceWaitVersion: sourceWaitVersion,
			expireAt:          expireAt,
		}
	}
	c.pending[sourceRegionID] = splitScatterPendingItem{
		regionID:          sourceRegionID,
		group:             group,
		sourceRegionID:    sourceRegionID,
		sourceWaitVersion: sourceWaitVersion,
		expireAt:          expireAt,
	}
	c.updatePendingGaugeLocked()
	c.nextDispatchAt = time.Time{}
}

func (c *splitScatterController) dispatchSplitScatterRegions() {
	now := time.Now()
	if c.cleanupExpiredPendingSplitScatter() == 0 {
		return
	}
	limit := c.cluster.GetCheckerConfig().GetSplitScatterScheduleLimit()
	if limit == 0 {
		splitScatterDispatchDisabledCounter.Inc()
		c.clearPendingSplitScatter()
		return
	}
	if c.skipDispatchUntil(now) {
		return
	}
	running := c.opController.OperatorCount(operator.OpSplitScatter)
	if running >= limit {
		splitScatterDispatchScheduleLimitCounter.Inc()
		operator.IncOperatorLimitCounter(types.SplitScatterChecker, operator.OpSplitScatter)
		c.delayNextDispatch(now)
		return
	}
	dispatchLimit := int(limit - running)
	// Dispatch sequentially so operators added for earlier pending items in this pass
	// are visible to later ScatterInternal calls through the running-operator delta.
	pendingItems := c.collectTopPendingSplitScatter(dispatchLimit)
	if len(pendingItems) == 0 {
		c.delayNextDispatch(now)
		return
	}
	for _, pending := range pendingItems {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			splitScatterDispatchRegionMissingCounter.Inc()
			continue
		}
		rangeHint := resolveSplitScatterRangeHintWithKeyspaceValidator(region, c.hasSplitScatterTxnKeyspaceBounds)
		scatterGroup := pending.group
		if rangeHint.scatterGroup != "" {
			scatterGroup = rangeHint.scatterGroup
		}
		if labeler := c.cluster.GetRegionLabeler(); labeler != nil && labeler.ScheduleDisabled(region) {
			splitScatterDispatchScheduleDisabledCounter.Inc()
			c.delayPendingSplitScatter(pending)
			continue
		}
		if !filter.IsRegionReplicated(c.cluster, region) {
			splitScatterDispatchNotReplicatedCounter.Inc()
			c.delayPendingSplitScatter(pending)
			log.Info("dispatch internal split scatter delayed",
				zap.Uint64("region-id", pending.regionID),
				zap.String("batch-group", pending.group),
				zap.String("scatter-group", scatterGroup),
				zap.String("reason", "not-fully-replicated"))
			continue
		}
		op, err := c.regionScatterer.ScatterInternal(region, scatterGroup, rangeHint.startKey, rangeHint.endKey)
		if err != nil {
			splitScatterDispatchScatterFailedCounter.Inc()
			c.delayPendingSplitScatter(pending)
			log.Info("dispatch internal split scatter failed",
				zap.Uint64("region-id", pending.regionID),
				zap.String("batch-group", pending.group),
				zap.String("scatter-group", scatterGroup),
				zap.Error(err))
			continue
		}
		if op != nil {
			op.SetAdditionalInfo("batch-group", pending.group)
			if c.opController.ExceedStoreLimit(op) {
				splitScatterDispatchStoreLimitCounter.Inc()
				c.delayPendingSplitScatter(pending)
				log.Info("dispatch internal split scatter delayed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("batch-group", pending.group),
					zap.String("scatter-group", scatterGroup),
					zap.String("reason", "exceed-store-limit"),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if !c.opController.AddOperatorWithGuard(func() bool {
				return !c.cluster.GetSharedConfig().IsSchedulingHalted()
			}, operator.SchedulingHalted, op) {
				splitScatterDispatchAddOperatorFailedCounter.Inc()
				c.delayPendingSplitScatter(pending)
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("batch-group", pending.group),
					zap.String("scatter-group", scatterGroup),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			splitScatterDispatchOperatorCreatedCounter.Inc()
		} else {
			splitScatterDispatchNoOperatorNeededCounter.Inc()
		}
		c.deletePendingSplitScatter(pending)
	}
}
