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
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitScatterPendingLimit = 1024
	splitScatterRetryBackoff = time.Second
	splitScatterPendingTTL   = 3 * time.Minute
)

type splitScatterPendingItem struct {
	regionID          uint64
	group             string
	sourceRegionID    uint64
	sourceWaitVersion uint64
	retryAt           time.Time
	expireAt          time.Time
}

type splitScatterController struct {
	cluster         sche.CheckerCluster
	opController    *operator.Controller
	regionScatterer *scatter.RegionScatterer

	pendingMu syncutil.RWMutex
	pending   map[uint64]splitScatterPendingItem
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

func (c *splitScatterController) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	if limit <= 0 {
		return nil
	}
	now := time.Now()
	c.pendingMu.RLock()

	candidates := make([]splitScatterPendingItem, 0, len(c.pending))
	expiredRegionIDs := make([]uint64, 0)
	for regionID, pending := range c.pending {
		if !pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
			expiredRegionIDs = append(expiredRegionIDs, regionID)
			continue
		}
		region := c.cluster.GetRegion(regionID)
		if region == nil {
			continue
		}
		if !pending.retryAt.IsZero() && now.Before(pending.retryAt) {
			continue
		}
		sourceRegion := c.cluster.GetRegion(pending.sourceRegionID)
		if sourceRegion == nil {
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
		if candidates[i].group != candidates[j].group {
			return candidates[i].group < candidates[j].group
		}
		return candidates[i].regionID < candidates[j].regionID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	c.pendingMu.RUnlock()

	if len(expiredRegionIDs) > 0 {
		expiredCount := 0
		c.pendingMu.Lock()
		for _, regionID := range expiredRegionIDs {
			pending, ok := c.pending[regionID]
			if !ok {
				continue
			}
			if !pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
				delete(c.pending, regionID)
				expiredCount++
			}
		}
		c.pendingMu.Unlock()
		if expiredCount > 0 {
			splitScatterPendingExpiredCounter.Add(float64(expiredCount))
		}
	}
	return candidates
}

func (c *splitScatterController) delayPendingSplitScatter(expected splitScatterPendingItem) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	pending, ok := c.pending[expected.regionID]
	if !ok || pending.group != expected.group {
		return
	}
	pending.retryAt = time.Now().Add(splitScatterRetryBackoff)
	c.pending[expected.regionID] = pending
}

func (c *splitScatterController) deletePendingSplitScatter(expected splitScatterPendingItem) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	pending, ok := c.pending[expected.regionID]
	if !ok || pending.group != expected.group {
		return
	}
	delete(c.pending, expected.regionID)
}

func (c *splitScatterController) removeExpiredPendingSplitScatterLocked(now time.Time) int {
	expiredCount := 0
	for regionID, pending := range c.pending {
		if !pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
			delete(c.pending, regionID)
			expiredCount++
		}
	}
	return expiredCount
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

// RecordSplitScatterBatch records a newly split batch for later scatter.
func (c *Controller) RecordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	c.splitScatter.recordSplitScatterBatch(sourceRegionID, newRegionIDs)
}

func (c *splitScatterController) recordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	expireAt := time.Now().Add(splitScatterPendingTTL)
	sourceWaitVersion := uint64(1)
	if sourceRegion := c.cluster.GetRegion(sourceRegionID); sourceRegion != nil && sourceRegion.GetRegionEpoch() != nil {
		sourceWaitVersion = sourceRegion.GetRegionEpoch().GetVersion() + 1
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if expiredCount := c.removeExpiredPendingSplitScatterLocked(time.Now()); expiredCount > 0 {
		splitScatterPendingExpiredCounter.Add(float64(expiredCount))
	}
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
}

func (c *splitScatterController) dispatchSplitScatterRegions() {
	limit := c.cluster.GetCheckerConfig().GetSplitScatterScheduleLimit()
	if limit == 0 {
		return
	}
	running := c.opController.OperatorCount(operator.OpSplitScatter)
	if running >= limit {
		operator.IncOperatorLimitCounter(types.SplitScatterChecker, operator.OpSplitScatter)
		return
	}
	dispatchLimit := int(limit - running)
	// Dispatch sequentially so operators added for earlier pending items in this pass
	// are visible to later ScatterInternal calls through the running-operator delta.
	for _, pending := range c.collectTopPendingSplitScatter(dispatchLimit) {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			continue
		}
		rangeHint := resolveSplitScatterRangeHint(region)
		scatterGroup := pending.group
		if rangeHint.scatterGroup != "" {
			scatterGroup = rangeHint.scatterGroup
		}
		if !filter.IsRegionReplicated(c.cluster, region) {
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
				c.delayPendingSplitScatter(pending)
				log.Info("dispatch internal split scatter delayed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("batch-group", pending.group),
					zap.String("scatter-group", scatterGroup),
					zap.String("reason", "exceed-store-limit"),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if !c.opController.AddOperator(op) {
				c.delayPendingSplitScatter(pending)
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("batch-group", pending.group),
					zap.String("scatter-group", scatterGroup),
					zap.String("operator-desc", op.Desc()))
				continue
			}
		}
		c.deletePendingSplitScatter(pending)
	}
}
