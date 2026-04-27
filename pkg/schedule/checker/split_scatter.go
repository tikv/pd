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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitScatterDispatchLimit = 4
	splitScatterRetryBackoff  = time.Second
	splitScatterPendingTTL    = 3 * time.Minute
)

type splitScatterPendingItem struct {
	regionID    uint64
	group       string
	waitVersion uint64
	retryAt     time.Time
	expireAt    time.Time
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
	startKey []byte
	endKey   []byte
}

func (c *splitScatterController) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	if limit <= 0 {
		return nil
	}
	// TODO: currently iterating over the pending map in random order and
	// truncating to limit; consider ordering by region priority score.
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
		currentVersion := uint64(0)
		if region.GetRegionEpoch() != nil {
			currentVersion = region.GetRegionEpoch().GetVersion()
		}
		if pending.waitVersion > 0 && currentVersion < pending.waitVersion {
			continue
		}
		candidates = append(candidates, pending)
	}
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	c.pendingMu.RUnlock()

	if len(expiredRegionIDs) > 0 {
		c.pendingMu.Lock()
		for _, regionID := range expiredRegionIDs {
			pending, ok := c.pending[regionID]
			if !ok {
				continue
			}
			if !pending.expireAt.IsZero() && !now.Before(pending.expireAt) {
				delete(c.pending, regionID)
			}
		}
		c.pendingMu.Unlock()
	}
	return candidates
}

func (c *splitScatterController) delayPendingSplitScatter(regionID uint64, delay time.Duration) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	pending, ok := c.pending[regionID]
	if !ok {
		return
	}
	pending.retryAt = time.Now().Add(delay)
	c.pending[regionID] = pending
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
	for _, regionID := range newRegionIDs {
		c.pending[regionID] = splitScatterPendingItem{regionID: regionID, group: group, expireAt: expireAt}
	}
	c.pending[sourceRegionID] = splitScatterPendingItem{regionID: sourceRegionID, group: group, waitVersion: sourceWaitVersion, expireAt: expireAt}
}

func (c *splitScatterController) dispatchSplitScatterRegions() {
	for _, pending := range c.collectTopPendingSplitScatter(splitScatterDispatchLimit) {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			continue
		}
		if !filter.IsRegionReplicated(c.cluster, region) {
			c.delayPendingSplitScatter(pending.regionID, splitScatterRetryBackoff)
			log.Info("dispatch internal split scatter delayed",
				zap.Uint64("region-id", pending.regionID),
				zap.String("group", pending.group),
				zap.String("reason", "not-fully-replicated"))
			continue
		}
		rangeHint := resolveSplitScatterRangeHint(region)
		op, err := c.regionScatterer.ScatterInternal(region, pending.group, rangeHint.startKey, rangeHint.endKey)
		if err != nil {
			log.Info("dispatch internal split scatter failed",
				zap.Uint64("region-id", pending.regionID),
				zap.String("group", pending.group),
				zap.Error(err))
			continue
		}
		if op != nil {
			if c.opController.ExceedStoreLimit(op) {
				c.delayPendingSplitScatter(pending.regionID, splitScatterRetryBackoff)
				log.Info("dispatch internal split scatter delayed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("group", pending.group),
					zap.String("reason", "exceed-store-limit"),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if !c.opController.AddOperator(op) {
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("group", pending.group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
		}
		c.pendingMu.Lock()
		delete(c.pending, pending.regionID)
		c.pendingMu.Unlock()
	}
}
