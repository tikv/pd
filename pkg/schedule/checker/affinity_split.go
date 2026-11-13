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
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
)

const (
	// SplitRegion.Timeout is max(60s, 0.6s * size in MiB). The checker skips
	// Regions with an operator, so a 1 GiB Region may wait about 10 minutes
	// for its split operator to time out. We intentionally do not cancel it early.
	defaultAffinitySplitWaitTimeout = 5 * time.Minute
	affinitySplitCacheSize          = 10000
	// Expire idle attempts on access. Active retries refresh this TTL without
	// resetting the first attempt; the LRU also bounds memory for idle entries.
	affinitySplitCacheTTL = time.Hour
)

var affinitySplitWaitTimeout = defaultAffinitySplitWaitTimeout

type affinitySplitAttempt struct {
	version       uint64
	voterStoreIDs []uint64
	startedAt     time.Time
	expiresAt     time.Time
	// split is the last split operator of the attempt. The operator controller
	// starts an operator when it admits it, and then sends the split request.
	// startedAt is the first split start time, or the time the checker starts
	// waiting for a non-positive size. It is zero until either wait starts.
	split *operator.Operator
}

// prepareAffinityPeerMove requests a SCAN split before adding a voter when size
// or keys exceed TiKV's ordinary split thresholds. It only looks at this Region:
// each child is checked on a later patrol after its heartbeat reports it, and an
// unchanged epoch can move after the zero-size or split wait timeout. Attempts belong
// to one move: other target voters start a new one, and Check removes an attempt
// once the Region is at the target.
func (c *AffinityChecker) prepareAffinityPeerMove(region *core.RegionInfo, group *affinity.GroupState) (*operator.Operator, bool) {
	if !slices.ContainsFunc(group.VoterStoreIDs, func(id uint64) bool { return region.GetStorePeer(id) == nil }) {
		return nil, true
	}

	wait := func(reason string) (*operator.Operator, bool) {
		log.Debug("wait before affinity peer move", zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID), zap.String("reason", reason))
		return nil, false
	}
	sizeUnknown := region.GetApproximateSize() <= 0
	storeConfig := c.cluster.GetStoreConfig()
	var (
		attempt   affinitySplitAttempt
		splitWait time.Duration
	)
	if sizeUnknown || uint64(region.GetApproximateSize()) > storeConfig.GetRegionSplitSize() ||
		uint64(region.GetApproximateKeys()) > storeConfig.GetRegionSplitKeys() {
		now := time.Now()
		attempt = affinitySplitAttempt{
			version:       region.GetRegionEpoch().GetVersion(),
			voterStoreIDs: group.VoterStoreIDs,
		}
		if value, ok := c.splitAttempts.Get(region.GetID()); ok {
			previous := value.(affinitySplitAttempt)
			// Both lists are sorted.
			if previous.version == attempt.version && slices.Equal(previous.voterStoreIDs, attempt.voterStoreIDs) &&
				now.Before(previous.expiresAt) {
				attempt = previous
			}
		}
		// A positive size must not reuse the wait for a non-positive size.
		if !sizeUnknown && attempt.split == nil {
			attempt.startedAt = time.Time{}
		}
		// A split that the controller rejects or cancels before it starts, for
		// example because the waiting queue is full, sends no request, so the
		// wait does not start and the next patrol requests the split again.
		if attempt.startedAt.IsZero() && attempt.split != nil {
			attempt.startedAt = attempt.split.GetStartTime()
		}
		if sizeUnknown && attempt.startedAt.IsZero() {
			attempt.startedAt = now
		}
		attempt.expiresAt = now.Add(affinitySplitCacheTTL)
		if !attempt.startedAt.IsZero() {
			splitWait = now.Sub(attempt.startedAt)
		}
		if splitWait <= affinitySplitWaitTimeout {
			if sizeUnknown {
				c.splitAttempts.Put(region.GetID(), attempt)
				return wait("unknown-size")
			}
			op, err := operator.CreateSplitRegionOperator("affinity-split-region", region,
				operator.OpAffinity|operator.OpSplit, pdpb.CheckPolicy_SCAN, nil)
			if err != nil {
				affinityCheckerCreateOpFailedCounter.Inc()
				log.Debug("create affinity split region operator failed", zap.Uint64("region-id", region.GetID()),
					zap.String("group-id", group.ID), errs.ZapError(err))
				return nil, false
			}
			attempt.split = op
			c.splitAttempts.Put(region.GetID(), attempt)
			return op, false
		}
		c.splitAttempts.Put(region.GetID(), attempt)
	} else {
		c.splitAttempts.Remove(region.GetID())
	}
	// Merges can leave a positive but stale size. Reuse the merge cooldown;
	// after a restart its cache is gone, so allow the same time for statistics.
	if time.Since(c.startTime) < recentMergeTTL || c.recentMergeCache.Exists(region.GetID()) {
		return wait("recent-merge-or-startup")
	}
	// The engine may have no split key or no positive size, so a timed-out attempt
	// moves without a preliminary split and accepts a potentially large snapshot.
	if splitWait > affinitySplitWaitTimeout {
		log.Debug("allow affinity peer move after split wait timeout", zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID), zap.Int64("size", region.GetApproximateSize()),
			zap.Int64("keys", region.GetApproximateKeys()), zap.Duration("wait-duration", splitWait))
		affinityCheckerSplitTimeoutCounter.Inc()
	}
	return nil, true
}
