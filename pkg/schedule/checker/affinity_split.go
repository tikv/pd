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
	"github.com/tikv/pd/pkg/schedule/placement"
)

const (
	// A SCAN split has 30s (about three Region heartbeats) from its first start.
	affinitySplitTimeout = 30 * time.Second
	// Waiting for size statistics is passive and does not use an operator slot.
	defaultAffinityUnknownSizeWait = 5 * time.Minute
	affinitySplitMaxAttempts       = 100_000
	// Expire idle attempts on access or under capacity pressure.
	affinitySplitAttemptTTL    = time.Hour
	affinitySplitPruneInterval = time.Second
)

var affinityUnknownSizeWait = defaultAffinityUnknownSizeWait

type affinitySplitAttempt struct {
	version       uint64
	voterStoreIDs []uint64
	startedAt     time.Time
	expiresAt     time.Time
	// split is the last split operator of the attempt. The operator controller
	// starts an operator when it admits it, and then sends the split request.
	// startedAt is the first split start time, or the time the checker starts
	// waiting for a non-positive size. It is zero until either wait starts.
	split            *operator.Operator
	fallbackRecorded bool
}

// pruneSplitAttempts reclaims idle attempts and operators that never started.
// The caller holds splitMu.
func (c *AffinityChecker) pruneSplitAttempts(now time.Time) {
	for id, attempt := range c.splitAttempts {
		if !now.Before(attempt.expiresAt) {
			delete(c.splitAttempts, id)
			continue
		}
		if split := attempt.split; attempt.startedAt.IsZero() && split != nil {
			status := split.Status()
			// Even a finished split can have started between checker passes.
			if (operator.IsEndStatus(status) || (status == operator.CREATED && split.CheckExpired())) && !split.HasStarted() {
				delete(c.splitAttempts, id)
			}
		}
	}
}

// prepareAffinityPeerMove requests a SCAN split before the move to roles adds a
// peer when size or keys exceed TiKV's ordinary split thresholds. A new peer is
// a target voter, or a learner moved off a target voter store, on a store
// without a peer of the Region; a role swap copies no data. It only looks at
// this Region: each child is checked after its heartbeat reports it, and an
// unchanged epoch can move after the zero-size or split wait timeout. Attempts
// belong to one move: other target voters start a new one, and Check removes
// an attempt once the Region is at the target.
func (c *AffinityChecker) prepareAffinityPeerMove(region *core.RegionInfo, group *affinity.GroupState,
	roles map[uint64]placement.PeerRoleType) (*operator.Operator, bool) {
	addsPeer := false
	for storeID := range roles {
		if region.GetStorePeer(storeID) == nil {
			addsPeer = true
			break
		}
	}
	if !addsPeer {
		return nil, true
	}
	c.splitMu.Lock()
	defer c.splitMu.Unlock()

	regionID := region.GetID()
	wait := func(reason string) (*operator.Operator, bool) {
		checkerCounter.WithLabelValues(affinityChecker, "wait-"+reason).Inc()
		log.Debug("wait before affinity peer move", zap.Uint64("region-id", regionID),
			zap.String("group-id", group.ID), zap.String("reason", reason))
		return nil, false
	}
	sizeUnknown := region.GetApproximateSize() <= 0
	waitTimeout := affinitySplitTimeout
	if sizeUnknown {
		waitTimeout = affinityUnknownSizeWait
	}
	storeConfig := c.cluster.GetStoreConfig()
	previous, tracked := c.splitAttempts[regionID]
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
		if tracked {
			if !now.Before(previous.expiresAt) {
				delete(c.splitAttempts, regionID)
			} else if previous.version == attempt.version && slices.Equal(previous.voterStoreIDs, attempt.voterStoreIDs) {
				attempt = previous
			}
		} else if len(c.splitAttempts) >= c.maxSplitAttempts {
			// Backpressure new attempts rather than resetting existing wait budgets.
			if now.Before(c.nextSplitPruneAt) {
				return wait("split-attempt-limit")
			}
			c.pruneSplitAttempts(now)
			c.nextSplitPruneAt = now.Add(affinitySplitPruneInterval)
			if len(c.splitAttempts) >= c.maxSplitAttempts {
				return wait("split-attempt-limit")
			}
		}
		attempt.expiresAt = now.Add(affinitySplitAttemptTTL)
		// A split that the controller rejects or cancels before it starts, for
		// example because the waiting queue is full, sends no request, so the
		// wait does not start and the next patrol requests the split again.
		if split := attempt.split; split != nil {
			// Keep a split that can still start between preparation and admission.
			// Expire unsubmitted operators so they cannot block later patrols.
			if split.Status() == operator.CREATED && !split.CheckExpired() {
				return nil, false
			}
			finished := split.IsEnd()
			if attempt.startedAt.IsZero() {
				attempt.startedAt = split.GetStartTime()
			}
			if !finished {
				c.splitAttempts[regionID] = attempt
				return nil, false
			}
			if attempt.startedAt.IsZero() {
				attempt.split = nil
			}
		}
		if !sizeUnknown && attempt.split == nil {
			// Start fresh when a positive size arrives after a passive wait.
			attempt.startedAt = time.Time{}
		}
		if sizeUnknown && attempt.startedAt.IsZero() {
			attempt.startedAt = now
		}
		if !attempt.startedAt.IsZero() {
			splitWait = time.Since(attempt.startedAt)
		}
		if splitWait < waitTimeout {
			if sizeUnknown {
				c.splitAttempts[regionID] = attempt
				return wait("unknown-size")
			}
			op, err := operator.CreateSplitRegionOperator("affinity-split-region", region,
				operator.OpAffinity|operator.OpSplit, pdpb.CheckPolicy_SCAN, nil)
			if err != nil {
				affinityCheckerCreateOpFailedCounter.Inc()
				log.Debug("create affinity split region operator failed", zap.Uint64("region-id", regionID),
					zap.String("group-id", group.ID), errs.ZapError(err))
				return nil, false
			}
			op.LimitTimeout(affinitySplitTimeout - splitWait)
			attempt.split = op
			c.splitAttempts[regionID] = attempt
			return op, false
		}
	} else if tracked && (previous.split == nil || region.GetRegionEpoch().GetVersion() > previous.version ||
		!slices.Equal(previous.voterStoreIDs, group.VoterStoreIDs)) {
		// Size can change without an epoch change. An older small snapshot must
		// not erase a split that can still start or already used its wait budget.
		delete(c.splitAttempts, regionID)
	}
	// Merges can leave a positive but stale size. Reuse the merge cooldown;
	// after a restart its cache is gone, so allow the same time for statistics.
	coolingDown := time.Since(c.startTime) < recentMergeTTL || c.recentMergeCache.Exists(regionID)
	// The engine may have no split key or no positive size, so a timed-out attempt
	// moves without a preliminary split and accepts a potentially large snapshot.
	if splitWait >= waitTimeout {
		if !coolingDown && !attempt.fallbackRecorded {
			attempt.fallbackRecorded = true
			log.Info("allow affinity peer move after split wait timeout", zap.Uint64("region-id", regionID),
				zap.String("group-id", group.ID), zap.Int64("size", region.GetApproximateSize()),
				zap.Int64("keys", region.GetApproximateKeys()), zap.Duration("wait-duration", splitWait))
			affinityCheckerSplitTimeoutCounter.Inc()
		}
		c.splitAttempts[regionID] = attempt
	}
	if coolingDown {
		return wait("recent-merge-or-startup")
	}
	return nil, true
}
