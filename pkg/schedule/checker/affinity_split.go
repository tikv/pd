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
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
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
	// Leave time for a patrol to claim a reservation, and reclaim it if the
	// Region no longer reaches split preparation.
	affinitySplitGrantTTL = 5 * time.Minute
)

const affinityUnknownSizeWait = defaultAffinityUnknownSizeWait

// affinitySplitWaitKind identifies what started the wait before a peer move.
type affinitySplitWaitKind uint8

const (
	affinitySplitWaitNone affinitySplitWaitKind = iota
	affinitySplitWaitUnknownSize
	affinitySplitWaitScan
)

type affinitySplitAttempt struct {
	version       uint64
	voterStoreIDs []uint64
	expiresAt     time.Time
	waitKind      affinitySplitWaitKind
	// startedAt is the start of the passive wait or the first admitted SCAN,
	// as indicated by waitKind. It is zero when there is no active clock.
	startedAt time.Time
	// split is the last split operator of the attempt. The operator controller
	// starts an operator when it admits it, and then sends the split request.
	split            *operator.Operator
	fallbackRecorded bool
}

// clearCompletedSplitAttempt releases split state after verifying alignment
// against the latest Region and affinity target.
func (c *AffinityChecker) clearCompletedSplitAttempt(region, current *core.RegionInfo) {
	epoch, currentEpoch := region.GetRegionEpoch(), current.GetRegionEpoch()
	if epoch.GetVersion() < currentEpoch.GetVersion() || epoch.GetConfVer() < currentEpoch.GetConfVer() {
		return
	}

	c.splitMu.Lock()
	defer c.splitMu.Unlock()

	regionID := region.GetID()
	attempt, tracked := c.splitAttempts[regionID]
	_, granted := c.splitGranted[regionID]
	if !tracked && !granted && c.splitWaitIndex[regionID] == nil {
		return
	}
	latest := c.cluster.GetRegion(regionID)
	if latest != nil {
		latestEpoch := latest.GetRegionEpoch()
		if epoch.GetVersion() < latestEpoch.GetVersion() || epoch.GetConfVer() < latestEpoch.GetConfVer() ||
			latestEpoch.GetVersion() < attempt.version {
			return
		}
		if _, aligned := c.affinityManager.GetRegionAffinityGroupState(latest); !aligned {
			return
		}
	}
	delete(c.splitAttempts, regionID)
	c.removeSplitWaiter(regionID)
	c.promoteSplitWaiters(time.Now())
}

// removeSplitWaiter drops any reservation or queue entry for the Region.
// The caller holds splitMu.
func (c *AffinityChecker) removeSplitWaiter(regionID uint64) {
	delete(c.splitGranted, regionID)
	if entry := c.splitWaitIndex[regionID]; entry != nil {
		c.splitWaitQueue.Remove(entry)
		delete(c.splitWaitIndex, regionID)
	}
}

// stillNeedsSplitAttempt checks whether a waiting Region still qualifies for
// a split attempt. The caller holds splitMu.
func (c *AffinityChecker) stillNeedsSplitAttempt(regionID uint64) bool {
	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return false
	}
	group, aligned := c.affinityManager.GetRegionAffinityGroupState(region)
	return group != nil && group.AffinitySchedulingAllowed && !aligned && c.needsSplitAttempt(region)
}

func (c *AffinityChecker) needsSplitAttempt(region *core.RegionInfo) bool {
	storeConfig := c.cluster.GetStoreConfig()
	size := region.GetApproximateSize()
	return size <= 0 || uint64(size) > storeConfig.GetRegionSplitSize() ||
		uint64(region.GetApproximateKeys()) > storeConfig.GetRegionSplitKeys()
}

// promoteSplitWaiters reserves free slots in FIFO order. Each queue entry is
// visited at most once. The caller holds splitMu.
func (c *AffinityChecker) promoteSplitWaiters(now time.Time) {
	for c.splitWaitQueue.Len() > 0 && len(c.splitAttempts)+len(c.splitGranted) < c.maxSplitAttempts {
		regionID := c.splitWaitQueue.Front().Value.(uint64)
		c.removeSplitWaiter(regionID)
		if _, tracked := c.splitAttempts[regionID]; !tracked && c.stillNeedsSplitAttempt(regionID) {
			c.splitGranted[regionID] = now.Add(affinitySplitGrantTTL)
		}
	}
}

// admitSplitAttempt claims a reservation or queues the Region if all slots
// are in use and the wait queue has room. Older waiters get released slots first.
// The caller holds splitMu.
func (c *AffinityChecker) admitSplitAttempt(regionID uint64, now time.Time) bool {
	if expiresAt, granted := c.splitGranted[regionID]; granted {
		delete(c.splitGranted, regionID)
		if now.Before(expiresAt) {
			return true
		}
	}

	c.promoteSplitWaiters(now)
	if _, granted := c.splitGranted[regionID]; !granted &&
		len(c.splitAttempts)+len(c.splitGranted) >= c.maxSplitAttempts && !now.Before(c.nextSplitPruneAt) {
		c.pruneSplitAttempts(now)
		c.nextSplitPruneAt = now.Add(affinitySplitPruneInterval)
		c.promoteSplitWaiters(now)
	}
	if _, granted := c.splitGranted[regionID]; granted || len(c.splitAttempts)+len(c.splitGranted) < c.maxSplitAttempts {
		c.removeSplitWaiter(regionID)
		return true
	}
	if c.splitWaitIndex[regionID] == nil && c.splitWaitQueue.Len() < c.maxSplitWaitQueue {
		c.splitWaitIndex[regionID] = c.splitWaitQueue.PushBack(regionID)
	}
	return false
}

// pruneSplitAttempts reclaims idle attempts, operators that never started, and
// reservations that expired or are no longer needed.
// The caller holds splitMu.
func (c *AffinityChecker) pruneSplitAttempts(now time.Time) {
	for id, attempt := range c.splitAttempts {
		if !now.Before(attempt.expiresAt) {
			delete(c.splitAttempts, id)
			continue
		}
		if split := attempt.split; attempt.waitKind == affinitySplitWaitNone && split != nil {
			status := split.Status()
			// Even a finished split can have started between checker passes.
			if (operator.IsEndStatus(status) || (status == operator.CREATED && split.CheckExpired())) && !split.HasStarted() {
				delete(c.splitAttempts, id)
			}
		}
	}
	for id, expiresAt := range c.splitGranted {
		if !now.Before(expiresAt) || !c.stillNeedsSplitAttempt(id) {
			delete(c.splitGranted, id)
		}
	}
}

// prepareAffinityPeerMove lets NextGen moves proceed because their snapshots
// reference shared files. On Classic, a move that adds a peer waits for size
// statistics or requests a SCAN split if the Region exceeds TiKV's split
// thresholds. Without joint consensus, demoting a voter also adds a peer by
// removing and recreating it. Each child is checked after its heartbeat reports
// it. A move can proceed after the wait times out and the cooldown has passed.
// Attempts are scoped to a Region version and target voters. When capacity
// is full, Regions can wait for a slot in the bounded FIFO queue.
func (c *AffinityChecker) prepareAffinityPeerMove(region *core.RegionInfo, group *affinity.GroupState,
	roles map[uint64]placement.PeerRoleType) (*operator.Operator, *core.RegionInfo, bool) {
	if kerneltype.IsNextGen() {
		return nil, region, true
	}

	c.splitMu.Lock()
	defer c.splitMu.Unlock()

	regionID := region.GetID()
	latest := c.cluster.GetRegion(regionID)
	if latest == nil {
		return nil, nil, false
	}
	epoch, latestEpoch := region.GetRegionEpoch(), latest.GetRegionEpoch()
	if latestEpoch.GetVersion() != epoch.GetVersion() || latestEpoch.GetConfVer() != epoch.GetConfVer() {
		return nil, nil, false
	}
	// Heartbeats can update the size and key count without changing the epoch.
	region = latest
	currentGroup, isAffinity := c.affinityManager.GetRegionAffinityGroupState(region)
	if currentGroup == nil || !currentGroup.AffinitySchedulingAllowed || isAffinity || !currentGroup.MatchesTarget(group) {
		return nil, nil, false
	}

	useJointConsensus := versioninfo.IsFeatureSupported(c.conf.GetClusterVersion(), versioninfo.ConfChangeV2) && c.conf.IsUseJointConsensus()
	addsPeer := false
	for storeID, role := range roles {
		peer := region.GetStorePeer(storeID)
		if peer == nil || (!useJointConsensus && role == placement.Learner && !core.IsLearner(peer)) {
			addsPeer = true
			break
		}
	}
	if !addsPeer {
		c.removeSplitWaiter(regionID)
		if c.splitWaitQueue.Len() > 0 {
			c.promoteSplitWaiters(time.Now())
		}
		return nil, region, true
	}

	wait := func(reason string) (*operator.Operator, *core.RegionInfo, bool) {
		checkerCounter.WithLabelValues(affinityChecker, "wait-"+reason).Inc()
		log.Debug("wait before affinity peer move", zap.Uint64("region-id", regionID),
			zap.String("group-id", group.ID), zap.String("reason", reason))
		return nil, nil, false
	}
	sizeUnknown := region.GetApproximateSize() <= 0
	version := region.GetRegionEpoch().GetVersion()
	previous, tracked := c.splitAttempts[regionID]
	var (
		attempt   affinitySplitAttempt
		splitWait time.Duration
		timedOut  bool
	)
	if c.needsSplitAttempt(region) {
		now := time.Now()
		attempt = affinitySplitAttempt{
			version:       version,
			voterStoreIDs: group.VoterStoreIDs,
		}
		if tracked {
			if !now.Before(previous.expiresAt) {
				delete(c.splitAttempts, regionID)
				tracked = false
			} else if version < previous.version {
				return nil, nil, false
			} else if version == previous.version && slices.Equal(previous.voterStoreIDs, attempt.voterStoreIDs) {
				attempt = previous
			} else {
				delete(c.splitAttempts, regionID)
				tracked = false
			}
		}
		var splitRunning bool
		if split := attempt.split; split != nil {
			// Keep a split that can still start between preparation and admission.
			// Expire unsubmitted operators so they cannot block later patrols.
			if split.Status() == operator.CREATED && !split.CheckExpired() {
				return nil, nil, false
			}
			splitRunning = !split.IsEnd()
			if attempt.waitKind == affinitySplitWaitNone {
				if startedAt := split.GetStartTime(); !startedAt.IsZero() {
					attempt.waitKind = affinitySplitWaitScan
					attempt.startedAt = startedAt
				} else if !splitRunning {
					// No request was sent. Release the slot before retrying through the FIFO queue.
					delete(c.splitAttempts, regionID)
					tracked = false
					attempt.split = nil
				}
			}
		}
		if !tracked && !c.admitSplitAttempt(regionID, now) {
			return wait("split-attempt-limit")
		}
		switch attempt.waitKind {
		case affinitySplitWaitNone:
			if sizeUnknown {
				attempt.waitKind = affinitySplitWaitUnknownSize
				attempt.startedAt = now
			}
		case affinitySplitWaitUnknownSize:
			if !sizeUnknown {
				// A SCAN starts its own clock when it is admitted.
				attempt.waitKind = affinitySplitWaitNone
				attempt.startedAt = time.Time{}
			}
		}
		if attempt.waitKind != affinitySplitWaitNone {
			splitWait = time.Since(attempt.startedAt)
		}
		waitTimeout := affinitySplitTimeout
		if sizeUnknown {
			// Unknown sizes use the longer limit even if a SCAN started the clock.
			waitTimeout = affinityUnknownSizeWait
		}
		timedOut = splitWait >= waitTimeout
		attempt.expiresAt = now.Add(affinitySplitAttemptTTL)
		if timedOut {
			// Rejected fallback moves must not extend the attempt's lifetime.
			attempt.expiresAt = attempt.startedAt.Add(waitTimeout + affinitySplitAttemptTTL)
		}
		if splitRunning {
			c.splitAttempts[regionID] = attempt
			return nil, nil, false
		}
		if !timedOut {
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
				return nil, nil, false
			}
			op.LimitTimeout(affinitySplitTimeout - splitWait)
			attempt.split = op
			c.splitAttempts[regionID] = attempt
			return op, nil, false
		}
	} else {
		c.removeSplitWaiter(regionID)
		if tracked && version >= previous.version && (previous.split == nil || version > previous.version ||
			!slices.Equal(previous.voterStoreIDs, group.VoterStoreIDs)) {
			// Size can change without an epoch change. Retain a pending or
			// started SCAN for the same version and target in case the size grows again.
			delete(c.splitAttempts, regionID)
		}
		if c.splitWaitQueue.Len() > 0 {
			c.promoteSplitWaiters(time.Now())
		}
	}
	// Merges can leave a positive but stale size. Reuse the merge cooldown;
	// after a restart its cache is gone, so allow the same time for statistics.
	coolingDown := time.Since(c.startTime) < recentMergeTTL || c.recentMergeCache.Exists(regionID)
	// The engine may have no split key or no positive size, so a timed-out attempt
	// moves without a preliminary split and accepts a potentially large snapshot.
	if timedOut {
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
	return nil, region, true
}
