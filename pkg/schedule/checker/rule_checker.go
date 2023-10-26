// Copyright 2019 TiKV Project Authors.
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
	"errors"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
)

const (
	maxPendingListLen = 100000
	ruleChecker       = "rule_checker"
	ruleCheckerName   = "rule-checker"
)

var (
	errNoStoreToAdd        = errors.New("no store to add peer")
	errNoStoreToReplace    = errors.New("no store to replace peer")
	errPeerCannotBeLeader  = errors.New("peer cannot be leader")
	errPeerCannotBeWitness = errors.New("peer cannot be witness")
	errNoNewLeader         = errors.New("no new leader")
	errRegionNoLeader      = errors.New("region no leader")
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	ruleCheckerCounter                            = checkerCounter.WithLabelValues(ruleChecker, "check")
	ruleCheckerPausedCounter                      = checkerCounter.WithLabelValues(ruleChecker, "paused")
	ruleCheckerRegionNoLeaderCounter              = checkerCounter.WithLabelValues(ruleChecker, "region-no-leader")
	ruleCheckerGetCacheCounter                    = checkerCounter.WithLabelValues(ruleChecker, "get-cache")
	ruleCheckerNeedSplitCounter                   = checkerCounter.WithLabelValues(ruleChecker, "need-split")
	ruleCheckerSetCacheCounter                    = checkerCounter.WithLabelValues(ruleChecker, "set-cache")
	ruleCheckerReplaceDownCounter                 = checkerCounter.WithLabelValues(ruleChecker, "replace-down")
	ruleCheckerPromoteWitnessCounter              = checkerCounter.WithLabelValues(ruleChecker, "promote-witness")
	ruleCheckerReplaceOfflineCounter              = checkerCounter.WithLabelValues(ruleChecker, "replace-offline")
	ruleCheckerAddRulePeerCounter                 = checkerCounter.WithLabelValues(ruleChecker, "add-rule-peer")
	ruleCheckerNoStoreAddCounter                  = checkerCounter.WithLabelValues(ruleChecker, "no-store-add")
	ruleCheckerNoStoreThenTryReplace              = checkerCounter.WithLabelValues(ruleChecker, "no-store-then-try-replace")
	ruleCheckerNoStoreReplaceCounter              = checkerCounter.WithLabelValues(ruleChecker, "no-store-replace")
	ruleCheckerFixPeerRoleCounter                 = checkerCounter.WithLabelValues(ruleChecker, "fix-peer-role")
	ruleCheckerFixLeaderRoleCounter               = checkerCounter.WithLabelValues(ruleChecker, "fix-leader-role")
	ruleCheckerNotAllowLeaderCounter              = checkerCounter.WithLabelValues(ruleChecker, "not-allow-leader")
	ruleCheckerFixFollowerRoleCounter             = checkerCounter.WithLabelValues(ruleChecker, "fix-follower-role")
	ruleCheckerNoNewLeaderCounter                 = checkerCounter.WithLabelValues(ruleChecker, "no-new-leader")
	ruleCheckerDemoteVoterRoleCounter             = checkerCounter.WithLabelValues(ruleChecker, "demote-voter-role")
	ruleCheckerRecentlyPromoteToNonWitnessCounter = checkerCounter.WithLabelValues(ruleChecker, "recently-promote-to-non-witness")
	ruleCheckerCancelSwitchToWitnessCounter       = checkerCounter.WithLabelValues(ruleChecker, "cancel-switch-to-witness")
	ruleCheckerSetVoterWitnessCounter             = checkerCounter.WithLabelValues(ruleChecker, "set-voter-witness")
	ruleCheckerSetLearnerWitnessCounter           = checkerCounter.WithLabelValues(ruleChecker, "set-learner-witness")
	ruleCheckerSetVoterNonWitnessCounter          = checkerCounter.WithLabelValues(ruleChecker, "set-voter-non-witness")
	ruleCheckerSetLearnerNonWitnessCounter        = checkerCounter.WithLabelValues(ruleChecker, "set-learner-non-witness")
	ruleCheckerMoveToBetterLocationCounter        = checkerCounter.WithLabelValues(ruleChecker, "move-to-better-location")
	ruleCheckerSkipRemoveOrphanPeerCounter        = checkerCounter.WithLabelValues(ruleChecker, "skip-remove-orphan-peer")
	ruleCheckerRemoveOrphanPeerCounter            = checkerCounter.WithLabelValues(ruleChecker, "remove-orphan-peer")
	ruleCheckerReplaceOrphanPeerCounter           = checkerCounter.WithLabelValues(ruleChecker, "replace-orphan-peer")
)

// RuleChecker fix/improve region by placement rules.
type RuleChecker struct {
	PauseController
	cluster            sche.CheckerCluster
	ruleManager        *placement.RuleManager
	name               string
	regionWaitingList  cache.Cache
	pendingList        cache.Cache
	switchWitnessCache *cache.TTLUint64
	record             *recorder
}

// NewRuleChecker creates a checker instance.
func NewRuleChecker(ctx context.Context, cluster sche.CheckerCluster, ruleManager *placement.RuleManager, regionWaitingList cache.Cache) *RuleChecker {
	return &RuleChecker{
		cluster:            cluster,
		ruleManager:        ruleManager,
		name:               ruleCheckerName,
		regionWaitingList:  regionWaitingList,
		pendingList:        cache.NewDefaultCache(maxPendingListLen),
		switchWitnessCache: cache.NewIDTTL(ctx, time.Minute, cluster.GetCheckerConfig().GetSwitchWitnessInterval()),
		record:             newRecord(),
	}
}

// GetType returns RuleChecker's Type
func (c *RuleChecker) GetType() string {
	return ruleCheckerName
}

// Check checks if the region matches placement rules and returns Operator to
// fix it.
func (c *RuleChecker) Check(region *core.RegionInfo) *operator.Operator {
	fit := c.cluster.GetRuleManager().FitRegion(c.cluster, region)
	return c.CheckWithFit(region, fit)
}

// CheckWithFit is similar with Checker with placement.RegionFit
func (c *RuleChecker) CheckWithFit(region *core.RegionInfo, fit *placement.RegionFit) (op *operator.Operator) {
	// checker is paused
	if c.IsPaused() {
		ruleCheckerPausedCounter.Inc()
		return nil
	}
	// skip no leader region
	if region.GetLeader() == nil {
		ruleCheckerRegionNoLeaderCounter.Inc()
		log.Debug("fail to check region", zap.Uint64("region-id", region.GetID()), zap.Error(errRegionNoLeader))
		return
	}

	// If the fit is calculated by FitRegion, which means we get a new fit result, thus we should
	// invalid the cache if it exists
	c.ruleManager.InvalidCache(region.GetID())

	ruleCheckerCounter.Inc()
	c.record.refresh(c.cluster)

	if len(fit.RuleFits) == 0 {
		ruleCheckerNeedSplitCounter.Inc()
		// If the region matches no rules, the most possible reason is it spans across
		// multiple rules.
		return nil
	}
	op, err := c.fixOrphanPeers(region, fit)
	if err != nil {
		log.Debug("fail to fix orphan peer", errs.ZapError(err))
	} else if op != nil {
		c.pendingList.Remove(region.GetID())
		return op
	}
	for _, rf := range fit.RuleFits {
		op, err := c.fixRulePeer(region, fit, rf)
		if err != nil {
			log.Debug("fail to fix rule peer", zap.String("rule-group", rf.Rule.GroupID), zap.String("rule-id", rf.Rule.ID), errs.ZapError(err))
			continue
		}
		if op != nil {
			c.pendingList.Remove(region.GetID())
			return op
		}
	}
	if c.cluster.GetCheckerConfig().IsPlacementRulesCacheEnabled() {
		if placement.ValidateFit(fit) && placement.ValidateRegion(region) && placement.ValidateStores(fit.GetRegionStores()) {
			// If there is no need to fix, we will cache the fit
			c.ruleManager.SetRegionFitCache(region, fit)
			ruleCheckerSetCacheCounter.Inc()
		}
	}
	return nil
}

// RecordRegionPromoteToNonWitness put the recently switch non-witness region into cache. RuleChecker
// will skip switch it back to witness for a while.
func (c *RuleChecker) RecordRegionPromoteToNonWitness(regionID uint64) {
	c.switchWitnessCache.PutWithTTL(regionID, nil, c.cluster.GetCheckerConfig().GetSwitchWitnessInterval())
}

func (c *RuleChecker) isWitnessEnabled() bool {
	return versioninfo.IsFeatureSupported(c.cluster.GetCheckerConfig().GetClusterVersion(), versioninfo.SwitchWitness) &&
		c.cluster.GetCheckerConfig().IsWitnessAllowed()
}

func (c *RuleChecker) fixRulePeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (*operator.Operator, error) {
	// make up peers.
	if len(rf.Peers) < rf.Rule.Count {
		return c.addRulePeer(region, fit, rf)
	}
	// fix down/offline peers.
	for _, peer := range rf.Peers {
		if c.isDownPeer(region, peer) {
			if c.isStoreDownTimeHitMaxDownTime(peer.GetStoreId()) {
				ruleCheckerReplaceDownCounter.Inc()
				return c.replaceUnexpectRulePeer(region, rf, fit, peer, downStatus)
			}
			// When witness placement rule is enabled, promotes the witness to voter when region has down voter.
			if c.isWitnessEnabled() && core.IsVoter(peer) {
				if witness, ok := c.hasAvailableWitness(region, peer); ok {
					ruleCheckerPromoteWitnessCounter.Inc()
					return operator.CreateNonWitnessPeerOperator("promote-witness-for-down", c.cluster, region, witness)
				}
			}
		}
		if c.isOfflinePeer(peer) {
			ruleCheckerReplaceOfflineCounter.Inc()
			return c.replaceUnexpectRulePeer(region, rf, fit, peer, offlineStatus)
		}
	}
	// fix loose matched peers.
	for _, peer := range rf.PeersWithDifferentRole {
		op, err := c.fixLooseMatchPeer(region, fit, rf, peer)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	return c.fixBetterLocation(region, rf)
}

func (c *RuleChecker) addRulePeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (*operator.Operator, error) {
	ruleCheckerAddRulePeerCounter.Inc()
	ruleStores := c.getRuleFitStores(rf)
	isWitness := rf.Rule.IsWitness && c.isWitnessEnabled()
	// If the peer to be added is a witness, since no snapshot is needed, we also reuse the fast failover logic.
	store, filterByTempState := c.strategy(region, rf.Rule, isWitness).SelectStoreToAdd(ruleStores)
	if store == 0 {
		ruleCheckerNoStoreAddCounter.Inc()
		c.handleFilterState(region, filterByTempState)
		// try to replace an existing peer that matches the label constraints.
		// issue: https://github.com/tikv/pd/issues/7185
		for _, p := range region.GetPeers() {
			s := c.cluster.GetStore(p.GetStoreId())
			if placement.MatchLabelConstraints(s, rf.Rule.LabelConstraints) {
				oldPeerRuleFit := fit.GetRuleFit(p.GetId())
				if oldPeerRuleFit == nil || !oldPeerRuleFit.IsSatisfied() || oldPeerRuleFit == rf {
					continue
				}
				ruleCheckerNoStoreThenTryReplace.Inc()
				op, err := c.replaceUnexpectRulePeer(region, oldPeerRuleFit, fit, p, "swap-fit")
				if err != nil {
					return nil, err
				}
				if op != nil {
					return op, nil
				}
			}
		}
		return nil, errNoStoreToAdd
	}
	peer := &metapb.Peer{StoreId: store, Role: rf.Rule.Role.MetaPeerRole(), IsWitness: isWitness}
	op, err := operator.CreateAddPeerOperator("add-rule-peer", c.cluster, region, peer, operator.OpReplica)
	if err != nil {
		return nil, err
	}
	op.SetPriorityLevel(constant.High)
	return op, nil
}

// The peer's store may in Offline or Down, need to be replace.
func (c *RuleChecker) replaceUnexpectRulePeer(region *core.RegionInfo, rf *placement.RuleFit, fit *placement.RegionFit, peer *metapb.Peer, status string) (*operator.Operator, error) {
	var fastFailover bool
	// If the store to which the original peer belongs is TiFlash, the new peer cannot be set to witness, nor can it perform fast failover
	if c.isWitnessEnabled() && !c.cluster.GetStore(peer.StoreId).IsTiFlash() {
		// No matter whether witness placement rule is enabled or disabled, when peer's downtime
		// exceeds the threshold(30min), quickly add a witness to speed up failover, then promoted
		// to non-witness gradually to improve availability.
		if status == "down" {
			fastFailover = true
		} else {
			fastFailover = rf.Rule.IsWitness
		}
	} else {
		fastFailover = false
	}
	ruleStores := c.getRuleFitStores(rf)
	store, filterByTempState := c.strategy(region, rf.Rule, fastFailover).SelectStoreToFix(ruleStores, peer.GetStoreId())
	if store == 0 {
		ruleCheckerNoStoreReplaceCounter.Inc()
		c.handleFilterState(region, filterByTempState)
		return nil, errNoStoreToReplace
	}
	newPeer := &metapb.Peer{StoreId: store, Role: rf.Rule.Role.MetaPeerRole(), IsWitness: fastFailover}
	//  pick the smallest leader store to avoid the Offline store be snapshot generator bottleneck.
	var newLeader *metapb.Peer
	if region.GetLeader().GetId() == peer.GetId() {
		minCount := uint64(math.MaxUint64)
		for _, p := range region.GetPeers() {
			count := c.record.getOfflineLeaderCount(p.GetStoreId())
			checkPeerhealth := func() bool {
				if p.GetId() == peer.GetId() {
					return true
				}
				if region.GetDownPeer(p.GetId()) != nil || region.GetPendingPeer(p.GetId()) != nil {
					return false
				}
				return c.allowLeader(fit, p)
			}
			if minCount > count && checkPeerhealth() {
				minCount = count
				newLeader = p
			}
		}
	}

	createOp := func() (*operator.Operator, error) {
		if newLeader != nil && newLeader.GetId() != peer.GetId() {
			return operator.CreateReplaceLeaderPeerOperator("replace-rule-"+status+"-leader-peer", c.cluster, region, operator.OpReplica, peer.StoreId, newPeer, newLeader)
		}
		var desc string
		if fastFailover {
			desc = "fast-replace-rule-" + status + "-peer"
		} else {
			desc = "replace-rule-" + status + "-peer"
		}
		return operator.CreateMovePeerOperator(desc, c.cluster, region, operator.OpReplica, peer.StoreId, newPeer)
	}
	op, err := createOp()
	if err != nil {
		return nil, err
	}
	if newLeader != nil {
		c.record.incOfflineLeaderCount(newLeader.GetStoreId())
	}
	if fastFailover {
		op.SetPriorityLevel(constant.Urgent)
	} else {
		op.SetPriorityLevel(constant.High)
	}
	return op, nil
}

func (c *RuleChecker) fixLooseMatchPeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit, peer *metapb.Peer) (*operator.Operator, error) {
	if core.IsLearner(peer) && rf.Rule.Role != placement.Learner {
		ruleCheckerFixPeerRoleCounter.Inc()
		return operator.CreatePromoteLearnerOperator("fix-peer-role", c.cluster, region, peer)
	}
	if region.GetLeader().GetId() != peer.GetId() && rf.Rule.Role == placement.Leader {
		ruleCheckerFixLeaderRoleCounter.Inc()
		if c.allowLeader(fit, peer) {
			return operator.CreateTransferLeaderOperator("fix-leader-role", c.cluster, region, region.GetLeader().GetStoreId(), peer.GetStoreId(), []uint64{}, 0)
		}
		ruleCheckerNotAllowLeaderCounter.Inc()
		return nil, errPeerCannotBeLeader
	}
	if region.GetLeader().GetId() == peer.GetId() && rf.Rule.Role == placement.Follower {
		ruleCheckerFixFollowerRoleCounter.Inc()
		for _, p := range region.GetPeers() {
			if c.allowLeader(fit, p) {
				return operator.CreateTransferLeaderOperator("fix-follower-role", c.cluster, region, peer.GetStoreId(), p.GetStoreId(), []uint64{}, 0)
			}
		}
		ruleCheckerNoNewLeaderCounter.Inc()
		return nil, errNoNewLeader
	}
	if core.IsVoter(peer) && rf.Rule.Role == placement.Learner {
		ruleCheckerDemoteVoterRoleCounter.Inc()
		return operator.CreateDemoteVoterOperator("fix-demote-voter", c.cluster, region, peer)
	}
	if region.GetLeader().GetId() == peer.GetId() && rf.Rule.IsWitness {
		return nil, errPeerCannotBeWitness
	}
	if !core.IsWitness(peer) && rf.Rule.IsWitness && c.isWitnessEnabled() {
		c.switchWitnessCache.UpdateTTL(c.cluster.GetCheckerConfig().GetSwitchWitnessInterval())
		if c.switchWitnessCache.Exists(region.GetID()) {
			ruleCheckerRecentlyPromoteToNonWitnessCounter.Inc()
			return nil, nil
		}
		if len(region.GetPendingPeers()) > 0 {
			ruleCheckerCancelSwitchToWitnessCounter.Inc()
			return nil, nil
		}
		if core.IsLearner(peer) {
			ruleCheckerSetLearnerWitnessCounter.Inc()
		} else {
			ruleCheckerSetVoterWitnessCounter.Inc()
		}
		return operator.CreateWitnessPeerOperator("fix-witness-peer", c.cluster, region, peer)
	} else if core.IsWitness(peer) && (!rf.Rule.IsWitness || !c.isWitnessEnabled()) {
		if core.IsLearner(peer) {
			ruleCheckerSetLearnerNonWitnessCounter.Inc()
		} else {
			ruleCheckerSetVoterNonWitnessCounter.Inc()
		}
		return operator.CreateNonWitnessPeerOperator("fix-non-witness-peer", c.cluster, region, peer)
	}
	return nil, nil
}

func (c *RuleChecker) allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	if core.IsLearner(peer) || core.IsWitness(peer) {
		return false
	}
	s := c.cluster.GetStore(peer.GetStoreId())
	if s == nil {
		return false
	}
	stateFilter := &filter.StoreStateFilter{ActionScope: "rule-checker", TransferLeader: true}
	if !stateFilter.Target(c.cluster.GetCheckerConfig(), s).IsOK() {
		return false
	}
	for _, rf := range fit.RuleFits {
		if (rf.Rule.Role == placement.Leader || rf.Rule.Role == placement.Voter) &&
			placement.MatchLabelConstraints(s, rf.Rule.LabelConstraints) {
			return true
		}
	}
	return false
}

func (c *RuleChecker) fixBetterLocation(region *core.RegionInfo, rf *placement.RuleFit) (*operator.Operator, error) {
	if len(rf.Rule.LocationLabels) == 0 {
		return nil, nil
	}

	isWitness := rf.Rule.IsWitness && c.isWitnessEnabled()
	// If the peer to be moved is a witness, since no snapshot is needed, we also reuse the fast failover logic.
	strategy := c.strategy(region, rf.Rule, isWitness)
	ruleStores := c.getRuleFitStores(rf)
	oldStore := strategy.SelectStoreToRemove(ruleStores)
	if oldStore == 0 {
		return nil, nil
	}
	var coLocationStores []*core.StoreInfo
	regionStores := c.cluster.GetRegionStores(region)
	for _, s := range regionStores {
		if placement.MatchLabelConstraints(s, rf.Rule.LabelConstraints) {
			coLocationStores = append(coLocationStores, s)
		}
	}

	newStore, filterByTempState := strategy.SelectStoreToImprove(coLocationStores, oldStore)
	if newStore == 0 {
		log.Debug("no replacement store", zap.Uint64("region-id", region.GetID()))
		c.handleFilterState(region, filterByTempState)
		return nil, nil
	}
	ruleCheckerMoveToBetterLocationCounter.Inc()
	newPeer := &metapb.Peer{StoreId: newStore, Role: rf.Rule.Role.MetaPeerRole(), IsWitness: isWitness}
	return operator.CreateMovePeerOperator("move-to-better-location", c.cluster, region, operator.OpReplica, oldStore, newPeer)
}

func (c *RuleChecker) fixOrphanPeers(region *core.RegionInfo, fit *placement.RegionFit) (*operator.Operator, error) {
	if len(fit.OrphanPeers) == 0 {
		return nil, nil
	}
	var pinDownPeer *metapb.Peer
	isUnhealthyPeer := func(id uint64) bool {
		for _, downPeer := range region.GetDownPeers() {
			if downPeer.Peer.GetId() == id {
				return true
			}
		}
		for _, pendingPeer := range region.GetPendingPeers() {
			if pendingPeer.GetId() == id {
				return true
			}
		}
		return false
	}
	// remove orphan peers only when all rules are satisfied (count+role) and all peers selected
	// by RuleFits is not pending or down.
	hasUnhealthyFit := false
loopFits:
	for _, rf := range fit.RuleFits {
		if !rf.IsSatisfied() {
			hasUnhealthyFit = true
			break
		}
		for _, p := range rf.Peers {
			if isUnhealthyPeer(p.GetId()) {
				// make sure is down peer.
				if region.GetDownPeer(p.GetId()) != nil {
					pinDownPeer = p
				}
				hasUnhealthyFit = true
				break loopFits
			}
			// avoid to meet down store when fix orpahn peers,
			// Isdisconnected is more strictly than IsUnhealthy.
			if c.cluster.GetStore(p.GetStoreId()).IsDisconnected() {
				hasUnhealthyFit = true
				pinDownPeer = p
				break loopFits
			}
		}
	}

	// If hasUnhealthyFit is false, it is safe to delete the OrphanPeer.
	if !hasUnhealthyFit {
		ruleCheckerRemoveOrphanPeerCounter.Inc()
		return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, fit.OrphanPeers[0].StoreId)
	}

	// try to use orphan peers to replace unhealthy down peers.
	for _, orphanPeer := range fit.OrphanPeers {
		if pinDownPeer != nil {
			if pinDownPeer.GetId() == orphanPeer.GetId() {
				continue
			}
			// make sure the orphan peer is healthy.
			if isUnhealthyPeer(orphanPeer.GetId()) {
				continue
			}
			// no consider witness in this path.
			if pinDownPeer.GetIsWitness() || orphanPeer.GetIsWitness() {
				continue
			}
			// down peer's store should be down.
			if !c.isStoreDownTimeHitMaxDownTime(pinDownPeer.GetStoreId()) {
				continue
			}
			// check if down peer can replace with orphan peer.
			dstStore := c.cluster.GetStore(orphanPeer.GetStoreId())
			if fit.Replace(pinDownPeer.GetStoreId(), dstStore) {
				destRole := pinDownPeer.GetRole()
				orphanPeerRole := orphanPeer.GetRole()
				ruleCheckerReplaceOrphanPeerCounter.Inc()
				switch {
				case orphanPeerRole == metapb.PeerRole_Learner && destRole == metapb.PeerRole_Voter:
					return operator.CreatePromoteLearnerOperatorAndRemovePeer("replace-down-peer-with-orphan-peer", c.cluster, region, orphanPeer, pinDownPeer)
				case orphanPeerRole == metapb.PeerRole_Voter && destRole == metapb.PeerRole_Learner:
					return operator.CreateDemoteLearnerOperatorAndRemovePeer("replace-down-peer-with-orphan-peer", c.cluster, region, orphanPeer, pinDownPeer)
				case orphanPeerRole == metapb.PeerRole_Voter && destRole == metapb.PeerRole_Voter &&
					c.cluster.GetStore(pinDownPeer.GetStoreId()).IsDisconnected() && !dstStore.IsDisconnected():
					return operator.CreateRemovePeerOperator("remove-replaced-orphan-peer", c.cluster, 0, region, pinDownPeer.GetStoreId())
				default:
					// destRole should not same with orphanPeerRole. if role is same, it fit with orphanPeer should be better than now.
					// destRole never be leader, so we not consider it.
				}
			}
		}
	}

	// If hasUnhealthyFit is true, try to remove unhealthy orphan peers only if number of OrphanPeers is >= 2.
	// Ref https://github.com/tikv/pd/issues/4045
	if len(fit.OrphanPeers) >= 2 {
		hasHealthPeer := false
		for _, orphanPeer := range fit.OrphanPeers {
			if isUnhealthyPeer(orphanPeer.GetId()) {
				ruleCheckerRemoveOrphanPeerCounter.Inc()
				return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, orphanPeer.StoreId)
			}
			if hasHealthPeer {
				// there already exists a healthy orphan peer, so we can remove other orphan Peers.
				ruleCheckerRemoveOrphanPeerCounter.Inc()
				return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, orphanPeer.StoreId)
			}
			hasHealthPeer = true
		}
	}
	ruleCheckerSkipRemoveOrphanPeerCounter.Inc()
	return nil, nil
}

func (c *RuleChecker) isDownPeer(region *core.RegionInfo, peer *metapb.Peer) bool {
	for _, stats := range region.GetDownPeers() {
		if stats.GetPeer().GetId() == peer.GetId() {
			storeID := peer.GetStoreId()
			store := c.cluster.GetStore(storeID)
			if store == nil {
				log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
				return false
			}
			return true
		}
	}
	return false
}

func (c *RuleChecker) isStoreDownTimeHitMaxDownTime(storeID uint64) bool {
	store := c.cluster.GetStore(storeID)
	if store == nil {
		log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
		return false
	}
	return store.DownTime() >= c.cluster.GetCheckerConfig().GetMaxStoreDownTime()
}

func (c *RuleChecker) isOfflinePeer(peer *metapb.Peer) bool {
	store := c.cluster.GetStore(peer.GetStoreId())
	if store == nil {
		log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", peer.StoreId))
		return false
	}
	return !store.IsPreparing() && !store.IsServing()
}

func (c *RuleChecker) hasAvailableWitness(region *core.RegionInfo, peer *metapb.Peer) (*metapb.Peer, bool) {
	witnesses := region.GetWitnesses()
	if len(witnesses) == 0 {
		return nil, false
	}
	isAvailable := func(downPeers []*pdpb.PeerStats, witness *metapb.Peer) bool {
		for _, stats := range downPeers {
			if stats.GetPeer().GetId() == witness.GetId() {
				return false
			}
		}
		return c.cluster.GetStore(witness.GetStoreId()) != nil
	}
	downPeers := region.GetDownPeers()
	for _, witness := range witnesses {
		if witness.GetId() != peer.GetId() && isAvailable(downPeers, witness) {
			return witness, true
		}
	}
	return nil, false
}

func (c *RuleChecker) strategy(region *core.RegionInfo, rule *placement.Rule, fastFailover bool) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.name,
		cluster:        c.cluster,
		isolationLevel: rule.IsolationLevel,
		locationLabels: rule.LocationLabels,
		region:         region,
		extraFilters:   []filter.Filter{filter.NewLabelConstraintFilter(c.name, rule.LabelConstraints)},
		fastFailover:   fastFailover,
	}
}

func (c *RuleChecker) getRuleFitStores(rf *placement.RuleFit) []*core.StoreInfo {
	var stores []*core.StoreInfo
	for _, p := range rf.Peers {
		if s := c.cluster.GetStore(p.GetStoreId()); s != nil {
			stores = append(stores, s)
		}
	}
	return stores
}

func (c *RuleChecker) handleFilterState(region *core.RegionInfo, filterByTempState bool) {
	if filterByTempState {
		c.regionWaitingList.Put(region.GetID(), nil)
		c.pendingList.Remove(region.GetID())
	} else {
		c.pendingList.Put(region.GetID(), nil)
	}
}

type recorder struct {
	offlineLeaderCounter map[uint64]uint64
	lastUpdateTime       time.Time
}

func newRecord() *recorder {
	return &recorder{
		offlineLeaderCounter: make(map[uint64]uint64),
		lastUpdateTime:       time.Now(),
	}
}

func (o *recorder) getOfflineLeaderCount(storeID uint64) uint64 {
	return o.offlineLeaderCounter[storeID]
}

func (o *recorder) incOfflineLeaderCount(storeID uint64) {
	o.offlineLeaderCounter[storeID] += 1
	o.lastUpdateTime = time.Now()
}

// Offline is triggered manually and only appears when the node makes some adjustments. here is an operator timeout / 2.
var offlineCounterTTL = 5 * time.Minute

func (o *recorder) refresh(cluster sche.CheckerCluster) {
	// re-count the offlineLeaderCounter if the store is already tombstone or store is gone.
	if len(o.offlineLeaderCounter) > 0 && time.Since(o.lastUpdateTime) > offlineCounterTTL {
		needClean := false
		for _, storeID := range o.offlineLeaderCounter {
			store := cluster.GetStore(storeID)
			if store == nil || store.IsRemoved() {
				needClean = true
				break
			}
		}
		if needClean {
			o.offlineLeaderCounter = make(map[uint64]uint64)
		}
	}
}
