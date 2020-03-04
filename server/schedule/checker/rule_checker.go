// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/pd/v4/server/schedule/selector"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// RuleChecker fix/improve region by placement rules.
type RuleChecker struct {
	cluster     opt.Cluster
	ruleManager *placement.RuleManager
	name        string
}

// NewRuleChecker creates a checker instance.
func NewRuleChecker(cluster opt.Cluster, ruleManager *placement.RuleManager) *RuleChecker {
	return &RuleChecker{
		cluster:     cluster,
		ruleManager: ruleManager,
		name:        "rule-checker",
	}
}

// Check checks if the region matches placement rules and returns Operator to
// fix it.
func (c *RuleChecker) Check(region *core.RegionInfo) *operator.Operator {
	checkerCounter.WithLabelValues("rule_checker", "check").Inc()

	fit := c.cluster.FitRegion(region)
	if len(fit.RuleFits) == 0 {
		checkerCounter.WithLabelValues("rule_checker", "fix-range").Inc()
		// If the region matches no rules, the most possible reason is it spans across
		// multiple rules.
		return c.fixRange(region)
	}
	for _, rf := range fit.RuleFits {
		op, err := c.fixRulePeer(region, fit, rf)
		if err != nil {
			log.Debug("fail to fix rule peer", zap.Error(err), zap.String("rule-group", rf.Rule.GroupID), zap.String("rule-id", rf.Rule.ID))
			return nil
		}
		if op != nil {
			return op
		}
	}
	op, err := c.fixOrphanPeers(region, fit)
	if err != nil {
		log.Debug("fail to fix orphan peer", zap.Error(err))
		return nil
	}
	return op
}

func (c *RuleChecker) fixRange(region *core.RegionInfo) *operator.Operator {
	keys := c.ruleManager.GetSplitKeys(region.GetStartKey(), region.GetEndKey())
	if len(keys) == 0 {
		return nil
	}
	return operator.CreateSplitRegionOperator("rule-split-region", region, 0, pdpb.CheckPolicy_USEKEY, keys)
}

func (c *RuleChecker) fixRulePeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (*operator.Operator, error) {
	// make up peers.
	if len(rf.Peers) < rf.Rule.Count {
		return c.addRulePeer(region, rf)
	}
	// fix down/offline peers.
	for _, peer := range rf.Peers {
		if c.isDownPeer(region, peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-down").Inc()
			return c.replaceRulePeer(region, fit, rf, peer, downStatus)
		}
		if c.isOfflinePeer(region, peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-offline").Inc()
			return c.replaceRulePeer(region, fit, rf, peer, offlineStatus)
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
	return c.fixBetterLocation(region, fit, rf)
}

func (c *RuleChecker) addRulePeer(region *core.RegionInfo, rf *placement.RuleFit) (*operator.Operator, error) {
	checkerCounter.WithLabelValues("rule_checker", "add-rule-peer").Inc()
	store := SelectStoreToAddPeerByRule(c.name, c.cluster, region, rf)
	if store == nil {
		checkerCounter.WithLabelValues("rule_checker", "no-store-add").Inc()
		return nil, errors.New("no store to add peer")
	}
	peer := &metapb.Peer{StoreId: store.GetID(), IsLearner: rf.Rule.Role == placement.Learner}
	return operator.CreateAddPeerOperator("add-rule-peer", c.cluster, region, peer, operator.OpReplica)
}

func (c *RuleChecker) replaceRulePeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit, peer *metapb.Peer, status string) (*operator.Operator, error) {
	store := SelectStoreToReplacePeerByRule(c.name, c.cluster, region, fit, rf, peer)
	if store == nil {
		checkerCounter.WithLabelValues("rule_checker", "no-store-replace").Inc()
		return nil, errors.New("no store to replace peer")
	}
	newPeer := &metapb.Peer{StoreId: store.GetID(), IsLearner: rf.Rule.Role == placement.Learner}
	return operator.CreateMovePeerOperator("replace-rule-"+status+"-peer", c.cluster, region, operator.OpReplica, peer.StoreId, newPeer)
}

func (c *RuleChecker) fixLooseMatchPeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit, peer *metapb.Peer) (*operator.Operator, error) {
	if peer.IsLearner && rf.Rule.Role != placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "fix-peer-role").Inc()
		return operator.CreatePromoteLearnerOperator("fix-peer-role", c.cluster, region, peer)
	}
	if region.GetLeader().GetId() == peer.GetId() && rf.Rule.Role == placement.Follower {
		checkerCounter.WithLabelValues("rule_checker", "fix-leader-role").Inc()
		for _, p := range region.GetPeers() {
			if c.allowLeader(fit, p) {
				return operator.CreateTransferLeaderOperator("fix-peer-role", c.cluster, region, peer.GetStoreId(), p.GetStoreId(), 0)
			}
		}
		checkerCounter.WithLabelValues("rule_checker", "no-new-leader").Inc()
		return nil, errors.New("no new leader")
	}
	return nil, nil
}

func (c *RuleChecker) allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	if peer.GetIsLearner() {
		return false
	}
	s := c.cluster.GetStore(peer.GetStoreId())
	if s == nil {
		return false
	}
	stateFilter := filter.StoreStateFilter{ActionScope: "rule-checker", TransferLeader: true}
	if !stateFilter.Target(c.cluster, s) {
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

func (c *RuleChecker) fixBetterLocation(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (*operator.Operator, error) {
	if len(rf.Rule.LocationLabels) == 0 || rf.Rule.Count <= 1 {
		return nil, nil
	}
	stores := getRuleFitStores(c.cluster, rf)
	s := selector.NewReplicaSelector(stores, rf.Rule.LocationLabels, filter.StoreStateFilter{ActionScope: "rule-checker", MoveRegion: true})
	oldPeerStore := s.SelectSource(c.cluster, stores)
	if oldPeerStore == nil {
		return nil, nil
	}
	oldPeer := region.GetStorePeer(oldPeerStore.GetID())
	newPeerStore := SelectStoreToReplacePeerByRule("rule-checker", c.cluster, region, fit, rf, oldPeer)
	if newPeerStore == nil {
		log.Debug("no replacement store", zap.Uint64("region-id", region.GetID()))
		return nil, nil
	}
	stores = getRuleFitStores(c.cluster, removePeerFromRuleFit(rf, oldPeer))
	oldScore := core.DistinctScore(rf.Rule.LocationLabels, stores, oldPeerStore)
	newScore := core.DistinctScore(rf.Rule.LocationLabels, stores, newPeerStore)
	if newScore <= oldScore {
		log.Debug("no better peer", zap.Uint64("region-id", region.GetID()), zap.Float64("new-score", newScore), zap.Float64("old-score", oldScore))
		return nil, nil
	}
	checkerCounter.WithLabelValues("rule_checker", "move-to-better-location").Inc()
	newPeer := &metapb.Peer{StoreId: newPeerStore.GetID(), IsLearner: oldPeer.IsLearner}
	return operator.CreateMovePeerOperator("move-to-better-location", c.cluster, region, operator.OpReplica, oldPeer.GetStoreId(), newPeer)
}

func (c *RuleChecker) fixOrphanPeers(region *core.RegionInfo, fit *placement.RegionFit) (*operator.Operator, error) {
	if len(fit.OrphanPeers) == 0 {
		return nil, nil
	}
	checkerCounter.WithLabelValues("rule_checker", "remove-orphan-peer").Inc()
	peer := fit.OrphanPeers[0]
	return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, peer.StoreId)
}

func (c *RuleChecker) isDownPeer(region *core.RegionInfo, peer *metapb.Peer) bool {
	for _, stats := range region.GetDownPeers() {
		if stats.GetPeer().GetId() != peer.GetId() {
			continue
		}
		storeID := peer.GetStoreId()
		store := c.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return false
		}
		if store.DownTime() < c.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(c.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return true
	}
	return false
}

func (c *RuleChecker) isOfflinePeer(region *core.RegionInfo, peer *metapb.Peer) bool {
	store := c.cluster.GetStore(peer.GetStoreId())
	if store == nil {
		log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", peer.StoreId))
		return false
	}
	return !store.IsUp()
}

// SelectStoreToAddPeerByRule selects a store to add peer in order to fit the placement rule.
func SelectStoreToAddPeerByRule(scope string, cluster opt.Cluster, region *core.RegionInfo, rf *placement.RuleFit, filters ...filter.Filter) *core.StoreInfo {
	fs := []filter.Filter{
		filter.StoreStateFilter{ActionScope: scope, MoveRegion: true},
		filter.NewStorageThresholdFilter(scope),
		filter.NewLabelConstaintFilter(scope, rf.Rule.LabelConstraints),
		filter.NewExcludedFilter(scope, nil, region.GetStoreIds()),
		filter.NewSpecialUseFilter(scope),
	}
	fs = append(fs, filters...)
	store := selector.NewReplicaSelector(getRuleFitStores(cluster, rf), rf.Rule.LocationLabels).
		SelectTarget(cluster, cluster.GetStores(), fs...)
	return store
}

// SelectStoreToReplacePeerByRule selects a store to replace a region peer in order to fit the placement rule.
func SelectStoreToReplacePeerByRule(scope string, cluster opt.Cluster, region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit, peer *metapb.Peer, filters ...filter.Filter) *core.StoreInfo {
	rf2 := removePeerFromRuleFit(rf, peer)
	return SelectStoreToAddPeerByRule(scope, cluster, region, rf2, filters...)
}

func getRuleFitStores(cluster opt.Cluster, fit *placement.RuleFit) []*core.StoreInfo {
	var stores []*core.StoreInfo
	for _, p := range fit.Peers {
		if s := cluster.GetStore(p.GetStoreId()); s != nil {
			stores = append(stores, s)
		}
	}
	return stores
}

func removePeerFromRuleFit(rf *placement.RuleFit, peer *metapb.Peer) *placement.RuleFit {
	rf2 := &placement.RuleFit{Rule: rf.Rule}
	for _, p := range rf.Peers {
		if p.GetId() != peer.GetId() {
			rf2.Peers = append(rf2.Peers, p)
		}
	}
	for _, p := range rf.PeersWithDifferentRole {
		if p.GetId() != peer.GetId() {
			rf2.PeersWithDifferentRole = append(rf2.PeersWithDifferentRole, p)
		}
	}
	return rf2
}
