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
	"errors"
	"math"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedule/plan"
	"go.uber.org/zap"
)

var (
	errNoStoreToAdd       = errors.New("no store to add peer")
	errNoStoreToReplace   = errors.New("no store to replace peer")
	errPeerCannotBeLeader = errors.New("peer cannot be leader")
	errNoNewLeader        = errors.New("no new leader")
)

const maxPendingListLen = 100000

// RuleChecker fix/improve region by placement rules.
type RuleChecker struct {
	PauseController
	cluster           schedule.Cluster
	ruleManager       *placement.RuleManager
	name              string
	regionWaitingList cache.Cache
	pendingList       cache.Cache
	record            *recorder
}

// NewRuleChecker creates a checker instance.
func NewRuleChecker(cluster schedule.Cluster, ruleManager *placement.RuleManager, regionWaitingList cache.Cache) *RuleChecker {
	return &RuleChecker{
		cluster:           cluster,
		ruleManager:       ruleManager,
		name:              "rule-checker",
		regionWaitingList: regionWaitingList,
		pendingList:       cache.NewDefaultCache(maxPendingListLen),
		record:            newRecord(),
	}
}

// GetType returns RuleChecker's Type
func (c *RuleChecker) GetType() string {
	return "rule-checker"
}

// Check checks if the region matches placement rules and returns true if need to
// fix. test only.
func (c *RuleChecker) Check(region *core.RegionInfo) *operator.Operator {
	checkPlan := newNode(c.GetType(), region, false)
	fit := c.cluster.GetRuleManager().FitRegion(c.cluster, region)
	ops := c.CheckWithFit(checkPlan, fit)
	if len(ops) == 0 {
		return nil
	}
	return ops[0]
}

// CheckWithFit is similar with Checker with placement.RegionFit
func (c *RuleChecker) CheckWithFit(p *checkNode, fit *placement.RegionFit) []*operator.Operator {
	curPlan := p.newSubCheck(c.GetType())
	if c.IsPaused() {
		checkerCounter.WithLabelValues("rule_checker", "paused").Inc()
		curPlan.StopWith(statusPaused)
		return nil
	}
	// If the fit is fetched from cache, it seems that the region doesn't need cache
	if c.cluster.GetOpts().IsPlacementRulesCacheEnabled() && fit.IsCached() {
		failpoint.Inject("assertShouldNotCache", func() {
			panic("cached shouldn't be used")
		})
		checkerCounter.WithLabelValues("rule_checker", "get-cache").Inc()
		curPlan.StopWith(plan.NewStatus(plan.StatusNoNeed, "get-cache"))
		return nil
	}
	failpoint.Inject("assertShouldCache", func() {
		panic("cached should be used")
	})

	// If the fit is calculated by FitRegion, which means we get a new fit result, thus we should
	// invalid the cache if it exists
	c.ruleManager.InvalidCache(curPlan.region.GetID())

	checkerCounter.WithLabelValues("rule_checker", "check").Inc()
	c.record.refresh(c.cluster)

	if len(fit.RuleFits) == 0 {
		checkerCounter.WithLabelValues("rule_checker", "need-split").Inc()
		// If the region matches no rules, the most possible reason is it spans across
		// multiple rules.
		return curPlan.StopWith(plan.NewStatus(plan.StatusNoNeed, "no fit rule, need-split"))
	}
	region := curPlan.region
	if ops := c.fixOrphanPeers(curPlan, fit); len(ops) != 0 {
		c.pendingList.Remove(region.GetID())
		return ops
	}
	for _, rf := range fit.RuleFits {
		ops := c.fixRulePeer(p, fit, rf)
		if len(ops) > 0 {
			c.pendingList.Remove(region.GetID())
			return ops
		}
	}
	if c.cluster.GetOpts().IsPlacementRulesCacheEnabled() {
		if placement.ValidateFit(fit) && placement.ValidateRegion(region) && placement.ValidateStores(fit.GetRegionStores()) {
			// If there is no need to fix, we will cache the fit
			c.ruleManager.SetRegionFitCache(region, fit)
			checkerCounter.WithLabelValues("rule_checker", "set-cache").Inc()
		}
	}
	return curPlan.noNeed("TheEnd", "no rule need to fix")
}

func (c *RuleChecker) fixRulePeer(p *checkNode, fit *placement.RegionFit, rf *placement.RuleFit) []*operator.Operator {
	curPlan := p.newSubCheck("fixRulePeer")
	region := curPlan.region
	// make up peers.
	if len(rf.Peers) < rf.Rule.Count {
		return c.addRulePeer(curPlan, rf)
	}
	// fix down/offline peers.
	for _, peer := range rf.Peers {
		if c.isDownPeer(region, peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-down").Inc()
			return c.replaceUnexpectRulePeer(p, rf, fit, peer, downStatus)
		}
		if c.isOfflinePeer(peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-offline").Inc()
			return c.replaceUnexpectRulePeer(p, rf, fit, peer, offlineStatus)
		}
	}
	// fix loose matched peers.
	for _, peer := range rf.PeersWithDifferentRole {
		ops := c.fixLooseMatchPeer(p, fit, rf, peer)
		if len(ops) > 0 {
			return ops
		}
	}
	return c.fixBetterLocation(p, rf)
}

func (c *RuleChecker) addRulePeer(p *checkNode, rf *placement.RuleFit) []*operator.Operator {
	checkerCounter.WithLabelValues("rule_checker", "add-rule-peer").Inc()
	curPlan := p.newSubCheck("add-rule-peer")
	region := curPlan.region
	ruleStores := c.getRuleFitStores(rf)
	store, filterByTempState := c.strategy(region, rf.Rule).SelectStoreToAdd(ruleStores)
	if store == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-store-add").Inc()
		c.handleFilterState(region, filterByTempState)
		return curPlan.StopWith(plan.NewStatus(plan.StatusNoStoreAvailable, errNoStoreToAdd.Error()))
	}
	peer := &metapb.Peer{StoreId: store, Role: rf.Rule.Role.MetaPeerRole()}
	op, err := operator.CreateAddPeerOperator("add-rule-peer", c.cluster, region, peer, operator.OpReplica)
	if op != nil {
		op.SetPriorityLevel(core.HighPriority)
	}
	return curPlan.StopAtCreateOps(err, op)
}

// The peer's store may in Offline or Down, need to be replace.
func (c *RuleChecker) replaceUnexpectRulePeer(p *checkNode, rf *placement.RuleFit, fit *placement.RegionFit, peer *metapb.Peer, status string) []*operator.Operator {
	curPlan := p.newSubCheck("replaceUnexpectRulePeer")
	region := curPlan.region
	ruleStores := c.getRuleFitStores(rf)
	store, filterByTempState := c.strategy(region, rf.Rule).SelectStoreToFix(ruleStores, peer.GetStoreId())
	if store == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-store-replace").Inc()
		c.handleFilterState(region, filterByTempState)
		return curPlan.StopAtStep("SelectStoreToFix", plan.NewStatus(plan.StatusNoStoreAvailable, errNoStoreToReplace.Error()))
	}
	newPeer := &metapb.Peer{StoreId: store, Role: rf.Rule.Role.MetaPeerRole()}
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
		return operator.CreateMovePeerOperator("replace-rule-"+status+"-peer", c.cluster, region, operator.OpReplica, peer.StoreId, newPeer)
	}
	op, err := createOp()
	if op != nil {
		op.SetPriorityLevel(core.HighPriority)
	}
	ops := curPlan.StopAtCreateOps(err, op)
	if err != nil {
		return ops
	}
	if newLeader != nil {
		c.record.incOfflineLeaderCount(newLeader.GetStoreId())
	}
	return ops
}

func (c *RuleChecker) fixLooseMatchPeer(p *checkNode, fit *placement.RegionFit, rf *placement.RuleFit, peer *metapb.Peer) []*operator.Operator {
	curPlan := p.newSubCheck("fixLooseMatchPeer")
	region := curPlan.region
	if core.IsLearner(peer) && rf.Rule.Role != placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "fix-peer-role").Inc()
		op, err := operator.CreatePromoteLearnerOperator("fix-peer-role", c.cluster, region, peer)
		return curPlan.StopAtCreateOps(err, op)
	}
	if region.GetLeader().GetId() != peer.GetId() && rf.Rule.Role == placement.Leader {
		checkerCounter.WithLabelValues("rule_checker", "fix-leader-role").Inc()
		if c.allowLeader(fit, peer) {
			op, err := operator.CreateTransferLeaderOperator("fix-leader-role", c.cluster, region, region.GetLeader().StoreId, peer.GetStoreId(), []uint64{}, 0)
			return curPlan.StopAtCreateOps(err, op)
		}
		checkerCounter.WithLabelValues("rule_checker", "not-allow-leader")
		return curPlan.StopAtStep("allowLeader", plan.NewStatus(plan.StatusRuleNotMatch, errPeerCannotBeLeader.Error()))
	}
	if region.GetLeader().GetId() == peer.GetId() && rf.Rule.Role == placement.Follower {
		checkerCounter.WithLabelValues("rule_checker", "fix-follower-role").Inc()
		for _, p := range region.GetPeers() {
			if c.allowLeader(fit, p) {
				op, err := operator.CreateTransferLeaderOperator("fix-follower-role", c.cluster, region, peer.GetStoreId(), p.GetStoreId(), []uint64{}, 0)
				return curPlan.StopAtCreateOps(err, op)
			}
		}
		checkerCounter.WithLabelValues("rule_checker", "no-new-leader").Inc()
		return curPlan.StopAtStep("allowLeader", plan.NewStatus(plan.StatusRuleNotMatch, errNoNewLeader.Error()))
	}
	if core.IsVoter(peer) && rf.Rule.Role == placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "demote-voter-role").Inc()
		op, err := operator.CreateDemoteVoterOperator("fix-demote-voter", c.cluster, region, peer)
		return curPlan.StopAtCreateOps(err, op)
	}
	return curPlan.noNeed("TheEnd", "")
}

func (c *RuleChecker) allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	if core.IsLearner(peer) {
		return false
	}
	s := c.cluster.GetStore(peer.GetStoreId())
	if s == nil {
		return false
	}
	stateFilter := &filter.StoreStateFilter{ActionScope: "rule-checker", TransferLeader: true}
	if !stateFilter.Target(c.cluster.GetOpts(), s).IsOK() {
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

func (c *RuleChecker) fixBetterLocation(p *checkNode, rf *placement.RuleFit) []*operator.Operator {
	curPlan := p.newSubCheck("fixBetterLocation")
	region := curPlan.region
	if len(rf.Rule.LocationLabels) == 0 || rf.Rule.Count <= 1 {
		return curPlan.noNeed("checkRule", "no location labels or rule count <= 1")
	}

	strategy := c.strategy(region, rf.Rule)
	ruleStores := c.getRuleFitStores(rf)
	oldStore := strategy.SelectStoreToRemove(ruleStores)
	if oldStore == 0 {
		return curPlan.StopAtStep("SelectStoreToRemove", plan.NewStatus(plan.StatusNoNeed))
	}
	newStore, filterByTempState := strategy.SelectStoreToImprove(ruleStores, oldStore)
	if newStore == 0 {
		log.Debug("no replacement store", zap.Uint64("region-id", region.GetID()))
		c.handleFilterState(region, filterByTempState)
		return curPlan.StopAtStep("SelectStoreToImprove", plan.NewStatus(plan.StatusNoNeed))
	}
	checkerCounter.WithLabelValues("rule_checker", "move-to-better-location").Inc()
	newPeer := &metapb.Peer{StoreId: newStore, Role: rf.Rule.Role.MetaPeerRole()}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", c.cluster, region, operator.OpReplica, oldStore, newPeer)
	return curPlan.StopAtCreateOps(err, op)
}

func (c *RuleChecker) fixOrphanPeers(p *checkNode, fit *placement.RegionFit) []*operator.Operator {
	checkNode := p.newSubCheck("fixOrphanPeers")
	if len(fit.OrphanPeers) == 0 {
		return checkNode.StopWith(plan.NewStatus(plan.StatusNoNeed, "no orphan peers"))
	}
	// remove orphan peers only when all rules are satisfied (count+role) and all peers selected
	// by RuleFits is not pending or down.
	for _, rf := range fit.RuleFits {
		if !rf.IsSatisfied() {
			checkerCounter.WithLabelValues("rule_checker", "skip-remove-orphan-peer").Inc()
			return checkNode.noNeed("IsSatisfied", "exist un statisfied rule")
		}
		for _, p := range rf.Peers {
			for _, pendingPeer := range checkNode.region.GetPendingPeers() {
				if pendingPeer.Id == p.Id {
					checkerCounter.WithLabelValues("rule_checker", "skip-remove-orphan-peer").Inc()
					return checkNode.noNeed("GetPendingPeers", "pending peer exist in rule")
				}
			}
			for _, downPeer := range checkNode.region.GetDownPeers() {
				if downPeer.Peer.Id == p.Id {
					checkerCounter.WithLabelValues("rule_checker", "skip-remove-orphan-peer").Inc()
					return checkNode.noNeed("GetDownPeers", "down peer exist in rule")
				}
			}
		}
	}
	checkerCounter.WithLabelValues("rule_checker", "remove-orphan-peer").Inc()
	peer := fit.OrphanPeers[0]
	op, err := operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, checkNode.region, peer.StoreId)
	if err != nil {
		log.Debug("fail to fix orphan peer", errs.ZapError(err))
	}
	return checkNode.StopAtCreateOps(err, op)
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
		// Only consider the state of the Store, not `stats.DownSeconds`.
		if store.DownTime() < c.cluster.GetOpts().GetMaxStoreDownTime() {
			continue
		}
		return true
	}
	return false
}

func (c *RuleChecker) isOfflinePeer(peer *metapb.Peer) bool {
	store := c.cluster.GetStore(peer.GetStoreId())
	if store == nil {
		log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", peer.StoreId))
		return false
	}
	return !store.IsPreparing() && !store.IsServing()
}

func (c *RuleChecker) strategy(region *core.RegionInfo, rule *placement.Rule) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.name,
		cluster:        c.cluster,
		isolationLevel: rule.IsolationLevel,
		locationLabels: rule.LocationLabels,
		region:         region,
		extraFilters:   []filter.Filter{filter.NewLabelConstaintFilter(c.name, rule.LabelConstraints)},
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

func (o *recorder) refresh(cluster schedule.Cluster) {
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
