// Copyright 2016 TiKV Project Authors.
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

package filter

import (
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedule/plan"
	"go.uber.org/zap"
)

var (
	statusStoreOK                 = plan.NewStatus(plan.StatusOK)
	statusStoreNoNeed             = plan.NewStatus(plan.StatusNoNeed)
	statusStoreDown               = plan.NewStatus(plan.StatusStoreUnavailable, "store is lost for longer than 'max-store-down-time' setting")
	statusStoreTombstone          = plan.NewStatus(plan.StatusStoreUnavailable, "store is tombstone")
	statusStoreDisconnected       = plan.NewStatus(plan.StatusStoreUnavailable, "store is lost for more than 20s")
	statusStoreBusy               = plan.NewStatus(plan.StatusStoreUnavailable, "store is busy")
	statusStoresOffline           = plan.NewStatus(plan.StatusStoreDraining, "store is in the process of offline")
	statusStoreLowSpace           = plan.NewStatus(plan.StatusStoreLowSpace, "store space is not enough, please scale out or change 'low-space-ratio' setting")
	statusStoreExcluded           = plan.NewStatus(plan.StatusStoreExcluded, "there has already had a peer or the peer is unhealthy in the store")
	statusStoreIsolation          = plan.NewStatus(plan.StatusIsolationNotMatch)
	statusStoreTooManySnapshot    = plan.NewStatus(plan.StatusStoreThrottled, "store snapshot has been piled up, the related setting is 'max-snapshot-count'")
	statusStoreTooManyPendingPeer = plan.NewStatus(plan.StatusStoreThrottled, "store has too many pending peers, the related setting is 'max-pending-peer-count'")
	statusStoreAddLimit           = plan.NewStatus(plan.StatusStoreThrottled, "store's add limit is exhausted, please check the setting of 'store limit'")
	statusStoreRemoveLimit        = plan.NewStatus(plan.StatusStoreThrottled, "store's remove limit is exhausted, please check the setting of 'store limit'")
	statusStoreLabel              = plan.NewStatus(plan.StatusLabelNotMatch)
	statusStoreRule               = plan.NewStatus(plan.StatusRuleNotMatch)
	statusStorePauseLeader        = plan.NewStatus(plan.StatusStoreBlocked, "the store is not allowed to transfer leader, there might be an evict-leader-scheduler")
	statusStoreRejectLeader       = plan.NewStatus(plan.StatusStoreBlocked, "the store is not allowed to transfer leader, please check 'label-property'")
	statusStoreSlow               = plan.NewStatus(plan.StatusStoreBlocked, "the store is slow and are evicting leaders, there might be an evict-slow-store-scheduler")
)

// SelectSourceStores selects stores that be selected as source store from the list.
func SelectSourceStores(stores []*core.StoreInfo, filters []Filter, opt *config.PersistOptions) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool {
			if !filters[i].Source(opt, s).IsOK() {
				sourceID := strconv.FormatUint(s.GetID(), 10)
				targetID := ""
				filterCounter.WithLabelValues("filter-source", s.GetAddress(),
					sourceID, filters[i].Scope(), filters[i].Type(), sourceID, targetID).Inc()
				return false
			}
			return true
		})
	})
}

// SelectTargetStores selects stores that be selected as target store from the list.
func SelectTargetStores(stores []*core.StoreInfo, filters []Filter, opt *config.PersistOptions) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool {
			filter := filters[i]
			if !filter.Target(opt, s).IsOK() {
				cfilter, ok := filter.(comparingFilter)
				targetID := strconv.FormatUint(s.GetID(), 10)
				sourceID := ""
				if ok {
					sourceID = strconv.FormatUint(cfilter.GetSourceStoreID(), 10)
				}
				filterCounter.WithLabelValues("filter-target", s.GetAddress(),
					targetID, filters[i].Scope(), filters[i].Type(), sourceID, targetID).Inc()
				return false
			}
			return true
		})
	})
}

func filterStoresBy(stores []*core.StoreInfo, keepPred func(*core.StoreInfo) bool) (selected []*core.StoreInfo) {
	for _, s := range stores {
		if keepPred(s) {
			selected = append(selected, s)
		}
	}
	return
}

// Filter is an interface to filter source and target store.
type Filter interface {
	// Scope is used to indicate where the filter will act on.
	Scope() string
	Type() string
	// Return true if the store can be used as a source store.
	Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status
	// Return true if the store can be used as a target store.
	Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status
}

// comparingFilter is an interface to filter target store by comparing source and target stores
type comparingFilter interface {
	Filter
	// GetSourceStoreID returns the source store when comparing.
	GetSourceStoreID() uint64
}

// Source checks if store can pass all Filters as source store.
func Source(opt *config.PersistOptions, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := strconv.FormatUint(store.GetID(), 10)
	for _, filter := range filters {
		if filter.Source(opt, store) != statusStoreOK {
			sourceID := storeID
			targetID := ""
			filterCounter.WithLabelValues("filter-source", storeAddress,
				sourceID, filter.Scope(), filter.Type(), sourceID, targetID).Inc()
			return false
		}
	}
	return true
}

// Target checks if store can pass all Filters as target store.
func Target(opt *config.PersistOptions, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := strconv.FormatUint(store.GetID(), 10)
	for _, filter := range filters {
		if filter.Target(opt, store) != statusStoreOK {
			cfilter, ok := filter.(comparingFilter)
			targetID := storeID
			sourceID := ""
			if ok {
				sourceID = strconv.FormatUint(cfilter.GetSourceStoreID(), 10)
			}
			filterCounter.WithLabelValues("filter-target", storeAddress,
				targetID, filter.Scope(), filter.Type(), sourceID, targetID).Inc()
			return false
		}
	}
	return true
}

type excludedFilter struct {
	scope   string
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(scope string, sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		scope:   scope,
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) Scope() string {
	return f.scope
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if _, ok := f.sources[store.GetID()]; ok {
		return statusStoreExcluded
	}
	return statusStoreOK
}

func (f *excludedFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if _, ok := f.targets[store.GetID()]; ok {
		return statusStoreExcluded
	}
	return statusStoreOK
}

type storageThresholdFilter struct{ scope string }

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter(scope string) Filter {
	return &storageThresholdFilter{scope: scope}
}

func (f *storageThresholdFilter) Scope() string {
	return f.scope
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	return statusStoreOK
}

func (f *storageThresholdFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !store.IsLowSpace(opt.GetLowSpaceRatio()) {
		return statusStoreOK
	}
	return statusStoreLowSpace
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	scope     string
	labels    []string
	stores    []*core.StoreInfo
	policy    string
	safeScore float64
	srcStore  uint64
}

const (
	// policies used by distinctScoreFilter.
	// 'safeguard' ensures replacement is NOT WORSE than before.
	// 'improve' ensures replacement is BETTER than before.
	locationSafeguard = "safeguard"
	locationImprove   = "improve"
)

// NewLocationSafeguard creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewLocationSafeguard(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	return newDistinctScoreFilter(scope, labels, stores, source, locationSafeguard)
}

// NewLocationImprover creates a filter that filters all stores that have
// lower or equal distinct score than specified store.
func NewLocationImprover(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	return newDistinctScoreFilter(scope, labels, stores, source, locationImprove)
}

func newDistinctScoreFilter(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo, policy string) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetID() == source.GetID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		scope:     scope,
		labels:    labels,
		stores:    newStores,
		safeScore: core.DistinctScore(labels, newStores, source),
		policy:    policy,
		srcStore:  source.GetID(),
	}
}

func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	return statusStoreOK
}

func (f *distinctScoreFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	score := core.DistinctScore(f.labels, f.stores, store)
	switch f.policy {
	case locationSafeguard:
		if score >= f.safeScore {
			return statusStoreOK
		}
	case locationImprove:
		if score > f.safeScore {
			return statusStoreOK
		}
	default:
	}
	return statusStoreIsolation
}

// GetSourceStoreID implements the ComparingFilter
func (f *distinctScoreFilter) GetSourceStoreID() uint64 {
	return f.srcStore
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	ActionScope string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
	// Set true if the scatter move the region
	ScatterRegion bool
	// Set true if allows temporary states.
	AllowTemporaryStates bool
	// Reason is used to distinguish the reason of store state filter
	Reason string
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *StoreStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f *StoreStateFilter) Type() string {
	return fmt.Sprintf("store-state-%s-filter", f.Reason)
}

// conditionFunc defines condition to determine a store should be selected.
// It should consider if the filter allows temporary states.
type conditionFunc func(*config.PersistOptions, *core.StoreInfo) plan.Status

func (f *StoreStateFilter) isRemoved(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if store.IsRemoved() {
		f.Reason = "tombstone"
		return statusStoreTombstone
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) isDown(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if store.DownTime() > opt.GetMaxStoreDownTime() {
		f.Reason = "down"
		return statusStoreDown
	}
	f.Reason = ""

	return statusStoreOK
}

func (f *StoreStateFilter) isRemoving(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if store.IsRemoving() {
		f.Reason = "offline"
		return statusStoresOffline
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) pauseLeaderTransfer(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !store.AllowLeaderTransfer() {
		f.Reason = "pause-leader"
		return statusStorePauseLeader
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) slowStoreEvicted(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if store.EvictedAsSlowStore() {
		f.Reason = "slow-store"
		return statusStoreSlow
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) isDisconnected(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.AllowTemporaryStates && store.IsDisconnected() {
		f.Reason = "disconnected"
		return statusStoreDisconnected
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) isBusy(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.AllowTemporaryStates && store.IsBusy() {
		f.Reason = "busy"
		return statusStoreBusy
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) exceedRemoveLimit(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.AllowTemporaryStates && !store.IsAvailable(storelimit.RemovePeer) {
		f.Reason = "exceed-remove-limit"
		return statusStoreRemoveLimit
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) exceedAddLimit(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.AllowTemporaryStates && !store.IsAvailable(storelimit.AddPeer) {
		f.Reason = "exceed-add-limit"
		return statusStoreAddLimit
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) tooManySnapshots(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.AllowTemporaryStates && (uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount()) {
		f.Reason = "too-many-snapshot"
		return statusStoreTooManySnapshot
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) tooManyPendingPeers(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.AllowTemporaryStates &&
		opt.GetMaxPendingPeerCount() > 0 &&
		store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount()) {
		f.Reason = "too-many-pending-peer"
		return statusStoreTooManyPendingPeer
	}
	f.Reason = ""
	return statusStoreOK
}

func (f *StoreStateFilter) hasRejectLeaderProperty(opts *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if opts.CheckLabelProperty(config.RejectLeader, store.GetLabels()) {
		f.Reason = "reject-leader"
		return statusStoreRejectLeader
	}
	f.Reason = ""
	return statusStoreOK
}

// The condition table.
// Y: the condition is temporary (expected to become false soon).
// N: the condition is expected to be true for a long time.
// X means when the condition is true, the store CANNOT be selected.
//
// Condition    Down Offline Tomb Pause Disconn Busy RmLimit AddLimit Snap Pending Reject
// IsTemporary  N    N       N    N     Y       Y    Y       Y        Y    Y       N
//
// LeaderSource X            X    X     X
// RegionSource                                 X    X                X
// LeaderTarget X    X       X    X     X       X                                  X
// RegionTarget X    X       X          X       X            X        X    X

const (
	leaderSource = iota
	regionSource
	leaderTarget
	regionTarget
	scatterRegionTarget
)

func (f *StoreStateFilter) anyConditionMatch(typ int, opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	var funcs []conditionFunc
	switch typ {
	case leaderSource:
		funcs = []conditionFunc{f.isRemoved, f.isDown, f.pauseLeaderTransfer, f.isDisconnected}
	case regionSource:
		funcs = []conditionFunc{f.isBusy, f.exceedRemoveLimit, f.tooManySnapshots}
	case leaderTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.pauseLeaderTransfer,
			f.slowStoreEvicted, f.isDisconnected, f.isBusy, f.hasRejectLeaderProperty}
	case regionTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.isDisconnected, f.isBusy,
			f.exceedAddLimit, f.tooManySnapshots, f.tooManyPendingPeers}
	case scatterRegionTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.isDisconnected, f.isBusy}
	}
	for _, cf := range funcs {
		if status := cf(opt, store); status != statusStoreOK {
			return status
		}
	}
	return statusStoreOK
}

// Source returns true when the store can be selected as the schedule
// source.
func (f *StoreStateFilter) Source(opts *config.PersistOptions, store *core.StoreInfo) (status plan.Status) {
	if f.TransferLeader {
		if status = f.anyConditionMatch(leaderSource, opts, store); status != statusStoreOK {
			return
		}
	}
	if f.MoveRegion {
		if status = f.anyConditionMatch(regionSource, opts, store); status != statusStoreOK {
			return
		}
	}
	return statusStoreOK
}

// Target returns true when the store can be selected as the schedule
// target.
func (f *StoreStateFilter) Target(opts *config.PersistOptions, store *core.StoreInfo) (status plan.Status) {
	if f.TransferLeader {
		if status = f.anyConditionMatch(leaderTarget, opts, store); status != statusStoreOK {
			return
		}
	}
	if f.MoveRegion && f.ScatterRegion {
		if status = f.anyConditionMatch(scatterRegionTarget, opts, store); status != statusStoreOK {
			return
		}
	}
	if f.MoveRegion && !f.ScatterRegion {
		if status = f.anyConditionMatch(regionTarget, opts, store); status != statusStoreOK {
			return
		}
	}
	return statusStoreOK
}

// labelConstraintFilter is a filter that selects stores satisfy the constraints.
type labelConstraintFilter struct {
	scope       string
	constraints []placement.LabelConstraint
}

// NewLabelConstaintFilter creates a filter that selects stores satisfy the constraints.
func NewLabelConstaintFilter(scope string, constraints []placement.LabelConstraint) Filter {
	return labelConstraintFilter{scope: scope, constraints: constraints}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f labelConstraintFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (f labelConstraintFilter) Type() string {
	return "label-constraint-filter"
}

// Source filters stores when select them as schedule source.
func (f labelConstraintFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if placement.MatchLabelConstraints(store, f.constraints) {
		return statusStoreOK
	}
	return statusStoreLabel
}

// Target filters stores when select them as schedule target.
func (f labelConstraintFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if placement.MatchLabelConstraints(store, f.constraints) {
		return statusStoreOK
	}
	return statusStoreLabel
}

type ruleFitFilter struct {
	scope       string
	cluster     *core.BasicCluster
	ruleManager *placement.RuleManager
	region      *core.RegionInfo
	oldFit      *placement.RegionFit
	srcStore    uint64
}

// newRuleFitFilter creates a filter that ensures after replace a peer with new
// one, the isolation level will not decrease. Its function is the same as
// distinctScoreFilter but used when placement rules is enabled.
func newRuleFitFilter(scope string, cluster *core.BasicCluster, ruleManager *placement.RuleManager, region *core.RegionInfo, oldStoreID uint64) Filter {
	return &ruleFitFilter{
		scope:       scope,
		cluster:     cluster,
		ruleManager: ruleManager,
		region:      region,
		oldFit:      ruleManager.FitRegion(cluster, region),
		srcStore:    oldStoreID,
	}
}

func (f *ruleFitFilter) Scope() string {
	return f.scope
}

func (f *ruleFitFilter) Type() string {
	return "rule-fit-filter"
}

func (f *ruleFitFilter) Source(options *config.PersistOptions, store *core.StoreInfo) plan.Status {
	return statusStoreOK
}

func (f *ruleFitFilter) Target(options *config.PersistOptions, store *core.StoreInfo) plan.Status {
	region := createRegionForRuleFit(f.region.GetStartKey(), f.region.GetEndKey(),
		f.region.GetPeers(), f.region.GetLeader(),
		core.WithReplacePeerStore(f.srcStore, store.GetID()))
	newFit := f.ruleManager.FitRegion(f.cluster, region)
	if placement.CompareRegionFit(f.oldFit, newFit) <= 0 {
		return statusStoreOK
	}
	return statusStoreRule
}

// GetSourceStoreID implements the ComparingFilter
func (f *ruleFitFilter) GetSourceStoreID() uint64 {
	return f.srcStore
}

type ruleLeaderFitFilter struct {
	scope            string
	cluster          *core.BasicCluster
	ruleManager      *placement.RuleManager
	region           *core.RegionInfo
	oldFit           *placement.RegionFit
	srcLeaderStoreID uint64
}

// newRuleLeaderFitFilter creates a filter that ensures after transfer leader with new store,
// the isolation level will not decrease.
func newRuleLeaderFitFilter(scope string, cluster *core.BasicCluster, ruleManager *placement.RuleManager, region *core.RegionInfo, srcLeaderStoreID uint64) Filter {
	return &ruleLeaderFitFilter{
		scope:            scope,
		cluster:          cluster,
		ruleManager:      ruleManager,
		region:           region,
		oldFit:           ruleManager.FitRegion(cluster, region),
		srcLeaderStoreID: srcLeaderStoreID,
	}
}

func (f *ruleLeaderFitFilter) Scope() string {
	return f.scope
}

func (f *ruleLeaderFitFilter) Type() string {
	return "rule-fit-leader-filter"
}

func (f *ruleLeaderFitFilter) Source(options *config.PersistOptions, store *core.StoreInfo) plan.Status {
	return statusStoreOK
}

func (f *ruleLeaderFitFilter) Target(options *config.PersistOptions, store *core.StoreInfo) plan.Status {
	targetPeer := f.region.GetStorePeer(store.GetID())
	if targetPeer == nil {
		log.Warn("ruleLeaderFitFilter couldn't find peer on target Store", zap.Uint64("target-store", store.GetID()))
		return statusStoreRule
	}
	copyRegion := createRegionForRuleFit(f.region.GetStartKey(), f.region.GetEndKey(),
		f.region.GetPeers(), f.region.GetLeader(),
		core.WithLeader(targetPeer))
	newFit := f.ruleManager.FitRegion(f.cluster, copyRegion)
	if placement.CompareRegionFit(f.oldFit, newFit) <= 0 {
		return statusStoreOK
	}
	return statusStoreRule
}

func (f *ruleLeaderFitFilter) GetSourceStoreID() uint64 {
	return f.srcLeaderStoreID
}

// NewPlacementSafeguard creates a filter that ensures after replace a peer with new
// peer, the placement restriction will not become worse.
func NewPlacementSafeguard(scope string, opt *config.PersistOptions, cluster *core.BasicCluster, ruleManager *placement.RuleManager, region *core.RegionInfo, sourceStore *core.StoreInfo) Filter {
	if opt.IsPlacementRulesEnabled() {
		return newRuleFitFilter(scope, cluster, ruleManager, region, sourceStore.GetID())
	}
	return NewLocationSafeguard(scope, opt.GetLocationLabels(), cluster.GetRegionStores(region), sourceStore)
}

// NewPlacementLeaderSafeguard creates a filter that ensures after transfer a leader with
// existed peer, the placement restriction will not become worse.
// Note that it only worked when PlacementRules enabled otherwise it will always permit the sourceStore.
func NewPlacementLeaderSafeguard(scope string, opt *config.PersistOptions, cluster *core.BasicCluster, ruleManager *placement.RuleManager, region *core.RegionInfo, sourceStore *core.StoreInfo) Filter {
	if opt.IsPlacementRulesEnabled() {
		return newRuleLeaderFitFilter(scope, cluster, ruleManager, region, sourceStore.GetID())
	}
	return nil
}

type engineFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewEngineFilter creates a filter that only keeps allowedEngines.
func NewEngineFilter(scope string, constraint placement.LabelConstraint) Filter {
	return &engineFilter{
		scope:      scope,
		constraint: constraint,
	}
}

func (f *engineFilter) Scope() string {
	return f.scope
}

func (f *engineFilter) Type() string {
	return "engine-filter"
}

func (f *engineFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if f.constraint.MatchStore(store) {
		return statusStoreOK
	}
	return statusStoreLabel
}

func (f *engineFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if f.constraint.MatchStore(store) {
		return statusStoreOK
	}
	return statusStoreLabel
}

type specialUseFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewSpecialUseFilter creates a filter that filters out normal stores.
// By default, all stores that are not marked with a special use will be filtered out.
// Specify the special use label if you want to include the special stores.
func NewSpecialUseFilter(scope string, allowUses ...string) Filter {
	var values []string
	for _, v := range allSpecialUses {
		if slice.NoneOf(allowUses, func(i int) bool { return allowUses[i] == v }) {
			values = append(values, v)
		}
	}
	return &specialUseFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: SpecialUseKey, Op: placement.In, Values: values},
	}
}

func (f *specialUseFilter) Scope() string {
	return f.scope
}

func (f *specialUseFilter) Type() string {
	return "special-use-filter"
}

func (f *specialUseFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if store.IsLowSpace(opt.GetLowSpaceRatio()) || !f.constraint.MatchStore(store) {
		return statusStoreOK
	}
	return statusStoreLabel
}

func (f *specialUseFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	if !f.constraint.MatchStore(store) {
		return statusStoreOK
	}
	return statusStoreLabel
}

const (
	// SpecialUseKey is the label used to indicate special use storage.
	SpecialUseKey = "specialUse"
	// SpecialUseHotRegion is the hot region value of special use label
	SpecialUseHotRegion = "hotRegion"
	// SpecialUseReserved is the reserved value of special use label
	SpecialUseReserved = "reserved"
)

var (
	allSpecialUses    = []string{SpecialUseHotRegion, SpecialUseReserved}
	allSpecialEngines = []string{core.EngineTiFlash}
	// NotSpecialEngines is used to filter the special engine.
	NotSpecialEngines = placement.LabelConstraint{Key: core.EngineKey, Op: placement.NotIn, Values: allSpecialEngines}
)

type isolationFilter struct {
	scope          string
	locationLabels []string
	constraintSet  [][]string
}

// NewIsolationFilter creates a filter that filters out stores with isolationLevel
// For example, a region has 3 replicas in z1, z2 and z3 individually.
// With isolationLevel = zone, if the region on z1 is down, we need to filter out z2 and z3
// because these two zones already have one of the region's replicas on them.
// We need to choose a store on z1 or z4 to place the new replica to meet the isolationLevel explicitly and forcibly.
func NewIsolationFilter(scope, isolationLevel string, locationLabels []string, regionStores []*core.StoreInfo) Filter {
	isolationFilter := &isolationFilter{
		scope:          scope,
		locationLabels: locationLabels,
		constraintSet:  make([][]string, 0),
	}
	// Get which idx this isolationLevel at according to locationLabels
	var isolationLevelIdx int
	for level, label := range locationLabels {
		if label == isolationLevel {
			isolationLevelIdx = level
			break
		}
	}
	// Collect all constraints for given isolationLevel
	for _, regionStore := range regionStores {
		var constraintList []string
		for i := 0; i <= isolationLevelIdx; i++ {
			constraintList = append(constraintList, regionStore.GetLabelValue(locationLabels[i]))
		}
		isolationFilter.constraintSet = append(isolationFilter.constraintSet, constraintList)
	}
	return isolationFilter
}

func (f *isolationFilter) Scope() string {
	return f.scope
}

func (f *isolationFilter) Type() string {
	return "isolation-filter"
}

func (f *isolationFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	return statusStoreOK
}

func (f *isolationFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	// No isolation constraint to fit
	if len(f.constraintSet) == 0 {
		return statusStoreIsolation
	}
	for _, constrainList := range f.constraintSet {
		match := true
		for idx, constraint := range constrainList {
			// Check every constraint in constrainList
			match = store.GetLabelValue(f.locationLabels[idx]) == constraint && match
		}
		if len(constrainList) > 0 && match {
			return statusStoreIsolation
		}
	}
	return statusStoreOK
}

// createRegionForRuleFit is used to create a clone region with RegionCreateOptions which is only used for
// FitRegion in filter
func createRegionForRuleFit(startKey, endKey []byte,
	peers []*metapb.Peer, leader *metapb.Peer, opts ...core.RegionCreateOption) *core.RegionInfo {
	copyLeader := proto.Clone(leader).(*metapb.Peer)
	copyPeers := make([]*metapb.Peer, 0, len(peers))
	for _, p := range peers {
		peer := &metapb.Peer{
			Id:      p.Id,
			StoreId: p.StoreId,
			Role:    p.Role,
		}
		copyPeers = append(copyPeers, peer)
	}
	cloneRegion := core.NewRegionInfo(&metapb.Region{
		StartKey: startKey,
		EndKey:   endKey,
		Peers:    copyPeers,
	}, copyLeader, opts...)
	return cloneRegion
}

// RegionScoreFilter filter target store that it's score must higher than the given score
type RegionScoreFilter struct {
	scope string
	score float64
}

// NewRegionScoreFilter creates a Filter that filters all high score stores.
func NewRegionScoreFilter(scope string, source *core.StoreInfo, opt *config.PersistOptions) Filter {
	return &RegionScoreFilter{
		scope: scope,
		score: source.RegionScore(opt.GetRegionScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0),
	}
}

// Scope scopes only for balance region
func (f *RegionScoreFilter) Scope() string {
	return f.scope
}

// Type types region score filter
func (f *RegionScoreFilter) Type() string {
	return "region-score-filter"
}

// Source ignore source
func (f *RegionScoreFilter) Source(opt *config.PersistOptions, _ *core.StoreInfo) plan.Status {
	return statusStoreOK
}

// Target return true if target's score less than source's score
func (f *RegionScoreFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) plan.Status {
	score := store.RegionScore(opt.GetRegionScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0)
	if score < f.score {
		return statusStoreOK
	}
	return statusStoreNoNeed
}
