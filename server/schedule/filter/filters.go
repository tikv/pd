// Copyright 2016 PingCAP, Inc.
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

package filter

import (
	"fmt"

	"github.com/pingcap/pd/v4/pkg/slice"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/pd/v4/server/schedule/storelimit"
)

// revive:disable:unused-parameter

// SelectSourceStores selects stores that be selected as source store from the list.
func SelectSourceStores(stores []*core.StoreInfo, filters []Filter, opt opt.Options) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool { return filters[i].Source(opt, s) })
	})
}

// SelectTargetStores selects stores that be selected as target store from the list.
func SelectTargetStores(stores []*core.StoreInfo, filters []Filter, opt opt.Options) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool { return filters[i].Target(opt, s) })
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
	Source(opt opt.Options, store *core.StoreInfo) bool
	// Return true if the store can be used as a target store.
	Target(opt opt.Options, store *core.StoreInfo) bool
}

// Source checks if store can pass all Filters as source store.
func Source(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if !filter.Source(opt, store) {
			filterCounter.WithLabelValues("filter-source", storeAddress, storeID, filter.Scope(), filter.Type()).Inc()
			return false
		}
	}
	return true
}

// Target checks if store can pass all Filters as target store.
func Target(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if !filter.Target(opt, store) {
			filterCounter.WithLabelValues("filter-target", storeAddress, storeID, filter.Scope(), filter.Type()).Inc()
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

func (f *excludedFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return !ok
}

func (f *excludedFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return !ok
}

type storeLimitFilter struct{ scope string }

// NewStoreLimitFilter creates a Filter that filters all stores those exceed the limit of a store.
func NewStoreLimitFilter(scope string) Filter {
	return &storeLimitFilter{scope: scope}
}

func (f *storeLimitFilter) Scope() string {
	return f.scope
}

func (f *storeLimitFilter) Type() string {
	return "store-limit-filter"
}

func (f *storeLimitFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsAvailable(storelimit.RemovePeer)
}

func (f *storeLimitFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsAvailable(storelimit.AddPeer)
}

type stateFilter struct{ scope string }

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter(scope string) Filter {
	return &stateFilter{scope: scope}
}

func (f *stateFilter) Scope() string {
	return f.scope
}

func (f *stateFilter) Type() string {
	return "state-filter"
}

func (f *stateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsTombstone()
}

func (f *stateFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsUp()
}

type healthFilter struct{ scope string }

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter(scope string) Filter {
	return &healthFilter{scope: scope}
}

func (f *healthFilter) Scope() string {
	return f.scope
}

func (f *healthFilter) Type() string {
	return "health-filter"
}

func (f *healthFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsBusy() {
		return false
	}
	return store.DownTime() <= opt.GetMaxStoreDownTime()
}

func (f *healthFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *healthFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type pendingPeerCountFilter struct{ scope string }

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pending peers.
func NewPendingPeerCountFilter(scope string) Filter {
	return &pendingPeerCountFilter{scope: scope}
}

func (p *pendingPeerCountFilter) Scope() string {
	return p.scope
}

func (p *pendingPeerCountFilter) Type() string {
	return "pending-peer-filter"
}

func (p *pendingPeerCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if opt.GetMaxPendingPeerCount() == 0 {
		return true
	}
	return store.GetPendingPeerCount() <= int(opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

func (p *pendingPeerCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

type snapshotCountFilter struct{ scope string }

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter(scope string) Filter {
	return &snapshotCountFilter{scope: scope}
}

func (f *snapshotCountFilter) Scope() string {
	return f.scope
}

func (f *snapshotCountFilter) Type() string {
	return "snapshot-filter"
}

func (f *snapshotCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	return uint64(store.GetSendingSnapCount()) <= opt.GetMaxSnapshotCount() &&
		uint64(store.GetReceivingSnapCount()) <= opt.GetMaxSnapshotCount() &&
		uint64(store.GetApplyingSnapCount()) <= opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *snapshotCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
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

func (f *storageThresholdFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *storageThresholdFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	scope     string
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(scope string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
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
	}
}

func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *distinctScoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return core.DistinctScore(f.labels, f.stores, store) >= f.safeScore
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	ActionScope string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f StoreStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// Source returns true when the store can be selected as the schedule
// source.
func (f StoreStateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return false
	}
	if f.TransferLeader && (store.IsDisconnected() || store.IsBlocked()) {
		return false
	}

	if f.MoveRegion && !f.filterMoveRegion(opt, true, store) {
		return false
	}
	return true
}

// Target returns true when the store can be selected as the schedule
// target.
func (f StoreStateFilter) Target(opts opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.IsOffline() ||
		store.DownTime() > opts.GetMaxStoreDownTime() {
		return false
	}
	if f.TransferLeader &&
		(store.IsDisconnected() ||
			store.IsBlocked() ||
			store.IsBusy() ||
			opts.CheckLabelProperty(opt.RejectLeader, store.GetLabels())) {
		return false
	}

	if f.MoveRegion {
		// only target consider the pending peers because pending more means the disk is slower.
		if opts.GetMaxPendingPeerCount() > 0 && store.GetPendingPeerCount() > int(opts.GetMaxPendingPeerCount()) {
			return false
		}

		if !f.filterMoveRegion(opts, false, store) {
			return false
		}
	}
	return true
}

func (f StoreStateFilter) filterMoveRegion(opt opt.Options, isSource bool, store *core.StoreInfo) bool {
	if store.IsBusy() {
		return false
	}

	if (isSource && !store.IsAvailable(storelimit.RemovePeer)) || (!isSource && !store.IsAvailable(storelimit.AddPeer)) {
		return false
	}

	if uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount() {
		return false
	}
	return true
}

// BlacklistType the type of BlackListStore Filter.
type BlacklistType int

// some flags about blacklist type.
const (
	// blacklist associated with the source.
	BlacklistSource BlacklistType = 1 << iota
	// blacklist associated with the target.
	BlacklistTarget
)

// BlacklistStoreFilter filters the store according to the blacklist.
type BlacklistStoreFilter struct {
	scope     string
	blacklist map[uint64]struct{}
	flag      BlacklistType
}

// NewBlacklistStoreFilter creates a blacklist filter.
func NewBlacklistStoreFilter(scope string, typ BlacklistType) *BlacklistStoreFilter {
	return &BlacklistStoreFilter{
		scope:     scope,
		blacklist: make(map[uint64]struct{}),
		flag:      typ,
	}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *BlacklistStoreFilter) Scope() string {
	return f.scope
}

// Type implements the Filter.
func (f *BlacklistStoreFilter) Type() string {
	return "blacklist-store-filter"
}

// Source implements the Filter.
func (f *BlacklistStoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistSource != BlacklistSource {
		return true
	}
	return f.filter(store)
}

// Add adds the store to the blacklist.
func (f *BlacklistStoreFilter) Add(storeID uint64) {
	f.blacklist[storeID] = struct{}{}
}

// Target implements the Filter.
func (f *BlacklistStoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistTarget != BlacklistTarget {
		return true
	}
	return f.filter(store)
}

func (f *BlacklistStoreFilter) filter(store *core.StoreInfo) bool {
	_, ok := f.blacklist[store.GetID()]
	return !ok
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
func (f labelConstraintFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return placement.MatchLabelConstraints(store, f.constraints)
}

// Target filters stores when select them as schedule target.
func (f labelConstraintFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return placement.MatchLabelConstraints(store, f.constraints)
}

// RegionFitter is the interface that can fit a region against placement rules.
type RegionFitter interface {
	FitRegion(*core.RegionInfo) *placement.RegionFit
}

type ruleFitFilter struct {
	scope    string
	fitter   RegionFitter
	region   *core.RegionInfo
	oldFit   *placement.RegionFit
	oldStore uint64
}

// NewRuleFitFilter creates a filter that ensures after replace a peer with new
// one, the isolation level will not decrease. Its function is the same as
// distinctScoreFilter but used when placement rules is enabled.
func NewRuleFitFilter(scope string, fitter RegionFitter, region *core.RegionInfo, oldStoreID uint64) Filter {
	return &ruleFitFilter{
		scope:    scope,
		fitter:   fitter,
		region:   region,
		oldFit:   fitter.FitRegion(region),
		oldStore: oldStoreID,
	}
}

func (f *ruleFitFilter) Scope() string {
	return f.scope
}

func (f *ruleFitFilter) Type() string {
	return "rule-fit-filter"
}

func (f *ruleFitFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *ruleFitFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	region := f.region.Clone(core.WithReplacePeerStore(f.oldStore, store.GetID()))
	newFit := f.fitter.FitRegion(region)
	return placement.CompareRegionFit(f.oldFit, newFit) <= 0
}

type ruleLeaderFitFilter struct {
	scope            string
	fitter           RegionFitter
	region           *core.RegionInfo
	oldFit           *placement.RegionFit
	oldLeaderStoreID uint64
}

// newRuleLeaderFitFilter creates a filter that ensures after transfer leader with new store,
// the isolation level will not decrease.
func newRuleLeaderFitFilter(scope string, fitter RegionFitter, region *core.RegionInfo, oldLeaderStoreID uint64) Filter {
	return &ruleLeaderFitFilter{
		scope:            scope,
		fitter:           fitter,
		region:           region,
		oldFit:           fitter.FitRegion(region),
		oldLeaderStoreID: oldLeaderStoreID,
	}
}

func (f *ruleLeaderFitFilter) Scope() string {
	return f.scope
}

func (f *ruleLeaderFitFilter) Type() string {
	return "rule-fit-leader-filter"
}

func (f *ruleLeaderFitFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return true
}

func (f *ruleLeaderFitFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	oldLeaderPeerReplica := *f.region.GetLeader()
	oldLeaderPeerReplica.StoreId = store.GetID()
	region := f.region.Clone(core.WithReplacePeerStore(f.oldLeaderStoreID, store.GetID()), core.WithLeader(&oldLeaderPeerReplica))
	newFit := f.fitter.FitRegion(region)
	return placement.CompareRegionFit(f.oldFit, newFit) <= 0
}

// NewPlacementLeaderSafeguard creates a filter that ensures after transfer a leader with
// existed peer, the placement restriction will not become worse.
// Note that it only worked when PlacementRules enabled otherwise it will always permit the sourceStore.
func NewPlacementLeaderSafeguard(scope string, cluster opt.Cluster, region *core.RegionInfo, sourceStore *core.StoreInfo) Filter {
	if cluster.IsPlacementRulesEnabled() {
		return newRuleLeaderFitFilter(scope, cluster, region, sourceStore.GetID())
	}
	return nil
}

type engineFilter struct {
	scope      string
	constraint placement.LabelConstraint
}

// NewEngineFilter creates a filter that filters out default engine stores.
// By default, all stores that are not marked with a special engine will be filtered out.
// Specify the special engine label if you want to include the special stores.
func NewEngineFilter(scope string, allowEngines ...string) Filter {
	var values []string
	for _, v := range allSpeicalEngines {
		if slice.NoneOf(allowEngines, func(i int) bool { return allowEngines[i] == v }) {
			values = append(values, v)
		}
	}
	return &engineFilter{
		scope:      scope,
		constraint: placement.LabelConstraint{Key: "engine", Op: "notIn", Values: values},
	}
}

func (f *engineFilter) Scope() string {
	return f.scope
}

func (f *engineFilter) Type() string {
	return "engine-filter"
}

func (f *engineFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.constraint.MatchStore(store)
}

func (f *engineFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.constraint.MatchStore(store)
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
		constraint: placement.LabelConstraint{Key: SpecialUseKey, Op: "in", Values: values},
	}
}

func (f *specialUseFilter) Scope() string {
	return f.scope
}

func (f *specialUseFilter) Type() string {
	return "special-use-filter"
}

func (f *specialUseFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsLowSpace(opt.GetLowSpaceRatio()) {
		return true
	}
	return !f.constraint.MatchStore(store)
}

func (f *specialUseFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !f.constraint.MatchStore(store)
}

const (
	// SpecialUseKey is the label used to indicate special use storage.
	SpecialUseKey = "specialUse"
	// SpecialUseHotRegion is the hot region value of special use label
	SpecialUseHotRegion = "hotRegion"
	// SpecialUseReserved is the reserved value of special use label
	SpecialUseReserved = "reserved"

	// EngineKey is the label key used to indicate engine.
	EngineKey = "engine"
	// EngineTiFlash is the tiflash value of the engine label.
	EngineTiFlash = "tiflash"
)

var allSpecialUses = []string{SpecialUseHotRegion, SpecialUseReserved}
var allSpeicalEngines = []string{EngineTiFlash}
