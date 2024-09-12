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
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

// SelectSourceStores selects stores that be selected as source store from the list.
func SelectSourceStores(stores []*core.StoreInfo, filters []Filter, conf config.SharedConfigProvider, collector *plan.Collector,
	counter *Counter) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool {
			status := filters[i].Source(conf, s)
			if !status.IsOK() {
				if counter != nil {
					counter.inc(source, filters[i].Type(), s.GetID(), 0)
				} else {
					sourceID := strconv.FormatUint(s.GetID(), 10)
					// TODO: pre-allocate gauge metrics
					filterCounter.WithLabelValues(source.String(), filters[i].Scope(), filters[i].Type().String(), sourceID, "").Inc()
				}
				if collector != nil {
					collector.Collect(plan.SetResource(s), plan.SetStatus(status))
				}
				return false
			}
			return true
		})
	})
}

// SelectUnavailableTargetStores selects unavailable stores that can't be selected as target store from the list.
func SelectUnavailableTargetStores(stores []*core.StoreInfo, filters []Filter, conf config.SharedConfigProvider,
	collector *plan.Collector, counter *Counter) []*core.StoreInfo {
	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		targetID := strconv.FormatUint(s.GetID(), 10)
		return slice.AnyOf(filters, func(i int) bool {
			status := filters[i].Target(conf, s)
			if !status.IsOK() {
				cfilter, ok := filters[i].(comparingFilter)
				sourceID := uint64(0)
				if ok {
					sourceID = cfilter.getSourceStoreID()
				}
				if counter != nil {
					counter.inc(target, filters[i].Type(), sourceID, s.GetID())
				} else {
					filterCounter.WithLabelValues(target.String(), filters[i].Scope(), filters[i].Type().String(),
						strconv.FormatUint(sourceID, 10), targetID).Inc()
				}

				if collector != nil {
					collector.Collect(plan.SetResourceWithStep(s, 2), plan.SetStatus(status))
				}
				return true
			}
			return false
		})
	})
}

// SelectTargetStores selects stores that be selected as target store from the list.
func SelectTargetStores(stores []*core.StoreInfo, filters []Filter, conf config.SharedConfigProvider, collector *plan.Collector,
	counter *Counter) []*core.StoreInfo {
	if len(filters) == 0 {
		return stores
	}

	return filterStoresBy(stores, func(s *core.StoreInfo) bool {
		return slice.AllOf(filters, func(i int) bool {
			filter := filters[i]
			status := filter.Target(conf, s)
			if !status.IsOK() {
				cfilter, ok := filter.(comparingFilter)
				sourceID := uint64(0)
				if ok {
					sourceID = cfilter.getSourceStoreID()
				}
				if counter != nil {
					counter.inc(target, filter.Type(), sourceID, s.GetID())
				} else {
					targetIDStr := strconv.FormatUint(s.GetID(), 10)
					sourceIDStr := strconv.FormatUint(sourceID, 10)
					filterCounter.WithLabelValues(target.String(), filter.Scope(), filter.Type().String(), sourceIDStr, targetIDStr).Inc()
				}
				if collector != nil {
					collector.Collect(plan.SetResource(s), plan.SetStatus(status))
				}
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
	Type() filterType
	// Source Return plan.Status to show whether be filtered as source
	Source(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status
	// Target Return plan.Status to show whether be filtered as target
	Target(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status
}

// comparingFilter is an interface to filter target store by comparing source and target stores
type comparingFilter interface {
	Filter
	// getSourceStoreID returns the source store when comparing.
	getSourceStoreID() uint64
}

// Target checks if store can pass all Filters as target store.
func Target(conf config.SharedConfigProvider, store *core.StoreInfo, filters []Filter) bool {
	storeID := strconv.FormatUint(store.GetID(), 10)
	for _, filter := range filters {
		status := filter.Target(conf, store)
		if !status.IsOK() {
			if status != statusStoreRemoved {
				cfilter, ok := filter.(comparingFilter)
				targetID := storeID
				sourceID := ""
				if ok {
					sourceID = strconv.FormatUint(cfilter.getSourceStoreID(), 10)
				}
				filterCounter.WithLabelValues(target.String(), filter.Scope(), filter.Type().String(), sourceID, targetID).Inc()
			}
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

// Scope returns the scheduler or the checker which the filter acts on.
func (f *excludedFilter) Scope() string {
	return f.scope
}

// Type returns the type of the filter.
func (*excludedFilter) Type() filterType {
	return excluded
}

// Source filters stores when select them as schedule source.
func (f *excludedFilter) Source(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if _, ok := f.sources[store.GetID()]; ok {
		return statusStoreAlreadyHasPeer
	}
	return statusOK
}

// Target filters stores when select them as schedule target.
func (f *excludedFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if _, ok := f.targets[store.GetID()]; ok {
		return statusStoreAlreadyHasPeer
	}
	return statusOK
}

type storageThresholdFilter struct{ scope string }

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter(scope string) Filter {
	return &storageThresholdFilter{scope: scope}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *storageThresholdFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*storageThresholdFilter) Type() filterType {
	return storageThreshold
}

// Source filters stores when select them as schedule source.
func (*storageThresholdFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores when select them as schedule target.
func (*storageThresholdFilter) Target(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !store.IsLowSpace(conf.GetLowSpaceRatio()) {
		return statusOK
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

// Scope returns the scheduler or the checker which the filter acts on.
func (f *distinctScoreFilter) Scope() string {
	return f.scope
}

// Type returns the type of the filter.
func (*distinctScoreFilter) Type() filterType {
	return distinctScore
}

// Source filters stores when select them as schedule source.
func (*distinctScoreFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores when select them as schedule target.
func (f *distinctScoreFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	score := core.DistinctScore(f.labels, f.stores, store)
	switch f.policy {
	case locationSafeguard:
		if score >= f.safeScore {
			return statusOK
		}
	case locationImprove:
		if score > f.safeScore {
			return statusOK
		}
	default:
	}
	return statusStoreNotMatchIsolation
}

// getSourceStoreID implements the ComparingFilter
func (f *distinctScoreFilter) getSourceStoreID() uint64 {
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
	// Set true if allows failover (through witness)
	AllowFastFailover bool
	// Set true if allows temporary states.
	AllowTemporaryStates bool
	// Set the priority level of the filter, it should be same with the operator level.
	// The priority level can be higher than the operator level in checker,
	// the operator controller should check it again by using the actual operator level.
	// If it checks failed, the operator will be put back to the waiting queue util the limit is available.
	// But the scheduler should keep the same with the operator level.
	OperatorLevel constant.PriorityLevel
	// Reason is used to distinguish the reason of store state filter
	Reason filterType
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *StoreStateFilter) Scope() string {
	return f.ActionScope
}

// Type returns the type of the Filter.
func (f *StoreStateFilter) Type() filterType {
	return f.Reason
}

// conditionFunc defines condition to determine a store should be selected.
// It should consider if the filter allows temporary states.
type conditionFunc func(config.SharedConfigProvider, *core.StoreInfo) *plan.Status

func (f *StoreStateFilter) isRemoved(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.IsRemoved() {
		f.Reason = storeStateTombstone
		return statusStoreRemoved
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) isDown(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.DownTime() > conf.GetMaxStoreDownTime() {
		f.Reason = storeStateDown
		return statusStoreDown
	}
	f.Reason = storeStateOK

	return statusOK
}

func (f *StoreStateFilter) isRemoving(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.IsRemoving() {
		f.Reason = storeStateOffline
		return statusStoresRemoving
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) pauseLeaderTransfer(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !store.AllowLeaderTransfer() {
		f.Reason = storeStatePauseLeader
		return statusStoreRejectLeader
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) slowStoreEvicted(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.EvictedAsSlowStore() {
		f.Reason = storeStateSlow
		return statusStoreRejectLeader
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) slowTrendEvicted(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.IsEvictedAsSlowTrend() {
		f.Reason = storeStateSlowTrend
		return statusStoreRejectLeader
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) isDisconnected(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.AllowTemporaryStates && store.IsDisconnected() {
		f.Reason = storeStateDisconnected
		return statusStoreDisconnected
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) isBusy(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.AllowTemporaryStates && store.IsBusy() {
		f.Reason = storeStateBusy
		return statusStoreBusy
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) exceedRemoveLimit(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.AllowTemporaryStates && !store.IsAvailable(storelimit.RemovePeer, f.OperatorLevel) {
		f.Reason = storeStateExceedRemoveLimit
		return statusStoreRemoveLimit
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) exceedAddLimit(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.AllowTemporaryStates && !store.IsAvailable(storelimit.AddPeer, f.OperatorLevel) {
		f.Reason = storeStateExceedAddLimit
		return statusStoreAddLimit
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) tooManySnapshots(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.AllowTemporaryStates && (uint64(store.GetSendingSnapCount()) > conf.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > conf.GetMaxSnapshotCount()) {
		f.Reason = storeStateTooManySnapshot
		return statusStoreSnapshotThrottled
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) tooManyPendingPeers(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.AllowTemporaryStates &&
		conf.GetMaxPendingPeerCount() > 0 &&
		store.GetPendingPeerCount() > int(conf.GetMaxPendingPeerCount()) {
		f.Reason = storeStateTooManyPendingPeer
		return statusStorePendingPeerThrottled
	}
	f.Reason = storeStateOK
	return statusOK
}

func (f *StoreStateFilter) hasRejectLeaderProperty(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if conf.CheckLabelProperty(config.RejectLeader, store.GetLabels()) {
		f.Reason = storeStateRejectLeader
		return statusStoreRejectLeader
	}
	f.Reason = storeStateOK
	return statusOK
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
	witnessSource
	leaderTarget
	regionTarget
	witnessTarget
	scatterRegionTarget
	fastFailoverTarget
)

func (f *StoreStateFilter) anyConditionMatch(typ int, conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	var funcs []conditionFunc
	switch typ {
	case leaderSource:
		funcs = []conditionFunc{f.isRemoved, f.isDown, f.pauseLeaderTransfer, f.isDisconnected}
	case regionSource:
		funcs = []conditionFunc{f.isBusy, f.exceedRemoveLimit, f.tooManySnapshots}
	case witnessSource:
		funcs = []conditionFunc{f.isBusy}
	case leaderTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.pauseLeaderTransfer,
			f.slowStoreEvicted, f.slowTrendEvicted, f.isDisconnected, f.isBusy, f.hasRejectLeaderProperty}
	case regionTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.isDisconnected, f.isBusy,
			f.exceedAddLimit, f.tooManySnapshots, f.tooManyPendingPeers}
	case witnessTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.isDisconnected, f.isBusy}
	case scatterRegionTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.isDisconnected, f.isBusy}
	case fastFailoverTarget:
		funcs = []conditionFunc{f.isRemoved, f.isRemoving, f.isDown, f.isDisconnected, f.isBusy}
	}
	for _, cf := range funcs {
		if status := cf(conf, store); !status.IsOK() {
			return status
		}
	}
	return statusOK
}

// Source returns true when the store can be selected as the schedule
// source.
func (f *StoreStateFilter) Source(conf config.SharedConfigProvider, store *core.StoreInfo) (status *plan.Status) {
	if f.TransferLeader {
		if status = f.anyConditionMatch(leaderSource, conf, store); !status.IsOK() {
			return
		}
	}
	if f.MoveRegion {
		if status = f.anyConditionMatch(regionSource, conf, store); !status.IsOK() {
			return
		}
	}

	return statusOK
}

// Target returns true when the store can be selected as the schedule
// target.
func (f *StoreStateFilter) Target(conf config.SharedConfigProvider, store *core.StoreInfo) (status *plan.Status) {
	if f.TransferLeader {
		if status = f.anyConditionMatch(leaderTarget, conf, store); !status.IsOK() {
			return
		}
	}
	if f.MoveRegion && f.AllowFastFailover {
		return f.anyConditionMatch(fastFailoverTarget, conf, store)
	}
	if f.MoveRegion && f.ScatterRegion {
		if status = f.anyConditionMatch(scatterRegionTarget, conf, store); !status.IsOK() {
			return
		}
	}
	if f.MoveRegion && !f.ScatterRegion {
		if status = f.anyConditionMatch(regionTarget, conf, store); !status.IsOK() {
			return
		}
	}
	return statusOK
}

// labelConstraintFilter is a filter that selects stores satisfy the constraints.
type labelConstraintFilter struct {
	scope       string
	constraints []placement.LabelConstraint
}

// NewLabelConstraintFilter creates a filter that selects stores satisfy the constraints.
func NewLabelConstraintFilter(scope string, constraints []placement.LabelConstraint) Filter {
	return labelConstraintFilter{scope: scope, constraints: constraints}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f labelConstraintFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (labelConstraintFilter) Type() filterType {
	return labelConstraint
}

// Source filters stores when select them as schedule source.
func (f labelConstraintFilter) Source(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if placement.MatchLabelConstraints(store, f.constraints) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

// Target filters stores when select them as schedule target.
func (f labelConstraintFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if placement.MatchLabelConstraints(store, f.constraints) {
		return statusOK
	}
	return statusStoreNotMatchRule
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
func newRuleFitFilter(scope string, cluster *core.BasicCluster, ruleManager *placement.RuleManager,
	region *core.RegionInfo, oldFit *placement.RegionFit, oldStoreID uint64) Filter {
	if oldFit == nil {
		oldFit = ruleManager.FitRegion(cluster, region)
	}
	return &ruleFitFilter{
		scope:       scope,
		cluster:     cluster,
		ruleManager: ruleManager,
		region:      region,
		oldFit:      oldFit,
		srcStore:    oldStoreID,
	}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *ruleFitFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*ruleFitFilter) Type() filterType {
	return ruleFit
}

// Source filters stores when select them as schedule source.
func (*ruleFitFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores when select them as schedule target.
// It ensures after replace a peer with new one, the isolation level will not decrease and
// the replaced store can match the source rule.
// RegionA:[1,2,3], move peer1 --> peer2 will not allow, because it's count not match the rule.
// but transfer role peer1 --> peer2, it will support.
func (f *ruleFitFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if f.oldFit.Replace(f.srcStore, store) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

// getSourceStoreID implements the ComparingFilter
func (f *ruleFitFilter) getSourceStoreID() uint64 {
	return f.srcStore
}

type ruleLeaderFitFilter struct {
	scope            string
	cluster          *core.BasicCluster
	ruleManager      *placement.RuleManager
	region           *core.RegionInfo
	oldFit           *placement.RegionFit
	srcLeaderStoreID uint64
	allowMoveLeader  bool
}

// newRuleLeaderFitFilter creates a filter that ensures after transfer leader with new store,
// the isolation level will not decrease.
func newRuleLeaderFitFilter(scope string, cluster *core.BasicCluster, ruleManager *placement.RuleManager, region *core.RegionInfo, srcLeaderStoreID uint64, allowMoveLeader bool) Filter {
	return &ruleLeaderFitFilter{
		scope:            scope,
		cluster:          cluster,
		ruleManager:      ruleManager,
		region:           region,
		oldFit:           ruleManager.FitRegion(cluster, region),
		srcLeaderStoreID: srcLeaderStoreID,
		allowMoveLeader:  allowMoveLeader,
	}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *ruleLeaderFitFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*ruleLeaderFitFilter) Type() filterType {
	return ruleLeader
}

// Source filters stores when select them as schedule source.
func (*ruleLeaderFitFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores when select them as schedule target.
func (f *ruleLeaderFitFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	targetStoreID := store.GetID()
	targetPeer := f.region.GetStorePeer(targetStoreID)
	if targetPeer == nil && !f.allowMoveLeader {
		log.Warn("ruleLeaderFitFilter couldn't find peer on target Store", zap.Uint64("target-store", store.GetID()))
		return statusStoreNotMatchRule
	}
	if targetPeer != nil && targetPeer.IsWitness {
		return statusStoreNotMatchRule
	}
	if f.oldFit.Replace(f.srcLeaderStoreID, store) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

func (f *ruleLeaderFitFilter) getSourceStoreID() uint64 {
	return f.srcLeaderStoreID
}

type ruleWitnessFitFilter struct {
	scope       string
	cluster     *core.BasicCluster
	ruleManager *placement.RuleManager
	region      *core.RegionInfo
	oldFit      *placement.RegionFit
	srcStore    uint64
}

func newRuleWitnessFitFilter(scope string, cluster *core.BasicCluster, ruleManager *placement.RuleManager,
	region *core.RegionInfo, oldFit *placement.RegionFit, oldStoreID uint64) Filter {
	if oldFit == nil {
		oldFit = ruleManager.FitRegion(cluster, region)
	}
	return &ruleWitnessFitFilter{
		scope:       scope,
		cluster:     cluster,
		ruleManager: ruleManager,
		region:      region,
		oldFit:      oldFit,
		srcStore:    oldStoreID,
	}
}

// Scope returns the scheduler or the checker which the filter acts on.
func (f *ruleWitnessFitFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*ruleWitnessFitFilter) Type() filterType {
	return ruleFit
}

// Source filters stores when select them as schedule source.
func (*ruleWitnessFitFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores when select them as schedule target.
func (f *ruleWitnessFitFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	targetStoreID := store.GetID()
	targetPeer := f.region.GetStorePeer(targetStoreID)
	if targetPeer == nil {
		log.Warn("ruleWitnessFitFilter couldn't find peer on target Store", zap.Uint64("target-store", store.GetID()))
		return statusStoreNotMatchRule
	}
	if targetPeer.Id == f.region.GetLeader().GetId() {
		return statusStoreNotMatchRule
	}
	if f.oldFit.Replace(f.srcStore, store) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

// NewPlacementSafeguard creates a filter that ensures after replace a peer with new
// peer, the placement restriction will not become worse.
func NewPlacementSafeguard(scope string, conf config.SharedConfigProvider, cluster *core.BasicCluster, ruleManager *placement.RuleManager,
	region *core.RegionInfo, sourceStore *core.StoreInfo, oldFit *placement.RegionFit) Filter {
	if conf.IsPlacementRulesEnabled() {
		return newRuleFitFilter(scope, cluster, ruleManager, region, oldFit, sourceStore.GetID())
	}
	return NewLocationSafeguard(scope, conf.GetLocationLabels(), cluster.GetRegionStores(region), sourceStore)
}

// NewPlacementLeaderSafeguard creates a filter that ensures after transfer a leader with
// existed peer, the placement restriction will not become worse.
// Note that it only worked when PlacementRules enabled otherwise it will always permit the sourceStore.
func NewPlacementLeaderSafeguard(scope string, conf config.SharedConfigProvider, cluster *core.BasicCluster, ruleManager *placement.RuleManager, region *core.RegionInfo, sourceStore *core.StoreInfo, allowMoveLeader bool) Filter {
	if conf.IsPlacementRulesEnabled() {
		return newRuleLeaderFitFilter(scope, cluster, ruleManager, region, sourceStore.GetID(), allowMoveLeader)
	}
	return nil
}

// NewPlacementWitnessSafeguard creates a filter that ensures after transfer a witness with
// existed peer, the placement restriction will not become worse.
// Note that it only worked when PlacementRules enabled otherwise it will always permit the sourceStore.
func NewPlacementWitnessSafeguard(scope string, conf config.SharedConfigProvider, cluster *core.BasicCluster, ruleManager *placement.RuleManager,
	region *core.RegionInfo, sourceStore *core.StoreInfo, oldFit *placement.RegionFit) Filter {
	if conf.IsPlacementRulesEnabled() {
		return newRuleWitnessFitFilter(scope, cluster, ruleManager, region, oldFit, sourceStore.GetID())
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

// Scope returns the scheduler or the checker which the filter acts on.
func (f *engineFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*engineFilter) Type() filterType {
	return engine
}

// Source filters stores when select them as schedule source.
func (f *engineFilter) Source(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if f.constraint.MatchStore(store) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

// Target filters stores when select them as schedule target.
func (f *engineFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if f.constraint.MatchStore(store) {
		return statusOK
	}
	return statusStoreNotMatchRule
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

// Scope returns the scheduler or the checker which the filter acts on.
func (f *specialUseFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*specialUseFilter) Type() filterType {
	return specialUse
}

// Source filters stores when select them as schedule source.
func (f *specialUseFilter) Source(conf config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.IsLowSpace(conf.GetLowSpaceRatio()) || !f.constraint.MatchStore(store) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

// Target filters stores when select them as schedule target.
func (f *specialUseFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if !f.constraint.MatchStore(store) {
		return statusOK
	}
	return statusStoreNotMatchRule
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

// Scope returns the scheduler or the checker which the filter acts on.
func (f *isolationFilter) Scope() string {
	return f.scope
}

// Type returns the name of the filter.
func (*isolationFilter) Type() filterType {
	return isolation
}

// Source filters stores when select them as schedule source.
func (*isolationFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target filters stores when select them as schedule target.
func (f *isolationFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	// No isolation constraint to fit
	if len(f.constraintSet) == 0 {
		return statusStoreNotMatchIsolation
	}
	for _, constrainList := range f.constraintSet {
		match := true
		for idx, constraint := range constrainList {
			// Check every constraint in constrainList
			match = store.GetLabelValue(f.locationLabels[idx]) == constraint && match
		}
		if len(constrainList) > 0 && match {
			return statusStoreNotMatchIsolation
		}
	}
	return statusOK
}

// createRegionForRuleFit is used to create a clone region with RegionCreateOptions which is only used for
// FitRegion in filter
func createRegionForRuleFit(startKey, endKey []byte,
	peers []*metapb.Peer, leader *metapb.Peer, config ...core.RegionCreateOption) *core.RegionInfo {
	copyLeader := typeutil.DeepClone(leader, core.RegionPeerFactory)
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
	}, copyLeader, config...)
	return cloneRegion
}
