// Copyright 2017 TiKV Project Authors.
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

package scatter

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sc "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const regionScatterName = "region-scatter"

var (
	gcInterval            = time.Minute
	gcTTL                 = time.Minute * 3
	operatorPriorityLevel = constant.High

	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	scatterSkipEmptyRegionCounter   = scatterCounter.WithLabelValues("skip", "empty-region")
	scatterSkipNoRegionCounter      = scatterCounter.WithLabelValues("skip", "no-region")
	scatterSkipNoLeaderCounter      = scatterCounter.WithLabelValues("skip", "no-leader")
	scatterSkipHotRegionCounter     = scatterCounter.WithLabelValues("skip", "hot")
	scatterSkipNotReplicatedCounter = scatterCounter.WithLabelValues("skip", "not-replicated")
	scatterSkipAffinityCounter      = scatterCounter.WithLabelValues("skip", "affinity")
	scatterUnnecessaryCounter       = scatterCounter.WithLabelValues("unnecessary", "")
	scatterFailCounter              = scatterCounter.WithLabelValues("fail", "")
	scatterSuccessCounter           = scatterCounter.WithLabelValues("success", "")
	scatterOperatorRunningCounter   = scatterCounter.WithLabelValues("skip", "running")
	scatterOperatorExistedCounter   = scatterCounter.WithLabelValues("fail", "other-existed")
)

const (
	maxSleepDuration     = time.Minute
	initialSleepDuration = 100 * time.Millisecond
	maxRetryLimit        = 30
	// AdminScatterOperatorDesc is used by external admin/API scatter requests.
	AdminScatterOperatorDesc = "scatter-region"
	// InternalScatterOperatorDesc is used by PD-internal split-scatter dispatch.
	InternalScatterOperatorDesc = "split-scatter-region"
)

type selectedStoreCounter interface {
	Get(id uint64, group string) uint64
}

type selectedStores struct {
	mu                syncutil.RWMutex
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
}

func newSelectedStores(ctx context.Context) *selectedStores {
	return &selectedStores{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

type localSelectedStores struct {
	groupDistribution map[string]map[uint64]uint64
	seededGroups      map[string]struct{}
}

func newLocalSelectedStores() *localSelectedStores {
	return &localSelectedStores{
		groupDistribution: make(map[string]map[uint64]uint64),
		seededGroups:      make(map[string]struct{}),
	}
}

func cloneDistribution(distribution map[uint64]uint64) map[uint64]uint64 {
	cloned := make(map[uint64]uint64, len(distribution))
	for id, count := range distribution {
		cloned[id] = count
	}
	return cloned
}

type storeLoadsProvider interface {
	GetStoresLoads() map[uint64]statistics.StoreKindLoads
}

type storeConfigProvider interface {
	GetStoreConfig() sc.StoreConfigProvider
}

type storeReadCPURecentMaxProvider interface {
	GetStoreReadCPURecentMax(storeID uint64) float64
}

func splitScatterReadCPUByStore(
	storesLoads map[uint64]statistics.StoreKindLoads,
	recentMaxProvider storeReadCPURecentMaxProvider,
) map[uint64]float64 {
	readCPUByStore := make(map[uint64]float64, len(storesLoads))
	for storeID, loads := range storesLoads {
		readCPU := loads[utils.StoreReadCPU]
		if recentMaxProvider != nil {
			if recentMax := recentMaxProvider.GetStoreReadCPURecentMax(storeID); recentMax > readCPU {
				readCPU = recentMax
			}
		}
		if readCPU > 0 {
			readCPUByStore[storeID] = readCPU
		}
	}
	return readCPUByStore
}

func decrementDistribution(distribution map[uint64]uint64, id uint64) {
	count, ok := distribution[id]
	if !ok {
		return
	}
	if count <= 1 {
		delete(distribution, id)
		return
	}
	distribution[id] = count - 1
}

// Put plus count by storeID and group
func (s *selectedStores) Put(id uint64, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		distribution = map[uint64]uint64{}
		distribution[id] = 0
	}
	distribution[id]++
	s.groupDistribution.Put(group, distribution)
}

// Get the count by storeID and group
func (s *selectedStores) Get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// GetGroupDistribution get distribution group by `group`
func (s *selectedStores) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getDistributionByGroupLocked(group)
}

// getDistributionByGroupLocked should be called with lock
func (s *selectedStores) getDistributionByGroupLocked(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

// Get the count by storeID and group.
func (s *localSelectedStores) Get(id uint64, group string) uint64 {
	distribution, ok := s.groupDistribution[group]
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// InitGroupDistribution seeds the distribution for a group if the group has not
// been tracked yet.
func (s *localSelectedStores) InitGroupDistribution(group string, distribution map[uint64]uint64) bool {
	if _, ok := s.groupDistribution[group]; ok {
		return false
	}
	s.groupDistribution[group] = cloneDistribution(distribution)
	s.seededGroups[group] = struct{}{}
	return true
}

// Update records the group distribution after scattering one more region.
// For seeded groups it applies the net old->new change. Otherwise it keeps the
// historical scatter behavior and only counts the new placement.
func (s *localSelectedStores) Update(group string, oldIDs, newIDs []uint64) {
	distribution, ok := s.groupDistribution[group]
	if !ok {
		distribution = map[uint64]uint64{}
	}
	if s.isSeededGroup(group) {
		for _, id := range oldIDs {
			decrementDistribution(distribution, id)
		}
	}
	for _, id := range newIDs {
		distribution[id]++
	}
	s.groupDistribution[group] = distribution
}

func (s *localSelectedStores) isSeededGroup(group string) bool {
	_, ok := s.seededGroups[group]
	return ok
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	ctx               context.Context
	name              string
	cluster           sche.SharedCluster
	ordinaryEngine    engineContext
	specialEngines    sync.Map
	opController      *operator.Controller
	addSuspectRegions func(bool, ...uint64)
	affinityFilter    filter.RegionFilter
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster sche.SharedCluster, opController *operator.Controller, addSuspectRegions func(bool, ...uint64)) *RegionScatterer {
	return &RegionScatterer{
		ctx:               ctx,
		name:              regionScatterName,
		cluster:           cluster,
		opController:      opController,
		addSuspectRegions: addSuspectRegions,
		affinityFilter:    filter.NewAffinityFilter(cluster),
		ordinaryEngine: newEngineContext(ctx, func() filter.Filter {
			return filter.NewEngineFilter(regionScatterName, filter.NotSpecialEngines)
		}),
	}
}

type filterFunc func() filter.Filter

type engineContext struct {
	filterFuncs    []filterFunc
	selectedPeer   *selectedStores
	selectedLeader *selectedStores
}

type localEngineContext struct {
	filterFuncs    []filterFunc
	selectedPeer   *localSelectedStores
	selectedLeader *localSelectedStores
}

type scatterSelectionContext struct {
	filterFuncs    []filterFunc
	selectedPeer   selectedStoreCounter
	selectedLeader selectedStoreCounter
}

func newEngineContext(ctx context.Context, filterFuncs ...filterFunc) engineContext {
	filterFuncs = append(filterFuncs, func() filter.Filter {
		return &filter.StoreStateFilter{ActionScope: regionScatterName, MoveRegion: true, ScatterRegion: true, OperatorLevel: operatorPriorityLevel}
	})
	return engineContext{
		filterFuncs:    filterFuncs,
		selectedPeer:   newSelectedStores(ctx),
		selectedLeader: newSelectedStores(ctx),
	}
}

func (ctx engineContext) asSelectionContext() scatterSelectionContext {
	return scatterSelectionContext{
		filterFuncs:    ctx.filterFuncs,
		selectedPeer:   ctx.selectedPeer,
		selectedLeader: ctx.selectedLeader,
	}
}

func (ctx localEngineContext) asSelectionContext() scatterSelectionContext {
	return scatterSelectionContext{
		filterFuncs:    ctx.filterFuncs,
		selectedPeer:   ctx.selectedPeer,
		selectedLeader: ctx.selectedLeader,
	}
}

func (r *RegionScatterer) getOrCreateSpecialEngineContext(engine string) engineContext {
	if ctx, ok := r.specialEngines.Load(engine); ok {
		return ctx.(engineContext)
	}
	ctx := newEngineContext(r.ctx, func() filter.Filter {
		return filter.NewEngineFilter(r.name, placement.LabelConstraint{Key: core.EngineKey, Op: placement.In, Values: []string{engine}})
	})
	r.specialEngines.Store(engine, ctx)
	return ctx
}

type scatterState struct {
	ordinaryEngine localEngineContext
	specialEngines map[string]localEngineContext
}

func (ctx engineContext) withEmptySelection() localEngineContext {
	return localEngineContext{
		filterFuncs:    ctx.filterFuncs,
		selectedPeer:   newLocalSelectedStores(),
		selectedLeader: newLocalSelectedStores(),
	}
}

func (r *RegionScatterer) newScatterState() *scatterState {
	return &scatterState{
		ordinaryEngine: r.ordinaryEngine.withEmptySelection(),
		specialEngines: make(map[string]localEngineContext),
	}
}

func (s *scatterState) getSpecialEngine(engine string) (localEngineContext, bool) {
	ctx, ok := s.specialEngines[engine]
	return ctx, ok
}

func (r *RegionScatterer) getOrCreateSpecialEngineState(state *scatterState, engine string) localEngineContext {
	if ctx, ok := state.getSpecialEngine(engine); ok {
		return ctx
	}
	ctx := r.getOrCreateSpecialEngineContext(engine).withEmptySelection()
	state.specialEngines[engine] = ctx
	return ctx
}

// seedScatterStateByRange seeds the scatter group with the current live peer and
// leader distribution of the specified key range.
func (r *RegionScatterer) seedScatterStateByRange(state *scatterState, group string, startKey, endKey []byte) {
	if group == "" || len(startKey) == 0 {
		return
	}
	if state.ordinaryEngine.selectedPeer.isSeededGroup(group) {
		return
	}

	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryPeer := make(map[uint64]uint64)
	ordinaryLeader := make(map[uint64]uint64)
	specialPeer := make(map[string]map[uint64]uint64)
	for _, store := range r.cluster.GetStores() {
		if store == nil {
			continue
		}
		storeID := store.GetID()
		peerCount := uint64(r.cluster.GetStorePeerCountByRange(storeID, startKey, endKey))
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			if peerCount > 0 {
				ordinaryPeer[storeID] = peerCount
			}
			if leaderCount := uint64(r.cluster.GetStoreLeaderCountByRange(storeID, startKey, endKey)); leaderCount > 0 {
				ordinaryLeader[storeID] = leaderCount
			}
			continue
		}
		if peerCount == 0 {
			continue
		}

		engine := store.GetLabelValue(core.EngineKey)
		if _, ok := specialPeer[engine]; !ok {
			specialPeer[engine] = make(map[uint64]uint64)
		}
		specialPeer[engine][storeID] = peerCount
	}

	state.ordinaryEngine.selectedPeer.InitGroupDistribution(group, ordinaryPeer)
	state.ordinaryEngine.selectedLeader.InitGroupDistribution(group, ordinaryLeader)
	for engine, distribution := range specialPeer {
		ctx := r.getOrCreateSpecialEngineState(state, engine)
		ctx.selectedPeer.InitGroupDistribution(group, distribution)
	}
}

func (r *RegionScatterer) newInternalScatterState(group string, startKey, endKey []byte) *scatterState {
	state := r.newScatterState()
	r.seedScatterStateByRange(state, group, startKey, endKey)
	r.applyRunningScatterOpsDelta(state, group)
	return state
}

func (r *RegionScatterer) applyRunningScatterOpsDelta(state *scatterState, group string) {
	if group == "" {
		return
	}
	for _, op := range r.opController.GetOperators() {
		if op == nil || op.Desc() != InternalScatterOperatorDesc {
			continue
		}
		opGroup, ok := op.GetAdditionalInfo("group")
		if !ok || opGroup != group {
			continue
		}
		region := r.cluster.GetRegion(op.RegionID())
		if region == nil || region.GetLeader() == nil {
			continue
		}
		targetPeers, targetLeader := finalPlacementAfterOperator(region, op)
		r.applyScatterStateDelta(state, region, targetPeers, targetLeader, group, false)
	}
}

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) (int, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errs.ErrEmptyRegion
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit, false)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int, skipStoreLimit bool) (int, map[uint64]error, error) {
	if len(regionsID) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errs.ErrEmptyRegion
	}
	if len(regionsID) == 1 {
		region := r.cluster.GetRegion(regionsID[0])
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			return 0, nil, errs.ErrRegionNotFound
		}
	}
	failures := make(map[uint64]error, len(regionsID))
	regions := make([]*core.RegionInfo, 0, len(regionsID))
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			log.Warn("failed to find region during scatter", zap.Uint64("region-id", id))
			failures[id] = errors.New(fmt.Sprintf("failed to find region %v", id))
			continue
		}
		regions = append(regions, region)
	}
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit, skipStoreLimit)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// scatterRegions relocates the regions. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the regions failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the regions which are failed to be relocated, the key of the failures indicates the regionID
// and the value of the failures indicates the failure error.
func (r *RegionScatterer) scatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int, skipStoreLimit bool) (int, error) {
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, errs.ErrEmptyRegion
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	// opsCount represents the number of regions successfully processed (not necessarily
	// with operators created). This includes regions skipped due to affinity or already
	// in ideal distribution.
	opsCount := 0
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, region := range regions {
			op, err := r.Scatter(region, group, skipStoreLimit)
			failpoint.Inject("scatterFail", func() {
				if region.GetID() == 1 {
					err = errors.New("mock error")
				}
			})
			if err != nil {
				failures[region.GetID()] = err
				continue
			}
			delete(regions, region.GetID())
			opsCount++
			if op != nil {
				if ok := r.opController.AddOperator(op); !ok {
					// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
					failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
					continue
				}
				failpoint.Inject("scatterHbStreamsDrain", func() {
					_ = r.opController.GetHBStreams().Drain(1)
					r.opController.RemoveOperator(op, operator.AdminStop)
				})
			}
			delete(failures, region.GetID())
		}
		// all regions have been relocated, break the loop.
		if len(regions) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return opsCount, nil
}

// Scatter relocates the region. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string, skipStoreLimit bool) (*operator.Operator, error) {
	return r.scatterWithOptions(region, group, skipStoreLimit, false, nil)
}

// ScatterInternal relocates the region for PD-internal split-scatter dispatch.
func (r *RegionScatterer) ScatterInternal(region *core.RegionInfo, group string, startKey, endKey []byte) (*operator.Operator, error) {
	return r.scatterWithOptions(region, group, false, true, r.newInternalScatterState(group, startKey, endKey))
}

func (r *RegionScatterer) scatterWithOptions(region *core.RegionInfo, group string, skipStoreLimit bool, internalScatter bool, state *scatterState) (*operator.Operator, error) {
	expectedDesc := AdminScatterOperatorDesc
	if internalScatter {
		expectedDesc = InternalScatterOperatorDesc
	}
	if !filter.IsRegionReplicated(r.cluster, region) {
		r.addSuspectRegions(false, region.GetID())
		scatterSkipNotReplicatedCounter.Inc()
		log.Warn("region not replicated during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	// Check if there is any existing operator for the region.
	// if the exist operator level is higher than scatter operator level, give up to create new scatter operator new.
	// otherwise, create new scatter operator to replace the existing one.
	if op := r.opController.GetOperator(region.GetID()); op != nil && op.GetPriorityLevel() >= operatorPriorityLevel {
		val, exist := op.GetAdditionalInfo("group")
		// If the existing operator is created by the same group scatterer, just skip creating a new one.
		if op.Desc() == expectedDesc && exist && val == group {
			if !isSameRegionEpoch(op.RegionEpoch(), region.GetRegionEpoch()) {
				scatterOperatorExistedCounter.Inc()
				log.Debug("same group scatter operator epoch does not match",
					zap.Uint64("region-id", region.GetID()),
					zap.Reflect("operator-epoch", op.RegionEpoch()),
					zap.Reflect("region-epoch", region.GetRegionEpoch()))
				return nil, errors.Errorf("the operator of region %d already exist with different epoch", region.GetID())
			}
			scatterOperatorRunningCounter.Inc()
			log.Debug("scatter operator is already running",
				zap.Uint64("region-id", region.GetID()))
			return nil, nil
		}
		scatterOperatorExistedCounter.Inc()
		log.Debug("the operator exist, but it does not meet requirement",
			zap.Uint64("region-id", region.GetID()),
			zap.String("additional-info-group", val),
			zap.String("operator-des", op.Desc()),
			zap.Bool("group-exist", exist),
		)
		return nil, errors.Errorf("the operator of region %d already exist", region.GetID())
	}

	if region.GetLeader() == nil {
		scatterSkipNoLeaderCounter.Inc()
		log.Warn("region no leader during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	// Check if region is in an affinity group that doesn't allow regular scheduling.
	// Unlike hot regions or regions without leaders (which are temporary states),
	// affinity is a configured persistent state. Returning nil error prevents the
	// client from retrying, as retrying won't change the affinity configuration.
	// Note: Returning (nil, nil) means:
	//   - The region will not be retried in scatterRegions loop
	//   - opsCount will still increment (representing "successfully processed", not "operator created")
	//   - The region won't appear in the failures map (client considers it successful)
	if !r.affinityFilter.Select(region).IsOK() {
		scatterSkipAffinityCounter.Inc()
		return nil, nil
	}

	if !internalScatter && r.cluster.IsRegionHot(region) {
		scatterSkipHotRegionCounter.Inc()
		log.Warn("region too hot during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegionWithType(region, group, skipStoreLimit, internalScatter, state)
}

func (r *RegionScatterer) scatterRegionWithType(region *core.RegionInfo, group string, skipStoreLimit bool, internalScatter bool, state *scatterState) (*operator.Operator, error) {
	desc := AdminScatterOperatorDesc
	if internalScatter {
		desc = InternalScatterOperatorDesc
	}
	ordinaryContext := r.ordinaryEngine.asSelectionContext()
	getSpecialEngineContext := func(engine string) scatterSelectionContext {
		return r.getOrCreateSpecialEngineContext(engine).asSelectionContext()
	}
	if state != nil {
		ordinaryContext = state.ordinaryEngine.asSelectionContext()
		getSpecialEngineContext = func(engine string) scatterSelectionContext {
			return r.getOrCreateSpecialEngineState(state, engine).asSelectionContext()
		}
	}
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	specialPeers := make(map[string]map[uint64]*metapb.Peer)
	oldFit := r.cluster.GetRuleManager().FitRegion(r.cluster, region)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return nil, errs.ErrGetSourceStore.FastGenByArgs(fmt.Sprintf("store not found, peer: %v, region id: %d", peer, region.GetID()))
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			ordinaryPeers[peer.GetStoreId()] = peer
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			if _, ok := specialPeers[engine]; !ok {
				specialPeers[engine] = make(map[uint64]*metapb.Peer)
			}
			specialPeers[engine][peer.GetStoreId()] = peer
		}
	}

	targetPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers())) // StoreID -> Peer
	selectedStores := make(map[uint64]struct{}, len(region.GetPeers()))  // selected StoreID set
	leaderCandidateStores := make([]uint64, 0, len(region.GetPeers()))   // StoreID allowed to become Leader
	if internalScatter {
		leader := region.GetLeader()
		if leader == nil {
			return nil, errs.ErrGetTargetStore.FastGenByArgs(fmt.Sprintf("no source leader store found, region: %v", region))
		}
		leaderStoreID := leader.GetStoreId()
		if !r.isAllowedLeaderSource(leaderStoreID) {
			peer := region.GetStorePeer(leaderStoreID)
			if peer == nil {
				return nil, errs.ErrGetTargetStore.FastGenByArgs(fmt.Sprintf("source leader peer not found, region: %v", region))
			}
			targetPeers[leaderStoreID] = peer
			selectedStores[leaderStoreID] = struct{}{}
		}
	}
	scatterWithSameEngine := func(
		peers map[uint64]*metapb.Peer,
		context scatterSelectionContext,
		collectLeaderCandidates bool,
	) { // peers: StoreID -> Peer
		filterLen := len(context.filterFuncs) + 2
		filters := make([]filter.Filter, filterLen)
		for i, filterFunc := range context.filterFuncs {
			filters[i] = filterFunc()
		}
		filters[filterLen-2] = filter.NewExcludedFilter(r.name, nil, selectedStores)
		for _, peer := range peers {
			if _, ok := selectedStores[peer.GetStoreId()]; ok {
				if collectLeaderCandidates && allowLeader(oldFit, peer) {
					leaderCandidateStores = append(leaderCandidateStores, peer.GetStoreId())
				}
				// It is both sourcePeer and targetPeer itself, no need to select.
				continue
			}
			sourceStore := r.cluster.GetStore(peer.GetStoreId())
			if sourceStore == nil {
				log.Error("failed to get the store", zap.Uint64("store-id", peer.GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
				continue
			}
			filters[filterLen-1] = filter.NewPlacementSafeguard(r.name, r.cluster.GetSharedConfig(), r.cluster.GetBasicCluster(), r.cluster.GetRuleManager(), region, sourceStore, oldFit)
			for {
				newPeer := r.selectNewPeer(context, group, peer, filters, internalScatter)
				targetPeers[newPeer.GetStoreId()] = newPeer
				selectedStores[newPeer.GetStoreId()] = struct{}{}
				// If the selected peer is a peer other than origin peer in this region,
				// it is considered that the selected peer select itself.
				// This origin peer re-selects.
				if _, ok := peers[newPeer.GetStoreId()]; !ok || peer.GetStoreId() == newPeer.GetStoreId() {
					selectedStores[peer.GetStoreId()] = struct{}{}
					if collectLeaderCandidates && allowLeader(oldFit, peer) {
						leaderCandidateStores = append(leaderCandidateStores, newPeer.GetStoreId())
					}
					break
				}
			}
		}
	}

	scatterWithSameEngine(ordinaryPeers, ordinaryContext, true)
	for engine, peers := range specialPeers {
		// Special-engine stores are only peer targets for now. Keep them out of
		// leader candidates so leader selection still only considers ordinary stores.
		scatterWithSameEngine(peers, getSpecialEngineContext(engine), false)
	}
	if internalScatter {
		var storesLoads map[uint64]statistics.StoreKindLoads
		if provider, ok := r.cluster.(storeLoadsProvider); ok {
			storesLoads = provider.GetStoresLoads()
		}
		var recentMaxProvider storeReadCPURecentMaxProvider
		if provider, ok := r.cluster.(storeReadCPURecentMaxProvider); ok {
			recentMaxProvider = provider
		}
		readCPUByStore := splitScatterReadCPUByStore(storesLoads, recentMaxProvider)
		leaderCandidateStores = r.filterAllowedLeaderCandidateStores(region, targetPeers, leaderCandidateStores, readCPUByStore)
	}
	// FIXME: target leader only considers the ordinary stores, maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader, leaderStorePickedCount := r.selectAvailableLeaderStore(group, region, leaderCandidateStores, ordinaryContext, internalScatter)
	if targetLeader == 0 {
		scatterSkipNoLeaderCounter.Inc()
		return nil, errs.ErrGetTargetStore.FastGenByArgs(fmt.Sprintf("no target leader store found, region: %v", region))
	}

	if isSameDistribution(region, targetPeers, targetLeader) {
		scatterUnnecessaryCounter.Inc()
		if state != nil {
			r.applyScatterStateDelta(state, region, targetPeers, targetLeader, group, !internalScatter)
		} else {
			r.Put(targetPeers, targetLeader, group)
		}
		return nil, nil
	}
	createScatterOperator := operator.CreateScatterRegionOperator
	if internalScatter {
		createScatterOperator = operator.CreateNonAdminScatterRegionOperator
	}
	op, err := createScatterOperator(desc, r.cluster, region, targetPeers, targetLeader, skipStoreLimit)
	if err != nil {
		scatterFailCounter.Inc()
		currentPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
		for _, peer := range region.GetPeers() {
			currentPeers[peer.GetStoreId()] = peer
		}
		if state != nil {
			r.applyScatterStateDelta(state, region, currentPeers, region.GetLeader().GetStoreId(), group, !internalScatter)
		} else {
			r.Put(currentPeers, region.GetLeader().GetStoreId(), group)
		}
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil, errs.ErrCreateOperator.FastGenByArgs(fmt.Sprintf("failed to create scatter region operator for region %v", region.GetID()))
	}
	if op != nil {
		scatterSuccessCounter.Inc()
		if state == nil {
			r.Put(targetPeers, targetLeader, group)
		}
		op.SetAdditionalInfo("group", group)
		if !internalScatter {
			op.SetAdditionalInfo("leader-picked-count", strconv.FormatUint(leaderStorePickedCount, 10))
		}
		op.SetPriorityLevel(operatorPriorityLevel)
	}
	return op, nil
}

func (r *RegionScatterer) filterAllowedLeaderCandidateStores(
	region *core.RegionInfo,
	targetPeers map[uint64]*metapb.Peer,
	candidateStores []uint64,
	readCPUByStore map[uint64]float64,
) []uint64 {
	if len(candidateStores) == 0 {
		return candidateStores
	}
	filtered := candidateStores[:0]
	leader := region.GetLeader()
	if leader == nil {
		return filtered
	}
	currentLeaderStoreID := leader.GetStoreId()
	if !r.isAllowedLeaderSource(currentLeaderStoreID) {
		for _, storeID := range candidateStores {
			if storeID == currentLeaderStoreID {
				return append(filtered, storeID)
			}
		}
		return filtered
	}
	readPoolThreadCount := uint64(0)
	if provider, ok := r.cluster.(storeConfigProvider); ok {
		readPoolThreadCount = provider.GetStoreConfig().GetUnifiedReadPoolMaxThreadCount()
	}
	readPoolPressureFilters := []filter.Filter{filter.NewReadPoolPressureFilter(r.name, readCPUByStore, readPoolThreadCount)}
	for _, storeID := range candidateStores {
		peer := targetPeers[storeID]
		if peer == nil {
			continue
		}
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		if !filter.Target(r.cluster.GetSharedConfig(), store, readPoolPressureFilters) {
			continue
		}
		if operator.IsAllowedLeaderTarget(r.cluster, region, peer) {
			filtered = append(filtered, storeID)
		}
	}
	return filtered
}

func (r *RegionScatterer) isAllowedLeaderSource(storeID uint64) bool {
	store := r.cluster.GetStore(storeID)
	if store == nil {
		return false
	}
	stateFilter := &filter.StoreStateFilter{ActionScope: r.name, TransferLeader: true}
	return filter.NewCandidates([]*core.StoreInfo{store}).
		FilterSource(r.cluster.GetSharedConfig(), nil, nil, stateFilter).
		Len() > 0
}

func allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}
	if peer.IsWitness {
		return false
	}
	peerFit := fit.GetRuleFit(peer.GetId())
	if peerFit == nil || peerFit.Rule == nil || peerFit.Rule.IsWitness {
		return false
	}
	switch peerFit.Rule.Role {
	case placement.Voter, placement.Leader:
		return true
	}
	return false
}

func isSameDistribution(region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64) bool {
	peers := region.GetPeers()
	for _, peer := range peers {
		if _, ok := targetPeers[peer.GetStoreId()]; !ok {
			return false
		}
	}
	return region.GetLeader().GetStoreId() == targetLeader
}

// selectNewPeer returns the new peer which pick the fewest picked count.
// it keeps the origin peer if the origin store's pick count is equal the fewest pick.
// it can be divided into three steps:
// 1. found the max pick count and the min pick count.
// 2. if max pick count equals min pick count, it means all store picked count are some, return the origin peer.
// 3. otherwise, select the store which pick count is the min pick count and pass all filter.
func (r *RegionScatterer) selectNewPeer(context scatterSelectionContext, group string, peer *metapb.Peer, filters []filter.Filter, internalScatter bool) *metapb.Peer {
	stores := r.cluster.GetStores()
	maxStoreTotalCount := uint64(0)
	minStoreTotalCount := uint64(math.MaxUint64)
	for _, store := range stores {
		count := context.selectedPeer.Get(store.GetID(), group)
		if count > maxStoreTotalCount {
			maxStoreTotalCount = count
		}
		if count < minStoreTotalCount {
			minStoreTotalCount = count
		}
	}

	var newPeer *metapb.Peer
	minCount := uint64(math.MaxUint64)
	originStorePickedCount := uint64(math.MaxUint64)
	bestStoreRegionCount := math.MaxInt
	for _, store := range stores {
		storeCount := context.selectedPeer.Get(store.GetID(), group)
		if store.GetID() == peer.GetStoreId() {
			originStorePickedCount = storeCount
		}
		// If storeCount is equal to the maxStoreTotalCount, we should skip this store as candidate.
		// If the storeCount are all the same for the whole cluster(maxStoreTotalCount == minStoreTotalCount), any store
		// could be selected as candidate.
		if storeCount >= maxStoreTotalCount && maxStoreTotalCount != minStoreTotalCount {
			continue
		}
		if !filter.Target(r.cluster.GetSharedConfig(), store, filters) {
			continue
		}
		candidate := &metapb.Peer{
			StoreId: store.GetID(),
			Role:    peer.GetRole(),
		}
		storeRegionCount := store.GetRegionCount()
		if storeCount < minCount ||
			(internalScatter && storeCount == minCount &&
				(storeRegionCount < bestStoreRegionCount ||
					(storeRegionCount == bestStoreRegionCount && (newPeer == nil || store.GetID() < newPeer.GetStoreId())))) {
			minCount = storeCount
			newPeer = candidate
			bestStoreRegionCount = storeRegionCount
		}
	}
	if internalScatter && newPeer != nil && peer.GetStoreId() != newPeer.GetStoreId() &&
		!peerMoveReducesSourceTargetGap(context.selectedPeer, group, peer.GetStoreId(), newPeer.GetStoreId()) {
		return peer
	}
	if originStorePickedCount <= minCount {
		return peer
	}
	if newPeer == nil {
		return peer
	}
	return newPeer
}

// selectAvailableLeaderStore selects the target leader store from candidate
// peer stores after ordinary and special-engine peer scatter. Special-engine
// stores are excluded from candidates because they cannot become leaders.
func (r *RegionScatterer) selectAvailableLeaderStore(group string, region *core.RegionInfo,
	leaderCandidateStores []uint64, context scatterSelectionContext, internalScatter bool) (leaderID uint64, leaderStorePickedCount uint64) {
	if r.cluster.GetStore(region.GetLeader().GetStoreId()) == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", region.GetLeader().GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
		return 0, 0
	}
	minStoreGroupLeader := uint64(math.MaxUint64)
	minStoreGroupPeer := uint64(math.MaxUint64)
	id := uint64(0)
	unusedAlternativeID := uint64(0)
	unusedAlternativePeerCount := uint64(math.MaxUint64)
	currentLeaderID := region.GetLeader().GetStoreId()
	currentLeaderCanStay := false
	for _, storeID := range leaderCandidateStores {
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		if storeID == currentLeaderID {
			currentLeaderCanStay = true
		}
		storeGroupLeaderCount := context.selectedLeader.Get(storeID, group)
		storeGroupPeerCount := context.selectedPeer.Get(storeID, group)
		if internalScatter && storeID != currentLeaderID && storeGroupLeaderCount == 0 {
			if unusedAlternativeID == 0 || storeGroupPeerCount < unusedAlternativePeerCount ||
				(storeGroupPeerCount == unusedAlternativePeerCount && storeID < unusedAlternativeID) {
				unusedAlternativeID = storeID
				unusedAlternativePeerCount = storeGroupPeerCount
			}
		}
		if id == 0 || minStoreGroupLeader > storeGroupLeaderCount ||
			(internalScatter && minStoreGroupLeader == storeGroupLeaderCount && minStoreGroupPeer > storeGroupPeerCount) {
			minStoreGroupLeader = storeGroupLeaderCount
			minStoreGroupPeer = storeGroupPeerCount
			id = storeID
		}
	}
	selectedID := id
	if internalScatter && unusedAlternativeID != 0 {
		selectedID = unusedAlternativeID
	}
	if selectedID == 0 {
		return 0, 0
	}
	if internalScatter && currentLeaderCanStay && selectedID != currentLeaderID &&
		!leaderMoveReducesSourceTargetGap(context.selectedLeader, group, currentLeaderID, selectedID) {
		selectedID = currentLeaderID
	}
	return selectedID, context.selectedLeader.Get(selectedID, group)
}

func peerMoveReducesSourceTargetGap(selectedPeers selectedStoreCounter, group string, fromStoreID, toStoreID uint64) bool {
	fromCount := selectedPeers.Get(fromStoreID, group)
	toCount := selectedPeers.Get(toStoreID, group)
	return fromCount > toCount+1
}

func leaderMoveReducesSourceTargetGap(selectedLeaders selectedStoreCounter, group string, fromStoreID, toStoreID uint64) bool {
	fromCount := selectedLeaders.Get(fromStoreID, group)
	toCount := selectedLeaders.Get(toStoreID, group)
	// A zero source count means this group has no seeded/current leader history
	// for the origin store, so keep the original scatter fallback behavior.
	return fromCount == 0 || fromCount > toCount+1
}

func isSameRegionEpoch(left, right *metapb.RegionEpoch) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.GetVersion() == right.GetVersion() && left.GetConfVer() == right.GetConfVer()
}

// finalPlacementAfterOperator projects the final target placement of an
// in-flight operator by walking its steps without considering partial execution
// progress. This is a forward-looking estimate: the caller uses it to deduct the
// operator's intended footprint from the next round of store selection, so the
// estimate is intentionally the end state rather than the current half-done state.
func finalPlacementAfterOperator(region *core.RegionInfo, op *operator.Operator) (map[uint64]*metapb.Peer, uint64) {
	targetPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		targetPeers[peer.GetStoreId()] = peer
	}
	targetLeader := region.GetLeader().GetStoreId()
	if op == nil {
		return targetPeers, targetLeader
	}
	for i := range op.Len() {
		switch step := op.Step(i).(type) {
		case operator.TransferLeader:
			targetLeader = step.ToStore
		case operator.AddPeer:
			targetPeers[step.ToStore] = &metapb.Peer{StoreId: step.ToStore}
		case operator.AddLearner:
			targetPeers[step.ToStore] = &metapb.Peer{StoreId: step.ToStore}
		case operator.RemovePeer:
			delete(targetPeers, step.FromStore)
		}
	}
	return targetPeers, targetLeader
}

// Put put the final distribution in the context no matter the operator was created
func (r *RegionScatterer) Put(peers map[uint64]*metapb.Peer, leaderStoreID uint64, group string) {
	if leaderStoreID == 0 {
		return
	}
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	// Group peers by the engine of their stores
	for _, peer := range peers {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			r.ordinaryEngine.selectedPeer.Put(storeID, group)
			scatterDistributionCounter.WithLabelValues(
				strconv.FormatUint(storeID, 10),
				strconv.FormatBool(false),
				core.EngineTiKV).Inc()
			continue
		}
		engine := store.GetLabelValue(core.EngineKey)
		ctx := r.getOrCreateSpecialEngineContext(engine)
		ctx.selectedPeer.Put(storeID, group)
		scatterDistributionCounter.WithLabelValues(
			strconv.FormatUint(storeID, 10),
			strconv.FormatBool(false),
			engine).Inc()
	}
	r.ordinaryEngine.selectedLeader.Put(leaderStoreID, group)
	scatterDistributionCounter.WithLabelValues(
		strconv.FormatUint(leaderStoreID, 10),
		strconv.FormatBool(true),
		core.EngineTiKV).Inc()
}

// applyScatterStateDelta records a local scatter state change after scattering a
// region to the target placement. Metrics are only emitted when this delta
// corresponds to a new scatter decision accepted in the current request/round.
func (r *RegionScatterer) applyScatterStateDelta(state *scatterState, region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64, group string, recordMetrics bool) {
	if state == nil || region == nil || region.GetLeader() == nil {
		return
	}
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryOldStores := make([]uint64, 0, len(region.GetPeers()))
	ordinaryNewStores := make([]uint64, 0, len(targetPeers))
	specialOldStores := make(map[string][]uint64)
	specialNewStores := make(map[string][]uint64)

	classifyStore := func(storeID uint64, ordinary *[]uint64, special map[string][]uint64) string {
		store := r.cluster.GetStore(storeID)
		if store == nil {
			return ""
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			*ordinary = append(*ordinary, storeID)
			return core.EngineTiKV
		}
		engine := store.GetLabelValue(core.EngineKey)
		special[engine] = append(special[engine], storeID)
		return engine
	}

	for _, peer := range region.GetPeers() {
		classifyStore(peer.GetStoreId(), &ordinaryOldStores, specialOldStores)
	}
	for _, peer := range targetPeers {
		storeID := peer.GetStoreId()
		engine := classifyStore(storeID, &ordinaryNewStores, specialNewStores)
		if recordMetrics && engine != "" {
			scatterDistributionCounter.WithLabelValues(
				strconv.FormatUint(storeID, 10),
				strconv.FormatBool(false),
				engine).Inc()
		}
	}

	state.ordinaryEngine.selectedPeer.Update(group, ordinaryOldStores, ordinaryNewStores)
	specialEngines := make(map[string]struct{}, len(specialOldStores)+len(specialNewStores))
	for engine := range specialOldStores {
		specialEngines[engine] = struct{}{}
	}
	for engine := range specialNewStores {
		specialEngines[engine] = struct{}{}
	}
	for engine := range specialEngines {
		ctx := r.getOrCreateSpecialEngineState(state, engine)
		ctx.selectedPeer.Update(group, specialOldStores[engine], specialNewStores[engine])
	}

	oldLeaderStoreID := region.GetLeader().GetStoreId()
	state.ordinaryEngine.selectedLeader.Update(group, []uint64{oldLeaderStoreID}, []uint64{targetLeader})
	if recordMetrics {
		scatterDistributionCounter.WithLabelValues(
			strconv.FormatUint(targetLeader, 10),
			strconv.FormatBool(true),
			core.EngineTiKV).Inc()
	}
}
