// Copyright 2025 TiKV Project Authors.
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

package server

import (
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	defaultConsumptionChanSize = 1024
	maxGroupNameLength         = 32
	middlePriority             = 8
	maxPriority                = 16
	// UnlimitedRate is the unlimited fill rate used for the default resource group.
	UnlimitedRate = math.MaxInt32
	// UnlimitedBurstLimit is the unlimited burst limit used for the default resource group.
	UnlimitedBurstLimit = -1
	// DefaultResourceGroupName is the reserved default resource group name within each keyspace.
	DefaultResourceGroupName = "default"
	// defaultRUTrackerTimeConstant is the default time-aware EMA time constant for the RU tracker.
	// The following values are reasonable for the RU tracker:
	//   - ~5s, which captures the RU/s spike quickly but may be too sensitive to the short-term fluctuations.
	//   - ~10s, which smooths the RU/s fluctuations but may not track the spike that quickly.
	//   - ~20s, which smooths the RU/s fluctuations but can only be used to observe the long-term trend.
	defaultRUTrackerTimeConstant = 5 * time.Second
	// minSampledRUPerSec is the minimum RU/s to be sampled by the RU tracker. If it's less than this value,
	// the sampled RU/s will be treated as 0.
	minSampledRUPerSec = 1.0
)

// consumptionItem is used to send the consumption info to the background metrics flusher.
type consumptionItem struct {
	keyspaceID        uint32
	keyspaceName      string
	resourceGroupName string
	*rmpb.Consumption
	isBackground bool
	isTiFlash    bool
}

type keyspaceResourceGroupManager struct {
	syncutil.RWMutex
	groups          map[string]*ResourceGroup
	groupRUTrackers map[string]*groupRUTracker
	sl              *serviceLimiter

	keyspaceID uint32
	storage    endpoint.ResourceGroupStorage
}

func newKeyspaceResourceGroupManager(keyspaceID uint32, storage endpoint.ResourceGroupStorage) *keyspaceResourceGroupManager {
	return &keyspaceResourceGroupManager{
		groups:          make(map[string]*ResourceGroup),
		groupRUTrackers: make(map[string]*groupRUTracker),
		keyspaceID:      keyspaceID,
		storage:         storage,
		sl:              newServiceLimiter(keyspaceID, 0, storage),
	}
}

func (krgm *keyspaceResourceGroupManager) addResourceGroupFromRaw(name string, rawValue string) error {
	group := &rmpb.ResourceGroup{}
	if err := proto.Unmarshal([]byte(rawValue), group); err != nil {
		log.Error("failed to parse the keyspace resource group meta info",
			zap.Uint32("keyspace-id", krgm.keyspaceID), zap.String("name", name), zap.String("raw-value", rawValue), zap.Error(err))
		return err
	}
	krgm.Lock()
	krgm.groups[group.Name] = FromProtoResourceGroup(group)
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) setRawStatesIntoResourceGroup(name string, rawValue string) error {
	tokens := &GroupStates{}
	if err := json.Unmarshal([]byte(rawValue), tokens); err != nil {
		log.Error("failed to parse the keyspace resource group state",
			zap.Uint32("keyspace-id", krgm.keyspaceID), zap.String("name", name), zap.String("raw-value", rawValue), zap.Error(err))
		return err
	}
	krgm.Lock()
	if group, ok := krgm.groups[name]; ok {
		group.SetStatesIntoResourceGroup(tokens)
	}
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) initDefaultResourceGroup() {
	krgm.RLock()
	_, ok := krgm.groups[DefaultResourceGroupName]
	krgm.RUnlock()
	if ok {
		return
	}
	defaultGroup := &ResourceGroup{
		Name: DefaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &RequestUnitSettings{
			RU: &GroupTokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   UnlimitedRate,
					BurstLimit: UnlimitedBurstLimit,
				},
			},
		},
		Priority: middlePriority,
	}
	if err := krgm.addResourceGroup(defaultGroup.IntoProtoResourceGroup(krgm.keyspaceID)); err != nil {
		log.Warn("init default group failed", zap.Uint32("keyspace-id", krgm.keyspaceID), zap.Error(err))
	}
}

func (krgm *keyspaceResourceGroupManager) addResourceGroup(grouppb *rmpb.ResourceGroup) error {
	if len(grouppb.Name) == 0 || len(grouppb.Name) > maxGroupNameLength {
		return errs.ErrInvalidGroup
	}
	// Check the Priority.
	if grouppb.GetPriority() > maxPriority {
		return errs.ErrInvalidGroup
	}
	group := FromProtoResourceGroup(grouppb)
	krgm.Lock()
	defer krgm.Unlock()
	if err := group.persistSettings(krgm.keyspaceID, krgm.storage); err != nil {
		return err
	}
	if err := group.persistStates(krgm.keyspaceID, krgm.storage); err != nil {
		return err
	}
	krgm.groups[group.Name] = group
	return nil
}

func (krgm *keyspaceResourceGroupManager) modifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errs.ErrInvalidGroup
	}
	krgm.RLock()
	curGroup, ok := krgm.groups[group.Name]
	krgm.RUnlock()
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(group.Name)
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	return curGroup.persistSettings(krgm.keyspaceID, krgm.storage)
}

func (krgm *keyspaceResourceGroupManager) deleteResourceGroup(name string) error {
	if name == DefaultResourceGroupName {
		return errs.ErrDeleteReservedGroup
	}
	krgm.RLock()
	_, ok := krgm.groups[name]
	krgm.RUnlock()
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(name)
	}
	if err := krgm.storage.DeleteResourceGroupSetting(krgm.keyspaceID, name); err != nil {
		return err
	}
	krgm.Lock()
	delete(krgm.groups, name)
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) getResourceGroup(name string, withStats bool) *ResourceGroup {
	krgm.RLock()
	defer krgm.RUnlock()
	if group, ok := krgm.groups[name]; ok {
		return group.Clone(withStats)
	}
	return nil
}

func (krgm *keyspaceResourceGroupManager) getMutableResourceGroup(name string) *ResourceGroup {
	krgm.Lock()
	defer krgm.Unlock()
	return krgm.groups[name]
}

func (krgm *keyspaceResourceGroupManager) getMutableResourceGroupList() []*ResourceGroup {
	krgm.Lock()
	defer krgm.Unlock()
	res := make([]*ResourceGroup, 0, len(krgm.groups))
	for _, group := range krgm.groups {
		res = append(res, group)
	}
	return res
}

func (krgm *keyspaceResourceGroupManager) getResourceGroupList(withStats, includeDefault bool) []*ResourceGroup {
	krgm.RLock()
	res := make([]*ResourceGroup, 0, len(krgm.groups))
	for _, group := range krgm.groups {
		if !includeDefault && group.Name == DefaultResourceGroupName {
			continue
		}
		res = append(res, group.Clone(withStats))
	}
	krgm.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

func (krgm *keyspaceResourceGroupManager) persistResourceGroupRunningState() {
	krgm.RLock()
	keys := make([]string, 0, len(krgm.groups))
	for k := range krgm.groups {
		keys = append(keys, k)
	}
	krgm.RUnlock()
	for idx := range keys {
		krgm.RLock()
		group, ok := krgm.groups[keys[idx]]
		if ok {
			if err := group.persistStates(krgm.keyspaceID, krgm.storage); err != nil {
				log.Error("persist keyspace resource group state failed",
					zap.Uint32("keyspace-id", krgm.keyspaceID),
					zap.String("group-name", group.Name),
					zap.Int("index", idx),
					zap.Error(err))
			}
		}
		krgm.RUnlock()
	}
}

func (krgm *keyspaceResourceGroupManager) setServiceLimit(serviceLimit float64) {
	krgm.RLock()
	sl := krgm.sl
	krgm.RUnlock()
	// Set the new service limit to the limiter.
	sl.setServiceLimit(serviceLimit)
	// Cleanup the overrides if the service limit is set to 0.
	if serviceLimit <= 0 {
		krgm.cleanupOverrides()
	} else {
		krgm.invalidateBurstability(serviceLimit)
	}
}

func (krgm *keyspaceResourceGroupManager) getServiceLimiter() *serviceLimiter {
	krgm.RLock()
	defer krgm.RUnlock()
	return krgm.sl
}

func (krgm *keyspaceResourceGroupManager) getServiceLimit() (float64, bool) {
	krgm.RLock()
	defer krgm.RUnlock()
	if krgm.sl == nil {
		return 0, false
	}
	serviceLimit := krgm.sl.getServiceLimit()
	if serviceLimit == 0 {
		return 0, false
	}
	return serviceLimit, true
}

func (krgm *keyspaceResourceGroupManager) getOrCreateGroupRUTracker(name string) *groupRUTracker {
	grt := krgm.getGroupRUTracker(name)
	if grt == nil {
		krgm.Lock()
		// Double check the RU tracker is not created by other goroutine.
		grt = krgm.groupRUTrackers[name]
		if grt == nil {
			grt = newGroupRUTracker()
			krgm.groupRUTrackers[name] = grt
		}
		krgm.Unlock()
	}
	return grt
}

func (krgm *keyspaceResourceGroupManager) getGroupRUTracker(name string) *groupRUTracker {
	krgm.RLock()
	defer krgm.RUnlock()
	return krgm.groupRUTrackers[name]
}

// ruTracker is used to track the RU consumption dynamically.
// It uses the algorithm of time-aware exponential moving average (EMA) to sample
// and calculate the real-time RU/s. The main reason for choosing this EMA algorithm
// is that conventional EMA algorithms or moving average algorithms over a time window
// cannot handle non-fixed frequency data sampling well. Since the reporting interval
// of RU consumption depends on the RU consumption rate of the workload, it is necessary
// to introduce a time dimension to calculate real-time RU/s more accurately.
type ruTracker struct {
	syncutil.RWMutex
	initialized bool
	// beta = ln(2) / τ, τ is the time constant which can be thought of as the half-life of the EMA.
	// For example, if τ = 5s, then the decay factor calculated by e^{-β·Δt} will be 0.5 when Δt = 5s,
	// which means the weight of the "old data" is 0.5 when the elapsed time is 5s.
	beta           float64
	lastSampleTime time.Time
	lastEMA        float64
}

func newRUTracker(timeConstant time.Duration) *ruTracker {
	return &ruTracker{
		beta: math.Log(2) / timeConstant.Seconds(),
	}
}

// Sample the RU consumption and calculate the real-time RU/s as `lastEMA`.
// - `now` is the current time point to sample the RU consumption.
// - `totalRU` is the total RU consumption within the `dur`.
func (rt *ruTracker) sample(clientUniqueID uint64, now time.Time, totalRU float64) {
	rt.Lock()
	defer rt.Unlock()
	// Calculate the elapsed duration since the last sample time.
	prevSampleTime := rt.lastSampleTime
	var dur time.Duration
	if !prevSampleTime.IsZero() {
		dur = now.Sub(prevSampleTime)
	}
	rt.lastSampleTime = now
	// If `dur` is not greater than 0, skip this record.
	if dur <= 0 {
		log.Info("skip ru tracker sample due to non-positive duration",
			zap.Uint64("client-unique-id", clientUniqueID),
			zap.Duration("dur", dur),
			zap.Time("prev-sample-time", prevSampleTime),
			zap.Time("now", now),
			zap.Float64("total-ru", totalRU),
		)
		return
	}
	durSeconds := dur.Seconds()
	// Calculate the average RU/s within the `dur`.
	ruPerSec := math.Max(0, totalRU) / durSeconds
	// If the RU tracker is not initialized, set the last EMA directly.
	if !rt.initialized {
		rt.initialized = true
		rt.lastEMA = ruPerSec
		log.Info("init ru tracker ema",
			zap.Uint64("client-unique-id", clientUniqueID),
			zap.Float64("ru-per-sec", ruPerSec),
			zap.Float64("total-ru", totalRU),
			zap.Duration("dur", dur),
		)
		return
	}
	// By using e^{-β·Δt} to calculate the decay factor, we can have the following behavior:
	//   1. The decay factor is always between 0 and 1.
	//   2. The decay factor is time-aware, the larger the time delta, the lower the weight of the "old data".
	decay := math.Exp(-rt.beta * durSeconds)
	rt.lastEMA = decay*rt.lastEMA + (1-decay)*ruPerSec
	// If the `lastEMA` is less than `minSampledRUPerSec`, set it to 0 to avoid converging into a very small value.
	if rt.lastEMA < minSampledRUPerSec {
		rt.lastEMA = 0
	}
}

// Get the real-time RU/s calculated by the EMA algorithm.
func (rt *ruTracker) getRUPerSec() float64 {
	rt.RLock()
	defer rt.RUnlock()
	return rt.lastEMA
}

func (rt *ruTracker) isInitialized() bool {
	rt.RLock()
	defer rt.RUnlock()
	return rt.initialized
}

// groupRUTracker is used to track the RU consumption of a resource group.
// It matains a map of RU trackers for each client unique ID.
type groupRUTracker struct {
	syncutil.RWMutex
	ruTrackers map[uint64]*ruTracker
}

func newGroupRUTracker() *groupRUTracker {
	return &groupRUTracker{
		ruTrackers: make(map[uint64]*ruTracker),
	}
}

func (grt *groupRUTracker) getOrCreateRUTracker(clientUniqueID uint64) *ruTracker {
	grt.RLock()
	rt := grt.ruTrackers[clientUniqueID]
	grt.RUnlock()
	if rt == nil {
		grt.Lock()
		// Double check the RU tracker is not created by other goroutine.
		rt = grt.ruTrackers[clientUniqueID]
		if rt == nil {
			rt = newRUTracker(defaultRUTrackerTimeConstant)
			grt.ruTrackers[clientUniqueID] = rt
			log.Info("create ru tracker",
				zap.Uint64("client-unique-id", clientUniqueID),
			)
		}
		grt.Unlock()
	}
	return rt
}

func (grt *groupRUTracker) sample(clientUniqueID uint64, now time.Time, totalRU float64) {
	grt.getOrCreateRUTracker(clientUniqueID).sample(clientUniqueID, now, totalRU)
}

func (grt *groupRUTracker) getRUPerSec() float64 {
	grt.RLock()
	defer grt.RUnlock()
	totalRUPerSec := 0.0
	for _, rt := range grt.ruTrackers {
		totalRUPerSec += rt.getRUPerSec()
	}
	return totalRUPerSec
}

func (grt *groupRUTracker) cleanupStaleRUTrackers() []uint64 {
	grt.Lock()
	defer grt.Unlock()
	staleClientUniqueIDs := make([]uint64, 0, len(grt.ruTrackers))
	for clientUniqueID, rt := range grt.ruTrackers {
		if time.Since(rt.lastSampleTime) < slotExpireTimeout {
			continue
		}
		delete(grt.ruTrackers, clientUniqueID)
		staleClientUniqueIDs = append(staleClientUniqueIDs, clientUniqueID)
	}
	return staleClientUniqueIDs
}

// conciliateFillRates is used to conciliate the fill rate of each resource group.
// Under the service limit, there might be multiple resource groups with different
// priorities consuming the RU at the same time. In this case, we need to conciliate
// the fill rate of the resource group to ensure the priority is properly enforced.
//
// The conciliation algorithm works as follows:
//  1. Sort resource groups by priority in descending order (higher priority first).
//  2. For each priority level (from highest to lowest):
//     a. Skip groups without RU tracker (inactive groups).
//     b. Calculate demands for all active groups in this priority level:
//     - Basic RU demand: min(realtime_RU_per_sec, fill_rate_setting)
//     - Burst RU demand:
//     * If burst_limit_setting is in [0, fill_rate_setting]: burst_demand = 0 (no extra tokens allowed)
//     * Otherwise: burst_demand = max(0, realtime_RU_per_sec - fill_rate_setting)
//     c. Determine allocation strategy based on remaining service limit:
//     - Case 1: If total RU demand (basic + burst) < remaining service limit:
//     * Grant all groups their full demanded resources (overrideFillRate = -1, overrideBurstLimit = -1)
//     * Deduct total RU demand from remaining service limit
//     - Case 2: If total RU demand >= remaining service limit:
//     * Sub-case 2.1: If basic RU demand >= remaining service limit:
//     - Sort groups by normalized RU demand (basicDemand / fillRateSetting) in ascending order
//     - Allocate proportionally based on fill rate settings:
//     overrideFillRate = min(basicDemand, remainingLimit * (fillRateSetting / totalFillRate))
//     - Set overrideBurstLimit = overrideFillRate (no extra burst allowed)
//     - Deduct allocated amount from remaining service limit and totalFillRate
//     * Sub-case 2.2: If basic RU demand < remaining service limit:
//     - First deduct totalBasicDemand from remaining service limit
//     - The remaining capacity becomes burst capacity
//     - Fully satisfy all basic demand first (overrideFillRate = -1)
//     - Distribute burst capacity proportionally among burst demand:
//     overrideBurstLimit = fillRateSetting + burstCapacity * (burstDemand / totalBurstDemand)
//     - Respect original burst_limit_setting if > 0
//     - Deduct actual burst supply (overrideBurstLimit - fillRateSetting) from remaining service limit
//  3. Continue to next priority level until all groups are processed or service limit is exhausted.
//  4. If service limit is exhausted (remainingServiceLimit == 0), set overrideFillRate = 0 and
//     overrideBurstLimit = 0 for all remaining lower priority groups (only for active groups).
//
// This ensures:
// - Higher priority groups get resources first
// - Within same priority, resources are allocated proportionally based on actual demand as much as possible
// - Basic needs are prioritized over burst needs
// - No group exceeds its configured limits
// - Inactive groups (no RU consumption) are skipped to avoid unnecessary computation
func (krgm *keyspaceResourceGroupManager) conciliateFillRates() {
	serviceLimit, isSet := krgm.getServiceLimit()
	// No need to conciliate if the service limit is not set or is 0.
	if !isSet || serviceLimit == 0 {
		return
	}
	priorityQueues := krgm.getPriorityQueues()
	if len(priorityQueues) == 0 {
		return
	}
	remainingServiceLimit := serviceLimit
	for _, queue := range priorityQueues {
		if len(queue) == 0 {
			continue
		}
		// If the remaining service limit is 0, set the fill rate of all the remaining resource groups to 0 directly.
		// Only apply this to active resource groups (those with RU tracker and actual RU consumption).
		if remainingServiceLimit == 0 {
			krgm.setZeroFillRateForAllGroups(queue)
			continue
		}

		// Calculate the basic and burst RU demands for all groups in this priority level.
		di := krgm.calculateDemandInfo(queue)
		totalRUDemand := di.totalRUDemand()
		if totalRUDemand == 0 {
			continue
		}
		// If the total real-time RU demand is greater than or equal to the remaining service limit,
		// we need to divide the remaining service limit proportionally within the same priority level.
		if totalRUDemand >= remainingServiceLimit {
			if di.totalBasicRUDemand >= remainingServiceLimit {
				// If the basic RU demand already exceeds the remaining service limit, then we can only try to meet the basic needs of
				// the resource groups through proportional allocation of the fill rate setting.
				remainingServiceLimit = di.allocateBasicRUDemand(queue, remainingServiceLimit)
			} else {
				// If the basic RU demand can be fully met, we can further satisfy the extra burst RU demand.
				remainingServiceLimit = di.allocateBurstRUDemand(queue, remainingServiceLimit)
			}
		} else {
			// If the total real-time RU demand is less than the remaining service limit, no need to divide the service limit,
			// just set the override fill rate to -1 to allow the resource group to consume as many RUs as they originally
			// need according to its fill rate setting.
			for _, group := range queue {
				if group.getBurstLimit(true) >= 0 {
					group.overrideFillRateAndBurstLimit(-1, -1)
				} else {
					// If the original burst limit is not set, set the override burst limit to the remaining service limit.
					group.overrideFillRateAndBurstLimit(-1, int64(remainingServiceLimit))
				}
			}
			// Although this priority level does not require resource limiting, it still needs to deduct the actual
			// RU consumption from `remainingServiceLimit` to reflect the concept of priority, so that the
			// available service limit share decreases level by level for lower priority groups.
			remainingServiceLimit -= totalRUDemand
		}
	}
}

func (krgm *keyspaceResourceGroupManager) getPriorityQueues() [][]*ResourceGroup {
	priorityQueues := make([][]*ResourceGroup, maxPriority+1)
	krgm.RLock()
	for _, group := range krgm.groups {
		priority := group.Priority
		priorityQueues[priority] = append(priorityQueues[priority], group)
	}
	krgm.RUnlock()
	// Sort the priority queues in descending order.
	sort.Slice(priorityQueues, func(i, j int) bool {
		queueI, queueJ := priorityQueues[i], priorityQueues[j]
		if len(queueI) == 0 {
			return true
		}
		if len(queueJ) == 0 {
			return false
		}
		return queueI[0].Priority > queueJ[0].Priority
	})
	// Filter out empty queues in-place
	n := 0
	for _, queue := range priorityQueues {
		if len(queue) == 0 {
			continue
		}
		priorityQueues[n] = queue
		n++
	}
	return priorityQueues[:n]
}

func (krgm *keyspaceResourceGroupManager) setZeroFillRateForAllGroups(queue []*ResourceGroup) {
	for _, group := range queue {
		grt := krgm.getGroupRUTracker(group.Name)
		if grt == nil || grt.getRUPerSec() == 0 {
			continue
		}
		// Only set the active resource groups to avoid unnecessary overrides.
		group.overrideFillRateAndBurstLimit(0, 0)
	}
}

func (krgm *keyspaceResourceGroupManager) calculateDemandInfo(queue []*ResourceGroup) *demandInfo {
	di := &demandInfo{
		basicRUDemandMap: make(map[string]float64, len(queue)),
		burstRUDemandMap: make(map[string]float64, len(queue)),
	}
	for _, group := range queue {
		grt := krgm.getGroupRUTracker(group.Name)
		// Not found the RU tracker, skip this group.
		if grt == nil {
			continue
		}
		ruPerSec := grt.getRUPerSec()
		fillRateSetting := group.getFillRate(true)
		burstLimitSetting := float64(group.getBurstLimit(true))
		// Calculate the basic RU demand of the resource group.
		// Basic demand is the minimum of real-time RU consumption and configured fill rate.
		basicRUDemand := math.Min(ruPerSec, fillRateSetting)
		di.totalBasicRUDemand += basicRUDemand
		di.basicRUDemandMap[group.Name] = basicRUDemand
		// Calculate the burst RU demand of the resource group.
		burstRUDemand := 0.0
		// If the burst limit setting is within the range of [0, fillRateSetting],
		// it means the resource group is not allowed to consume extra tokens,
		// so the burst RU demand should always be 0.
		if 0 <= burstLimitSetting && burstLimitSetting <= fillRateSetting {
			burstRUDemand = 0
		} else {
			burstRUDemand = math.Max(0, ruPerSec-fillRateSetting)
		}
		di.totalBurstRUDemand += burstRUDemand
		di.burstRUDemandMap[group.Name] = burstRUDemand
		// Calculate the total fill rate of all the resource groups in this priority level.
		di.totalFillRate += fillRateSetting
	}
	return di
}

type demandInfo struct {
	totalFillRate      float64
	totalBasicRUDemand float64
	totalBurstRUDemand float64
	basicRUDemandMap   map[string]float64
	burstRUDemandMap   map[string]float64
}

func (di *demandInfo) totalRUDemand() float64 {
	return di.totalBasicRUDemand + di.totalBurstRUDemand
}

func (di *demandInfo) allocateBasicRUDemand(
	queue []*ResourceGroup,
	remainingServiceLimit float64,
) float64 {
	type groupInfo struct {
		group              *ResourceGroup
		basicRUDemand      float64
		normalizedRUDemand float64
	}
	groupInfos := make([]*groupInfo, 0, len(queue))
	for _, group := range queue {
		basicRUDemand := di.basicRUDemandMap[group.Name]
		groupInfos = append(groupInfos, &groupInfo{
			group:         group,
			basicRUDemand: basicRUDemand,
			// The normalized RU demand is the basic RU demand divided by the fill rate setting.
			// This can represent its demand level under the fill rate setting.
			normalizedRUDemand: basicRUDemand / group.getFillRate(true),
		})
	}
	// Sort the group infos by the normalized RU demand in ascending order, low demand first.
	sort.Slice(groupInfos, func(i, j int) bool {
		return groupInfos[i].normalizedRUDemand < groupInfos[j].normalizedRUDemand
	})
	for _, gi := range groupInfos {
		fillRateSetting := gi.group.getFillRate(true)
		proportionalFillRate := remainingServiceLimit * fillRateSetting / di.totalFillRate
		// Allocate the remaining service limit proportionally based on basic demand.
		overrideFillRate := math.Min(
			gi.basicRUDemand,
			proportionalFillRate,
		)
		// Do not allow the resource group to consume extra tokens in this case,
		// so the override burst limit is set to the same as the override fill rate.
		gi.group.overrideFillRateAndBurstLimit(overrideFillRate, int64(overrideFillRate))
		// Deduct the basic RU demand from the remaining service limit.
		remainingServiceLimit -= overrideFillRate
		di.totalFillRate -= fillRateSetting
	}
	return remainingServiceLimit
}

func (di *demandInfo) allocateBurstRUDemand(
	queue []*ResourceGroup,
	remainingServiceLimit float64,
) float64 {
	remainingServiceLimit -= di.totalBasicRUDemand
	// The remaining service limit is the burst capacity.
	burstCapacity := remainingServiceLimit
	for _, group := range queue {
		burstRUDemand := di.burstRUDemandMap[group.Name]
		fillRateSetting := group.getFillRate(true)
		// Allocate the remaining service limit proportionally based on burst demand.
		overrideBurstLimit := fillRateSetting + burstCapacity*burstRUDemand/di.totalBurstRUDemand
		// Should not exceed the original burst limit setting if it's greater than 0.
		burstLimitSetting := float64(group.getBurstLimit(true))
		if burstLimitSetting > 0 {
			overrideBurstLimit = math.Min(overrideBurstLimit, burstLimitSetting)
		}
		// The basic RU demand is already met, so the override fill rate is set to -1
		// to allow the resource group to consume as many RUs as they originally need
		// according to its fill rate setting.
		group.overrideFillRateAndBurstLimit(-1, int64(overrideBurstLimit))
		// Deduct the burst supply from the remaining service limit.
		remainingServiceLimit -= math.Max(0, overrideBurstLimit-fillRateSetting)
	}
	return remainingServiceLimit
}

// Cleanup the overrides for all the resource groups.
func (krgm *keyspaceResourceGroupManager) cleanupOverrides() {
	for _, group := range krgm.getMutableResourceGroupList() {
		group.overrideFillRateAndBurstLimit(-1, -1)
	}
}

// Since the burstable resource groups won't require tokens from the server anymore,
// we have to override the burst limit of all the resource groups to the service limit.
// This ensures the burstability of the resource groups can be properly invalidated.
func (krgm *keyspaceResourceGroupManager) invalidateBurstability(serviceLimit float64) {
	for _, group := range krgm.getMutableResourceGroupList() {
		if group.getBurstLimit(true) >= 0 {
			continue
		}
		group.overrideBurstLimit(int64(serviceLimit))
	}
}
