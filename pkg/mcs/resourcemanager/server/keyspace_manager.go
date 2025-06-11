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
	unlimitedRate              = math.MaxInt32
	unlimitedBurstLimit        = -1
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
	resourceGroupName string
	*rmpb.Consumption
	isBackground bool
	isTiFlash    bool
}

type keyspaceResourceGroupManager struct {
	syncutil.RWMutex
	groups     map[string]*ResourceGroup
	ruTrackers map[string]*ruTracker
	sl         *serviceLimiter

	keyspaceID uint32
	storage    endpoint.ResourceGroupStorage
}

func newKeyspaceResourceGroupManager(keyspaceID uint32, storage endpoint.ResourceGroupStorage) *keyspaceResourceGroupManager {
	return &keyspaceResourceGroupManager{
		groups:     make(map[string]*ResourceGroup),
		ruTrackers: make(map[string]*ruTracker),
		keyspaceID: keyspaceID,
		storage:    storage,
		sl:         newServiceLimiter(keyspaceID, 0, storage),
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
					FillRate:   unlimitedRate,
					BurstLimit: unlimitedBurstLimit,
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
}

func (krgm *keyspaceResourceGroupManager) getServiceLimiter() *serviceLimiter {
	krgm.RLock()
	defer krgm.RUnlock()
	return krgm.sl
}

func (krgm *keyspaceResourceGroupManager) getOrCreateRUTracker(name string) *ruTracker {
	rt := krgm.getRUTracker(name)
	if rt == nil {
		krgm.Lock()
		// Double check the RU tracker is not created by other goroutine.
		rt = krgm.ruTrackers[name]
		if rt == nil {
			rt = newRUTracker(defaultRUTrackerTimeConstant)
			krgm.ruTrackers[name] = rt
		}
		krgm.Unlock()
	}
	return rt
}

func (krgm *keyspaceResourceGroupManager) getRUTracker(name string) *ruTracker {
	krgm.RLock()
	defer krgm.RUnlock()
	return krgm.ruTrackers[name]
}

// ruTracker is used to track the RU consumption within a keyspace.
// It uses the algorithm of time-aware exponential moving average (EMA) to
// sample and calculate the real-time RU/s of each resource group. The main
// reason for choosing this EMA algorithm is that conventional EMA algorithms or
// moving average algorithms over a time window cannot handle non-fixed frequency
// data sampling well. Since the reporting interval of RU consumption depends on
// the RU consumption rate of the workload, it is necessary to introduce a time
// dimension to calculate real-time RU/s more accurately.
type ruTracker struct {
	syncutil.RWMutex
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
// - `dur` is the time cost to run out of the `totalRU`.
func (rt *ruTracker) sample(now time.Time, totalRU float64, dur time.Duration) {
	rt.Lock()
	defer rt.Unlock()
	// If `dur` is not greater than 0, skip this record.
	if dur <= 0 {
		return
	}
	// Calculate the average RU/s within the `dur`.
	ruPerSec := math.Max(0, totalRU) / dur.Seconds()
	// If the last sample time is not set, set the last EMA directly.
	if rt.lastSampleTime.IsZero() {
		rt.lastEMA = ruPerSec
		rt.lastSampleTime = now
		return
	}
	// Calculate the time delta between the last sample time and the current time.
	dt := now.Sub(rt.lastSampleTime).Seconds()
	if dt <= 0 {
		dt = 1e-3 // Avoid division by zero or negative value, use 1 millisecond as the minimum time delta.
	}
	// By using e^{-β·Δt} to calculate the decay factor, we can have the following behavior:
	//   1. The decay factor is always between 0 and 1.
	//   2. The decay factor is time-aware, the larger the time delta, the lower the weight of the "old data".
	decay := math.Exp(-rt.beta * dt)
	rt.lastEMA = decay*rt.lastEMA + (1-decay)*ruPerSec
	// If the `lastEMA` is less than `minSampledRUPerSec`, set it to 0 to avoid converging into a very small value.
	if rt.lastEMA < minSampledRUPerSec {
		rt.lastEMA = 0
	}
	rt.lastSampleTime = now
}

// Get the real-time RU/s calculated by the EMA algorithm.
func (rt *ruTracker) getRUPerSec() float64 {
	rt.RLock()
	defer rt.RUnlock()
	return rt.lastEMA
}

// conciliateFillRates is used to conciliate the fill rate of each resource group.
// Under the service limit, there might be multiple resource groups with different
// priorities consuming the RU at the same time. In this case, we need to conciliate
// the fill rate of the resource group to ensure the priority, for example, there are
// three resource groups with priorities 1, 2, and 3, and the service limit is 50 RU/s:
// - Group A: priority 1, fill rate 30 RU/s
// - Group B: priority 2, fill rate 20 RU/s
// - Group C: priority 3, fill rate 10 RU/s
// In this case, the fill rate of the resource group should be 30 RU/s, 20 RU/s, and 0 RU/s,
// respectively. The conciliation algorithm is as follows:
//  1. Sort the resource groups by the priority.
//  2. Start from the highest priority resource group, get the real-time RU/s of each.
//  3. Calculate the total real-time RU/s of the resource groups within the same priority.
//     a. If the total real-time RU/s is greater than the service limit,
//     allocate the service limit to all resource groups based on the proportion of RU/s.
//     b. If the total real-time RU/s is less or equal to the service limit,
//     allocate the needed RU/s to the resource group respectively.
//  4. Calculate the remaining service limit, and repeat the process for the next priority
//     until the service limit is allocated.
func (krgm *keyspaceResourceGroupManager) conciliateFillRates() {
	serviceLimiter := krgm.getServiceLimiter()
	// No need to conciliate if the service limit is not set or is 0.
	if serviceLimiter == nil || serviceLimiter.ServiceLimit == 0 {
		return
	}
	priorityQueues := krgm.getPriorityQueues()
	if len(priorityQueues) == 0 {
		return
	}
	remainingServiceLimit := serviceLimiter.ServiceLimit
	for _, queue := range priorityQueues {
		if len(queue) == 0 {
			continue
		}
		totalRUPerSec := 0.0 // The total real-time RU/s of all resource groups in the current priority.
		ruPerSecMap := make(map[string]float64, len(queue))
		for _, group := range queue {
			ruTracker := krgm.getRUTracker(group.Name)
			// Not found the RU tracker, skip this group.
			if ruTracker == nil {
				continue
			}
			ruPerSec := ruTracker.getRUPerSec()
			totalRUPerSec += ruPerSec
			ruPerSecMap[group.Name] = ruPerSec
		}
		if totalRUPerSec > remainingServiceLimit {
			// If the total real-time RU/s is greater than the service limit,
			// allocate the service limit to all resource groups based on the proportion of RU/s.
			for _, group := range queue {
				ruPerSec := ruPerSecMap[group.Name]
				overrideFillRate := math.Min(
					remainingServiceLimit*ruPerSec/totalRUPerSec,
					group.getFillRate(), // Should not exceed the fill rate setting.
				)
				group.overrideFillRate(overrideFillRate)
			}
			remainingServiceLimit = 0
		} else {
			// If the total real-time RU/s is less or equal to the service limit,
			// allocate the needed RU/s to the resource group respectively.
			for _, group := range queue {
				// By setting the override fill rate to -1 to allow the resource group to consume
				// as many RUs as they originally need according to its fill rate setting. This is
				// to prevent the true demand from being suppressed due to setting `overrideFillRate`,
				// thereby wasting idle resources of the Service Limit.
				group.overrideFillRate(-1)
			}
			// Although this layer does not set `overrideFillRate`, it still needs to deduct the actual
			// RU consumption from `remainingFillRate` to reflect the concept of priority, so that the
			// available service limit share decreases level by level.
			remainingServiceLimit -= totalRUPerSec
		}
	}
}

func (krgm *keyspaceResourceGroupManager) getPriorityQueues() [][]*ResourceGroup {
	priorityQueues := make([][]*ResourceGroup, maxPriority+1)
	krgm.RLock()
	for _, group := range krgm.groups {
		if group.Name == DefaultResourceGroupName {
			continue
		}
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
