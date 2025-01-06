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

package core

import (
	"math"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

const (
	// Interval to save store meta (including heartbeat ts) to etcd.
	storePersistInterval = 5 * time.Minute
	initialMinSpace      = 8 * units.GiB // 2^33=8GB
	slowStoreThreshold   = 80
	awakenStoreInterval  = 10 * time.Minute // 2 * slowScoreRecoveryTime

	// EngineKey is the label key used to indicate engine.
	EngineKey = "engine"
	// EngineTiFlash is the tiflash value of the engine label.
	EngineTiFlash = "tiflash"
	// EngineTiKV indicates the tikv engine in metrics
	EngineTiKV = "tikv"
)

// StoreInfo contains information about a store.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type StoreInfo struct {
	meta *metapb.Store
	*storeStats
	pauseLeaderTransfer bool // not allow to be used as source or target of transfer leader
	slowStoreEvicted    bool // this store has been evicted as a slow store, should not transfer leader to it
	slowTrendEvicted    bool // this store has been evicted as a slow store by trend, should not transfer leader to it
	leaderCount         int
	regionCount         int
	learnerCount        int
	witnessCount        int
	leaderSize          int64
	regionSize          int64
	pendingPeerCount    int
	lastPersistTime     time.Time
	leaderWeight        float64
	regionWeight        float64
	limiter             storelimit.StoreLimit
	minResolvedTS       uint64
	lastAwakenTime      time.Time
}

// NewStoreInfo creates StoreInfo with meta data.
func NewStoreInfo(store *metapb.Store, opts ...StoreCreateOption) *StoreInfo {
	storeInfo := &StoreInfo{
		meta:          store,
		storeStats:    newStoreStats(),
		leaderWeight:  1.0,
		regionWeight:  1.0,
		limiter:       storelimit.NewStoreRateLimit(0.0),
		minResolvedTS: 0,
	}
	for _, opt := range opts {
		opt(storeInfo)
	}
	return storeInfo
}

// NewStoreInfoWithLabel is create a store with specified labels. only for test purpose.
func NewStoreInfoWithLabel(id uint64, labels map[string]string) *StoreInfo {
	storeLabels := make([]*metapb.StoreLabel, 0, len(labels))
	for k, v := range labels {
		storeLabels = append(storeLabels, &metapb.StoreLabel{
			Key:   k,
			Value: v,
		})
	}
	store := NewStoreInfo(
		&metapb.Store{
			Id:     id,
			Labels: storeLabels,
		},
	)
	return store
}

// Clone creates a copy of current StoreInfo.
func (s *StoreInfo) Clone(opts ...StoreCreateOption) *StoreInfo {
	store := *s
	store.meta = typeutil.DeepClone(s.meta, StoreFactory)
	for _, opt := range opts {
		if opt != nil {
			opt(&store)
		}
	}
	return &store
}

// LimitVersion returns the limit version of the store.
func (s *StoreInfo) LimitVersion() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.limiter.Version()
}

// Feedback is used to update the store's limit.
func (s *StoreInfo) Feedback(e float64) {
	if limit := s.limiter; limit != nil {
		limit.Feedback(e)
	}
}

// ShallowClone creates a copy of current StoreInfo, but not clone 'meta'.
func (s *StoreInfo) ShallowClone(opts ...StoreCreateOption) *StoreInfo {
	store := *s
	for _, opt := range opts {
		opt(&store)
	}
	return &store
}

// AllowLeaderTransfer returns if the store is allowed to be selected
// as source or target of transfer leader.
func (s *StoreInfo) AllowLeaderTransfer() bool {
	return !s.pauseLeaderTransfer
}

// EvictedAsSlowStore returns if the store should be evicted as a slow store.
func (s *StoreInfo) EvictedAsSlowStore() bool {
	return s.slowStoreEvicted
}

// IsEvictedAsSlowTrend returns if the store should be evicted as a slow store by trend.
func (s *StoreInfo) IsEvictedAsSlowTrend() bool {
	return s.slowTrendEvicted
}

// IsAvailable returns if the store bucket of limitation is available
func (s *StoreInfo) IsAvailable(limitType storelimit.Type, level constant.PriorityLevel) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.limiter.Available(storelimit.RegionInfluence[limitType], limitType, level)
}

// IsTiFlash returns true if the store is tiflash.
func (s *StoreInfo) IsTiFlash() bool {
	return IsStoreContainLabel(s.GetMeta(), EngineKey, EngineTiFlash)
}

// IsUp returns true if store is serving or preparing.
func (s *StoreInfo) IsUp() bool {
	return s.IsServing() || s.IsPreparing()
}

// IsPreparing checks if the store's state is preparing.
func (s *StoreInfo) IsPreparing() bool {
	return s.GetNodeState() == metapb.NodeState_Preparing
}

// IsServing checks if the store's state is serving.
func (s *StoreInfo) IsServing() bool {
	return s.GetNodeState() == metapb.NodeState_Serving
}

// IsRemoving checks if the store's state is removing.
func (s *StoreInfo) IsRemoving() bool {
	return s.GetNodeState() == metapb.NodeState_Removing
}

// IsRemoved checks if the store's state is removed.
func (s *StoreInfo) IsRemoved() bool {
	return s.GetNodeState() == metapb.NodeState_Removed
}

// GetSlowScore returns the slow score of the store.
func (s *StoreInfo) GetSlowScore() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rawStats.GetSlowScore()
}

// WitnessScore returns the store's witness score.
func (s *StoreInfo) WitnessScore(delta int64) float64 {
	return float64(int64(s.GetWitnessCount()) + delta)
}

// IsSlow checks if the slow score reaches the threshold.
func (s *StoreInfo) IsSlow() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slowTrendEvicted || s.rawStats.GetSlowScore() >= slowStoreThreshold
}

// GetSlowTrend returns the slow trend information of the store.
func (s *StoreInfo) GetSlowTrend() *pdpb.SlowTrend {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rawStats.GetSlowTrend()
}

// IsPhysicallyDestroyed checks if the store's physically destroyed.
func (s *StoreInfo) IsPhysicallyDestroyed() bool {
	return s.GetMeta().GetPhysicallyDestroyed()
}

// DownTime returns the time elapsed since last heartbeat.
func (s *StoreInfo) DownTime() time.Duration {
	return time.Since(s.GetLastHeartbeatTS())
}

// GetMeta returns the meta information of the store.
func (s *StoreInfo) GetMeta() *metapb.Store {
	return s.meta
}

// GetState returns the state of the store.
func (s *StoreInfo) GetState() metapb.StoreState {
	return s.meta.GetState()
}

// GetNodeState returns the state of the node.
func (s *StoreInfo) GetNodeState() metapb.NodeState {
	return s.meta.GetNodeState()
}

// GetStatusAddress returns the http address of the store.
func (s *StoreInfo) GetStatusAddress() string {
	return s.meta.GetStatusAddress()
}

// GetAddress returns the address of the store.
func (s *StoreInfo) GetAddress() string {
	return s.meta.GetAddress()
}

// GetVersion returns the version of the store.
func (s *StoreInfo) GetVersion() string {
	return s.meta.GetVersion()
}

// GetLabels returns the labels of the store.
func (s *StoreInfo) GetLabels() []*metapb.StoreLabel {
	return s.meta.GetLabels()
}

// GetID returns the ID of the store.
func (s *StoreInfo) GetID() uint64 {
	return s.meta.GetId()
}

// GetLeaderCount returns the leader count of the store.
func (s *StoreInfo) GetLeaderCount() int {
	return s.leaderCount
}

// GetRegionCount returns the Region count of the store.
func (s *StoreInfo) GetRegionCount() int {
	return s.regionCount
}

// GetLearnerCount returns the learner count of the store.
func (s *StoreInfo) GetLearnerCount() int {
	return s.learnerCount
}

// GetWitnessCount returns the witness count of the store.
func (s *StoreInfo) GetWitnessCount() int {
	return s.witnessCount
}

// GetLeaderSize returns the leader size of the store.
func (s *StoreInfo) GetLeaderSize() int64 {
	return s.leaderSize
}

// GetRegionSize returns the Region size of the store.
func (s *StoreInfo) GetRegionSize() int64 {
	return s.regionSize
}

// GetPendingPeerCount returns the pending peer count of the store.
func (s *StoreInfo) GetPendingPeerCount() int {
	return s.pendingPeerCount
}

// GetLeaderWeight returns the leader weight of the store.
func (s *StoreInfo) GetLeaderWeight() float64 {
	return s.leaderWeight
}

// GetRegionWeight returns the Region weight of the store.
func (s *StoreInfo) GetRegionWeight() float64 {
	return s.regionWeight
}

// GetLastHeartbeatTS returns the last heartbeat timestamp of the store.
func (s *StoreInfo) GetLastHeartbeatTS() time.Time {
	return time.Unix(0, s.meta.GetLastHeartbeat())
}

// NeedPersist returns if it needs to save to etcd.
func (s *StoreInfo) NeedPersist() bool {
	return s.GetLastHeartbeatTS().Sub(s.lastPersistTime) > storePersistInterval
}

// GetStoreLimit return the limit of a specific store.
func (s *StoreInfo) GetStoreLimit() storelimit.StoreLimit {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.limiter
}

const minWeight = 1e-6
const maxScore = 1024 * 1024 * 1024

// LeaderScore returns the store's leader score.
func (s *StoreInfo) LeaderScore(policy constant.SchedulePolicy, delta int64) float64 {
	switch policy {
	case constant.BySize:
		return float64(s.GetLeaderSize()+delta) / math.Max(s.GetLeaderWeight(), minWeight)
	case constant.ByCount:
		return float64(int64(s.GetLeaderCount())+delta) / math.Max(s.GetLeaderWeight(), minWeight)
	default:
		return 0
	}
}

// RegionScore returns the store's region score.
// Deviation It is used to control the direction of the deviation considered
// when calculating the region score. It is set to -1 when it is the source
// store of balance, 1 when it is the target, and 0 in the rest of cases.
func (s *StoreInfo) RegionScore(version string, highSpaceRatio, lowSpaceRatio float64, delta int64) float64 {
	switch version {
	case "v2":
		return s.regionScoreV2(delta, lowSpaceRatio)
	case "v1":
		fallthrough
	default:
		return s.regionScoreV1(highSpaceRatio, lowSpaceRatio, delta)
	}
}

func (s *StoreInfo) regionScoreV1(highSpaceRatio, lowSpaceRatio float64, delta int64) float64 {
	var score float64
	var amplification float64
	available := float64(s.GetAvailable()) / units.MiB
	used := float64(s.GetUsedSize()) / units.MiB
	capacity := float64(s.GetCapacity()) / units.MiB

	if s.GetRegionSize() == 0 || used == 0 {
		amplification = 1
	} else {
		// because of rocksdb compression, region size is larger than actual used size
		amplification = float64(s.GetRegionSize()) / used
	}

	// highSpaceBound is the lower bound of the high space stage.
	highSpaceBound := (1 - highSpaceRatio) * capacity
	// lowSpaceBound is the upper bound of the low space stage.
	lowSpaceBound := (1 - lowSpaceRatio) * capacity
	if available-float64(delta)/amplification >= highSpaceBound {
		score = float64(s.GetRegionSize() + delta)
	} else if available-float64(delta)/amplification <= lowSpaceBound {
		score = maxScore - (available - float64(delta)/amplification)
	} else {
		// to make the score function continuous, we use linear function y = k * x + b as transition period
		// from above we know that there are two points must on the function image
		// note that it is possible that other irrelative files occupy a lot of storage, so capacity == available + used + irrelative
		// and we regarded irrelative as a fixed value.
		// Then amp = size / used = size / (capacity - irrelative - available)
		//
		// When available == highSpaceBound,
		// we can conclude that size = (capacity - irrelative - highSpaceBound) * amp = (used + available - highSpaceBound) * amp
		// Similarly, when available == lowSpaceBound,
		// we can conclude that size = (capacity - irrelative - lowSpaceBound) * amp = (used + available - lowSpaceBound) * amp
		// These are the two fixed points' x-coordinates, and y-coordinates which can be easily obtained from the above two functions.
		x1, y1 := (used+available-highSpaceBound)*amplification, (used+available-highSpaceBound)*amplification
		x2, y2 := (used+available-lowSpaceBound)*amplification, maxScore-lowSpaceBound

		k := (y2 - y1) / (x2 - x1)
		b := y1 - k*x1
		score = k*float64(s.GetRegionSize()+delta) + b
	}

	return score / math.Max(s.GetRegionWeight(), minWeight)
}

func (s *StoreInfo) regionScoreV2(delta int64, lowSpaceRatio float64) float64 {
	A := float64(s.GetAvgAvailable()) / units.GiB
	C := float64(s.GetCapacity()) / units.GiB
	// the used size always be accurate, it only statistics the raftDB|rocksDB|snap directory, so we use it directly.
	U := float64(s.GetUsedSize()) / units.GiB
	// the diff maybe not zero if the disk has other files.
	diff := C - A - U
	R := float64(s.GetRegionSize() + delta)
	if R < 0 {
		R = float64(s.GetRegionSize())
	}

	if s.GetRegionSize() != 0 {
		U += U * (float64(delta)) / float64(s.GetRegionSize())
		if A1 := C - U - diff; A1 > 0 && A1 < C {
			A = A1
		}
	}
	var (
		K, M float64 = 1, 256 // Experience value to control the weight of the available influence on score
		F    float64 = 50     // Experience value to prevent some nodes from running out of disk space prematurely.
		B            = 1e10
	)
	F = math.Max(F, C*(1-lowSpaceRatio))
	var score float64
	if A >= C || C < 1 {
		score = R
	} else if A > F {
		// As the amount of data increases (available becomes smaller), the weight of region size on total score
		// increases. Ideally, all nodes converge at the position where remaining space is F (default 20GiB).
		score = (K + M*(math.Log(C)-math.Log(A-F+1))/(C-A+F-1)) * R
	} else {
		// When remaining space is less then F, the score is mainly determined by available space.
		// store's score will increase rapidly after it has few space. and it will reach similar score when they has no space
		score = (K+M*math.Log(C)/C)*R + B*(F-A)/F
	}
	return score / math.Max(s.GetRegionWeight(), minWeight)
}

// StorageSize returns store's used storage size reported from tikv.
func (s *StoreInfo) StorageSize() uint64 {
	return s.GetUsedSize()
}

// AvailableRatio is store's freeSpace/capacity.
func (s *StoreInfo) AvailableRatio() float64 {
	if s.GetCapacity() == 0 {
		return 0
	}
	return float64(s.GetAvailable()) / float64(s.GetCapacity())
}

// IsLowSpace checks if the store is lack of space. Not check if region count less
// than InitClusterRegionThreshold and available space more than initialMinSpace
func (s *StoreInfo) IsLowSpace(lowSpaceRatio float64) bool {
	if s.GetStoreStats() == nil {
		return false
	}
	// See https://github.com/tikv/pd/issues/3444 and https://github.com/tikv/pd/issues/5391
	// TODO: we need find a better way to get the init region number when starting a new cluster.
	// We don't need to consider the store as low space when the capacity is 0.
	if s.regionCount < InitClusterRegionThreshold && s.GetAvailable() > initialMinSpace || s.GetCapacity() == 0 {
		return false
	}
	return s.AvailableRatio() < 1-lowSpaceRatio
}

// ResourceCount returns count of leader/region in the store.
func (s *StoreInfo) ResourceCount(kind constant.ResourceKind) uint64 {
	switch kind {
	case constant.LeaderKind:
		return uint64(s.GetLeaderCount())
	case constant.RegionKind:
		return uint64(s.GetRegionCount())
	default:
		return 0
	}
}

// ResourceSize returns size of leader/region in the store
func (s *StoreInfo) ResourceSize(kind constant.ResourceKind) int64 {
	switch kind {
	case constant.LeaderKind:
		return s.GetLeaderSize()
	case constant.RegionKind:
		return s.GetRegionSize()
	default:
		return 0
	}
}

// ResourceWeight returns weight of leader/region in the score
func (s *StoreInfo) ResourceWeight(kind constant.ResourceKind) float64 {
	switch kind {
	case constant.LeaderKind:
		leaderWeight := s.GetLeaderWeight()
		if leaderWeight <= 0 {
			return minWeight
		}
		return leaderWeight
	case constant.RegionKind:
		regionWeight := s.GetRegionWeight()
		if regionWeight <= 0 {
			return minWeight
		}
		return regionWeight
	default:
		return 0
	}
}

// GetStartTime returns the start timestamp.
func (s *StoreInfo) GetStartTime() time.Time {
	return time.Unix(s.meta.GetStartTimestamp(), 0)
}

// GetUptime returns the uptime.
func (s *StoreInfo) GetUptime() time.Duration {
	uptime := s.GetLastHeartbeatTS().Sub(s.GetStartTime())
	if uptime > 0 {
		return uptime
	}
	return 0
}

// GetMinResolvedTS returns min resolved ts.
func (s *StoreInfo) GetMinResolvedTS() uint64 {
	return s.minResolvedTS
}

// NeedAwakenStore checks whether all hibernated regions in this store should
// be awaken or not.
func (s *StoreInfo) NeedAwakenStore() bool {
	return s.GetLastHeartbeatTS().Sub(s.lastAwakenTime) > awakenStoreInterval
}

var (
	// If a store's last heartbeat is storeDisconnectDuration ago, the store will
	// be marked as disconnected state. The value should be greater than tikv's
	// store heartbeat interval (default 10s).
	storeDisconnectDuration = 20 * time.Second
	storeUnhealthyDuration  = 10 * time.Minute
)

// IsDisconnected checks if a store is disconnected, which means PD misses
// tikv's store heartbeat for a short time, maybe caused by process restart or
// temporary network failure.
func (s *StoreInfo) IsDisconnected() bool {
	return s.DownTime() > storeDisconnectDuration
}

// IsUnhealthy checks if a store is unhealthy.
func (s *StoreInfo) IsUnhealthy() bool {
	return s.DownTime() > storeUnhealthyDuration
}

// GetLabelValue returns a label's value (if exists).
func (s *StoreInfo) GetLabelValue(key string) string {
	for _, label := range s.GetLabels() {
		if strings.EqualFold(label.GetKey(), key) {
			return label.GetValue()
		}
	}
	return ""
}

// CompareLocation compares 2 stores' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (s *StoreInfo) CompareLocation(other *StoreInfo, labels []string) int {
	for i, key := range labels {
		v1, v2 := s.GetLabelValue(key), other.GetLabelValue(key)
		// If label is not set, the store is considered at the same location
		// with any other store.
		if v1 != "" && v2 != "" && !strings.EqualFold(v1, v2) {
			return i
		}
	}
	return -1
}

const replicaBaseScore = 100

// DistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func DistinctScore(labels []string, stores []*StoreInfo, other *StoreInfo) float64 {
	var score float64
	for _, s := range stores {
		if s.GetID() == other.GetID() {
			continue
		}
		if index := s.CompareLocation(other, labels); index != -1 {
			score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
		}
	}
	return score
}

// MergeLabels merges the passed in labels with origins, overriding duplicated ones.
// Note: To prevent potential data races, it is advisable to refrain from directly modifying the 'origin' variable.
func MergeLabels(origin []*metapb.StoreLabel, labels []*metapb.StoreLabel) []*metapb.StoreLabel {
	results := make([]*metapb.StoreLabel, 0, len(origin))
	for _, label := range origin {
		results = append(results, &metapb.StoreLabel{
			Key:   label.Key,
			Value: label.Value,
		})
	}

	for _, newLabel := range labels {
		found := false
		for _, label := range results {
			if strings.EqualFold(label.Key, newLabel.Key) {
				// Update the value for an existing key.
				label.Value = newLabel.Value
				found = true
				break
			}
		}
		// Add a new label if the key doesn't exist in the original slice.
		if !found {
			results = append(results, newLabel)
		}
	}

	// Filter out labels with an empty value.
	res := results[:0]
	for _, l := range results {
		if l.Value != "" {
			res = append(res, l)
		}
	}
	return res
}

// StoresInfo contains information about all stores.
type StoresInfo struct {
	syncutil.RWMutex
	stores map[uint64]*StoreInfo
}

// NewStoresInfo create a StoresInfo with map of storeID to StoreInfo
func NewStoresInfo() *StoresInfo {
	return &StoresInfo{
		stores: make(map[uint64]*StoreInfo),
	}
}

/* Stores read operations */

// GetStore returns a copy of the StoreInfo with the specified storeID.
func (s *StoresInfo) GetStore(storeID uint64) *StoreInfo {
	s.RLock()
	defer s.RUnlock()
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

// GetStores gets a complete set of StoreInfo.
func (s *StoresInfo) GetStores() []*StoreInfo {
	s.RLock()
	defer s.RUnlock()
	stores := make([]*StoreInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store)
	}
	return stores
}

// GetMetaStores gets a complete set of metapb.Store.
func (s *StoresInfo) GetMetaStores() []*metapb.Store {
	s.RLock()
	defer s.RUnlock()
	stores := make([]*metapb.Store, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.GetMeta())
	}
	return stores
}

// GetStoreIDs returns a list of store ids.
func (s *StoresInfo) GetStoreIDs() []uint64 {
	s.RLock()
	defer s.RUnlock()
	count := len(s.stores)
	storeIDs := make([]uint64, 0, count)
	for _, store := range s.stores {
		storeIDs = append(storeIDs, store.GetID())
	}
	return storeIDs
}

// GetFollowerStores returns all Stores that contains the region's follower peer.
func (s *StoresInfo) GetFollowerStores(region *RegionInfo) []*StoreInfo {
	s.RLock()
	defer s.RUnlock()
	var stores []*StoreInfo
	for id := range region.GetFollowers() {
		if store, ok := s.stores[id]; ok && store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetRegionStores returns all Stores that contains the region's peer.
func (s *StoresInfo) GetRegionStores(region *RegionInfo) []*StoreInfo {
	s.RLock()
	defer s.RUnlock()
	var stores []*StoreInfo
	for id := range region.GetStoreIDs() {
		if store, ok := s.stores[id]; ok && store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetLeaderStore returns all Stores that contains the region's leader peer.
func (s *StoresInfo) GetLeaderStore(region *RegionInfo) *StoreInfo {
	s.RLock()
	defer s.RUnlock()
	if store, ok := s.stores[region.GetLeader().GetStoreId()]; ok && store != nil {
		return store
	}
	return nil
}

// GetStoreCount returns the total count of storeInfo.
func (s *StoresInfo) GetStoreCount() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.stores)
}

// GetNonWitnessVoterStores returns all Stores that contains the non-witness's voter peer.
func (s *StoresInfo) GetNonWitnessVoterStores(region *RegionInfo) []*StoreInfo {
	s.RLock()
	defer s.RUnlock()
	var stores []*StoreInfo
	for id := range region.GetNonWitnessVoters() {
		if store, ok := s.stores[id]; ok && store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

/* Stores write operations */

// PutStore sets a StoreInfo with storeID.
func (s *StoresInfo) PutStore(store *StoreInfo) {
	s.Lock()
	defer s.Unlock()
	s.putStoreLocked(store)
}

// putStoreLocked sets a StoreInfo with storeID.
func (s *StoresInfo) putStoreLocked(store *StoreInfo) {
	s.stores[store.GetID()] = store
}

// ResetStores resets the store cache.
func (s *StoresInfo) ResetStores() {
	s.Lock()
	defer s.Unlock()
	s.stores = make(map[uint64]*StoreInfo)
}

// PauseLeaderTransfer pauses a StoreInfo with storeID.
func (s *StoresInfo) PauseLeaderTransfer(storeID uint64) error {
	s.Lock()
	defer s.Unlock()
	log.Info("pause store leader transfer", zap.Uint64("store-id", storeID))
	store, ok := s.stores[storeID]
	if !ok {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}
	if !store.AllowLeaderTransfer() {
		return errs.ErrPauseLeaderTransfer.FastGenByArgs(storeID)
	}
	s.stores[storeID] = store.Clone(PauseLeaderTransfer())
	return nil
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected
// as source or target of TransferLeader again.
func (s *StoresInfo) ResumeLeaderTransfer(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	log.Info("resume store leader transfer", zap.Uint64("store-id", storeID))
	store, ok := s.stores[storeID]
	if !ok {
		log.Warn("try to clean a store's pause state, but it is not found. It may be cleanup",
			zap.Uint64("store-id", storeID))
		return
	}
	s.stores[storeID] = store.Clone(ResumeLeaderTransfer())
}

// SlowStoreEvicted marks a store as a slow store and prevents transferring
// leader to the store
func (s *StoresInfo) SlowStoreEvicted(storeID uint64) error {
	s.Lock()
	defer s.Unlock()
	store, ok := s.stores[storeID]
	if !ok {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}
	if store.EvictedAsSlowStore() {
		return errs.ErrSlowStoreEvicted.FastGenByArgs(storeID)
	}
	s.stores[storeID] = store.Clone(SlowStoreEvicted())
	return nil
}

// SlowStoreRecovered cleans the evicted state of a store.
func (s *StoresInfo) SlowStoreRecovered(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	store, ok := s.stores[storeID]
	if !ok {
		log.Warn("try to clean a store's evicted as a slow store state, but it is not found. It may be cleanup",
			zap.Uint64("store-id", storeID))
		return
	}
	s.stores[storeID] = store.Clone(SlowStoreRecovered())
}

// SlowTrendEvicted marks a store as a slow trend and prevents transferring
// leader to the store
func (s *StoresInfo) SlowTrendEvicted(storeID uint64) error {
	s.Lock()
	defer s.Unlock()
	store, ok := s.stores[storeID]
	if !ok {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}
	if store.IsEvictedAsSlowTrend() {
		return errs.ErrSlowTrendEvicted.FastGenByArgs(storeID)
	}
	s.stores[storeID] = store.Clone(SlowTrendEvicted())
	return nil
}

// SlowTrendRecovered cleans the evicted by trend state of a store.
func (s *StoresInfo) SlowTrendRecovered(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	store, ok := s.stores[storeID]
	if !ok {
		log.Warn("try to clean a store's evicted by trend as a slow store state, but it is not found. It may be cleanup",
			zap.Uint64("store-id", storeID))
		return
	}
	s.stores[storeID] = store.Clone(SlowTrendRecovered())
}

// ResetStoreLimit resets the limit for a specific store.
func (s *StoresInfo) ResetStoreLimit(storeID uint64, limitType storelimit.Type, ratePerSec ...float64) {
	s.Lock()
	defer s.Unlock()
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(ResetStoreLimit(limitType, ratePerSec...))
	}
}

// DeleteStore deletes tombstone record form store
func (s *StoresInfo) DeleteStore(store *StoreInfo) {
	s.Lock()
	defer s.Unlock()
	delete(s.stores, store.GetID())
}

// UpdateStoreStatus updates the information of the store.
func (s *StoresInfo) UpdateStoreStatus(storeID uint64, leaderCount, regionCount, witnessCount, learnerCount, pendingPeerCount int, leaderSize int64, regionSize int64) {
	s.Lock()
	defer s.Unlock()
	if store, ok := s.stores[storeID]; ok {
		newStore := store.ShallowClone(SetLeaderCount(leaderCount),
			SetRegionCount(regionCount),
			SetWitnessCount(witnessCount),
			SetLearnerCount(learnerCount),
			SetPendingPeerCount(pendingPeerCount),
			SetLeaderSize(leaderSize),
			SetRegionSize(regionSize))
		s.putStoreLocked(newStore)
	}
}

// IsStoreContainLabel returns if the store contains the given label.
func IsStoreContainLabel(store *metapb.Store, key, value string) bool {
	for _, l := range store.GetLabels() {
		if l.GetKey() == key && l.GetValue() == value {
			return true
		}
	}
	return false
}

// IsAvailableForMinResolvedTS returns if the store is available for min resolved ts.
func IsAvailableForMinResolvedTS(s *StoreInfo) bool {
	// If a store is tombstone or no leader, it is not meaningful for min resolved ts.
	// And we will skip tiflash, because it does not report min resolved ts.
	return !s.IsRemoved() && !s.IsTiFlash() && s.GetLeaderCount() != 0
}
