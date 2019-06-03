// Copyright 2017 PingCAP, Inc.
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

package schedule

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"go.uber.org/zap"
)

// MockHeadbeatStream is used to mock HeadbeatStream for test use.
type MockHeadbeatStream struct{}

// SendMsg is used to send the message.
func (m MockHeadbeatStream) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	return
}

// MockCluster is used to mock clusterInfo for test use.
type MockCluster struct {
	*BasicCluster
	*core.MockIDAllocator
	*MockSchedulerOptions
	ID uint64
}

// NewMockCluster creates a new MockCluster
func NewMockCluster(opt *MockSchedulerOptions) *MockCluster {
	return &MockCluster{
		BasicCluster:         NewBasicCluster(),
		MockIDAllocator:      core.NewMockIDAllocator(),
		MockSchedulerOptions: opt,
	}
}

func (mc *MockCluster) allocID() (uint64, error) {
	return mc.Alloc()
}

// ScanRegions scans region with start key, until number greater than limit.
func (mc *MockCluster) ScanRegions(startKey []byte, limit int) []*core.RegionInfo {
	return mc.Regions.ScanRange(startKey, limit)
}

// LoadRegion puts region info without leader
func (mc *MockCluster) LoadRegion(regionID uint64, followerIds ...uint64) {
	//  regions load from etcd will have no leader
	r := mc.newMockRegionInfo(regionID, 0, followerIds...).Clone(core.WithLeader(nil))
	mc.PutRegion(r)
}

// GetStoreRegionCount gets region count with a given store.
func (mc *MockCluster) GetStoreRegionCount(storeID uint64) int {
	return mc.Regions.GetStoreRegionCount(storeID)
}

// IsRegionHot checks if the region is hot
func (mc *MockCluster) IsRegionHot(id uint64) bool {
	return mc.BasicCluster.IsRegionHot(id, mc.GetHotRegionCacheHitsThreshold())
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (mc *MockCluster) RandHotRegionFromStore(store uint64, kind FlowKind) *core.RegionInfo {
	r := mc.HotCache.RandHotRegionFromStore(store, kind, mc.GetHotRegionCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return mc.GetRegion(r.RegionID)
}

// AllocPeer allocs a new peer on a store.
func (mc *MockCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := mc.allocID()
	if err != nil {
		log.Error("failed to alloc peer", zap.Error(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

// SetStoreUp sets store state to be up.
func (mc *MockCluster) SetStoreUp(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// SetStoreDisconnect changes a store's state to disconnected.
func (mc *MockCluster) SetStoreDisconnect(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Now().Add(-time.Second*30)),
	)
	mc.PutStore(newStore)
}

// SetStoreDown sets store down.
func (mc *MockCluster) SetStoreDown(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	mc.PutStore(newStore)
}

// SetStoreOffline sets store state to be offline.
func (mc *MockCluster) SetStoreOffline(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	mc.PutStore(newStore)
}

// SetStoreBusy sets store busy.
func (mc *MockCluster) SetStoreBusy(storeID uint64, busy bool) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.IsBusy = busy
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// AddLeaderStore adds store with specified count of leader.
func (mc *MockCluster) AddLeaderStore(storeID uint64, leaderCount int) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = 1000 * (1 << 20)
	stats.Available = stats.Capacity - uint64(leaderCount)*10
	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(store)
}

// AddRegionStore adds store with specified count of region.
func (mc *MockCluster) AddRegionStore(storeID uint64, regionCount int) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = 1000 * (1 << 20)
	stats.Available = stats.Capacity - uint64(regionCount)*10
	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(store)
}

// AddLabelsStore adds store with specified count of region and labels.
func (mc *MockCluster) AddLabelsStore(storeID uint64, regionCount int, labels map[string]string) {
	var newLabels []*metapb.StoreLabel
	for k, v := range labels {
		newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
	}
	stats := &pdpb.StoreStats{}
	stats.Capacity = 1000 * (1 << 20)
	stats.Available = stats.Capacity - uint64(regionCount)*10
	store := core.NewStoreInfo(
		&metapb.Store{
			Id:     storeID,
			Labels: newLabels,
		},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(store)
}

// AddLeaderRegion adds region with specified leader and followers.
func (mc *MockCluster) AddLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	origin := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	region := origin.Clone(core.SetApproximateSize(10), core.SetApproximateKeys(10))
	mc.PutRegion(region)
}

// AddLeaderRegionWithRange adds region with specified leader, followers and key range.
func (mc *MockCluster) AddLeaderRegionWithRange(regionID uint64, startKey string, endKey string, leaderID uint64, followerIds ...uint64) {
	o := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r := o.Clone(
		core.WithStartKey([]byte(startKey)),
		core.WithEndKey([]byte(endKey)),
	)
	mc.PutRegion(r)
}

// AddLeaderRegionWithReadInfo adds region with specified leader, followers and read info.
func (mc *MockCluster) AddLeaderRegionWithReadInfo(regionID uint64, leaderID uint64, readBytes uint64, followerIds ...uint64) {
	r := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r = r.Clone(core.SetReadBytes(readBytes))
	isUpdate, item := mc.BasicCluster.CheckReadStatus(r)
	if isUpdate {
		mc.HotCache.Update(regionID, item, ReadFlow)
	}
	mc.PutRegion(r)
}

// AddLeaderRegionWithWriteInfo adds region with specified leader, followers and write info.
func (mc *MockCluster) AddLeaderRegionWithWriteInfo(regionID uint64, leaderID uint64, writtenBytes uint64, followerIds ...uint64) {
	r := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r = r.Clone(core.SetWrittenBytes(writtenBytes))
	isUpdate, item := mc.BasicCluster.CheckWriteStatus(r)
	if isUpdate {
		mc.HotCache.Update(regionID, item, WriteFlow)
	}
	mc.PutRegion(r)
}

// UpdateStoreLeaderWeight updates store leader weight.
func (mc *MockCluster) UpdateStoreLeaderWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetLeaderWeight(weight))
	mc.PutStore(newStore)
}

// UpdateStoreRegionWeight updates store region weight.
func (mc *MockCluster) UpdateStoreRegionWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetRegionWeight(weight))
	mc.PutStore(newStore)
}

// UpdateStoreLeaderSize updates store leader size.
func (mc *MockCluster) UpdateStoreLeaderSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.Available = newStats.Capacity - uint64(store.GetLeaderSize())
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetLeaderSize(size),
	)
	mc.PutStore(newStore)
}

// UpdateStoreRegionSize updates store region size.
func (mc *MockCluster) UpdateStoreRegionSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.Available = newStats.Capacity - uint64(store.GetRegionSize())
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetRegionSize(size),
	)
	mc.PutStore(newStore)
}

// UpdateLeaderCount updates store leader count.
func (mc *MockCluster) UpdateLeaderCount(storeID uint64, leaderCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*10),
	)
	mc.PutStore(newStore)
}

// UpdateRegionCount updates store region count.
func (mc *MockCluster) UpdateRegionCount(storeID uint64, regionCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*10),
	)
	mc.PutStore(newStore)
}

// UpdateSnapshotCount updates store snapshot count.
func (mc *MockCluster) UpdateSnapshotCount(storeID uint64, snapshotCount int) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.ApplyingSnapCount = uint32(snapshotCount)
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdatePendingPeerCount updates store pending peer count.
func (mc *MockCluster) UpdatePendingPeerCount(storeID uint64, pendingPeerCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetPendingPeerCount(pendingPeerCount))
	mc.PutStore(newStore)
}

// UpdateStorageRatio updates store storage ratio count.
func (mc *MockCluster) UpdateStorageRatio(storeID uint64, usedRatio, availableRatio float64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.Capacity = 1000 * (1 << 20)
	newStats.UsedSize = uint64(float64(newStats.Capacity) * usedRatio)
	newStats.Available = uint64(float64(newStats.Capacity) * availableRatio)
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdateStorageWrittenBytes updates store written bytes.
func (mc *MockCluster) UpdateStorageWrittenBytes(storeID uint64, bytesWritten uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.BytesWritten = bytesWritten
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - storeHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdateStorageReadBytes updates store read bytes.
func (mc *MockCluster) UpdateStorageReadBytes(storeID uint64, bytesRead uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.BytesRead = bytesRead
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - storeHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdateStoreStatus updates store status.
func (mc *MockCluster) UpdateStoreStatus(id uint64) {
	leaderCount := mc.Regions.GetStoreLeaderCount(id)
	regionCount := mc.Regions.GetStoreRegionCount(id)
	pendingPeerCount := mc.Regions.GetStorePendingPeerCount(id)
	leaderSize := mc.Regions.GetStoreLeaderRegionSize(id)
	regionSize := mc.Regions.GetStoreRegionSize(id)
	store := mc.Stores.GetStore(id)
	stats := &pdpb.StoreStats{}
	stats.Capacity = 1000 * (1 << 20)
	stats.Available = stats.Capacity - uint64(store.GetRegionSize())
	stats.UsedSize = uint64(store.GetRegionSize())
	newStore := store.Clone(
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetRegionCount(regionCount),
		core.SetPendingPeerCount(pendingPeerCount),
		core.SetLeaderSize(leaderSize),
		core.SetRegionSize(regionSize),
	)
	mc.PutStore(newStore)
}

func (mc *MockCluster) newMockRegionInfo(regionID uint64, leaderID uint64, followerIds ...uint64) *core.RegionInfo {
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:   []byte(fmt.Sprintf("%20d", regionID+1)),
	}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}

	return core.NewRegionInfo(region, leader)
}

// ApplyOperatorStep mocks apply operator step.
func (mc *MockCluster) ApplyOperatorStep(region *core.RegionInfo, op *Operator) *core.RegionInfo {
	if step := op.Check(region); step != nil {
		switch s := step.(type) {
		case TransferLeader:
			region = region.Clone(core.WithLeader(region.GetStorePeer(s.ToStore)))
		case AddPeer:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add peer that exists")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case AddLightPeer:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add peer that exists")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case RemovePeer:
			if region.GetStorePeer(s.FromStore) == nil {
				panic("Remove peer that doesn't exist")
			}
			if region.GetLeader().GetStoreId() == s.FromStore {
				panic("Cannot remove the leader peer")
			}
			region = region.Clone(core.WithRemoveStorePeer(s.FromStore))
		case AddLearner:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add learner that exists")
			}
			peer := &metapb.Peer{
				Id:        s.PeerID,
				StoreId:   s.ToStore,
				IsLearner: true,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case AddLightLearner:
			if region.GetStorePeer(s.ToStore) != nil {
				panic("Add learner that exists")
			}
			peer := &metapb.Peer{
				Id:        s.PeerID,
				StoreId:   s.ToStore,
				IsLearner: true,
			}
			region = region.Clone(core.WithAddPeer(peer))
		case PromoteLearner:
			if region.GetStoreLearner(s.ToStore) == nil {
				panic("Promote peer that doesn't exist")
			}
			peer := &metapb.Peer{
				Id:      s.PeerID,
				StoreId: s.ToStore,
			}
			region = region.Clone(core.WithRemoveStorePeer(s.ToStore), core.WithAddPeer(peer))
		default:
			panic("Unknown operator step")
		}
	}
	return region
}

// ApplyOperator mocks apply operator.
func (mc *MockCluster) ApplyOperator(op *Operator) {
	origin := mc.GetRegion(op.RegionID())
	region := origin
	for !op.IsFinish() {
		region = mc.ApplyOperatorStep(region, op)
	}
	mc.PutRegion(region)
	for id := range region.GetStoreIds() {
		mc.UpdateStoreStatus(id)
	}
	for id := range origin.GetStoreIds() {
		mc.UpdateStoreStatus(id)
	}
}

// GetOpt mocks method.
func (mc *MockCluster) GetOpt() NamespaceOptions {
	return mc.MockSchedulerOptions
}

// GetLeaderScheduleLimit mocks method.
func (mc *MockCluster) GetLeaderScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetLeaderScheduleLimit(namespace.DefaultNamespace)
}

// GetRegionScheduleLimit mocks method.
func (mc *MockCluster) GetRegionScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetRegionScheduleLimit(namespace.DefaultNamespace)
}

// GetReplicaScheduleLimit mocks method.
func (mc *MockCluster) GetReplicaScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetReplicaScheduleLimit(namespace.DefaultNamespace)
}

// GetMergeScheduleLimit mocks method.
func (mc *MockCluster) GetMergeScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetMergeScheduleLimit(namespace.DefaultNamespace)
}

// GetHotRegionScheduleLimit mocks method.
func (mc *MockCluster) GetHotRegionScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetHotRegionScheduleLimit(namespace.DefaultNamespace)
}

// GetMaxReplicas mocks method.
func (mc *MockCluster) GetMaxReplicas() int {
	return mc.MockSchedulerOptions.GetMaxReplicas(namespace.DefaultNamespace)
}

// CheckLabelProperty checks label property.
func (mc *MockCluster) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	for _, cfg := range mc.LabelProperties[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

const (
	defaultMaxReplicas                 = 3
	defaultMaxSnapshotCount            = 3
	defaultMaxPendingPeerCount         = 16
	defaultMaxMergeRegionSize          = 0
	defaultMaxMergeRegionKeys          = 0
	defaultSplitMergeInterval          = 0
	defaultTwoWayMerge                 = false
	defaultMaxStoreDownTime            = 30 * time.Minute
	defaultLeaderScheduleLimit         = 4
	defaultRegionScheduleLimit         = 4
	defaultReplicaScheduleLimit        = 8
	defaultMergeScheduleLimit          = 8
	defaultHotRegionScheduleLimit      = 2
	defaultStoreBalanceRate            = 1
	defaultTolerantSizeRatio           = 2.5
	defaultLowSpaceRatio               = 0.8
	defaultHighSpaceRatio              = 0.6
	defaultHotRegionCacheHitsThreshold = 3
	defaultStrictlyMatchLabel          = true
)

// MockSchedulerOptions is a mock of SchedulerOptions
// which implements Options interface
type MockSchedulerOptions struct {
	RegionScheduleLimit          uint64
	LeaderScheduleLimit          uint64
	ReplicaScheduleLimit         uint64
	MergeScheduleLimit           uint64
	HotRegionScheduleLimit       uint64
	StoreBalanceRate             float64
	MaxSnapshotCount             uint64
	MaxPendingPeerCount          uint64
	MaxMergeRegionSize           uint64
	MaxMergeRegionKeys           uint64
	SplitMergeInterval           time.Duration
	EnableTwoWayMerge            bool
	MaxStoreDownTime             time.Duration
	MaxReplicas                  int
	LocationLabels               []string
	StrictlyMatchLabel           bool
	HotRegionCacheHitsThreshold  int
	TolerantSizeRatio            float64
	LowSpaceRatio                float64
	HighSpaceRatio               float64
	DisableLearner               bool
	DisableRemoveDownReplica     bool
	DisableReplaceOfflineReplica bool
	DisableMakeUpReplica         bool
	DisableRemoveExtraReplica    bool
	DisableLocationReplacement   bool
	DisableNamespaceRelocation   bool
	LabelProperties              map[string][]*metapb.StoreLabel
}

// NewMockSchedulerOptions creates a mock schedule option.
func NewMockSchedulerOptions() *MockSchedulerOptions {
	mso := &MockSchedulerOptions{}
	mso.RegionScheduleLimit = defaultRegionScheduleLimit
	mso.LeaderScheduleLimit = defaultLeaderScheduleLimit
	mso.ReplicaScheduleLimit = defaultReplicaScheduleLimit
	mso.MergeScheduleLimit = defaultMergeScheduleLimit
	mso.HotRegionScheduleLimit = defaultHotRegionScheduleLimit
	mso.StoreBalanceRate = defaultStoreBalanceRate
	mso.MaxSnapshotCount = defaultMaxSnapshotCount
	mso.MaxMergeRegionSize = defaultMaxMergeRegionSize
	mso.MaxMergeRegionKeys = defaultMaxMergeRegionKeys
	mso.SplitMergeInterval = defaultSplitMergeInterval
	mso.EnableTwoWayMerge = defaultTwoWayMerge
	mso.MaxStoreDownTime = defaultMaxStoreDownTime
	mso.MaxReplicas = defaultMaxReplicas
	mso.StrictlyMatchLabel = defaultStrictlyMatchLabel
	mso.HotRegionCacheHitsThreshold = defaultHotRegionCacheHitsThreshold
	mso.MaxPendingPeerCount = defaultMaxPendingPeerCount
	mso.TolerantSizeRatio = defaultTolerantSizeRatio
	mso.LowSpaceRatio = defaultLowSpaceRatio
	mso.HighSpaceRatio = defaultHighSpaceRatio
	return mso
}

// GetLeaderScheduleLimit mock method
func (mso *MockSchedulerOptions) GetLeaderScheduleLimit(name string) uint64 {
	return mso.LeaderScheduleLimit
}

// GetRegionScheduleLimit mock method
func (mso *MockSchedulerOptions) GetRegionScheduleLimit(name string) uint64 {
	return mso.RegionScheduleLimit
}

// GetReplicaScheduleLimit mock method
func (mso *MockSchedulerOptions) GetReplicaScheduleLimit(name string) uint64 {
	return mso.ReplicaScheduleLimit
}

// GetMergeScheduleLimit mock method
func (mso *MockSchedulerOptions) GetMergeScheduleLimit(name string) uint64 {
	return mso.MergeScheduleLimit
}

// GetHotRegionScheduleLimit mock method
func (mso *MockSchedulerOptions) GetHotRegionScheduleLimit(name string) uint64 {
	return mso.HotRegionScheduleLimit
}

// GetStoreBalanceRate mock method
func (mso *MockSchedulerOptions) GetStoreBalanceRate() float64 {
	return mso.StoreBalanceRate
}

// GetMaxSnapshotCount mock method
func (mso *MockSchedulerOptions) GetMaxSnapshotCount() uint64 {
	return mso.MaxSnapshotCount
}

// GetMaxPendingPeerCount mock method
func (mso *MockSchedulerOptions) GetMaxPendingPeerCount() uint64 {
	return mso.MaxPendingPeerCount
}

// GetMaxMergeRegionSize mock method
func (mso *MockSchedulerOptions) GetMaxMergeRegionSize() uint64 {
	return mso.MaxMergeRegionSize
}

// GetMaxMergeRegionKeys mock method
func (mso *MockSchedulerOptions) GetMaxMergeRegionKeys() uint64 {
	return mso.MaxMergeRegionKeys
}

// GetSplitMergeInterval mock method
func (mso *MockSchedulerOptions) GetSplitMergeInterval() time.Duration {
	return mso.SplitMergeInterval
}

// GetEnableTwoWayMerge mock method
func (mso *MockSchedulerOptions) GetEnableTwoWayMerge() bool {
	return mso.EnableTwoWayMerge
}

// GetMaxStoreDownTime mock method
func (mso *MockSchedulerOptions) GetMaxStoreDownTime() time.Duration {
	return mso.MaxStoreDownTime
}

// GetMaxReplicas mock method
func (mso *MockSchedulerOptions) GetMaxReplicas(name string) int {
	return mso.MaxReplicas
}

// GetLocationLabels mock method
func (mso *MockSchedulerOptions) GetLocationLabels() []string {
	return mso.LocationLabels
}

// GetStrictlyMatchLabel mock method
func (mso *MockSchedulerOptions) GetStrictlyMatchLabel() bool {
	return mso.StrictlyMatchLabel
}

// GetHotRegionCacheHitsThreshold mock method
func (mso *MockSchedulerOptions) GetHotRegionCacheHitsThreshold() int {
	return mso.HotRegionCacheHitsThreshold
}

// GetTolerantSizeRatio mock method
func (mso *MockSchedulerOptions) GetTolerantSizeRatio() float64 {
	return mso.TolerantSizeRatio
}

// GetLowSpaceRatio mock method
func (mso *MockSchedulerOptions) GetLowSpaceRatio() float64 {
	return mso.LowSpaceRatio
}

// GetHighSpaceRatio mock method
func (mso *MockSchedulerOptions) GetHighSpaceRatio() float64 {
	return mso.HighSpaceRatio
}

// SetMaxReplicas mock method
func (mso *MockSchedulerOptions) SetMaxReplicas(replicas int) {
	mso.MaxReplicas = replicas
}

// IsRaftLearnerEnabled mock method
func (mso *MockSchedulerOptions) IsRaftLearnerEnabled() bool {
	return !mso.DisableLearner
}

// IsRemoveDownReplicaEnabled mock method.
func (mso *MockSchedulerOptions) IsRemoveDownReplicaEnabled() bool {
	return !mso.DisableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled mock method.
func (mso *MockSchedulerOptions) IsReplaceOfflineReplicaEnabled() bool {
	return !mso.DisableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled mock method.
func (mso *MockSchedulerOptions) IsMakeUpReplicaEnabled() bool {
	return !mso.DisableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled mock method.
func (mso *MockSchedulerOptions) IsRemoveExtraReplicaEnabled() bool {
	return !mso.DisableRemoveExtraReplica
}

// IsLocationReplacementEnabled mock method.
func (mso *MockSchedulerOptions) IsLocationReplacementEnabled() bool {
	return !mso.DisableLocationReplacement
}

// IsNamespaceRelocationEnabled mock method.
func (mso *MockSchedulerOptions) IsNamespaceRelocationEnabled() bool {
	return !mso.DisableNamespaceRelocation
}

// MockHeartbeatStreams is used to mock heartbeatstreams for test use.
type MockHeartbeatStreams struct {
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	msgCh     chan *pdpb.RegionHeartbeatResponse
}

// NewMockHeartbeatStreams creates a new MockHeartbeatStreams.
func NewMockHeartbeatStreams(clusterID uint64) *MockHeartbeatStreams {
	ctx, cancel := context.WithCancel(context.Background())
	hs := &MockHeartbeatStreams{
		ctx:       ctx,
		cancel:    cancel,
		clusterID: clusterID,
		msgCh:     make(chan *pdpb.RegionHeartbeatResponse, 1024),
	}
	return hs
}

// SendMsg is used to send the message.
func (mhs *MockHeartbeatStreams) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	if region.GetLeader() == nil {
		return
	}

	msg.Header = &pdpb.ResponseHeader{ClusterId: mhs.clusterID}
	msg.RegionId = region.GetID()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.GetLeader()

	select {
	case mhs.msgCh <- msg:
	case <-mhs.ctx.Done():
	}
}
