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

package opt

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/pd/v4/server/statistics"
)

// Options for schedulers.
type Options interface {
	GetLeaderScheduleLimit() uint64
	GetRegionScheduleLimit() uint64
	GetReplicaScheduleLimit() uint64
	GetMergeScheduleLimit() uint64
	GetHotRegionScheduleLimit() uint64

	// store limit
	GetStoreBalanceRate() float64

	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxStoreDownTime() time.Duration
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetSplitMergeInterval() time.Duration
	IsOneWayMergeEnabled() bool
	IsCrossTableMergeEnabled() bool

	GetMaxReplicas() int
	GetLocationLabels() []string
	GetStrictlyMatchLabel() bool
	IsPlacementRulesEnabled() bool

	GetHotRegionCacheHitsThreshold() int
	GetTolerantSizeRatio() float64
	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64
	GetSchedulerMaxWaitingOperator() uint64

	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsLocationReplacementEnabled() bool
	IsDebugMetricsEnabled() bool
	GetLeaderSchedulePolicy() core.SchedulePolicy
	GetKeyType() core.KeyType

	RemoveScheduler(name string) error

	CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool
}

const (
	// RejectLeader is the label property type that suggests a store should not
	// have any region leaders.
	RejectLeader = "reject-leader"
)

// Cluster provides an overview of a cluster's regions distribution.
// TODO: This interface should be moved to a better place.
type Cluster interface {
	core.RegionSetInformer
	core.StoreSetInformer
	core.StoreSetController

	statistics.RegionStatInformer
	statistics.StoreStatInformer
	Options

	AllocID() (uint64, error)
	FitRegion(*core.RegionInfo) *placement.RegionFit
}

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(*pdpb.RegionHeartbeatResponse) error
}

// HeartbeatStreams is an interface of async region heartbeat.
type HeartbeatStreams interface {
	SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse)
	BindStream(storeID uint64, stream HeartbeatStream)
}
