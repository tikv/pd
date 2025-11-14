// Copyright 2023 TiKV Project Authors.
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

package config

import (
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

// RejectLeader is the label property type that suggests a store should not
// have any region leaders.
const RejectLeader = "reject-leader"

var schedulerMap sync.Map

// RegisterScheduler registers the scheduler type.
func RegisterScheduler(typ types.CheckerSchedulerType) {
	schedulerMap.Store(typ, struct{}{})
}

// SchedulerConfigProvider is the interface for scheduler configurations.
type SchedulerConfigProvider interface {
	SharedConfigProvider

	SetSchedulingAllowanceStatus(bool, string)
	GetStoresLimit() map[uint64]StoreLimitConfig

	IsSchedulerDisabled(types.CheckerSchedulerType) bool
	AddSchedulerCfg(types.CheckerSchedulerType, []string)
	RemoveSchedulerCfg(types.CheckerSchedulerType)
	Persist(endpoint.ConfigStorage) error

	GetRegionScheduleLimit() uint64
	GetLeaderScheduleLimit() uint64
	GetHotRegionScheduleLimit() uint64
	GetWitnessScheduleLimit() uint64

	GetHotRegionCacheHitsThreshold() int
	GetMaxMovableHotPeerSize() int64
	IsTraceRegionFlow() bool

	GetTolerantSizeRatio() float64
	GetLeaderSchedulePolicy() constant.SchedulePolicy

	IsDebugMetricsEnabled() bool
	IsDiagnosticAllowed() bool
	GetSlowStoreEvictingAffectedStoreRatioThreshold() float64

	GetScheduleConfig() *ScheduleConfig
	SetScheduleConfig(*ScheduleConfig)
}

// CheckerConfigProvider is the interface for checker configurations.
type CheckerConfigProvider interface {
	SharedConfigProvider
	StoreConfigProvider

	GetSwitchWitnessInterval() time.Duration
	IsRemoveExtraReplicaEnabled() bool
	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsLocationReplacementEnabled() bool
	GetIsolationLevel() string
	GetSplitMergeInterval() time.Duration
	GetPatrolRegionInterval() time.Duration
	GetPatrolRegionWorkerCount() int
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetReplicaScheduleLimit() uint64
}

// SharedConfigProvider is the interface for shared configurations.
type SharedConfigProvider interface {
	GetMaxReplicas() int
	IsPlacementRulesEnabled() bool
	IsAffinitySchedulingEnabled() bool
	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64
	GetMaxStoreDownTime() time.Duration
	GetLocationLabels() []string
	CheckLabelProperty(string, []*metapb.StoreLabel) bool
	GetClusterVersion() *semver.Version
	IsUseJointConsensus() bool
	GetKeyType() constant.KeyType
	IsCrossTableMergeEnabled() bool
	IsOneWayMergeEnabled() bool
	GetMergeScheduleLimit() uint64
	GetRegionScoreFormulaVersion() string
	GetSchedulerMaxWaitingOperator() uint64
	GetStoreLimitByType(uint64, storelimit.Type) float64
	IsWitnessAllowed() bool
	IsPlacementRulesCacheEnabled() bool
	SetHaltScheduling(bool, string)
	GetHotRegionCacheHitsThreshold() int

	// for test purpose
	SetPlacementRuleEnabled(bool)
	SetPlacementRulesCacheEnabled(bool)
	SetEnableWitness(bool)
}

// ConfProvider is the interface that wraps the ConfProvider related methods.
type ConfProvider interface {
	SchedulerConfigProvider
	CheckerConfigProvider
	StoreConfigProvider
	// for test purpose
	SetPlacementRuleEnabled(bool)
	SetSplitMergeInterval(time.Duration)
	SetMaxReplicas(int)
	SetAllStoresLimit(storelimit.Type, float64)
}

// StoreConfigProvider is the interface that wraps the StoreConfigProvider related methods.
type StoreConfigProvider interface {
	GetRegionMaxSize() uint64
	GetRegionMaxKeys() uint64
	GetRegionSplitSize() uint64
	GetRegionSplitKeys() uint64
	CheckRegionSize(uint64, uint64) error
	CheckRegionKeys(uint64, uint64) error
	IsEnableRegionBucket() bool
	IsRaftKV2() bool
}
