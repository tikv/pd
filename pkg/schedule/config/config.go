package config

import (
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
)

// RejectLeader is the label property type that suggests a store should not
// have any region leaders.
const RejectLeader = "reject-leader"

var schedulerMap sync.Map

// RegisterScheduler registers the scheduler type.
func RegisterScheduler(typ string) {
	schedulerMap.Store(typ, struct{}{})
}

// IsSchedulerRegistered checks if the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap.Load(name)
	return ok
}

// Config is the interface that wraps the Config related methods.
type Config interface {
	IsSchedulingHalted() bool

	GetReplicaScheduleLimit() uint64
	GetRegionScheduleLimit() uint64
	GetMergeScheduleLimit() uint64
	GetLeaderScheduleLimit() uint64
	GetHotRegionScheduleLimit() uint64
	GetWitnessScheduleLimit() uint64

	GetHotRegionCacheHitsThreshold() int
	GetMaxMovableHotPeerSize() int64
	IsTraceRegionFlow() bool

	GetSplitMergeInterval() time.Duration
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetKeyType() constant.KeyType
	IsOneWayMergeEnabled() bool
	IsCrossTableMergeEnabled() bool

	IsPlacementRulesEnabled() bool
	IsPlacementRulesCacheEnabled() bool

	GetMaxReplicas() int
	GetPatrolRegionInterval() time.Duration
	GetMaxStoreDownTime() time.Duration
	GetLocationLabels() []string
	GetIsolationLevel() string
	IsReplaceOfflineReplicaEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsLocationReplacementEnabled() bool
	IsRemoveDownReplicaEnabled() bool

	GetSwitchWitnessInterval() time.Duration
	IsWitnessAllowed() bool

	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64
	GetTolerantSizeRatio() float64
	GetLeaderSchedulePolicy() constant.SchedulePolicy
	GetRegionScoreFormulaVersion() string

	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetSchedulerMaxWaitingOperator() uint64
	GetStoreLimitByType(uint64, storelimit.Type) float64
	SetAllStoresLimit(storelimit.Type, float64)
	GetSlowStoreEvictingAffectedStoreRatioThreshold() float64
	IsUseJointConsensus() bool
	CheckLabelProperty(string, []*metapb.StoreLabel) bool
	IsDebugMetricsEnabled() bool
	GetClusterVersion() *semver.Version
	GetStoreLimitVersion() string
	IsDiagnosticAllowed() bool
	// for test purpose
	SetPlacementRuleEnabled(bool)
	SetSplitMergeInterval(time.Duration)
	SetMaxReplicas(int)
	SetPlacementRulesCacheEnabled(bool)
	SetEnableWitness(bool)
	// only for store configuration
	UseRaftV2()
}

// StoreConfig is the interface that wraps the StoreConfig related methods.
type StoreConfig interface {
	GetRegionMaxSize() uint64
	CheckRegionSize(uint64, uint64) error
	CheckRegionKeys(uint64, uint64) error
	IsEnableRegionBucket() bool
	IsRaftKV2() bool
	// for test purpose
	SetRegionBucketEnabled(bool)
}
