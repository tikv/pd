package config

import (
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/storage/endpoint"
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

// SchedulerConfig is the interface for scheduler configurations.
type SchedulerConfig interface {
	SharedConfig

	IsSchedulingHalted() bool

	IsSchedulerDisabled(string) bool
	AddSchedulerCfg(string, []string)
	RemoveSchedulerCfg(string)
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
}

// CheckerConfig is the interface for checker configurations.
type CheckerConfig interface {
	SharedConfig

	GetSwitchWitnessInterval() time.Duration
	IsRemoveExtraReplicaEnabled() bool
	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsLocationReplacementEnabled() bool
	GetIsolationLevel() string
	GetSplitMergeInterval() time.Duration
	GetPatrolRegionInterval() time.Duration
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetReplicaScheduleLimit() uint64
}

// SharedConfig is the interface for shared configurations.
type SharedConfig interface {
	GetMaxReplicas() int
	IsPlacementRulesEnabled() bool
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

	// for test purpose
	SetPlacementRulesCacheEnabled(bool)
	SetEnableWitness(bool)
}

// Config is the interface that wraps the Config related methods.
type Config interface {
	SchedulerConfig
	CheckerConfig
	// for test purpose
	SetPlacementRuleEnabled(bool)
	SetSplitMergeInterval(time.Duration)
	SetMaxReplicas(int)
	SetAllStoresLimit(typ storelimit.Type, ratePerMin float64)
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
