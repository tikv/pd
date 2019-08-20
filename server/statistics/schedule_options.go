package statistics

import "time"

// ScheduleOptions is an interface to access configurations.
// TODO: merge the Options to schedule.Options
type ScheduleOptions interface {
	GetLocationLabels() []string

	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64
	GetTolerantSizeRatio() float64
	GetStoreBalanceRate() float64

	GetSchedulerMaxWaitingOperator() uint64
	GetLeaderScheduleLimit(name string) uint64
	GetRegionScheduleLimit(name string) uint64
	GetReplicaScheduleLimit(name string) uint64
	GetMergeScheduleLimit(name string) uint64
	GetHotRegionScheduleLimit(name string) uint64
	GetMaxReplicas(name string) int
	GetHotRegionCacheHitsThreshold() int
	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64

	IsRaftLearnerEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool

	GetMaxStoreDownTime() time.Duration
}
