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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	// DefaultMaxReplicas is the default number of replicas for each region.
	DefaultMaxReplicas            = 3
	defaultMaxSnapshotCount       = 64
	defaultMaxPendingPeerCount    = 64
	defaultMaxMergeRegionSize     = 20
	defaultLeaderScheduleLimit    = 4
	defaultRegionScheduleLimit    = 2048
	defaultWitnessScheduleLimit   = 4
	defaultReplicaScheduleLimit   = 64
	defaultMergeScheduleLimit     = 8
	defaultHotRegionScheduleLimit = 4
	defaultTolerantSizeRatio      = 0
	defaultLowSpaceRatio          = 0.8
	defaultHighSpaceRatio         = 0.7
	// defaultHotRegionCacheHitsThreshold is the low hit number threshold of the
	// hot region.
	defaultHotRegionCacheHitsThreshold = 3
	defaultSchedulerMaxWaitingOperator = 5
	defaultHotRegionsReservedDays      = 7
	// When a slow store affected more than 30% of total stores, it will trigger evicting.
	defaultSlowStoreEvictingAffectedStoreRatioThreshold = 0.3
	defaultMaxMovableHotPeerSize                        = int64(512)

	defaultEnableJointConsensus            = true
	defaultEnableTiKVSplitRegion           = true
	defaultEnableHeartbeatBreakdownMetrics = true
	defaultEnableHeartbeatConcurrentRunner = false
	defaultEnableCrossTableMerge           = true
	defaultEnableDiagnostic                = true
	defaultStrictlyMatchLabel              = false
	defaultEnablePlacementRules            = true
	defaultEnableWitness                   = false
	defaultHaltScheduling                  = false

	defaultRegionScoreFormulaVersion = "v2"
	defaultLeaderSchedulePolicy      = "count"
	defaultStoreLimitVersion         = "v1"
	defaultPatrolRegionConcurrency   = 1
	defaultPatrolRegionBatchLimit    = 128
	// DefaultSplitMergeInterval is the default value of config split merge interval.
	DefaultSplitMergeInterval      = time.Hour
	defaultSwitchWitnessInterval   = time.Hour
	defaultPatrolRegionInterval    = 10 * time.Millisecond
	defaultMaxStoreDownTime        = 30 * time.Minute
	defaultHotRegionsWriteInterval = 10 * time.Minute
	// It means we skip the preparing stage after the 48 hours no matter if the store has finished preparing stage.
	defaultMaxStorePreparingTime = 48 * time.Hour
)

var (
	defaultLocationLabels = []string{}
	// DefaultStoreLimit is the default store limit of add peer and remove peer.
	DefaultStoreLimit = StoreLimit{AddPeer: 15, RemovePeer: 15}
	// DefaultTiFlashStoreLimit is the default TiFlash store limit of add peer and remove peer.
	DefaultTiFlashStoreLimit = StoreLimit{AddPeer: 30, RemovePeer: 30}
)

// The following consts are used to identify the config item that needs to set TTL.
const (
	// TTLConfigPrefix is the prefix of the config item that needs to set TTL.
	TTLConfigPrefix = "/config/ttl"

	MaxSnapshotCountKey            = "schedule.max-snapshot-count"
	MaxMergeRegionSizeKey          = "schedule.max-merge-region-size"
	MaxPendingPeerCountKey         = "schedule.max-pending-peer-count"
	MaxMergeRegionKeysKey          = "schedule.max-merge-region-keys"
	LeaderScheduleLimitKey         = "schedule.leader-schedule-limit"
	RegionScheduleLimitKey         = "schedule.region-schedule-limit"
	WitnessScheduleLimitKey        = "schedule.witness-schedule-limit"
	ReplicaRescheduleLimitKey      = "schedule.replica-schedule-limit"
	MergeScheduleLimitKey          = "schedule.merge-schedule-limit"
	HotRegionScheduleLimitKey      = "schedule.hot-region-schedule-limit"
	SchedulerMaxWaitingOperatorKey = "schedule.scheduler-max-waiting-operator"
	EnableLocationReplacement      = "schedule.enable-location-replacement"
	DefaultAddPeer                 = "default-add-peer"
	DefaultRemovePeer              = "default-remove-peer"

	// EnableTiKVSplitRegion is the option to enable tikv split region.
	// it's related to schedule, but it's not an explicit config
	EnableTiKVSplitRegion = "schedule.enable-tikv-split-region"

	DefaultGCInterval = 5 * time.Second
	DefaultTTL        = 5 * time.Minute
)

// StoreLimit is the default limit of adding peer and removing peer when putting stores.
type StoreLimit struct {
	mu syncutil.RWMutex
	// AddPeer is the default rate of adding peers for store limit (per minute).
	AddPeer float64
	// RemovePeer is the default rate of removing peers for store limit (per minute).
	RemovePeer float64
}

// SetDefaultStoreLimit sets the default store limit for a given type.
func (sl *StoreLimit) SetDefaultStoreLimit(typ storelimit.Type, ratePerMin float64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	switch typ {
	case storelimit.AddPeer:
		sl.AddPeer = ratePerMin
	case storelimit.RemovePeer:
		sl.RemovePeer = ratePerMin
	}
}

// GetDefaultStoreLimit gets the default store limit for a given type.
func (sl *StoreLimit) GetDefaultStoreLimit(typ storelimit.Type) float64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	switch typ {
	case storelimit.AddPeer:
		return sl.AddPeer
	case storelimit.RemovePeer:
		return sl.RemovePeer
	default:
		panic("invalid type")
	}
}

func adjustSchedulers(v *SchedulerConfigs, defValue SchedulerConfigs) {
	if len(*v) == 0 {
		// Make a copy to avoid changing DefaultSchedulers unexpectedly.
		// When reloading from storage, the config is passed to json.Unmarshal.
		// Without clone, the DefaultSchedulers could be overwritten.
		*v = append(defValue[:0:0], defValue...)
	}
}

// ScheduleConfig is the schedule configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ScheduleConfig struct {
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount    uint64 `toml:"max-snapshot-count" json:"max-snapshot-count"`
	MaxPendingPeerCount uint64 `toml:"max-pending-peer-count" json:"max-pending-peer-count"`
	// If both the size of region is smaller than MaxMergeRegionSize
	// and the number of rows in region is smaller than MaxMergeRegionKeys,
	// it will try to merge with adjacent regions.
	MaxMergeRegionSize uint64 `toml:"max-merge-region-size" json:"max-merge-region-size"`
	MaxMergeRegionKeys uint64 `toml:"max-merge-region-keys" json:"max-merge-region-keys"`
	// SplitMergeInterval is the minimum interval time to permit merge after split.
	SplitMergeInterval typeutil.Duration `toml:"split-merge-interval" json:"split-merge-interval"`
	// SwitchWitnessInterval is the minimum interval that allows a peer to become a witness again after it is promoted to non-witness.
	SwitchWitnessInterval typeutil.Duration `toml:"switch-witness-interval" json:"switch-witness-interval"`
	// EnableOneWayMerge is the option to enable one way merge. This means a Region can only be merged into the next region of it.
	EnableOneWayMerge bool `toml:"enable-one-way-merge" json:"enable-one-way-merge,string"`
	// EnableCrossTableMerge is the option to enable cross table merge. This means two Regions can be merged with different table IDs.
	// This option only works when key type is "table".
	EnableCrossTableMerge bool `toml:"enable-cross-table-merge" json:"enable-cross-table-merge,string"`
	// PatrolRegionInterval is the interval for scanning region during patrol.
	PatrolRegionInterval typeutil.Duration `toml:"patrol-region-interval" json:"patrol-region-interval"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownTime typeutil.Duration `toml:"max-store-down-time" json:"max-store-down-time"`
	// MaxStorePreparingTime is the max duration after which
	// a store will be considered to be preparing.
	MaxStorePreparingTime typeutil.Duration `toml:"max-store-preparing-time" json:"max-store-preparing-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit" json:"leader-schedule-limit"`
	// LeaderSchedulePolicy is the option to balance leader, there are some policies supported: ["count", "size"], default: "count"
	LeaderSchedulePolicy string `toml:"leader-schedule-policy" json:"leader-schedule-policy"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `toml:"region-schedule-limit" json:"region-schedule-limit"`
	// WitnessScheduleLimit is the max coexist witness schedules.
	WitnessScheduleLimit uint64 `toml:"witness-schedule-limit" json:"witness-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `toml:"replica-schedule-limit" json:"replica-schedule-limit"`
	// MergeScheduleLimit is the max coexist merge schedules.
	MergeScheduleLimit uint64 `toml:"merge-schedule-limit" json:"merge-schedule-limit"`
	// HotRegionScheduleLimit is the max coexist hot region schedules.
	HotRegionScheduleLimit uint64 `toml:"hot-region-schedule-limit" json:"hot-region-schedule-limit"`
	// HotRegionCacheHitThreshold is the cache hits threshold of the hot region.
	// If the number of times a region hits the hot cache is greater than this
	// threshold, it is considered a hot region.
	HotRegionCacheHitsThreshold uint64 `toml:"hot-region-cache-hits-threshold" json:"hot-region-cache-hits-threshold"`
	// StoreBalanceRate is the maximum of balance rate for each store.
	// WARN: StoreBalanceRate is deprecated.
	StoreBalanceRate float64 `toml:"store-balance-rate" json:"store-balance-rate,omitempty"`
	// StoreLimit is the limit of scheduling for stores.
	StoreLimit map[uint64]StoreLimitConfig `toml:"store-limit" json:"store-limit"`
	// TolerantSizeRatio is the ratio of buffer size for balance scheduler.
	TolerantSizeRatio float64 `toml:"tolerant-size-ratio" json:"tolerant-size-ratio"`
	//
	//      high space stage         transition stage           low space stage
	//   |--------------------|-----------------------------|-------------------------|
	//   ^                    ^                             ^                         ^
	//   0       HighSpaceRatio * capacity       LowSpaceRatio * capacity          capacity
	//
	// LowSpaceRatio is the lowest usage ratio of store which regraded as low space.
	// When in low space, store region score increases to very large and varies inversely with available size.
	LowSpaceRatio float64 `toml:"low-space-ratio" json:"low-space-ratio"`
	// HighSpaceRatio is the highest usage ratio of store which regraded as high space.
	// High space means there is a lot of spare capacity, and store region score varies directly with used size.
	HighSpaceRatio float64 `toml:"high-space-ratio" json:"high-space-ratio"`
	// RegionScoreFormulaVersion is used to control the formula used to calculate region score.
	RegionScoreFormulaVersion string `toml:"region-score-formula-version" json:"region-score-formula-version"`
	// SchedulerMaxWaitingOperator is the max coexist operators for each scheduler.
	SchedulerMaxWaitingOperator uint64 `toml:"scheduler-max-waiting-operator" json:"scheduler-max-waiting-operator"`
	// WARN: DisableLearner is deprecated.
	// DisableLearner is the option to disable using AddLearnerNode instead of AddNode.
	DisableLearner bool `toml:"disable-raft-learner" json:"disable-raft-learner,string,omitempty"`
	// DisableRemoveDownReplica is the option to prevent replica checker from
	// removing down replicas.
	// WARN: DisableRemoveDownReplica is deprecated.
	DisableRemoveDownReplica bool `toml:"disable-remove-down-replica" json:"disable-remove-down-replica,string,omitempty"`
	// DisableReplaceOfflineReplica is the option to prevent replica checker from
	// replacing offline replicas.
	// WARN: DisableReplaceOfflineReplica is deprecated.
	DisableReplaceOfflineReplica bool `toml:"disable-replace-offline-replica" json:"disable-replace-offline-replica,string,omitempty"`
	// DisableMakeUpReplica is the option to prevent replica checker from making up
	// replicas when replica count is less than expected.
	// WARN: DisableMakeUpReplica is deprecated.
	DisableMakeUpReplica bool `toml:"disable-make-up-replica" json:"disable-make-up-replica,string,omitempty"`
	// DisableRemoveExtraReplica is the option to prevent replica checker from
	// removing extra replicas.
	// WARN: DisableRemoveExtraReplica is deprecated.
	DisableRemoveExtraReplica bool `toml:"disable-remove-extra-replica" json:"disable-remove-extra-replica,string,omitempty"`
	// DisableLocationReplacement is the option to prevent replica checker from
	// moving replica to a better location.
	// WARN: DisableLocationReplacement is deprecated.
	DisableLocationReplacement bool `toml:"disable-location-replacement" json:"disable-location-replacement,string,omitempty"`

	// EnableRemoveDownReplica is the option to enable replica checker to remove down replica.
	EnableRemoveDownReplica bool `toml:"enable-remove-down-replica" json:"enable-remove-down-replica,string"`
	// EnableReplaceOfflineReplica is the option to enable replica checker to replace offline replica.
	EnableReplaceOfflineReplica bool `toml:"enable-replace-offline-replica" json:"enable-replace-offline-replica,string"`
	// EnableMakeUpReplica is the option to enable replica checker to make up replica.
	EnableMakeUpReplica bool `toml:"enable-make-up-replica" json:"enable-make-up-replica,string"`
	// EnableRemoveExtraReplica is the option to enable replica checker to remove extra replica.
	EnableRemoveExtraReplica bool `toml:"enable-remove-extra-replica" json:"enable-remove-extra-replica,string"`
	// EnableLocationReplacement is the option to enable replica checker to move replica to a better location.
	EnableLocationReplacement bool `toml:"enable-location-replacement" json:"enable-location-replacement,string"`
	// EnableDebugMetrics is the option to enable debug metrics.
	EnableDebugMetrics bool `toml:"enable-debug-metrics" json:"enable-debug-metrics,string"`
	// EnableJointConsensus is the option to enable using joint consensus as an operator step.
	EnableJointConsensus bool `toml:"enable-joint-consensus" json:"enable-joint-consensus,string"`
	// EnableTiKVSplitRegion is the option to enable tikv split region.
	// on ebs-based BR we need to disable it with TTL
	EnableTiKVSplitRegion bool `toml:"enable-tikv-split-region" json:"enable-tikv-split-region,string"`

	// EnableHeartbeatBreakdownMetrics is the option to enable heartbeat stats metrics.
	EnableHeartbeatBreakdownMetrics bool `toml:"enable-heartbeat-breakdown-metrics" json:"enable-heartbeat-breakdown-metrics,string"`

	// EnableHeartbeatConcurrentRunner is the option to enable heartbeat concurrent runner.
	EnableHeartbeatConcurrentRunner bool `toml:"enable-heartbeat-concurrent-runner" json:"enable-heartbeat-concurrent-runner,string"`

	// Schedulers support for loading customized schedulers
	Schedulers SchedulerConfigs `toml:"schedulers" json:"schedulers-v2"` // json v2 is for the sake of compatible upgrade

	// Only used to display
	SchedulersPayload map[string]any `toml:"schedulers-payload" json:"schedulers-payload"`

	// Controls the time interval between write hot regions info into leveldb.
	HotRegionsWriteInterval typeutil.Duration `toml:"hot-regions-write-interval" json:"hot-regions-write-interval"`

	// The day of hot regions data to be reserved. 0 means close.
	HotRegionsReservedDays uint64 `toml:"hot-regions-reserved-days" json:"hot-regions-reserved-days"`

	// MaxMovableHotPeerSize is the threshold of region size for balance hot region and split bucket scheduler.
	// Hot region must be split before moved if it's region size is greater than MaxMovableHotPeerSize.
	MaxMovableHotPeerSize int64 `toml:"max-movable-hot-peer-size" json:"max-movable-hot-peer-size,omitempty"`

	// EnableDiagnostic is the option to enable using diagnostic
	EnableDiagnostic bool `toml:"enable-diagnostic" json:"enable-diagnostic,string"`

	// EnableWitness is the option to enable using witness
	EnableWitness bool `toml:"enable-witness" json:"enable-witness,string"`

	// SlowStoreEvictingAffectedStoreRatioThreshold is the affected ratio threshold when judging a store is slow
	// A store's slowness must affect more than `store-count * SlowStoreEvictingAffectedStoreRatioThreshold` to trigger evicting.
	SlowStoreEvictingAffectedStoreRatioThreshold float64 `toml:"slow-store-evicting-affected-store-ratio-threshold" json:"slow-store-evicting-affected-store-ratio-threshold,omitempty"`

	// StoreLimitVersion is the version of store limit.
	// v1: which is based on the region count by rate limit.
	// v2: which is based on region size by window size.
	StoreLimitVersion string `toml:"store-limit-version" json:"store-limit-version,omitempty"`

	// HaltScheduling is the option to halt the scheduling. Once it's on, PD will halt the scheduling,
	// and any other scheduling configs will be ignored.
	HaltScheduling bool `toml:"halt-scheduling" json:"halt-scheduling,string,omitempty"`

	// PatrolRegionConcurrency is the number of workers to patrol region.
	PatrolRegionConcurrency uint64 `toml:"patrol-worker-count" json:"patrol-worker-count"`

	// PatrolRegionBatchLimit is the number of regions to patrol in one batch.
	PatrolRegionBatchLimit uint64 `toml:"patrol-region-batch-limit" json:"patrol-region-batch-limit"`
}

// Clone returns a cloned scheduling configuration.
func (c *ScheduleConfig) Clone() *ScheduleConfig {
	schedulers := append(c.Schedulers[:0:0], c.Schedulers...)
	var storeLimit map[uint64]StoreLimitConfig
	if c.StoreLimit != nil {
		storeLimit = make(map[uint64]StoreLimitConfig, len(c.StoreLimit))
		for k, v := range c.StoreLimit {
			storeLimit[k] = v
		}
	}
	cfg := *c
	cfg.StoreLimit = storeLimit
	cfg.Schedulers = schedulers
	cfg.SchedulersPayload = nil
	return &cfg
}

// Adjust adjusts the config.
func (c *ScheduleConfig) Adjust(meta *configutil.ConfigMetaData, reloading bool) error {
	if !meta.IsDefined("max-snapshot-count") {
		configutil.AdjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	}
	if !meta.IsDefined("max-pending-peer-count") {
		configutil.AdjustUint64(&c.MaxPendingPeerCount, defaultMaxPendingPeerCount)
	}
	if !meta.IsDefined("max-merge-region-size") {
		configutil.AdjustUint64(&c.MaxMergeRegionSize, defaultMaxMergeRegionSize)
	}
	configutil.AdjustDuration(&c.SplitMergeInterval, DefaultSplitMergeInterval)
	configutil.AdjustDuration(&c.SwitchWitnessInterval, defaultSwitchWitnessInterval)
	configutil.AdjustDuration(&c.PatrolRegionInterval, defaultPatrolRegionInterval)
	configutil.AdjustDuration(&c.MaxStoreDownTime, defaultMaxStoreDownTime)
	configutil.AdjustDuration(&c.HotRegionsWriteInterval, defaultHotRegionsWriteInterval)
	configutil.AdjustDuration(&c.MaxStorePreparingTime, defaultMaxStorePreparingTime)
	if !meta.IsDefined("leader-schedule-limit") {
		configutil.AdjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	}
	if !meta.IsDefined("region-schedule-limit") {
		configutil.AdjustUint64(&c.RegionScheduleLimit, defaultRegionScheduleLimit)
	}
	if !meta.IsDefined("witness-schedule-limit") {
		configutil.AdjustUint64(&c.WitnessScheduleLimit, defaultWitnessScheduleLimit)
	}
	if !meta.IsDefined("replica-schedule-limit") {
		configutil.AdjustUint64(&c.ReplicaScheduleLimit, defaultReplicaScheduleLimit)
	}
	if !meta.IsDefined("merge-schedule-limit") {
		configutil.AdjustUint64(&c.MergeScheduleLimit, defaultMergeScheduleLimit)
	}
	if !meta.IsDefined("hot-region-schedule-limit") {
		configutil.AdjustUint64(&c.HotRegionScheduleLimit, defaultHotRegionScheduleLimit)
	}
	if !meta.IsDefined("hot-region-cache-hits-threshold") {
		configutil.AdjustUint64(&c.HotRegionCacheHitsThreshold, defaultHotRegionCacheHitsThreshold)
	}
	if !meta.IsDefined("tolerant-size-ratio") {
		configutil.AdjustFloat64(&c.TolerantSizeRatio, defaultTolerantSizeRatio)
	}
	if !meta.IsDefined("scheduler-max-waiting-operator") {
		configutil.AdjustUint64(&c.SchedulerMaxWaitingOperator, defaultSchedulerMaxWaitingOperator)
	}
	if !meta.IsDefined("leader-schedule-policy") {
		configutil.AdjustString(&c.LeaderSchedulePolicy, defaultLeaderSchedulePolicy)
	}
	if !meta.IsDefined("store-limit-version") {
		configutil.AdjustString(&c.StoreLimitVersion, defaultStoreLimitVersion)
	}
	if !meta.IsDefined("patrol-worker-count") {
		configutil.AdjustUint64(&c.PatrolRegionConcurrency, defaultPatrolRegionConcurrency)
	}

	if !meta.IsDefined("patrol-region-batch-limit") {
		configutil.AdjustUint64(&c.PatrolRegionBatchLimit, defaultPatrolRegionBatchLimit)
	}

	if !meta.IsDefined("enable-joint-consensus") {
		c.EnableJointConsensus = defaultEnableJointConsensus
	}
	if !meta.IsDefined("enable-tikv-split-region") {
		c.EnableTiKVSplitRegion = defaultEnableTiKVSplitRegion
	}

	if !meta.IsDefined("enable-heartbeat-breakdown-metrics") {
		c.EnableHeartbeatBreakdownMetrics = defaultEnableHeartbeatBreakdownMetrics
	}

	if !meta.IsDefined("enable-heartbeat-concurrent-runner") {
		c.EnableHeartbeatConcurrentRunner = defaultEnableHeartbeatConcurrentRunner
	}

	if !meta.IsDefined("enable-cross-table-merge") {
		c.EnableCrossTableMerge = defaultEnableCrossTableMerge
	}
	configutil.AdjustFloat64(&c.LowSpaceRatio, defaultLowSpaceRatio)
	configutil.AdjustFloat64(&c.HighSpaceRatio, defaultHighSpaceRatio)
	if !meta.IsDefined("enable-diagnostic") {
		c.EnableDiagnostic = defaultEnableDiagnostic
	}

	if !meta.IsDefined("enable-witness") {
		c.EnableWitness = defaultEnableWitness
	}

	// new cluster:v2, old cluster:v1
	if !meta.IsDefined("region-score-formula-version") && !reloading {
		configutil.AdjustString(&c.RegionScoreFormulaVersion, defaultRegionScoreFormulaVersion)
	}

	if !meta.IsDefined("halt-scheduling") {
		c.HaltScheduling = defaultHaltScheduling
	}

	adjustSchedulers(&c.Schedulers, DefaultSchedulers)

	for k, b := range c.migrateConfigurationMap() {
		v, err := parseDeprecatedFlag(meta, k, *b[0], *b[1])
		if err != nil {
			return err
		}
		*b[0], *b[1] = false, v // reset old flag false to make it ignored when marshal to JSON
	}

	if c.StoreBalanceRate != 0 {
		DefaultStoreLimit = StoreLimit{AddPeer: c.StoreBalanceRate, RemovePeer: c.StoreBalanceRate}
		c.StoreBalanceRate = 0
	}

	if c.StoreLimit == nil {
		c.StoreLimit = make(map[uint64]StoreLimitConfig)
	}

	if !meta.IsDefined("hot-regions-reserved-days") {
		configutil.AdjustUint64(&c.HotRegionsReservedDays, defaultHotRegionsReservedDays)
	}

	if !meta.IsDefined("max-movable-hot-peer-size") {
		configutil.AdjustInt64(&c.MaxMovableHotPeerSize, defaultMaxMovableHotPeerSize)
	}

	if !meta.IsDefined("slow-store-evicting-affected-store-ratio-threshold") {
		configutil.AdjustFloat64(&c.SlowStoreEvictingAffectedStoreRatioThreshold, defaultSlowStoreEvictingAffectedStoreRatioThreshold)
	}
	return c.Validate()
}

func (c *ScheduleConfig) migrateConfigurationMap() map[string][2]*bool {
	return map[string][2]*bool{
		"remove-down-replica":     {&c.DisableRemoveDownReplica, &c.EnableRemoveDownReplica},
		"replace-offline-replica": {&c.DisableReplaceOfflineReplica, &c.EnableReplaceOfflineReplica},
		"make-up-replica":         {&c.DisableMakeUpReplica, &c.EnableMakeUpReplica},
		"remove-extra-replica":    {&c.DisableRemoveExtraReplica, &c.EnableRemoveExtraReplica},
		"location-replacement":    {&c.DisableLocationReplacement, &c.EnableLocationReplacement},
	}
}

// GetMaxMergeRegionKeys returns the max merge keys.
// it should keep consistent with tikv: https://github.com/tikv/tikv/pull/12484
func (c *ScheduleConfig) GetMaxMergeRegionKeys() uint64 {
	if keys := c.MaxMergeRegionKeys; keys != 0 {
		return keys
	}
	return c.MaxMergeRegionSize * 10000
}

func parseDeprecatedFlag(meta *configutil.ConfigMetaData, name string, old, new bool) (bool, error) {
	oldName, newName := "disable-"+name, "enable-"+name
	defineOld, defineNew := meta.IsDefined(oldName), meta.IsDefined(newName)
	switch {
	case defineNew && defineOld:
		if new == old {
			return false, errors.Errorf("config item %s and %s(deprecated) are conflict", newName, oldName)
		}
		return new, nil
	case defineNew && !defineOld:
		return new, nil
	case !defineNew && defineOld:
		return !old, nil // use !disable-*
	case !defineNew && !defineOld:
		return true, nil // use default value true
	}
	return false, nil // unreachable.
}

// MigrateDeprecatedFlags updates new flags according to deprecated flags.
func (c *ScheduleConfig) MigrateDeprecatedFlags() {
	c.DisableLearner = false
	if c.StoreBalanceRate != 0 {
		DefaultStoreLimit = StoreLimit{AddPeer: c.StoreBalanceRate, RemovePeer: c.StoreBalanceRate}
		c.StoreBalanceRate = 0
	}
	for _, b := range c.migrateConfigurationMap() {
		// If old=false (previously disabled), set both old and new to false.
		if *b[0] {
			*b[0], *b[1] = false, false
		}
	}
}

// Validate is used to validate if some scheduling configurations are right.
func (c *ScheduleConfig) Validate() error {
	if c.TolerantSizeRatio < 0 {
		return errors.New("tolerant-size-ratio should be non-negative")
	}
	if c.LowSpaceRatio < 0 || c.LowSpaceRatio > 1 {
		return errors.New("low-space-ratio should between 0 and 1")
	}
	if c.HighSpaceRatio < 0 || c.HighSpaceRatio > 1 {
		return errors.New("high-space-ratio should between 0 and 1")
	}
	if c.LowSpaceRatio <= c.HighSpaceRatio {
		return errors.New("low-space-ratio should be larger than high-space-ratio")
	}
	if c.LeaderSchedulePolicy != "count" && c.LeaderSchedulePolicy != "size" {
		return errors.Errorf("leader-schedule-policy %v is invalid", c.LeaderSchedulePolicy)
	}
	if c.SlowStoreEvictingAffectedStoreRatioThreshold == 0 {
		return errors.Errorf("slow-store-evicting-affected-store-ratio-threshold is not set")
	}
	return nil
}

// Deprecated is used to find if there is an option has been deprecated.
func (c *ScheduleConfig) Deprecated() error {
	if c.DisableLearner {
		return errors.New("disable-raft-learner has already been deprecated")
	}
	if c.DisableRemoveDownReplica {
		return errors.New("disable-remove-down-replica has already been deprecated")
	}
	if c.DisableReplaceOfflineReplica {
		return errors.New("disable-replace-offline-replica has already been deprecated")
	}
	if c.DisableMakeUpReplica {
		return errors.New("disable-make-up-replica has already been deprecated")
	}
	if c.DisableRemoveExtraReplica {
		return errors.New("disable-remove-extra-replica has already been deprecated")
	}
	if c.DisableLocationReplacement {
		return errors.New("disable-location-replacement has already been deprecated")
	}
	if c.StoreBalanceRate != 0 {
		return errors.New("store-balance-rate has already been deprecated")
	}
	return nil
}

// StoreLimitConfig is a config about scheduling rate limit of different types for a store.
type StoreLimitConfig struct {
	AddPeer    float64 `toml:"add-peer" json:"add-peer"`
	RemovePeer float64 `toml:"remove-peer" json:"remove-peer"`
}

// SchedulerConfigs is a slice of customized scheduler configuration.
type SchedulerConfigs []SchedulerConfig

// SchedulerConfig is customized scheduler configuration
type SchedulerConfig struct {
	Type        string   `toml:"type" json:"type"`
	Args        []string `toml:"args" json:"args"`
	Disable     bool     `toml:"disable" json:"disable"`
	ArgsPayload string   `toml:"args-payload" json:"args-payload"`
}

// DefaultSchedulers are the schedulers be created by default.
// If these schedulers are not in the persistent configuration, they
// will be created automatically when reloading.
var DefaultSchedulers = SchedulerConfigs{
	{Type: "balance-region"},
	{Type: "balance-leader"},
	{Type: "hot-region"},
	{Type: "evict-slow-store"},
}

// IsDefaultScheduler checks whether the scheduler is enabled by default.
func IsDefaultScheduler(typ string) bool {
	for _, c := range DefaultSchedulers {
		if typ == c.Type {
			return true
		}
	}
	return false
}

// ReplicationConfig is the replication configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReplicationConfig struct {
	// MaxReplicas is the number of replicas for each region.
	MaxReplicas uint64 `toml:"max-replicas" json:"max-replicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels typeutil.StringSlice `toml:"location-labels" json:"location-labels"`
	// StrictlyMatchLabel strictly checks if the label of TiKV is matched with LocationLabels.
	StrictlyMatchLabel bool `toml:"strictly-match-label" json:"strictly-match-label,string"`

	// When PlacementRules feature is enabled. MaxReplicas, LocationLabels and IsolationLabels are not used any more.
	EnablePlacementRules bool `toml:"enable-placement-rules" json:"enable-placement-rules,string"`

	// EnablePlacementRuleCache controls whether use cache during rule checker
	EnablePlacementRulesCache bool `toml:"enable-placement-rules-cache" json:"enable-placement-rules-cache,string"`

	// IsolationLevel is used to isolate replicas explicitly and forcibly if it's not empty.
	// Its value must be empty or one of LocationLabels.
	// Example:
	// location-labels = ["zone", "rack", "host"]
	// isolation-level = "zone"
	// With configuration like above, PD ensure that all replicas be placed in different zones.
	// Even if a zone is down, PD will not try to make up replicas in other zone
	// because other zones already have replicas on it.
	IsolationLevel string `toml:"isolation-level" json:"isolation-level"`
}

// Clone makes a deep copy of the config.
func (c *ReplicationConfig) Clone() *ReplicationConfig {
	locationLabels := append(c.LocationLabels[:0:0], c.LocationLabels...)
	cfg := *c
	cfg.LocationLabels = locationLabels
	return &cfg
}

// Validate is used to validate if some replication configurations are right.
func (c *ReplicationConfig) Validate() error {
	foundIsolationLevel := false
	for _, label := range c.LocationLabels {
		err := ValidateLabels([]*metapb.StoreLabel{{Key: label}})
		if err != nil {
			return err
		}
		// IsolationLevel should be empty or one of LocationLabels
		if !foundIsolationLevel && label == c.IsolationLevel {
			foundIsolationLevel = true
		}
	}
	if c.IsolationLevel != "" && !foundIsolationLevel {
		return errors.New("isolation-level must be one of location-labels or empty")
	}
	return nil
}

// Adjust adjusts the config.
func (c *ReplicationConfig) Adjust(meta *configutil.ConfigMetaData) error {
	configutil.AdjustUint64(&c.MaxReplicas, DefaultMaxReplicas)
	if !meta.IsDefined("enable-placement-rules") {
		c.EnablePlacementRules = defaultEnablePlacementRules
	}
	if !meta.IsDefined("strictly-match-label") {
		c.StrictlyMatchLabel = defaultStrictlyMatchLabel
	}
	if !meta.IsDefined("location-labels") {
		c.LocationLabels = defaultLocationLabels
	}
	return c.Validate()
}
