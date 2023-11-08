// Copyright 2017 TiKV Project Authors.
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
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// PersistOptions wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistOptions struct {
	// configuration -> ttl value
	ttl             *cache.TTLString
	schedule        atomic.Value
	replication     atomic.Value
	pdServerConfig  atomic.Value
	replicationMode atomic.Value
	labelProperty   atomic.Value
	keyspace        atomic.Value
	storeConfig     atomic.Value
	clusterVersion  unsafe.Pointer
}

// NewPersistOptions creates a new PersistOptions instance.
func NewPersistOptions(cfg *Config) *PersistOptions {
	o := &PersistOptions{}
	o.schedule.Store(&cfg.Schedule)
	o.replication.Store(&cfg.Replication)
	o.pdServerConfig.Store(&cfg.PDServerCfg)
	o.replicationMode.Store(&cfg.ReplicationMode)
	o.labelProperty.Store(cfg.LabelProperty)
	o.keyspace.Store(&cfg.Keyspace)
	// storeConfig will be fetched from TiKV later,
	// set it to an empty config here first.
	o.storeConfig.Store(&sc.StoreConfig{})
	o.SetClusterVersion(&cfg.ClusterVersion)
	o.ttl = nil
	return o
}

// GetScheduleConfig returns scheduling configurations.
func (o *PersistOptions) GetScheduleConfig() *sc.ScheduleConfig {
	return o.schedule.Load().(*sc.ScheduleConfig)
}

// SetScheduleConfig sets the PD scheduling configuration.
func (o *PersistOptions) SetScheduleConfig(cfg *sc.ScheduleConfig) {
	o.schedule.Store(cfg)
}

// GetReplicationConfig returns replication configurations.
func (o *PersistOptions) GetReplicationConfig() *sc.ReplicationConfig {
	return o.replication.Load().(*sc.ReplicationConfig)
}

// SetReplicationConfig sets the PD replication configuration.
func (o *PersistOptions) SetReplicationConfig(cfg *sc.ReplicationConfig) {
	o.replication.Store(cfg)
}

// GetPDServerConfig returns pd server configurations.
func (o *PersistOptions) GetPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
}

// SetPDServerConfig sets the PD configuration.
func (o *PersistOptions) SetPDServerConfig(cfg *PDServerConfig) {
	o.pdServerConfig.Store(cfg)
}

// GetReplicationModeConfig returns the replication mode config.
func (o *PersistOptions) GetReplicationModeConfig() *ReplicationModeConfig {
	return o.replicationMode.Load().(*ReplicationModeConfig)
}

// SetReplicationModeConfig sets the replication mode config.
func (o *PersistOptions) SetReplicationModeConfig(cfg *ReplicationModeConfig) {
	o.replicationMode.Store(cfg)
}

// GetLabelPropertyConfig returns the label property.
func (o *PersistOptions) GetLabelPropertyConfig() LabelPropertyConfig {
	return o.labelProperty.Load().(LabelPropertyConfig)
}

// SetLabelPropertyConfig sets the label property configuration.
func (o *PersistOptions) SetLabelPropertyConfig(cfg LabelPropertyConfig) {
	o.labelProperty.Store(cfg)
}

// GetKeyspaceConfig returns the keyspace config.
func (o *PersistOptions) GetKeyspaceConfig() *KeyspaceConfig {
	return o.keyspace.Load().(*KeyspaceConfig)
}

// SetKeyspaceConfig sets the keyspace configuration.
func (o *PersistOptions) SetKeyspaceConfig(cfg *KeyspaceConfig) {
	o.keyspace.Store(cfg)
}

// GetStoreConfig returns the store config.
func (o *PersistOptions) GetStoreConfig() *sc.StoreConfig {
	return o.storeConfig.Load().(*sc.StoreConfig)
}

// SetStoreConfig sets the store configuration.
func (o *PersistOptions) SetStoreConfig(cfg *sc.StoreConfig) {
	o.storeConfig.Store(cfg)
}

// GetClusterVersion returns the cluster version.
func (o *PersistOptions) GetClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// SetClusterVersion sets the cluster version.
func (o *PersistOptions) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// CASClusterVersion sets the cluster version.
func (o *PersistOptions) CASClusterVersion(old, new *semver.Version) bool {
	return atomic.CompareAndSwapPointer(&o.clusterVersion, unsafe.Pointer(old), unsafe.Pointer(new))
}

// GetLocationLabels returns the location labels for each region.
func (o *PersistOptions) GetLocationLabels() []string {
	return o.GetReplicationConfig().LocationLabels
}

// SetLocationLabels sets the location labels.
func (o *PersistOptions) SetLocationLabels(labels []string) {
	v := o.GetReplicationConfig().Clone()
	v.LocationLabels = labels
	o.SetReplicationConfig(v)
}

// GetIsolationLevel returns the isolation label for each region.
func (o *PersistOptions) GetIsolationLevel() string {
	return o.GetReplicationConfig().IsolationLevel
}

// IsPlacementRulesEnabled returns if the placement rules is enabled.
func (o *PersistOptions) IsPlacementRulesEnabled() bool {
	return o.GetReplicationConfig().EnablePlacementRules
}

// SetPlacementRuleEnabled set PlacementRuleEnabled
func (o *PersistOptions) SetPlacementRuleEnabled(enabled bool) {
	v := o.GetReplicationConfig().Clone()
	v.EnablePlacementRules = enabled
	o.SetReplicationConfig(v)
}

// IsPlacementRulesCacheEnabled returns if the placement rules cache is enabled
func (o *PersistOptions) IsPlacementRulesCacheEnabled() bool {
	return o.GetReplicationConfig().EnablePlacementRulesCache
}

// SetPlacementRulesCacheEnabled set EnablePlacementRulesCache
func (o *PersistOptions) SetPlacementRulesCacheEnabled(enabled bool) {
	v := o.GetReplicationConfig().Clone()
	v.EnablePlacementRulesCache = enabled
	o.SetReplicationConfig(v)
}

// GetStrictlyMatchLabel returns whether check label strict.
func (o *PersistOptions) GetStrictlyMatchLabel() bool {
	return o.GetReplicationConfig().StrictlyMatchLabel
}

// GetMaxReplicas returns the number of replicas for each region.
func (o *PersistOptions) GetMaxReplicas() int {
	return int(o.GetReplicationConfig().MaxReplicas)
}

// SetMaxReplicas sets the number of replicas for each region.
func (o *PersistOptions) SetMaxReplicas(replicas int) {
	v := o.GetReplicationConfig().Clone()
	v.MaxReplicas = uint64(replicas)
	o.SetReplicationConfig(v)
}

const (
	maxSnapshotCountKey            = "schedule.max-snapshot-count"
	maxMergeRegionSizeKey          = "schedule.max-merge-region-size"
	maxPendingPeerCountKey         = "schedule.max-pending-peer-count"
	maxMergeRegionKeysKey          = "schedule.max-merge-region-keys"
	leaderScheduleLimitKey         = "schedule.leader-schedule-limit"
	regionScheduleLimitKey         = "schedule.region-schedule-limit"
	witnessScheduleLimitKey        = "schedule.witness-schedule-limit"
	replicaRescheduleLimitKey      = "schedule.replica-schedule-limit"
	mergeScheduleLimitKey          = "schedule.merge-schedule-limit"
	hotRegionScheduleLimitKey      = "schedule.hot-region-schedule-limit"
	schedulerMaxWaitingOperatorKey = "schedule.scheduler-max-waiting-operator"
	enableLocationReplacement      = "schedule.enable-location-replacement"
	// it's related to schedule, but it's not an explicit config
	enableTiKVSplitRegion = "schedule.enable-tikv-split-region"
)

var supportedTTLConfigs = []string{
	maxSnapshotCountKey,
	maxMergeRegionSizeKey,
	maxPendingPeerCountKey,
	maxMergeRegionKeysKey,
	leaderScheduleLimitKey,
	regionScheduleLimitKey,
	replicaRescheduleLimitKey,
	mergeScheduleLimitKey,
	hotRegionScheduleLimitKey,
	schedulerMaxWaitingOperatorKey,
	enableLocationReplacement,
	enableTiKVSplitRegion,
	"default-add-peer",
	"default-remove-peer",
}

// IsSupportedTTLConfig checks whether a key is a supported config item with ttl
func IsSupportedTTLConfig(key string) bool {
	for _, supportedConfig := range supportedTTLConfigs {
		if key == supportedConfig {
			return true
		}
	}
	return strings.HasPrefix(key, "add-peer-") || strings.HasPrefix(key, "remove-peer-")
}

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (o *PersistOptions) GetMaxSnapshotCount() uint64 {
	return o.getTTLUintOr(maxSnapshotCountKey, o.GetScheduleConfig().MaxSnapshotCount)
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (o *PersistOptions) GetMaxPendingPeerCount() uint64 {
	return o.getTTLUintOr(maxPendingPeerCountKey, o.GetScheduleConfig().MaxPendingPeerCount)
}

// GetMaxMergeRegionSize returns the max region size.
func (o *PersistOptions) GetMaxMergeRegionSize() uint64 {
	return o.getTTLUintOr(maxMergeRegionSizeKey, o.GetScheduleConfig().MaxMergeRegionSize)
}

// GetMaxMergeRegionKeys returns the max number of keys.
// It returns size * 10000 if the key of max-merge-region-Keys doesn't exist.
func (o *PersistOptions) GetMaxMergeRegionKeys() uint64 {
	keys, exist, err := o.getTTLUint(maxMergeRegionKeysKey)
	if exist && err == nil {
		return keys
	}
	size, exist, err := o.getTTLUint(maxMergeRegionSizeKey)
	if exist && err == nil {
		return size * 10000
	}
	return o.GetScheduleConfig().GetMaxMergeRegionKeys()
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (o *PersistOptions) GetSplitMergeInterval() time.Duration {
	return o.GetScheduleConfig().SplitMergeInterval.Duration
}

// SetSplitMergeInterval to set the interval between finishing split and starting to merge. It's only used to test.
func (o *PersistOptions) SetSplitMergeInterval(splitMergeInterval time.Duration) {
	v := o.GetScheduleConfig().Clone()
	v.SplitMergeInterval = typeutil.Duration{Duration: splitMergeInterval}
	o.SetScheduleConfig(v)
}

// GetSwitchWitnessInterval returns the interval between promote to non-witness and starting to switch to witness.
func (o *PersistOptions) GetSwitchWitnessInterval() time.Duration {
	return o.GetScheduleConfig().SwitchWitnessInterval.Duration
}

// IsDiagnosticAllowed returns whether is enable to use diagnostic.
func (o *PersistOptions) IsDiagnosticAllowed() bool {
	return o.GetScheduleConfig().EnableDiagnostic
}

// SetEnableDiagnostic to set the option for diagnose. It's only used to test.
func (o *PersistOptions) SetEnableDiagnostic(enable bool) {
	v := o.GetScheduleConfig().Clone()
	v.EnableDiagnostic = enable
	o.SetScheduleConfig(v)
}

// IsWitnessAllowed returns whether is enable to use witness.
func (o *PersistOptions) IsWitnessAllowed() bool {
	return o.GetScheduleConfig().EnableWitness
}

// SetEnableWitness to set the option for witness. It's only used to test.
func (o *PersistOptions) SetEnableWitness(enable bool) {
	v := o.GetScheduleConfig().Clone()
	v.EnableWitness = enable
	o.SetScheduleConfig(v)
}

// SetMaxStoreDownTime to set the max store down time. It's only used to test.
func (o *PersistOptions) SetMaxStoreDownTime(time time.Duration) {
	v := o.GetScheduleConfig().Clone()
	v.MaxStoreDownTime = typeutil.NewDuration(time)
	o.SetScheduleConfig(v)
}

// SetMaxMergeRegionSize sets the max merge region size.
func (o *PersistOptions) SetMaxMergeRegionSize(maxMergeRegionSize uint64) {
	v := o.GetScheduleConfig().Clone()
	v.MaxMergeRegionSize = maxMergeRegionSize
	o.SetScheduleConfig(v)
}

// SetMaxMergeRegionKeys sets the max merge region keys.
func (o *PersistOptions) SetMaxMergeRegionKeys(maxMergeRegionKeys uint64) {
	v := o.GetScheduleConfig().Clone()
	v.MaxMergeRegionKeys = maxMergeRegionKeys
	o.SetScheduleConfig(v)
}

// SetStoreLimit sets a store limit for a given type and rate.
func (o *PersistOptions) SetStoreLimit(storeID uint64, typ storelimit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	var slc sc.StoreLimitConfig
	var rate float64
	switch typ {
	case storelimit.AddPeer:
		if _, ok := v.StoreLimit[storeID]; !ok {
			rate = sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
		} else {
			rate = v.StoreLimit[storeID].RemovePeer
		}
		slc = sc.StoreLimitConfig{AddPeer: ratePerMin, RemovePeer: rate}
	case storelimit.RemovePeer:
		if _, ok := v.StoreLimit[storeID]; !ok {
			rate = sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
		} else {
			rate = v.StoreLimit[storeID].AddPeer
		}
		slc = sc.StoreLimitConfig{AddPeer: rate, RemovePeer: ratePerMin}
	}
	v.StoreLimit[storeID] = slc
	o.SetScheduleConfig(v)
}

// SetAllStoresLimit sets all store limit for a given type and rate.
func (o *PersistOptions) SetAllStoresLimit(typ storelimit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	switch typ {
	case storelimit.AddPeer:
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, ratePerMin)
		for storeID := range v.StoreLimit {
			sc := sc.StoreLimitConfig{AddPeer: ratePerMin, RemovePeer: v.StoreLimit[storeID].RemovePeer}
			v.StoreLimit[storeID] = sc
		}
	case storelimit.RemovePeer:
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, ratePerMin)
		for storeID := range v.StoreLimit {
			sc := sc.StoreLimitConfig{AddPeer: v.StoreLimit[storeID].AddPeer, RemovePeer: ratePerMin}
			v.StoreLimit[storeID] = sc
		}
	}

	o.SetScheduleConfig(v)
}

// IsOneWayMergeEnabled returns if a region can only be merged into the next region of it.
func (o *PersistOptions) IsOneWayMergeEnabled() bool {
	return o.GetScheduleConfig().EnableOneWayMerge
}

// IsCrossTableMergeEnabled returns if across table merge is enabled.
func (o *PersistOptions) IsCrossTableMergeEnabled() bool {
	return o.GetScheduleConfig().EnableCrossTableMerge
}

// GetPatrolRegionInterval returns the interval of patrolling region.
func (o *PersistOptions) GetPatrolRegionInterval() time.Duration {
	return o.GetScheduleConfig().PatrolRegionInterval.Duration
}

// GetMaxStoreDownTime returns the max down time of a store.
func (o *PersistOptions) GetMaxStoreDownTime() time.Duration {
	return o.GetScheduleConfig().MaxStoreDownTime.Duration
}

// GetMaxStorePreparingTime returns the max preparing time of a store.
func (o *PersistOptions) GetMaxStorePreparingTime() time.Duration {
	return o.GetScheduleConfig().MaxStorePreparingTime.Duration
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *PersistOptions) GetLeaderScheduleLimit() uint64 {
	return o.getTTLUintOr(leaderScheduleLimitKey, o.GetScheduleConfig().LeaderScheduleLimit)
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (o *PersistOptions) GetRegionScheduleLimit() uint64 {
	return o.getTTLUintOr(regionScheduleLimitKey, o.GetScheduleConfig().RegionScheduleLimit)
}

// GetWitnessScheduleLimit returns the limit for region schedule.
func (o *PersistOptions) GetWitnessScheduleLimit() uint64 {
	return o.getTTLUintOr(witnessScheduleLimitKey, o.GetScheduleConfig().WitnessScheduleLimit)
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *PersistOptions) GetReplicaScheduleLimit() uint64 {
	return o.getTTLUintOr(replicaRescheduleLimitKey, o.GetScheduleConfig().ReplicaScheduleLimit)
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *PersistOptions) GetMergeScheduleLimit() uint64 {
	return o.getTTLUintOr(mergeScheduleLimitKey, o.GetScheduleConfig().MergeScheduleLimit)
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (o *PersistOptions) GetHotRegionScheduleLimit() uint64 {
	return o.getTTLUintOr(hotRegionScheduleLimitKey, o.GetScheduleConfig().HotRegionScheduleLimit)
}

// GetStoreLimit returns the limit of a store.
func (o *PersistOptions) GetStoreLimit(storeID uint64) (returnSC sc.StoreLimitConfig) {
	defer func() {
		returnSC.RemovePeer = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", storeID), returnSC.RemovePeer)
		returnSC.AddPeer = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", storeID), returnSC.AddPeer)
	}()
	if limit, ok := o.GetScheduleConfig().StoreLimit[storeID]; ok {
		return limit
	}
	cfg := o.GetScheduleConfig().Clone()
	sc := sc.StoreLimitConfig{
		AddPeer:    sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
		RemovePeer: sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
	}
	v, ok1, err := o.getTTLFloat("default-add-peer")
	if err != nil {
		log.Warn("failed to parse default-add-peer from PersistOptions's ttl storage", zap.Error(err))
	}
	canSetAddPeer := ok1 && err == nil
	if canSetAddPeer {
		returnSC.AddPeer = v
	}

	v, ok2, err := o.getTTLFloat("default-remove-peer")
	if err != nil {
		log.Warn("failed to parse default-remove-peer from PersistOptions's ttl storage", zap.Error(err))
	}
	canSetRemovePeer := ok2 && err == nil
	if canSetRemovePeer {
		returnSC.RemovePeer = v
	}

	if canSetAddPeer || canSetRemovePeer {
		return returnSC
	}
	cfg.StoreLimit[storeID] = sc
	o.SetScheduleConfig(cfg)
	return o.GetScheduleConfig().StoreLimit[storeID]
}

// GetStoreLimitByType returns the limit of a store with a given type.
func (o *PersistOptions) GetStoreLimitByType(storeID uint64, typ storelimit.Type) (returned float64) {
	defer func() {
		if typ == storelimit.RemovePeer {
			returned = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", storeID), returned)
		} else if typ == storelimit.AddPeer {
			returned = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", storeID), returned)
		}
	}()
	limit := o.GetStoreLimit(storeID)
	switch typ {
	case storelimit.AddPeer:
		return limit.AddPeer
	case storelimit.RemovePeer:
		return limit.RemovePeer
	// todo: impl it in store limit v2.
	case storelimit.SendSnapshot:
		return 0.0
	default:
		panic("no such limit type")
	}
}

// GetAllStoresLimit returns the limit of all stores.
func (o *PersistOptions) GetAllStoresLimit() map[uint64]sc.StoreLimitConfig {
	return o.GetScheduleConfig().StoreLimit
}

// GetStoreLimitVersion returns the limit version of store.
func (o *PersistOptions) GetStoreLimitVersion() string {
	return o.GetScheduleConfig().StoreLimitVersion
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *PersistOptions) GetTolerantSizeRatio() float64 {
	return o.GetScheduleConfig().TolerantSizeRatio
}

// GetLowSpaceRatio returns the low space ratio.
func (o *PersistOptions) GetLowSpaceRatio() float64 {
	return o.GetScheduleConfig().LowSpaceRatio
}

// GetSlowStoreEvictingAffectedStoreRatioThreshold returns the affected ratio threshold when judging a store is slow.
func (o *PersistOptions) GetSlowStoreEvictingAffectedStoreRatioThreshold() float64 {
	return o.GetScheduleConfig().SlowStoreEvictingAffectedStoreRatioThreshold
}

// GetHighSpaceRatio returns the high space ratio.
func (o *PersistOptions) GetHighSpaceRatio() float64 {
	return o.GetScheduleConfig().HighSpaceRatio
}

// GetRegionScoreFormulaVersion returns the formula version config.
func (o *PersistOptions) GetRegionScoreFormulaVersion() string {
	return o.GetScheduleConfig().RegionScoreFormulaVersion
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (o *PersistOptions) GetSchedulerMaxWaitingOperator() uint64 {
	return o.getTTLUintOr(schedulerMaxWaitingOperatorKey, o.GetScheduleConfig().SchedulerMaxWaitingOperator)
}

// GetLeaderSchedulePolicy is to get leader schedule policy.
func (o *PersistOptions) GetLeaderSchedulePolicy() constant.SchedulePolicy {
	return constant.StringToSchedulePolicy(o.GetScheduleConfig().LeaderSchedulePolicy)
}

// GetKeyType is to get key type.
func (o *PersistOptions) GetKeyType() constant.KeyType {
	return constant.StringToKeyType(o.GetPDServerConfig().KeyType)
}

// GetMaxResetTSGap gets the max gap to reset the tso.
func (o *PersistOptions) GetMaxResetTSGap() time.Duration {
	return o.GetPDServerConfig().MaxResetTSGap.Duration
}

// GetDashboardAddress gets dashboard address.
func (o *PersistOptions) GetDashboardAddress() string {
	return o.GetPDServerConfig().DashboardAddress
}

// IsUseRegionStorage returns if the independent region storage is enabled.
func (o *PersistOptions) IsUseRegionStorage() bool {
	return o.GetPDServerConfig().UseRegionStorage
}

// GetServerMemoryLimit gets ServerMemoryLimit config.
func (o *PersistOptions) GetServerMemoryLimit() float64 {
	return o.GetPDServerConfig().ServerMemoryLimit
}

// GetServerMemoryLimitGCTrigger gets the ServerMemoryLimitGCTrigger config.
func (o *PersistOptions) GetServerMemoryLimitGCTrigger() float64 {
	return o.GetPDServerConfig().ServerMemoryLimitGCTrigger
}

// GetEnableGOGCTuner gets the EnableGOGCTuner config.
func (o *PersistOptions) GetEnableGOGCTuner() bool {
	return o.GetPDServerConfig().EnableGOGCTuner
}

// GetGCTunerThreshold gets the GC tuner threshold.
func (o *PersistOptions) GetGCTunerThreshold() float64 {
	return o.GetPDServerConfig().GCTunerThreshold
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (o *PersistOptions) IsRemoveDownReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (o *PersistOptions) IsReplaceOfflineReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (o *PersistOptions) IsMakeUpReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (o *PersistOptions) IsRemoveExtraReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableRemoveExtraReplica
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (o *PersistOptions) IsLocationReplacementEnabled() bool {
	return o.getTTLBoolOr(enableLocationReplacement, o.GetScheduleConfig().EnableLocationReplacement)
}

// IsTikvRegionSplitEnabled returns whether tikv split region is disabled.
func (o *PersistOptions) IsTikvRegionSplitEnabled() bool {
	return o.getTTLBoolOr(enableTiKVSplitRegion, o.GetScheduleConfig().EnableTiKVSplitRegion)
}

// GetMaxMovableHotPeerSize returns the max movable hot peer size.
func (o *PersistOptions) GetMaxMovableHotPeerSize() int64 {
	return o.GetScheduleConfig().MaxMovableHotPeerSize
}

// IsDebugMetricsEnabled returns if debug metrics is enabled.
func (o *PersistOptions) IsDebugMetricsEnabled() bool {
	return o.GetScheduleConfig().EnableDebugMetrics
}

// IsUseJointConsensus returns if using joint consensus as an operator step is enabled.
func (o *PersistOptions) IsUseJointConsensus() bool {
	return o.GetScheduleConfig().EnableJointConsensus
}

// SetEnableUseJointConsensus to set the option for using joint consensus. It's only used to test.
func (o *PersistOptions) SetEnableUseJointConsensus(enable bool) {
	v := o.GetScheduleConfig().Clone()
	v.EnableJointConsensus = enable
	o.SetScheduleConfig(v)
}

// IsTraceRegionFlow returns if the region flow is tracing.
// If the accuracy cannot reach 0.1 MB, it is considered not.
func (o *PersistOptions) IsTraceRegionFlow() bool {
	return o.GetPDServerConfig().FlowRoundByDigit <= maxTraceFlowRoundByDigit
}

// GetHotRegionCacheHitsThreshold is a threshold to decide if a region is hot.
func (o *PersistOptions) GetHotRegionCacheHitsThreshold() int {
	return int(o.GetScheduleConfig().HotRegionCacheHitsThreshold)
}

// GetStoresLimit gets the stores' limit.
func (o *PersistOptions) GetStoresLimit() map[uint64]sc.StoreLimitConfig {
	return o.GetScheduleConfig().StoreLimit
}

// GetSchedulers gets the scheduler configurations.
func (o *PersistOptions) GetSchedulers() sc.SchedulerConfigs {
	return o.GetScheduleConfig().Schedulers
}

// IsSchedulerDisabled returns if the scheduler is disabled.
func (o *PersistOptions) IsSchedulerDisabled(t string) bool {
	schedulers := o.GetScheduleConfig().Schedulers
	for _, s := range schedulers {
		if t == s.Type {
			return s.Disable
		}
	}
	return false
}

// GetHotRegionsWriteInterval gets interval for PD to store Hot Region information.
func (o *PersistOptions) GetHotRegionsWriteInterval() time.Duration {
	return o.GetScheduleConfig().HotRegionsWriteInterval.Duration
}

// GetHotRegionsReservedDays gets days hot region information is kept.
func (o *PersistOptions) GetHotRegionsReservedDays() uint64 {
	return o.GetScheduleConfig().HotRegionsReservedDays
}

// AddSchedulerCfg adds the scheduler configurations.
func (o *PersistOptions) AddSchedulerCfg(tp string, args []string) {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// comparing args is to cover the case that there are schedulers in same type but not with same name
		// such as two schedulers of type "evict-leader",
		// one name is "evict-leader-scheduler-1" and the other is "evict-leader-scheduler-2"
		if reflect.DeepEqual(schedulerCfg, sc.SchedulerConfig{Type: tp, Args: args, Disable: false}) {
			return
		}

		if reflect.DeepEqual(schedulerCfg, sc.SchedulerConfig{Type: tp, Args: args, Disable: true}) {
			schedulerCfg.Disable = false
			v.Schedulers[i] = schedulerCfg
			o.SetScheduleConfig(v)
			return
		}
	}
	v.Schedulers = append(v.Schedulers, sc.SchedulerConfig{Type: tp, Args: args, Disable: false})
	o.SetScheduleConfig(v)
}

// RemoveSchedulerCfg removes the scheduler configurations.
func (o *PersistOptions) RemoveSchedulerCfg(tp string) {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		if tp == schedulerCfg.Type {
			if sc.IsDefaultScheduler(tp) {
				schedulerCfg.Disable = true
				v.Schedulers[i] = schedulerCfg
			} else {
				v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			}
			o.SetScheduleConfig(v)
			return
		}
	}
}

// SetLabelProperty sets the label property.
func (o *PersistOptions) SetLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.GetLabelPropertyConfig().Clone()
	for _, l := range cfg[typ] {
		if l.Key == labelKey && l.Value == labelValue {
			return
		}
	}
	cfg[typ] = append(cfg[typ], StoreLabel{Key: labelKey, Value: labelValue})
	o.labelProperty.Store(cfg)
}

// DeleteLabelProperty deletes the label property.
func (o *PersistOptions) DeleteLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.GetLabelPropertyConfig().Clone()
	oldLabels := cfg[typ]
	cfg[typ] = []StoreLabel{}
	for _, l := range oldLabels {
		if l.Key == labelKey && l.Value == labelValue {
			continue
		}
		cfg[typ] = append(cfg[typ], l)
	}
	if len(cfg[typ]) == 0 {
		delete(cfg, typ)
	}
	o.labelProperty.Store(cfg)
}

// persistedConfig is used to merge all configs into one before saving to storage.
type persistedConfig struct {
	*Config
	// StoreConfig is injected into Config to avoid breaking the original API.
	StoreConfig sc.StoreConfig `json:"store"`
}

// SwitchRaftV2 update some config if tikv raft engine switch into partition raft v2
func (o *PersistOptions) SwitchRaftV2(storage endpoint.ConfigStorage) error {
	o.GetScheduleConfig().StoreLimitVersion = "v2"
	return o.Persist(storage)
}

// Persist saves the configuration to the storage.
func (o *PersistOptions) Persist(storage endpoint.ConfigStorage) error {
	cfg := &persistedConfig{
		Config: &Config{
			Schedule:        *o.GetScheduleConfig(),
			Replication:     *o.GetReplicationConfig(),
			PDServerCfg:     *o.GetPDServerConfig(),
			ReplicationMode: *o.GetReplicationModeConfig(),
			LabelProperty:   o.GetLabelPropertyConfig(),
			Keyspace:        *o.GetKeyspaceConfig(),
			ClusterVersion:  *o.GetClusterVersion(),
		},
		StoreConfig: *o.GetStoreConfig(),
	}
	failpoint.Inject("persistFail", func() {
		failpoint.Return(errors.New("fail to persist"))
	})
	return storage.SaveConfig(cfg)
}

// Reload reloads the configuration from the storage.
func (o *PersistOptions) Reload(storage endpoint.ConfigStorage) error {
	cfg := &persistedConfig{Config: &Config{}}
	// Pass nil to initialize cfg to default values (all items undefined)
	cfg.Adjust(nil, true)

	isExist, err := storage.LoadConfig(cfg)
	if err != nil {
		return err
	}
	o.adjustScheduleCfg(&cfg.Schedule)
	// Some fields may not be stored in the storage, we need to calculate them manually.
	cfg.StoreConfig.Adjust()
	cfg.PDServerCfg.MigrateDeprecatedFlags()
	if isExist {
		o.schedule.Store(&cfg.Schedule)
		o.replication.Store(&cfg.Replication)
		o.pdServerConfig.Store(&cfg.PDServerCfg)
		o.replicationMode.Store(&cfg.ReplicationMode)
		o.labelProperty.Store(cfg.LabelProperty)
		o.keyspace.Store(&cfg.Keyspace)
		o.storeConfig.Store(&cfg.StoreConfig)
		o.SetClusterVersion(&cfg.ClusterVersion)
	}
	return nil
}

func (o *PersistOptions) adjustScheduleCfg(scheduleCfg *sc.ScheduleConfig) {
	// In case we add new default schedulers.
	for _, ps := range sc.DefaultSchedulers {
		if slice.NoneOf(scheduleCfg.Schedulers, func(i int) bool {
			return scheduleCfg.Schedulers[i].Type == ps.Type
		}) {
			scheduleCfg.Schedulers = append(scheduleCfg.Schedulers, ps)
		}
	}
	scheduleCfg.MigrateDeprecatedFlags()
}

// CheckLabelProperty checks the label property.
func (o *PersistOptions) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	pc := o.labelProperty.Load().(LabelPropertyConfig)
	for _, cfg := range pc[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

// GetMinResolvedTSPersistenceInterval gets the interval for PD to save min resolved ts.
func (o *PersistOptions) GetMinResolvedTSPersistenceInterval() time.Duration {
	return o.GetPDServerConfig().MinResolvedTSPersistenceInterval.Duration
}

const ttlConfigPrefix = "/config/ttl"

// SetTTLData set temporary configuration
func (o *PersistOptions) SetTTLData(parCtx context.Context, client *clientv3.Client, key string, value string, ttl time.Duration) error {
	if o.ttl == nil {
		o.ttl = cache.NewStringTTL(parCtx, time.Second*5, time.Minute*5)
	}
	_, err := etcdutil.EtcdKVPutWithTTL(parCtx, client, ttlConfigPrefix+"/"+key, value, int64(ttl.Seconds()))
	if err != nil {
		return err
	}
	o.ttl.PutWithTTL(key, value, ttl)
	return nil
}

func (o *PersistOptions) getTTLUint(key string) (uint64, bool, error) {
	stringForm, ok := o.GetTTLData(key)
	if !ok {
		return 0, false, nil
	}
	r, err := strconv.ParseUint(stringForm, 10, 64)
	return r, true, err
}

func (o *PersistOptions) getTTLUintOr(key string, defaultValue uint64) uint64 {
	if v, ok, err := o.getTTLUint(key); ok {
		if err == nil {
			return v
		}
		log.Warn("failed to parse "+key+" from PersistOptions's ttl storage", zap.Error(err))
	}
	return defaultValue
}

func (o *PersistOptions) getTTLBool(key string) (result bool, contains bool, err error) {
	stringForm, ok := o.GetTTLData(key)
	if !ok {
		return
	}
	result, err = strconv.ParseBool(stringForm)
	contains = true
	return
}

func (o *PersistOptions) getTTLBoolOr(key string, defaultValue bool) bool {
	if v, ok, err := o.getTTLBool(key); ok {
		if err == nil {
			return v
		}
		log.Warn("failed to parse "+key+" from PersistOptions's ttl storage", zap.Error(err))
	}
	return defaultValue
}

func (o *PersistOptions) getTTLFloat(key string) (float64, bool, error) {
	stringForm, ok := o.GetTTLData(key)
	if !ok {
		return 0, false, nil
	}
	r, err := strconv.ParseFloat(stringForm, 64)
	return r, true, err
}

func (o *PersistOptions) getTTLFloatOr(key string, defaultValue float64) float64 {
	if v, ok, err := o.getTTLFloat(key); ok {
		if err == nil {
			return v
		}
		log.Warn("failed to parse "+key+" from PersistOptions's ttl storage", zap.Error(err))
	}
	return defaultValue
}

// GetTTLData returns if there is a TTL data for a given key.
func (o *PersistOptions) GetTTLData(key string) (string, bool) {
	if o.ttl == nil {
		return "", false
	}
	if result, ok := o.ttl.Get(key); ok {
		return result.(string), ok
	}
	return "", false
}

// LoadTTLFromEtcd loads temporary configuration which was persisted into etcd
func (o *PersistOptions) LoadTTLFromEtcd(ctx context.Context, client *clientv3.Client) error {
	resps, err := etcdutil.EtcdKVGet(client, ttlConfigPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if o.ttl == nil {
		o.ttl = cache.NewStringTTL(ctx, time.Second*5, time.Minute*5)
	}
	for _, resp := range resps.Kvs {
		key := string(resp.Key)[len(ttlConfigPrefix)+1:]
		value := string(resp.Value)
		leaseID := resp.Lease
		resp, err := client.TimeToLive(ctx, clientv3.LeaseID(leaseID))
		if err != nil {
			return err
		}
		o.ttl.PutWithTTL(key, value, time.Duration(resp.TTL)*time.Second)
	}
	return nil
}

// SetAllStoresLimitTTL sets all store limit for a given type and rate with ttl.
func (o *PersistOptions) SetAllStoresLimitTTL(ctx context.Context, client *clientv3.Client, typ storelimit.Type, ratePerMin float64, ttl time.Duration) error {
	var err error
	switch typ {
	case storelimit.AddPeer:
		err = o.SetTTLData(ctx, client, "default-add-peer", fmt.Sprint(ratePerMin), ttl)
	case storelimit.RemovePeer:
		err = o.SetTTLData(ctx, client, "default-remove-peer", fmt.Sprint(ratePerMin), ttl)
	}
	return err
}

var haltSchedulingStatus = schedulingAllowanceStatusGauge.WithLabelValues("halt-scheduling")

// SetHaltScheduling set HaltScheduling.
func (o *PersistOptions) SetHaltScheduling(halt bool, source string) {
	v := o.GetScheduleConfig().Clone()
	v.HaltScheduling = halt
	o.SetScheduleConfig(v)
	if halt {
		haltSchedulingStatus.Set(1)
		schedulingAllowanceStatusGauge.WithLabelValues(source).Set(1)
	} else {
		haltSchedulingStatus.Set(0)
		schedulingAllowanceStatusGauge.WithLabelValues(source).Set(0)
	}
}

// IsSchedulingHalted returns if PD scheduling is halted.
func (o *PersistOptions) IsSchedulingHalted() bool {
	if o == nil {
		return false
	}
	return o.GetScheduleConfig().HaltScheduling
}

// GetRegionMaxSize returns the max region size in MB
func (o *PersistOptions) GetRegionMaxSize() uint64 {
	return o.GetStoreConfig().GetRegionMaxSize()
}

// GetRegionMaxKeys returns the max region keys
func (o *PersistOptions) GetRegionMaxKeys() uint64 {
	return o.GetStoreConfig().GetRegionMaxKeys()
}

// GetRegionSplitSize returns the region split size in MB
func (o *PersistOptions) GetRegionSplitSize() uint64 {
	return o.GetStoreConfig().GetRegionSplitSize()
}

// GetRegionSplitKeys returns the region split keys
func (o *PersistOptions) GetRegionSplitKeys() uint64 {
	return o.GetStoreConfig().GetRegionSplitKeys()
}

// CheckRegionSize return error if the smallest region's size is less than mergeSize
func (o *PersistOptions) CheckRegionSize(size, mergeSize uint64) error {
	return o.GetStoreConfig().CheckRegionSize(size, mergeSize)
}

// CheckRegionKeys return error if the smallest region's keys is less than mergeKeys
func (o *PersistOptions) CheckRegionKeys(keys, mergeKeys uint64) error {
	return o.GetStoreConfig().CheckRegionKeys(keys, mergeKeys)
}

// IsEnableRegionBucket return true if the region bucket is enabled.
func (o *PersistOptions) IsEnableRegionBucket() bool {
	return o.GetStoreConfig().IsEnableRegionBucket()
}

// IsRaftKV2 returns true if the raft kv is v2.
func (o *PersistOptions) IsRaftKV2() bool {
	return o.GetStoreConfig().IsRaftKV2()
}

// SetRegionBucketEnabled sets if the region bucket is enabled.
func (o *PersistOptions) SetRegionBucketEnabled(enabled bool) {
	cfg := o.GetStoreConfig().Clone()
	cfg.SetRegionBucketEnabled(enabled)
	o.SetStoreConfig(cfg)
}

// GetRegionBucketSize returns the region bucket size.
func (o *PersistOptions) GetRegionBucketSize() uint64 {
	return o.GetStoreConfig().GetRegionBucketSize()
}
