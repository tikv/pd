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
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"go.etcd.io/etcd/clientv3"
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
	o.SetClusterVersion(&cfg.ClusterVersion)
	o.ttl = nil
	return o
}

// GetScheduleConfig returns scheduling configurations.
func (o *PersistOptions) GetScheduleConfig() *ScheduleConfig {
	return o.schedule.Load().(*ScheduleConfig)
}

// SetScheduleConfig sets the PD scheduling configuration.
func (o *PersistOptions) SetScheduleConfig(cfg *ScheduleConfig) {
	o.schedule.Store(cfg)
}

// GetReplicationConfig returns replication configurations.
func (o *PersistOptions) GetReplicationConfig() *ReplicationConfig {
	return o.replication.Load().(*ReplicationConfig)
}

// SetReplicationConfig sets the PD replication configuration.
func (o *PersistOptions) SetReplicationConfig(cfg *ReplicationConfig) {
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
	replicaRescheduleLimitKey      = "schedule.replica-schedule-limit"
	mergeScheduleLimitKey          = "schedule.merge-schedule-limit"
	hotRegionScheduleLimitKey      = "schedule.hot-region-schedule-limit"
	schedulerMaxWaitingOperatorKey = "schedule.scheduler-max-waiting-operator"
	enableLocationReplacement      = "schedule.enable-location-replacement"
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
func (o *PersistOptions) GetMaxMergeRegionKeys() uint64 {
	return o.getTTLUintOr(maxMergeRegionKeysKey, o.GetScheduleConfig().MaxMergeRegionKeys)
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

// SetStoreLimit sets a store limit for a given type and rate.
func (o *PersistOptions) SetStoreLimit(storeID uint64, typ storelimit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	var sc StoreLimitConfig
	var rate float64
	switch typ {
	case storelimit.AddPeer:
		if _, ok := v.StoreLimit[storeID]; !ok {
			rate = DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
		} else {
			rate = v.StoreLimit[storeID].RemovePeer
		}
		sc = StoreLimitConfig{AddPeer: ratePerMin, RemovePeer: rate}
	case storelimit.RemovePeer:
		if _, ok := v.StoreLimit[storeID]; !ok {
			rate = DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
		} else {
			rate = v.StoreLimit[storeID].AddPeer
		}
		sc = StoreLimitConfig{AddPeer: rate, RemovePeer: ratePerMin}
	}
	v.StoreLimit[storeID] = sc
	o.SetScheduleConfig(v)
}

// SetAllStoresLimit sets all store limit for a given type and rate.
func (o *PersistOptions) SetAllStoresLimit(typ storelimit.Type, ratePerMin float64) {
	v := o.GetScheduleConfig().Clone()
	switch typ {
	case storelimit.AddPeer:
		DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, ratePerMin)
		for storeID := range v.StoreLimit {
			sc := StoreLimitConfig{AddPeer: ratePerMin, RemovePeer: v.StoreLimit[storeID].RemovePeer}
			v.StoreLimit[storeID] = sc
		}
	case storelimit.RemovePeer:
		DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, ratePerMin)
		for storeID := range v.StoreLimit {
			sc := StoreLimitConfig{AddPeer: v.StoreLimit[storeID].AddPeer, RemovePeer: ratePerMin}
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

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *PersistOptions) GetLeaderScheduleLimit() uint64 {
	return o.getTTLUintOr(leaderScheduleLimitKey, o.GetScheduleConfig().LeaderScheduleLimit)
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (o *PersistOptions) GetRegionScheduleLimit() uint64 {
	return o.getTTLUintOr(regionScheduleLimitKey, o.GetScheduleConfig().RegionScheduleLimit)
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
func (o *PersistOptions) GetStoreLimit(storeID uint64) (returnSC StoreLimitConfig) {
	defer func() {
		returnSC.RemovePeer = o.getTTLFloatOr(fmt.Sprintf("remove-peer-%v", storeID), returnSC.RemovePeer)
		returnSC.AddPeer = o.getTTLFloatOr(fmt.Sprintf("add-peer-%v", storeID), returnSC.AddPeer)
	}()
	if limit, ok := o.GetScheduleConfig().StoreLimit[storeID]; ok {
		return limit
	}
	cfg := o.GetScheduleConfig().Clone()
	sc := StoreLimitConfig{
		AddPeer:    DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
		RemovePeer: DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
	}
	v, ok1, err := o.getTTLFloat("default-add-peer")
	if err != nil {
		log.Warn("failed to parse default-add-peer from PersistOptions's ttl storage")
	}
	canSetAddPeer := ok1 && err == nil
	if canSetAddPeer {
		returnSC.AddPeer = v
	}

	v, ok2, err := o.getTTLFloat("default-remove-peer")
	if err != nil {
		log.Warn("failed to parse default-remove-peer from PersistOptions's ttl storage")
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
	default:
		panic("no such limit type")
	}
}

// GetAllStoresLimit returns the limit of all stores.
func (o *PersistOptions) GetAllStoresLimit() map[uint64]StoreLimitConfig {
	return o.GetScheduleConfig().StoreLimit
}

// GetStoreLimitMode returns the limit mode of store.
func (o *PersistOptions) GetStoreLimitMode() string {
	return o.GetScheduleConfig().StoreLimitMode
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *PersistOptions) GetTolerantSizeRatio() float64 {
	return o.GetScheduleConfig().TolerantSizeRatio
}

// GetLowSpaceRatio returns the low space ratio.
func (o *PersistOptions) GetLowSpaceRatio() float64 {
	return o.GetScheduleConfig().LowSpaceRatio
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
func (o *PersistOptions) GetLeaderSchedulePolicy() core.SchedulePolicy {
	return core.StringToSchedulePolicy(o.GetScheduleConfig().LeaderSchedulePolicy)
}

// GetKeyType is to get key type.
func (o *PersistOptions) GetKeyType() core.KeyType {
	return core.StringToKeyType(o.GetPDServerConfig().KeyType)
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
	if v, ok := o.getTTLData(enableLocationReplacement); ok {
		result, err := strconv.ParseBool(v)
		if err == nil {
			return result
		}
		log.Warn("failed to parse " + enableLocationReplacement + " from PersistOptions's ttl storage")
	}
	return o.GetScheduleConfig().EnableLocationReplacement
}

// IsDebugMetricsEnabled returns if debug metrics is enabled.
func (o *PersistOptions) IsDebugMetricsEnabled() bool {
	return o.GetScheduleConfig().EnableDebugMetrics
}

// IsUseJointConsensus returns if using joint consensus as a operator step is enabled.
func (o *PersistOptions) IsUseJointConsensus() bool {
	return o.GetScheduleConfig().EnableJointConsensus
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
func (o *PersistOptions) GetStoresLimit() map[uint64]StoreLimitConfig {
	return o.GetScheduleConfig().StoreLimit
}

// GetSchedulers gets the scheduler configurations.
func (o *PersistOptions) GetSchedulers() SchedulerConfigs {
	return o.GetScheduleConfig().Schedulers
}

// AddSchedulerCfg adds the scheduler configurations.
func (o *PersistOptions) AddSchedulerCfg(tp string, args []string) {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// comparing args is to cover the case that there are schedulers in same type but not with same name
		// such as two schedulers of type "evict-leader",
		// one name is "evict-leader-scheduler-1" and the other is "evict-leader-scheduler-2"
		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{Type: tp, Args: args, Disable: false}) {
			return
		}

		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{Type: tp, Args: args, Disable: true}) {
			schedulerCfg.Disable = false
			v.Schedulers[i] = schedulerCfg
			o.SetScheduleConfig(v)
			return
		}
	}
	v.Schedulers = append(v.Schedulers, SchedulerConfig{Type: tp, Args: args, Disable: false})
	o.SetScheduleConfig(v)
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

// Persist saves the configuration to the storage.
func (o *PersistOptions) Persist(storage *core.Storage) error {
	cfg := &Config{
		Schedule:        *o.GetScheduleConfig(),
		Replication:     *o.GetReplicationConfig(),
		PDServerCfg:     *o.GetPDServerConfig(),
		ReplicationMode: *o.GetReplicationModeConfig(),
		LabelProperty:   o.GetLabelPropertyConfig(),
		ClusterVersion:  *o.GetClusterVersion(),
	}
	err := storage.SaveConfig(cfg)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	return err
}

// Reload reloads the configuration from the storage.
func (o *PersistOptions) Reload(storage *core.Storage) error {
	cfg := &Config{}
	// pass nil to initialize cfg to default values (all items undefined)
	cfg.Adjust(nil, true)

	isExist, err := storage.LoadConfig(cfg)
	if err != nil {
		return err
	}
	o.adjustScheduleCfg(&cfg.Schedule)
	cfg.PDServerCfg.MigrateDeprecatedFlags()
	if isExist {
		o.schedule.Store(&cfg.Schedule)
		o.replication.Store(&cfg.Replication)
		o.pdServerConfig.Store(&cfg.PDServerCfg)
		o.replicationMode.Store(&cfg.ReplicationMode)
		o.labelProperty.Store(cfg.LabelProperty)
		o.SetClusterVersion(&cfg.ClusterVersion)
	}
	return nil
}

func (o *PersistOptions) adjustScheduleCfg(scheduleCfg *ScheduleConfig) {
	// In case we add new default schedulers.
	for _, ps := range DefaultSchedulers {
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
	stringForm, ok := o.getTTLData(key)
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
		log.Warn("failed to parse " + key + " from PersistOptions's ttl storage")
	}
	return defaultValue
}

func (o *PersistOptions) getTTLFloat(key string) (float64, bool, error) {
	stringForm, ok := o.getTTLData(key)
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
		log.Warn("failed to parse " + key + " from PersistOptions's ttl storage")
	}
	return defaultValue
}

func (o *PersistOptions) getTTLData(key string) (string, bool) {
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
