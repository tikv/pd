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

package config

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/typeutil"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/kv"
	"github.com/pingcap/pd/v4/server/schedule"
)

// PersistOptions wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistOptions struct {
	schedule        atomic.Value
	replication     *Replication
	labelProperty   atomic.Value
	clusterVersion  unsafe.Pointer
	pdServerConfig  atomic.Value
	logConfig       atomic.Value
	replicationMode atomic.Value
}

// NewPersistOptions creates a new PersistOptions instance.
func NewPersistOptions(cfg *Config) *PersistOptions {
	o := &PersistOptions{}
	o.Store(&cfg.Schedule)
	o.replication = newReplication(&cfg.Replication)
	o.pdServerConfig.Store(&cfg.PDServerCfg)
	o.labelProperty.Store(cfg.LabelProperty)
	o.SetClusterVersion(&cfg.ClusterVersion)
	o.logConfig.Store(&cfg.Log)
	o.replicationMode.Store(&cfg.ReplicationMode)
	return o
}

// Load returns scheduling configurations.
func (o *PersistOptions) Load() *ScheduleConfig {
	return o.schedule.Load().(*ScheduleConfig)
}

// Store sets scheduling configurations.
func (o *PersistOptions) Store(cfg *ScheduleConfig) {
	o.schedule.Store(cfg)
}

// GetReplication returns replication configurations.
func (o *PersistOptions) GetReplication() *Replication {
	return o.replication
}

// GetPDServerConfig returns pd server configurations.
func (o *PersistOptions) GetPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
}

// SetPDServerConfig sets the PD configuration.
func (o *PersistOptions) SetPDServerConfig(cfg *PDServerConfig) {
	o.pdServerConfig.Store(cfg)
}

// GetLogConfig returns log configuration.
func (o *PersistOptions) GetLogConfig() *log.Config {
	return o.logConfig.Load().(*log.Config)
}

// SetLogConfig sets the log configuration.
func (o *PersistOptions) SetLogConfig(cfg *log.Config) {
	o.logConfig.Store(cfg)
}

// GetReplicationModeConfig returns the replication mode config.
func (o *PersistOptions) GetReplicationModeConfig() *ReplicationModeConfig {
	return o.replicationMode.Load().(*ReplicationModeConfig)
}

// SetReplicationModeConfig sets the replication mode config.
func (o *PersistOptions) SetReplicationModeConfig(cfg *ReplicationModeConfig) {
	o.replicationMode.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (o *PersistOptions) GetMaxReplicas() int {
	return o.replication.GetMaxReplicas()
}

// SetMaxReplicas sets the number of replicas for each region.
func (o *PersistOptions) SetMaxReplicas(replicas int) {
	o.replication.SetMaxReplicas(replicas)
}

// GetLocationLabels returns the location labels for each region.
func (o *PersistOptions) GetLocationLabels() []string {
	return o.replication.GetLocationLabels()
}

// IsPlacementRulesEnabled returns if the placement rules is enabled.
func (o *PersistOptions) IsPlacementRulesEnabled() bool {
	return o.replication.IsPlacementRulesEnabled()
}

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (o *PersistOptions) GetMaxSnapshotCount() uint64 {
	return o.Load().MaxSnapshotCount
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (o *PersistOptions) GetMaxPendingPeerCount() uint64 {
	return o.Load().MaxPendingPeerCount
}

// GetMaxMergeRegionSize returns the max region size.
func (o *PersistOptions) GetMaxMergeRegionSize() uint64 {
	return o.Load().MaxMergeRegionSize
}

// GetMaxMergeRegionKeys returns the max number of keys.
func (o *PersistOptions) GetMaxMergeRegionKeys() uint64 {
	return o.Load().MaxMergeRegionKeys
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (o *PersistOptions) GetSplitMergeInterval() time.Duration {
	return o.Load().SplitMergeInterval.Duration
}

// SetSplitMergeInterval to set the interval between finishing split and starting to merge. It's only used to test.
func (o *PersistOptions) SetSplitMergeInterval(splitMergeInterval time.Duration) {
	o.Load().SplitMergeInterval = typeutil.Duration{Duration: splitMergeInterval}
}

// IsOneWayMergeEnabled returns if a region can only be merged into the next region of it.
func (o *PersistOptions) IsOneWayMergeEnabled() bool {
	return o.Load().EnableOneWayMerge
}

// IsCrossTableMergeEnabled returns if across table merge is enabled.
func (o *PersistOptions) IsCrossTableMergeEnabled() bool {
	return o.Load().EnableCrossTableMerge
}

// GetPatrolRegionInterval returns the interval of patroling region.
func (o *PersistOptions) GetPatrolRegionInterval() time.Duration {
	return o.Load().PatrolRegionInterval.Duration
}

// GetMaxStoreDownTime returns the max down time of a store.
func (o *PersistOptions) GetMaxStoreDownTime() time.Duration {
	return o.Load().MaxStoreDownTime.Duration
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *PersistOptions) GetLeaderScheduleLimit() uint64 {
	return o.Load().LeaderScheduleLimit
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (o *PersistOptions) GetRegionScheduleLimit() uint64 {
	return o.Load().RegionScheduleLimit
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *PersistOptions) GetReplicaScheduleLimit() uint64 {
	return o.Load().ReplicaScheduleLimit
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *PersistOptions) GetMergeScheduleLimit() uint64 {
	return o.Load().MergeScheduleLimit
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (o *PersistOptions) GetHotRegionScheduleLimit() uint64 {
	return o.Load().HotRegionScheduleLimit
}

// GetStoreBalanceRate returns the balance rate of a store.
func (o *PersistOptions) GetStoreBalanceRate() float64 {
	return o.Load().StoreBalanceRate
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *PersistOptions) GetTolerantSizeRatio() float64 {
	return o.Load().TolerantSizeRatio
}

// GetLowSpaceRatio returns the low space ratio.
func (o *PersistOptions) GetLowSpaceRatio() float64 {
	return o.Load().LowSpaceRatio
}

// GetHighSpaceRatio returns the high space ratio.
func (o *PersistOptions) GetHighSpaceRatio() float64 {
	return o.Load().HighSpaceRatio
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (o *PersistOptions) GetSchedulerMaxWaitingOperator() uint64 {
	return o.Load().SchedulerMaxWaitingOperator
}

// GetLeaderSchedulePolicy is to get leader schedule policy.
func (o *PersistOptions) GetLeaderSchedulePolicy() core.SchedulePolicy {
	return core.StringToSchedulePolicy(o.Load().LeaderSchedulePolicy)
}

// GetKeyType is to get key type.
func (o *PersistOptions) GetKeyType() core.KeyType {
	return core.StringToKeyType(o.LoadPDServerConfig().KeyType)
}

// GetDashboardAddress gets dashboard address.
func (o *PersistOptions) GetDashboardAddress() string {
	return o.LoadPDServerConfig().DashboardAddress
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (o *PersistOptions) IsRemoveDownReplicaEnabled() bool {
	return o.Load().EnableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (o *PersistOptions) IsReplaceOfflineReplicaEnabled() bool {
	return o.Load().EnableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (o *PersistOptions) IsMakeUpReplicaEnabled() bool {
	return o.Load().EnableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (o *PersistOptions) IsRemoveExtraReplicaEnabled() bool {
	return o.Load().EnableRemoveExtraReplica
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (o *PersistOptions) IsLocationReplacementEnabled() bool {
	return o.Load().EnableLocationReplacement
}

// IsDebugMetricsEnabled mocks method
func (o *PersistOptions) IsDebugMetricsEnabled() bool {
	return o.Load().EnableDebugMetrics
}

// GetSchedulers gets the scheduler configurations.
func (o *PersistOptions) GetSchedulers() SchedulerConfigs {
	return o.Load().Schedulers
}

// AddSchedulerCfg adds the scheduler configurations.
func (o *PersistOptions) AddSchedulerCfg(tp string, args []string) {
	c := o.Load()
	v := c.Clone()
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
			o.Store(v)
			return
		}
	}
	v.Schedulers = append(v.Schedulers, SchedulerConfig{Type: tp, Args: args, Disable: false})
	o.Store(v)
}

// RemoveSchedulerCfg removes the scheduler configurations.
func (o *PersistOptions) RemoveSchedulerCfg(ctx context.Context, name string) error {
	c := o.Load()
	v := c.Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewOperatorController(ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), decoder)
		if err != nil {
			return err
		}
		if tmp.GetName() == name {
			if IsDefaultScheduler(tmp.GetType()) {
				schedulerCfg.Disable = true
				v.Schedulers[i] = schedulerCfg
			} else {
				v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			}
			o.Store(v)
			return nil
		}
	}
	return nil
}

// SetLabelProperty sets the label property.
func (o *PersistOptions) SetLabelProperty(typ, labelKey, labelValue string) {
	cfg := o.LoadLabelPropertyConfig().Clone()
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
	cfg := o.LoadLabelPropertyConfig().Clone()
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

// LoadLabelPropertyConfig returns the label property.
func (o *PersistOptions) LoadLabelPropertyConfig() LabelPropertyConfig {
	return o.labelProperty.Load().(LabelPropertyConfig)
}

// SetLabelPropertyConfig sets the label property configuration.
func (o *PersistOptions) SetLabelPropertyConfig(cfg LabelPropertyConfig) {
	o.labelProperty.Store(cfg)
}

// SetClusterVersion sets the cluster version.
func (o *PersistOptions) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// CASClusterVersion sets the cluster version.
func (o *PersistOptions) CASClusterVersion(old, new *semver.Version) bool {
	return atomic.CompareAndSwapPointer(&o.clusterVersion, unsafe.Pointer(old), unsafe.Pointer(new))
}

// LoadClusterVersion returns the cluster version.
func (o *PersistOptions) LoadClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// LoadPDServerConfig returns PD server configuration.
func (o *PersistOptions) LoadPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
}

// LoadLogConfig returns log configuration.
func (o *PersistOptions) LoadLogConfig() *log.Config {
	return o.logConfig.Load().(*log.Config)
}

// Persist saves the configuration to the storage.
func (o *PersistOptions) Persist(storage *core.Storage) error {
	cfg := &Config{
		Schedule:        *o.Load(),
		Replication:     *o.replication.Load(),
		LabelProperty:   o.LoadLabelPropertyConfig(),
		ClusterVersion:  *o.LoadClusterVersion(),
		PDServerCfg:     *o.LoadPDServerConfig(),
		Log:             *o.LoadLogConfig(),
		ReplicationMode: *o.GetReplicationModeConfig(),
	}
	err := storage.SaveConfig(cfg)
	return err
}

// Reload reloads the configuration from the storage.
func (o *PersistOptions) Reload(storage *core.Storage) error {
	cfg := &Config{
		Schedule:        *o.Load().Clone(),
		Replication:     *o.replication.Load(),
		LabelProperty:   o.LoadLabelPropertyConfig().Clone(),
		ClusterVersion:  *o.LoadClusterVersion(),
		PDServerCfg:     *o.LoadPDServerConfig(),
		Log:             *o.LoadLogConfig(),
		ReplicationMode: *o.GetReplicationModeConfig().Clone(),
	}
	isExist, err := storage.LoadConfig(cfg)
	if err != nil {
		return err
	}
	o.adjustScheduleCfg(cfg)
	if isExist {
		o.Store(&cfg.Schedule)
		o.replication.Store(&cfg.Replication)
		o.labelProperty.Store(cfg.LabelProperty)
		o.SetClusterVersion(&cfg.ClusterVersion)
		o.pdServerConfig.Store(&cfg.PDServerCfg)
		o.logConfig.Store(&cfg.Log)
		o.replicationMode.Store(&cfg.ReplicationMode)
	}
	return nil
}

func (o *PersistOptions) adjustScheduleCfg(persistentCfg *Config) {
	scheduleCfg := o.Load().Clone()
	for i, s := range scheduleCfg.Schedulers {
		for _, ps := range persistentCfg.Schedule.Schedulers {
			if s.Type == ps.Type && reflect.DeepEqual(s.Args, ps.Args) {
				scheduleCfg.Schedulers[i].Disable = ps.Disable
				break
			}
		}
	}
	restoredSchedulers := make([]SchedulerConfig, 0, len(persistentCfg.Schedule.Schedulers))
	for _, ps := range persistentCfg.Schedule.Schedulers {
		needRestore := true
		for _, s := range scheduleCfg.Schedulers {
			if s.Type == ps.Type && reflect.DeepEqual(s.Args, ps.Args) {
				needRestore = false
				break
			}
		}
		if needRestore {
			restoredSchedulers = append(restoredSchedulers, ps)
		}
	}
	scheduleCfg.Schedulers = append(scheduleCfg.Schedulers, restoredSchedulers...)
	persistentCfg.Schedule.Schedulers = scheduleCfg.Schedulers
	persistentCfg.Schedule.MigrateDeprecatedFlags()
	o.Store(scheduleCfg)
}

// GetHotRegionCacheHitsThreshold is a threshold to decide if a region is hot.
func (o *PersistOptions) GetHotRegionCacheHitsThreshold() int {
	return int(o.Load().HotRegionCacheHitsThreshold)
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

// Replication provides some help to do replication.
type Replication struct {
	replicateCfg atomic.Value
}

func newReplication(cfg *ReplicationConfig) *Replication {
	r := &Replication{}
	r.Store(cfg)
	return r
}

// Load returns replication configurations.
func (r *Replication) Load() *ReplicationConfig {
	return r.replicateCfg.Load().(*ReplicationConfig)
}

// Store sets replication configurations.
func (r *Replication) Store(cfg *ReplicationConfig) {
	r.replicateCfg.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (r *Replication) GetMaxReplicas() int {
	return int(r.Load().MaxReplicas)
}

// SetMaxReplicas set the replicas for each region.
func (r *Replication) SetMaxReplicas(replicas int) {
	c := r.Load()
	v := c.clone()
	v.MaxReplicas = uint64(replicas)
	r.Store(v)
}

// GetLocationLabels returns the location labels for each region.
func (r *Replication) GetLocationLabels() []string {
	return r.Load().LocationLabels
}

// GetStrictlyMatchLabel returns whether check label strict.
func (r *Replication) GetStrictlyMatchLabel() bool {
	return r.Load().StrictlyMatchLabel
}

// IsPlacementRulesEnabled returns whether the feature is enabled.
func (r *Replication) IsPlacementRulesEnabled() bool {
	return r.Load().EnablePlacementRules
}
