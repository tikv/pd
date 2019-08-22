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
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

// ScheduleOption is a wrapper to access the configuration safely.
type ScheduleOption struct {
	v              atomic.Value
	rep            *Replication
	ns             sync.Map // concurrent map[string]*namespaceOption
	labelProperty  atomic.Value
	clusterVersion unsafe.Pointer
	pdServerConfig atomic.Value
}

// NewScheduleOption creates a new ScheduleOption.
func NewScheduleOption(cfg *Config) *ScheduleOption {
	o := &ScheduleOption{}
	o.Store(&cfg.Schedule)
	o.ns = sync.Map{}
	for name, nsCfg := range cfg.Namespace {
		nsCfg := nsCfg
		o.ns.Store(name, NewNamespaceOption(&nsCfg))
	}
	o.rep = newReplication(&cfg.Replication)
	o.pdServerConfig.Store(&cfg.PDServerCfg)
	o.labelProperty.Store(cfg.LabelProperty)
	o.SetClusterVersion(&cfg.ClusterVersion)
	return o
}

// Load returns scheduling configurations.
func (o *ScheduleOption) Load() *ScheduleConfig {
	return o.v.Load().(*ScheduleConfig)
}

// Store sets scheduling configurations.
func (o *ScheduleOption) Store(cfg *ScheduleConfig) {
	o.v.Store(cfg)
}

// GetReplication returns replication configurations.
func (o *ScheduleOption) GetReplication() *Replication {
	return o.rep
}

// SetPDServerConfig sets the PD configuration.
func (o *ScheduleOption) SetPDServerConfig(cfg *PDServerConfig) {
	o.pdServerConfig.Store(cfg)
}

// SetNS sets the namespace configurations.
func (o *ScheduleOption) SetNS(name string, nsOpt *namespaceOption) {
	o.ns.Store(name, nsOpt)
}

// DeleteNS deletes the namespace configurations.
func (o *ScheduleOption) DeleteNS(name string) {
	o.ns.Delete(name)
}

// GetNS gets the namespace configurations.
func (o *ScheduleOption) GetNS(name string) (*namespaceOption, bool) {
	if n, ok := o.ns.Load(name); ok {
		if n, ok := n.(*namespaceOption); ok {
			return n, true
		}
	}
	return nil, false
}

// LoadNSConfig loads the namespace configurations.
func (o *ScheduleOption) LoadNSConfig() map[string]NamespaceConfig {
	namespaces := make(map[string]NamespaceConfig)
	f := func(k, v interface{}) bool {
		var kstr string
		var ok bool
		if kstr, ok = k.(string); !ok {
			return false
		}
		if ns, ok := v.(*namespaceOption); ok {
			namespaces[kstr] = *ns.Load()
			return true
		}
		return false
	}
	o.ns.Range(f)

	return namespaces
}

// GetMaxReplicas returns the number of replicas for each region.
func (o *ScheduleOption) GetMaxReplicas(name string) int {
	if n, ok := o.GetNS(name); ok {
		return n.GetMaxReplicas()
	}
	return o.rep.GetMaxReplicas()
}

// SetMaxReplicas sets the number of replicas for each region.
func (o *ScheduleOption) SetMaxReplicas(replicas int) {
	o.rep.SetMaxReplicas(replicas)
}

// GetLocationLabels returns the location labels for each region.
func (o *ScheduleOption) GetLocationLabels() []string {
	return o.rep.GetLocationLabels()
}

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (o *ScheduleOption) GetMaxSnapshotCount() uint64 {
	return o.Load().MaxSnapshotCount
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (o *ScheduleOption) GetMaxPendingPeerCount() uint64 {
	return o.Load().MaxPendingPeerCount
}

// GetMaxMergeRegionSize returns the max region size.
func (o *ScheduleOption) GetMaxMergeRegionSize() uint64 {
	return o.Load().MaxMergeRegionSize
}

// GetMaxMergeRegionKeys returns the max number of keys.
func (o *ScheduleOption) GetMaxMergeRegionKeys() uint64 {
	return o.Load().MaxMergeRegionKeys
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (o *ScheduleOption) GetSplitMergeInterval() time.Duration {
	return o.Load().SplitMergeInterval.Duration
}

// GetEnableOneWayMerge returns if the one way merge is enabled.
func (o *ScheduleOption) GetEnableOneWayMerge() bool {
	return o.Load().EnableOneWayMerge
}

// GetPatrolRegionInterval returns the interval of patroling region.
func (o *ScheduleOption) GetPatrolRegionInterval() time.Duration {
	return o.Load().PatrolRegionInterval.Duration
}

// GetMaxStoreDownTime returns the max down time of a store.
func (o *ScheduleOption) GetMaxStoreDownTime() time.Duration {
	return o.Load().MaxStoreDownTime.Duration
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *ScheduleOption) GetLeaderScheduleLimit(name string) uint64 {
	if n, ok := o.GetNS(name); ok {
		return n.GetLeaderScheduleLimit()
	}
	return o.Load().LeaderScheduleLimit
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (o *ScheduleOption) GetRegionScheduleLimit(name string) uint64 {
	if n, ok := o.GetNS(name); ok {
		return n.GetRegionScheduleLimit()
	}
	return o.Load().RegionScheduleLimit
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *ScheduleOption) GetReplicaScheduleLimit(name string) uint64 {
	if n, ok := o.GetNS(name); ok {
		return n.GetReplicaScheduleLimit()
	}
	return o.Load().ReplicaScheduleLimit
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *ScheduleOption) GetMergeScheduleLimit(name string) uint64 {
	if n, ok := o.GetNS(name); ok {
		return n.GetMergeScheduleLimit()
	}
	return o.Load().MergeScheduleLimit
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (o *ScheduleOption) GetHotRegionScheduleLimit(name string) uint64 {
	if n, ok := o.GetNS(name); ok {
		return n.GetHotRegionScheduleLimit()
	}
	return o.Load().HotRegionScheduleLimit
}

// GetStoreBalanceRate returns the balance rate of a store.
func (o *ScheduleOption) GetStoreBalanceRate() float64 {
	return o.Load().StoreBalanceRate
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *ScheduleOption) GetTolerantSizeRatio() float64 {
	return o.Load().TolerantSizeRatio
}

// GetLowSpaceRatio returns the low space ratio.
func (o *ScheduleOption) GetLowSpaceRatio() float64 {
	return o.Load().LowSpaceRatio
}

// GetHighSpaceRatio returns the high space ratio.
func (o *ScheduleOption) GetHighSpaceRatio() float64 {
	return o.Load().HighSpaceRatio
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (o *ScheduleOption) GetSchedulerMaxWaitingOperator() uint64 {
	return o.Load().SchedulerMaxWaitingOperator
}

// IsRaftLearnerEnabled returns if raft learner is enabled.
func (o *ScheduleOption) IsRaftLearnerEnabled() bool {
	return !o.Load().DisableLearner
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (o *ScheduleOption) IsRemoveDownReplicaEnabled() bool {
	return !o.Load().DisableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (o *ScheduleOption) IsReplaceOfflineReplicaEnabled() bool {
	return !o.Load().DisableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (o *ScheduleOption) IsMakeUpReplicaEnabled() bool {
	return !o.Load().DisableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (o *ScheduleOption) IsRemoveExtraReplicaEnabled() bool {
	return !o.Load().DisableRemoveExtraReplica
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (o *ScheduleOption) IsLocationReplacementEnabled() bool {
	return !o.Load().DisableLocationReplacement
}

// IsNamespaceRelocationEnabled returns if namespace relocation is enabled.
func (o *ScheduleOption) IsNamespaceRelocationEnabled() bool {
	return !o.Load().DisableNamespaceRelocation
}

// GetSchedulers gets the scheduler configurations.
func (o *ScheduleOption) GetSchedulers() SchedulerConfigs {
	return o.Load().Schedulers
}

// AddSchedulerCfg adds the scheduler configurations.
func (o *ScheduleOption) AddSchedulerCfg(tp string, args []string) {
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
func (o *ScheduleOption) RemoveSchedulerCfg(name string) error {
	c := o.Load()
	v := c.Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewOperatorController(nil, nil), schedulerCfg.Args...)
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
func (o *ScheduleOption) SetLabelProperty(typ, labelKey, labelValue string) {
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
func (o *ScheduleOption) DeleteLabelProperty(typ, labelKey, labelValue string) {
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
func (o *ScheduleOption) LoadLabelPropertyConfig() LabelPropertyConfig {
	return o.labelProperty.Load().(LabelPropertyConfig)
}

// SetClusterVersion sets the cluster version.
func (o *ScheduleOption) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// CASClusterVersion sets the cluster version.
func (o *ScheduleOption) CASClusterVersion(old, new *semver.Version) bool {
	return atomic.CompareAndSwapPointer(&o.clusterVersion, unsafe.Pointer(old), unsafe.Pointer(new))
}

// LoadClusterVersion returns the cluster version.
func (o *ScheduleOption) LoadClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// LoadPDServerConfig returns PD server configurations.
func (o *ScheduleOption) LoadPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
}

// Persist saves the configuration to the storage.
func (o *ScheduleOption) Persist(storage *core.Storage) error {
	namespaces := o.LoadNSConfig()

	cfg := &Config{
		Schedule:       *o.Load(),
		Replication:    *o.rep.Load(),
		Namespace:      namespaces,
		LabelProperty:  o.LoadLabelPropertyConfig(),
		ClusterVersion: *o.LoadClusterVersion(),
		PDServerCfg:    *o.LoadPDServerConfig(),
	}
	err := storage.SaveConfig(cfg)
	return err
}

// Reload reloads the configuration from the storage.
func (o *ScheduleOption) Reload(storage *core.Storage) error {
	namespaces := o.LoadNSConfig()

	cfg := &Config{
		Schedule:       *o.Load().Clone(),
		Replication:    *o.rep.Load(),
		Namespace:      namespaces,
		LabelProperty:  o.LoadLabelPropertyConfig().Clone(),
		ClusterVersion: *o.LoadClusterVersion(),
		PDServerCfg:    *o.LoadPDServerConfig(),
	}
	isExist, err := storage.LoadConfig(cfg)
	if err != nil {
		return err
	}
	o.adjustScheduleCfg(cfg)
	if isExist {
		o.Store(&cfg.Schedule)
		o.rep.Store(&cfg.Replication)
		for name, nsCfg := range cfg.Namespace {
			nsCfg := nsCfg
			o.ns.Store(name, NewNamespaceOption(&nsCfg))
		}
		o.labelProperty.Store(cfg.LabelProperty)
		o.SetClusterVersion(&cfg.ClusterVersion)
		o.pdServerConfig.Store(&cfg.PDServerCfg)
	}
	return nil
}

func (o *ScheduleOption) adjustScheduleCfg(persistentCfg *Config) {
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
	o.Store(scheduleCfg)
}

// GetHotRegionCacheHitsThreshold is a threshold to decide if a region is hot.
func (o *ScheduleOption) GetHotRegionCacheHitsThreshold() int {
	return int(o.Load().HotRegionCacheHitsThreshold)
}

// CheckLabelProperty checks the label property.
func (o *ScheduleOption) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
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

// namespaceOption is a wrapper to access the configuration safely.
type namespaceOption struct {
	namespaceCfg atomic.Value
}

// NewNamespaceOption creates a new namespaceOption.
func NewNamespaceOption(cfg *NamespaceConfig) *namespaceOption {
	n := &namespaceOption{}
	n.Store(cfg)
	return n
}

func (n *namespaceOption) Load() *NamespaceConfig {
	return n.namespaceCfg.Load().(*NamespaceConfig)
}

func (n *namespaceOption) Store(cfg *NamespaceConfig) {
	n.namespaceCfg.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (n *namespaceOption) GetMaxReplicas() int {
	return int(n.Load().MaxReplicas)
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (n *namespaceOption) GetLeaderScheduleLimit() uint64 {
	return n.Load().LeaderScheduleLimit
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (n *namespaceOption) GetRegionScheduleLimit() uint64 {
	return n.Load().RegionScheduleLimit
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (n *namespaceOption) GetReplicaScheduleLimit() uint64 {
	return n.Load().ReplicaScheduleLimit
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (n *namespaceOption) GetMergeScheduleLimit() uint64 {
	return n.Load().MergeScheduleLimit
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (n *namespaceOption) GetHotRegionScheduleLimit() uint64 {
	return n.Load().HotRegionScheduleLimit
}
