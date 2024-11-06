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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	mcsconstant "github.com/tikv/pd/pkg/mcs/utils/constant"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

const (
	defaultName             = "scheduling"
	defaultBackendEndpoints = "http://127.0.0.1:2379"
	defaultListenAddr       = "http://127.0.0.1:3379"
)

// Config is the configuration for the scheduling.
type Config struct {
	BackendEndpoints    string `toml:"backend-endpoints" json:"backend-endpoints"`
	ListenAddr          string `toml:"listen-addr" json:"listen-addr"`
	AdvertiseListenAddr string `toml:"advertise-listen-addr" json:"advertise-listen-addr"`
	Name                string `toml:"name" json:"name"`
	DataDir             string `toml:"data-dir" json:"data-dir"` // TODO: remove this after refactoring
	EnableGRPCGateway   bool   `json:"enable-grpc-gateway"`      // TODO: use it

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	// Log related config.
	Log      log.Config         `toml:"log" json:"log"`
	Logger   *zap.Logger        `json:"-"`
	LogProps *log.ZapProperties `json:"-"`

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	// WarningMsgs contains all warnings during parsing.
	WarningMsgs []string

	// LeaderLease defines the time within which a Scheduling primary/leader must
	// update its TTL in etcd, otherwise etcd will expire the leader key and other servers
	// can campaign the primary/leader again. Etcd only supports seconds TTL, so here is
	// second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	ClusterVersion semver.Version `toml:"cluster-version" json:"cluster-version"`

	Schedule    sc.ScheduleConfig    `toml:"schedule" json:"schedule"`
	Replication sc.ReplicationConfig `toml:"replication" json:"replication"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(flagSet *pflag.FlagSet) error {
	// Load config file if specified.
	var (
		meta *toml.MetaData
		err  error
	)
	if configFile, _ := flagSet.GetString("config"); configFile != "" {
		meta, err = configutil.ConfigFromFile(c, configFile)
		if err != nil {
			return err
		}
	}

	// Ignore the error check here
	configutil.AdjustCommandLineString(flagSet, &c.Name, "name")
	configutil.AdjustCommandLineString(flagSet, &c.Log.Level, "log-level")
	configutil.AdjustCommandLineString(flagSet, &c.Log.File.Filename, "log-file")
	configutil.AdjustCommandLineString(flagSet, &c.Metric.PushAddress, "metrics-addr")
	configutil.AdjustCommandLineString(flagSet, &c.Security.CAPath, "cacert")
	configutil.AdjustCommandLineString(flagSet, &c.Security.CertPath, "cert")
	configutil.AdjustCommandLineString(flagSet, &c.Security.KeyPath, "key")
	configutil.AdjustCommandLineString(flagSet, &c.BackendEndpoints, "backend-endpoints")
	configutil.AdjustCommandLineString(flagSet, &c.ListenAddr, "listen-addr")
	configutil.AdjustCommandLineString(flagSet, &c.AdvertiseListenAddr, "advertise-listen-addr")
	return c.adjust(meta)
}

// adjust is used to adjust the scheduling configurations.
func (c *Config) adjust(meta *toml.MetaData) error {
	configMetaData := configutil.NewConfigMetadata(meta)
	if err := configMetaData.CheckUndecoded(); err != nil {
		c.WarningMsgs = append(c.WarningMsgs, err.Error())
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		configutil.AdjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	configutil.AdjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))
	configutil.AdjustPath(&c.DataDir)

	if err := c.validate(); err != nil {
		return err
	}

	configutil.AdjustString(&c.BackendEndpoints, defaultBackendEndpoints)
	configutil.AdjustString(&c.ListenAddr, defaultListenAddr)
	configutil.AdjustString(&c.AdvertiseListenAddr, c.ListenAddr)

	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = mcsconstant.DefaultEnableGRPCGateway
	}

	c.adjustLog(configMetaData.Child("log"))
	if err := c.Security.Encryption.Adjust(); err != nil {
		return err
	}

	configutil.AdjustInt64(&c.LeaderLease, mcsconstant.DefaultLeaderLease)

	if err := c.Schedule.Adjust(configMetaData.Child("schedule"), false); err != nil {
		return err
	}
	return c.Replication.Adjust(configMetaData.Child("replication"))
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = mcsconstant.DefaultDisableErrorVerbose
	}
	configutil.AdjustString(&c.Log.Format, mcsconstant.DefaultLogFormat)
	configutil.AdjustString(&c.Log.Level, mcsconstant.DefaultLogLevel)
}

// GetName returns the Name
func (c *Config) GetName() string {
	return c.Name
}

// GeBackendEndpoints returns the BackendEndpoints
func (c *Config) GeBackendEndpoints() string {
	return c.BackendEndpoints
}

// GetListenAddr returns the ListenAddr
func (c *Config) GetListenAddr() string {
	return c.ListenAddr
}

// GetAdvertiseListenAddr returns the AdvertiseListenAddr
func (c *Config) GetAdvertiseListenAddr() string {
	return c.AdvertiseListenAddr
}

// GetTLSConfig returns the TLS config.
func (c *Config) GetTLSConfig() *grpcutil.TLSConfig {
	return &c.Security.TLSConfig
}

// validate is used to validate if some configurations are right.
func (c *Config) validate() error {
	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	logFile, err := filepath.Abs(c.Log.File.Filename)
	if err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(dataDir, filepath.Dir(logFile))
	if err != nil {
		return errors.WithStack(err)
	}
	if !strings.HasPrefix(rel, "..") {
		return errors.New("log directory shouldn't be the subdirectory of data directory")
	}

	return nil
}

// Clone creates a copy of current config.
func (c *Config) Clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

// PersistConfig wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistConfig struct {
	ttl *cache.TTLString
	// Store the global configuration that is related to the scheduling.
	clusterVersion unsafe.Pointer
	schedule       atomic.Value
	replication    atomic.Value
	storeConfig    atomic.Value
	// schedulersUpdatingNotifier is used to notify that the schedulers have been updated.
	// Store as `chan<- struct{}`.
	schedulersUpdatingNotifier atomic.Value
}

// NewPersistConfig creates a new PersistConfig instance.
func NewPersistConfig(cfg *Config, ttl *cache.TTLString) *PersistConfig {
	o := &PersistConfig{}
	o.SetClusterVersion(&cfg.ClusterVersion)
	o.schedule.Store(&cfg.Schedule)
	o.replication.Store(&cfg.Replication)
	// storeConfig will be fetched from TiKV by PD API server,
	// so we just set an empty value here first.
	o.storeConfig.Store(&sc.StoreConfig{})
	o.ttl = ttl
	return o
}

// SetSchedulersUpdatingNotifier sets the schedulers updating notifier.
func (o *PersistConfig) SetSchedulersUpdatingNotifier(notifier chan<- struct{}) {
	o.schedulersUpdatingNotifier.Store(notifier)
}

func (o *PersistConfig) getSchedulersUpdatingNotifier() chan<- struct{} {
	v := o.schedulersUpdatingNotifier.Load()
	if v == nil {
		return nil
	}
	return v.(chan<- struct{})
}

func (o *PersistConfig) tryNotifySchedulersUpdating() {
	notifier := o.getSchedulersUpdatingNotifier()
	if notifier == nil {
		return
	}
	notifier <- struct{}{}
}

// GetClusterVersion returns the cluster version.
func (o *PersistConfig) GetClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// SetClusterVersion sets the cluster version.
func (o *PersistConfig) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// GetScheduleConfig returns the scheduling configurations.
func (o *PersistConfig) GetScheduleConfig() *sc.ScheduleConfig {
	return o.schedule.Load().(*sc.ScheduleConfig)
}

// SetScheduleConfig sets the scheduling configuration dynamically.
func (o *PersistConfig) SetScheduleConfig(cfg *sc.ScheduleConfig) {
	old := o.GetScheduleConfig()
	o.schedule.Store(cfg)
	// The coordinator is not aware of the underlying scheduler config changes,
	// we should notify it to update the schedulers proactively.
	if !reflect.DeepEqual(old.Schedulers, cfg.Schedulers) {
		o.tryNotifySchedulersUpdating()
	}
}

// AdjustScheduleCfg adjusts the schedule config during the initialization.
func AdjustScheduleCfg(scheduleCfg *sc.ScheduleConfig) {
	// In case we add new default schedulers.
	for _, ps := range sc.DefaultSchedulers {
		if slice.NoneOf(scheduleCfg.Schedulers, func(i int) bool {
			return scheduleCfg.Schedulers[i].Type == ps.Type
		}) {
			scheduleCfg.Schedulers = append(scheduleCfg.Schedulers, ps)
		}
	}
}

// GetReplicationConfig returns replication configurations.
func (o *PersistConfig) GetReplicationConfig() *sc.ReplicationConfig {
	return o.replication.Load().(*sc.ReplicationConfig)
}

// SetReplicationConfig sets the PD replication configuration.
func (o *PersistConfig) SetReplicationConfig(cfg *sc.ReplicationConfig) {
	o.replication.Store(cfg)
}

// SetStoreConfig sets the TiKV store configuration.
func (o *PersistConfig) SetStoreConfig(cfg *sc.StoreConfig) {
	// Some of the fields won't be persisted and watched,
	// so we need to adjust it here before storing it.
	cfg.Adjust()
	o.storeConfig.Store(cfg)
}

// GetStoreConfig returns the TiKV store configuration.
func (o *PersistConfig) GetStoreConfig() *sc.StoreConfig {
	return o.storeConfig.Load().(*sc.StoreConfig)
}

// GetMaxReplicas returns the max replicas.
func (o *PersistConfig) GetMaxReplicas() int {
	return int(o.GetReplicationConfig().MaxReplicas)
}

// IsPlacementRulesEnabled returns if the placement rules is enabled.
func (o *PersistConfig) IsPlacementRulesEnabled() bool {
	return o.GetReplicationConfig().EnablePlacementRules
}

// GetLowSpaceRatio returns the low space ratio.
func (o *PersistConfig) GetLowSpaceRatio() float64 {
	return o.GetScheduleConfig().LowSpaceRatio
}

// GetHighSpaceRatio returns the high space ratio.
func (o *PersistConfig) GetHighSpaceRatio() float64 {
	return o.GetScheduleConfig().HighSpaceRatio
}

// GetLeaderSchedulePolicy is to get leader schedule policy.
func (o *PersistConfig) GetLeaderSchedulePolicy() constant.SchedulePolicy {
	return constant.StringToSchedulePolicy(o.GetScheduleConfig().LeaderSchedulePolicy)
}

// GetMaxStoreDownTime returns the max store downtime.
func (o *PersistConfig) GetMaxStoreDownTime() time.Duration {
	return o.GetScheduleConfig().MaxStoreDownTime.Duration
}

// GetIsolationLevel returns the isolation label for each region.
func (o *PersistConfig) GetIsolationLevel() string {
	return o.GetReplicationConfig().IsolationLevel
}

// GetLocationLabels returns the location labels.
func (o *PersistConfig) GetLocationLabels() []string {
	return o.GetReplicationConfig().LocationLabels
}

// IsUseJointConsensus returns if the joint consensus is enabled.
func (o *PersistConfig) IsUseJointConsensus() bool {
	return o.GetScheduleConfig().EnableJointConsensus
}

// GetKeyType returns the key type.
func (*PersistConfig) GetKeyType() constant.KeyType {
	return constant.StringToKeyType("table")
}

// IsCrossTableMergeEnabled returns if the cross table merge is enabled.
func (o *PersistConfig) IsCrossTableMergeEnabled() bool {
	return o.GetScheduleConfig().EnableCrossTableMerge
}

// IsOneWayMergeEnabled returns if the one way merge is enabled.
func (o *PersistConfig) IsOneWayMergeEnabled() bool {
	return o.GetScheduleConfig().EnableOneWayMerge
}

// GetRegionScoreFormulaVersion returns the region score formula version.
func (o *PersistConfig) GetRegionScoreFormulaVersion() string {
	return o.GetScheduleConfig().RegionScoreFormulaVersion
}

// GetHotRegionCacheHitsThreshold returns the hot region cache hits threshold.
func (o *PersistConfig) GetHotRegionCacheHitsThreshold() int {
	return int(o.GetScheduleConfig().HotRegionCacheHitsThreshold)
}

// GetPatrolRegionWorkerCount returns the worker count of the patrol.
func (o *PersistConfig) GetPatrolRegionWorkerCount() int {
	return o.GetScheduleConfig().PatrolRegionWorkerCount
}

// GetMaxMovableHotPeerSize returns the max movable hot peer size.
func (o *PersistConfig) GetMaxMovableHotPeerSize() int64 {
	return o.GetScheduleConfig().MaxMovableHotPeerSize
}

// GetSwitchWitnessInterval returns the interval between promote to non-witness and starting to switch to witness.
func (o *PersistConfig) GetSwitchWitnessInterval() time.Duration {
	return o.GetScheduleConfig().SwitchWitnessInterval.Duration
}

// GetSplitMergeInterval returns the interval between finishing split and starting to merge.
func (o *PersistConfig) GetSplitMergeInterval() time.Duration {
	return o.GetScheduleConfig().SplitMergeInterval.Duration
}

// GetSlowStoreEvictingAffectedStoreRatioThreshold returns the affected ratio threshold when judging a store is slow.
func (o *PersistConfig) GetSlowStoreEvictingAffectedStoreRatioThreshold() float64 {
	return o.GetScheduleConfig().SlowStoreEvictingAffectedStoreRatioThreshold
}

// GetPatrolRegionInterval returns the interval of patrolling region.
func (o *PersistConfig) GetPatrolRegionInterval() time.Duration {
	return o.GetScheduleConfig().PatrolRegionInterval.Duration
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (o *PersistConfig) GetTolerantSizeRatio() float64 {
	return o.GetScheduleConfig().TolerantSizeRatio
}

// IsDebugMetricsEnabled returns if debug metrics is enabled.
func (o *PersistConfig) IsDebugMetricsEnabled() bool {
	return o.GetScheduleConfig().EnableDebugMetrics
}

// IsDiagnosticAllowed returns whether is enable to use diagnostic.
func (o *PersistConfig) IsDiagnosticAllowed() bool {
	return o.GetScheduleConfig().EnableDiagnostic
}

// IsRemoveDownReplicaEnabled returns if remove down replica is enabled.
func (o *PersistConfig) IsRemoveDownReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableRemoveDownReplica
}

// IsReplaceOfflineReplicaEnabled returns if replace offline replica is enabled.
func (o *PersistConfig) IsReplaceOfflineReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableReplaceOfflineReplica
}

// IsMakeUpReplicaEnabled returns if make up replica is enabled.
func (o *PersistConfig) IsMakeUpReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableMakeUpReplica
}

// IsRemoveExtraReplicaEnabled returns if remove extra replica is enabled.
func (o *PersistConfig) IsRemoveExtraReplicaEnabled() bool {
	return o.GetScheduleConfig().EnableRemoveExtraReplica
}

// IsWitnessAllowed returns if the witness is allowed.
func (o *PersistConfig) IsWitnessAllowed() bool {
	return o.GetScheduleConfig().EnableWitness
}

// IsPlacementRulesCacheEnabled returns if the placement rules cache is enabled.
func (o *PersistConfig) IsPlacementRulesCacheEnabled() bool {
	return o.GetReplicationConfig().EnablePlacementRulesCache
}

// IsSchedulingHalted returns if PD scheduling is halted.
func (o *PersistConfig) IsSchedulingHalted() bool {
	return o.GetScheduleConfig().HaltScheduling
}

// GetStoresLimit gets the stores' limit.
func (o *PersistConfig) GetStoresLimit() map[uint64]sc.StoreLimitConfig {
	return o.GetScheduleConfig().StoreLimit
}

// TTL related methods.

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (o *PersistConfig) GetLeaderScheduleLimit() uint64 {
	return o.getTTLUintOr(sc.LeaderScheduleLimitKey, o.GetScheduleConfig().LeaderScheduleLimit)
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (o *PersistConfig) GetRegionScheduleLimit() uint64 {
	return o.getTTLUintOr(sc.RegionScheduleLimitKey, o.GetScheduleConfig().RegionScheduleLimit)
}

// GetWitnessScheduleLimit returns the limit for region schedule.
func (o *PersistConfig) GetWitnessScheduleLimit() uint64 {
	return o.getTTLUintOr(sc.WitnessScheduleLimitKey, o.GetScheduleConfig().WitnessScheduleLimit)
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (o *PersistConfig) GetReplicaScheduleLimit() uint64 {
	return o.getTTLUintOr(sc.ReplicaRescheduleLimitKey, o.GetScheduleConfig().ReplicaScheduleLimit)
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (o *PersistConfig) GetMergeScheduleLimit() uint64 {
	return o.getTTLUintOr(sc.MergeScheduleLimitKey, o.GetScheduleConfig().MergeScheduleLimit)
}

// GetHotRegionScheduleLimit returns the limit for hot region schedule.
func (o *PersistConfig) GetHotRegionScheduleLimit() uint64 {
	return o.getTTLUintOr(sc.HotRegionScheduleLimitKey, o.GetScheduleConfig().HotRegionScheduleLimit)
}

// GetStoreLimit returns the limit of a store.
func (o *PersistConfig) GetStoreLimit(storeID uint64) (returnSC sc.StoreLimitConfig) {
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
func (o *PersistConfig) GetStoreLimitByType(storeID uint64, typ storelimit.Type) (returned float64) {
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

// GetMaxSnapshotCount returns the number of the max snapshot which is allowed to send.
func (o *PersistConfig) GetMaxSnapshotCount() uint64 {
	return o.getTTLUintOr(sc.MaxSnapshotCountKey, o.GetScheduleConfig().MaxSnapshotCount)
}

// GetMaxPendingPeerCount returns the number of the max pending peers.
func (o *PersistConfig) GetMaxPendingPeerCount() uint64 {
	return o.getTTLUintOr(sc.MaxPendingPeerCountKey, o.GetScheduleConfig().MaxPendingPeerCount)
}

// GetMaxMergeRegionSize returns the max region size.
func (o *PersistConfig) GetMaxMergeRegionSize() uint64 {
	return o.getTTLUintOr(sc.MaxMergeRegionSizeKey, o.GetScheduleConfig().MaxMergeRegionSize)
}

// GetMaxMergeRegionKeys returns the max number of keys.
// It returns size * 10000 if the key of max-merge-region-Keys doesn't exist.
func (o *PersistConfig) GetMaxMergeRegionKeys() uint64 {
	keys, exist, err := o.getTTLUint(sc.MaxMergeRegionKeysKey)
	if exist && err == nil {
		return keys
	}
	size, exist, err := o.getTTLUint(sc.MaxMergeRegionSizeKey)
	if exist && err == nil {
		return size * 10000
	}
	return o.GetScheduleConfig().GetMaxMergeRegionKeys()
}

// GetSchedulerMaxWaitingOperator returns the number of the max waiting operators.
func (o *PersistConfig) GetSchedulerMaxWaitingOperator() uint64 {
	return o.getTTLUintOr(sc.SchedulerMaxWaitingOperatorKey, o.GetScheduleConfig().SchedulerMaxWaitingOperator)
}

// IsLocationReplacementEnabled returns if location replace is enabled.
func (o *PersistConfig) IsLocationReplacementEnabled() bool {
	return o.getTTLBoolOr(sc.EnableLocationReplacement, o.GetScheduleConfig().EnableLocationReplacement)
}

// IsTikvRegionSplitEnabled returns whether tikv split region is enabled.
func (o *PersistConfig) IsTikvRegionSplitEnabled() bool {
	return o.getTTLBoolOr(sc.EnableTiKVSplitRegion, o.GetScheduleConfig().EnableTiKVSplitRegion)
}

// SetAllStoresLimit sets all store limit for a given type and rate.
func (o *PersistConfig) SetAllStoresLimit(typ storelimit.Type, ratePerMin float64) {
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

// SetMaxReplicas sets the number of replicas for each region.
func (o *PersistConfig) SetMaxReplicas(replicas int) {
	v := o.GetReplicationConfig().Clone()
	v.MaxReplicas = uint64(replicas)
	o.SetReplicationConfig(v)
}

// IsSchedulerDisabled returns if the scheduler is disabled.
func (o *PersistConfig) IsSchedulerDisabled(tp types.CheckerSchedulerType) bool {
	oldType := types.SchedulerTypeCompatibleMap[tp]
	schedulers := o.GetScheduleConfig().Schedulers
	for _, s := range schedulers {
		if oldType == s.Type {
			return s.Disable
		}
	}
	return false
}

// SetPlacementRulesCacheEnabled sets if the placement rules cache is enabled.
func (o *PersistConfig) SetPlacementRulesCacheEnabled(enabled bool) {
	v := o.GetReplicationConfig().Clone()
	v.EnablePlacementRulesCache = enabled
	o.SetReplicationConfig(v)
}

// SetEnableWitness sets if the witness is enabled.
func (o *PersistConfig) SetEnableWitness(enable bool) {
	v := o.GetScheduleConfig().Clone()
	v.EnableWitness = enable
	o.SetScheduleConfig(v)
}

// SetPlacementRuleEnabled set PlacementRuleEnabled
func (o *PersistConfig) SetPlacementRuleEnabled(enabled bool) {
	v := o.GetReplicationConfig().Clone()
	v.EnablePlacementRules = enabled
	o.SetReplicationConfig(v)
}

// SetSplitMergeInterval to set the interval between finishing split and starting to merge. It's only used to test.
func (o *PersistConfig) SetSplitMergeInterval(splitMergeInterval time.Duration) {
	v := o.GetScheduleConfig().Clone()
	v.SplitMergeInterval = typeutil.Duration{Duration: splitMergeInterval}
	o.SetScheduleConfig(v)
}

// SetSchedulingAllowanceStatus sets the scheduling allowance status to help distinguish the source of the halt.
// TODO: support this metrics for the scheduling service in the future.
func (*PersistConfig) SetSchedulingAllowanceStatus(bool, string) {}

// SetHaltScheduling set HaltScheduling.
func (o *PersistConfig) SetHaltScheduling(halt bool, _ string) {
	v := o.GetScheduleConfig().Clone()
	v.HaltScheduling = halt
	o.SetScheduleConfig(v)
}

// CheckRegionKeys return error if the smallest region's keys is less than mergeKeys
func (o *PersistConfig) CheckRegionKeys(keys, mergeKeys uint64) error {
	return o.GetStoreConfig().CheckRegionKeys(keys, mergeKeys)
}

// CheckRegionSize return error if the smallest region's size is less than mergeSize
func (o *PersistConfig) CheckRegionSize(size, mergeSize uint64) error {
	return o.GetStoreConfig().CheckRegionSize(size, mergeSize)
}

// GetRegionMaxSize returns the max region size in MB
func (o *PersistConfig) GetRegionMaxSize() uint64 {
	return o.GetStoreConfig().GetRegionMaxSize()
}

// GetRegionMaxKeys returns the max region keys
func (o *PersistConfig) GetRegionMaxKeys() uint64 {
	return o.GetStoreConfig().GetRegionMaxKeys()
}

// GetRegionSplitSize returns the region split size in MB
func (o *PersistConfig) GetRegionSplitSize() uint64 {
	return o.GetStoreConfig().GetRegionSplitSize()
}

// GetRegionSplitKeys returns the region split keys
func (o *PersistConfig) GetRegionSplitKeys() uint64 {
	return o.GetStoreConfig().GetRegionSplitKeys()
}

// IsEnableRegionBucket return true if the region bucket is enabled.
func (o *PersistConfig) IsEnableRegionBucket() bool {
	return o.GetStoreConfig().IsEnableRegionBucket()
}

// IsRaftKV2 returns the whether the cluster use `raft-kv2` engine.
func (o *PersistConfig) IsRaftKV2() bool {
	return o.GetStoreConfig().IsRaftKV2()
}

// TODO: implement the following methods

// AddSchedulerCfg adds the scheduler configurations.
// This method is a no-op since we only use configurations derived from one-way synchronization from API server now.
func (*PersistConfig) AddSchedulerCfg(types.CheckerSchedulerType, []string) {}

// RemoveSchedulerCfg removes the scheduler configurations.
// This method is a no-op since we only use configurations derived from one-way synchronization from API server now.
func (*PersistConfig) RemoveSchedulerCfg(types.CheckerSchedulerType) {}

// CheckLabelProperty checks if the label property is satisfied.
func (*PersistConfig) CheckLabelProperty(string, []*metapb.StoreLabel) bool {
	return false
}

// IsTraceRegionFlow returns if the region flow is tracing.
// If the accuracy cannot reach 0.1 MB, it is considered not.
func (*PersistConfig) IsTraceRegionFlow() bool {
	return false
}

// Persist saves the configuration to the storage.
func (*PersistConfig) Persist(endpoint.ConfigStorage) error {
	return nil
}

func (o *PersistConfig) getTTLUint(key string) (uint64, bool, error) {
	stringForm, ok := o.GetTTLData(key)
	if !ok {
		return 0, false, nil
	}
	r, err := strconv.ParseUint(stringForm, 10, 64)
	return r, true, err
}

func (o *PersistConfig) getTTLUintOr(key string, defaultValue uint64) uint64 {
	if v, ok, err := o.getTTLUint(key); ok {
		if err == nil {
			return v
		}
		log.Warn("failed to parse "+key+" from PersistOptions's ttl storage", zap.Error(err))
	}
	return defaultValue
}

func (o *PersistConfig) getTTLBool(key string) (result bool, contains bool, err error) {
	stringForm, ok := o.GetTTLData(key)
	if !ok {
		return
	}
	result, err = strconv.ParseBool(stringForm)
	contains = true
	return
}

func (o *PersistConfig) getTTLBoolOr(key string, defaultValue bool) bool {
	if v, ok, err := o.getTTLBool(key); ok {
		if err == nil {
			return v
		}
		log.Warn("failed to parse "+key+" from PersistOptions's ttl storage", zap.Error(err))
	}
	return defaultValue
}

func (o *PersistConfig) getTTLFloat(key string) (float64, bool, error) {
	stringForm, ok := o.GetTTLData(key)
	if !ok {
		return 0, false, nil
	}
	r, err := strconv.ParseFloat(stringForm, 64)
	return r, true, err
}

func (o *PersistConfig) getTTLFloatOr(key string, defaultValue float64) float64 {
	if v, ok, err := o.getTTLFloat(key); ok {
		if err == nil {
			return v
		}
		log.Warn("failed to parse "+key+" from PersistOptions's ttl storage", zap.Error(err))
	}
	return defaultValue
}

// GetTTLData returns if there is a TTL data for a given key.
func (o *PersistConfig) GetTTLData(key string) (string, bool) {
	if o.ttl == nil {
		return "", false
	}
	if result, ok := o.ttl.Get(key); ok {
		return result.(string), ok
	}
	return "", false
}
