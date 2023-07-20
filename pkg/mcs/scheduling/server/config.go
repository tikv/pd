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

package server

import (
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

const (
	defaultName             = "Scheduling"
	defaultBackendEndpoints = "http://127.0.0.1:2379"
	defaultListenAddr       = "http://127.0.0.1:3379"
)

const (
	defaultMaxReplicas                 = 3
	defaultMaxSnapshotCount            = 64
	defaultMaxPendingPeerCount         = 64
	defaultMaxStoreDownTime            = 30 * time.Minute
	defaultMergeScheduleLimit          = 8
	defaultSchedulerMaxWaitingOperator = 5
	defaultEnableCrossTableMerge       = true
	defaultEnableOneWayMerge           = false
	defaultLowSpaceRatio               = 0.8
	defaultHighSpaceRatio              = 0.7
	defaultRegionScoreFormulaVersion   = "v2"
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
	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	// WarningMsgs contains all warnings during parsing.
	WarningMsgs []string

	// LeaderLease defines the time within which a Scheduling primary/leader must
	// update its TTL in etcd, otherwise etcd will expire the leader key and other servers
	// can campaign the primary/leader again. Etcd only supports seconds TTL, so here is
	// second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	ClusterVersion semver.Version `toml:"cluster-version" json:"cluster-version"`

	SchedulingConfig SchedulingConfig `toml:"scheduling" json:"scheduling"`
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
		c.EnableGRPCGateway = utils.DefaultEnableGRPCGateway
	}

	c.adjustLog(configMetaData.Child("log"))
	c.Security.Encryption.Adjust()

	if len(c.Log.Format) == 0 {
		c.Log.Format = utils.DefaultLogFormat
	}

	configutil.AdjustInt64(&c.LeaderLease, utils.DefaultLeaderLease)

	return c.SchedulingConfig.adjust(configMetaData.Child("scheduling"))
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = utils.DefaultDisableErrorVerbose
	}
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

// SchedulingConfig is the configuration for the scheduling.
type SchedulingConfig struct {
	EnablePlacementRules      bool                        `toml:"enable-placement-rules" json:"enable-placement-rules,string"`
	EnableCrossTableMerge     bool                        `toml:"enable-cross-table-merge" json:"enable-cross-table-merge,string"`
	EnableOneWayMerge         bool                        `toml:"enable-one-way-merge" json:"enable-one-way-merge,string"`
	MaxReplicas               int                         `toml:"max-replicas" json:"max-replicas"`
	MaxSnapshotCount          uint64                      `toml:"max-snapshot-count" json:"max-snapshot-count"`
	MaxPendingPeerCount       uint64                      `toml:"max-pending-peer-count" json:"max-pending-peer-count"`
	MergeScheduleLimit        uint64                      `toml:"merge-schedule-limit" json:"merge-schedule-limit"`
	MaxWaitingOperator        uint64                      `toml:"max-waiting-operator" json:"max-waiting-operator"`
	LowSpaceRatio             float64                     `toml:"low-space-ratio" json:"low-space-ratio"`
	HighSpaceRatio            float64                     `toml:"high-space-ratio" json:"high-space-ratio"`
	MaxStoreDownTime          typeutil.Duration           `toml:"max-store-down-time" json:"max-store-down-time"`
	RegionScoreFormulaVersion string                      `toml:"region-score-formula-version" json:"region-score-formula-version"`
	LocationLabels            []string                    `toml:"location-labels" json:"location-labels"`
	StoreLimit                map[storelimit.Type]float64 `toml:"store-limit" json:"store-limit"`
}

func (c *SchedulingConfig) adjust(meta *configutil.ConfigMetaData) error {
	if !meta.IsDefined("max-replicas") {
		configutil.AdjustInt(&c.MaxReplicas, defaultMaxReplicas)
	}
	if !meta.IsDefined("max-snapshot-count") {
		configutil.AdjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	}
	if !meta.IsDefined("max-pending-peer-count") {
		configutil.AdjustUint64(&c.MaxPendingPeerCount, defaultMaxPendingPeerCount)
	}
	if !meta.IsDefined("max-store-down-time") {
		configutil.AdjustDuration(&c.MaxStoreDownTime, defaultMaxStoreDownTime)
	}
	if !meta.IsDefined("merge-schedule-limit") {
		configutil.AdjustUint64(&c.MergeScheduleLimit, defaultMergeScheduleLimit)
	}
	if !meta.IsDefined("max-waiting-operator") {
		configutil.AdjustUint64(&c.MaxWaitingOperator, defaultSchedulerMaxWaitingOperator)
	}
	if !meta.IsDefined("enable-cross-table-merge") {
		c.EnableCrossTableMerge = defaultEnableCrossTableMerge
	}
	if !meta.IsDefined("enable-one-way-merge") {
		c.EnableOneWayMerge = defaultEnableOneWayMerge
	}
	if !meta.IsDefined("low-space-ratio") {
		configutil.AdjustFloat64(&c.LowSpaceRatio, defaultLowSpaceRatio)
	}
	if !meta.IsDefined("high-space-ratio") {
		configutil.AdjustFloat64(&c.HighSpaceRatio, defaultHighSpaceRatio)
	}
	// new cluster:v2, old cluster:v1
	if !meta.IsDefined("region-score-formula-version") {
		configutil.AdjustString(&c.RegionScoreFormulaVersion, defaultRegionScoreFormulaVersion)
	}

	return c.validate()
}

// validate is used to validate if some scheduling configurations are right.
func (c *SchedulingConfig) validate() error {
	if c.LowSpaceRatio < 0 || c.LowSpaceRatio > 1 {
		return errors.New("low-space-ratio should between 0 and 1")
	}
	if c.HighSpaceRatio < 0 || c.HighSpaceRatio > 1 {
		return errors.New("high-space-ratio should between 0 and 1")
	}
	if c.LowSpaceRatio <= c.HighSpaceRatio {
		return errors.New("low-space-ratio should be larger than high-space-ratio")
	}
	return nil
}

// PersistConfig wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistConfig struct {
	clusterVersion unsafe.Pointer
	scheduling     atomic.Value
}

// NewPersistConfig creates a new PersistConfig instance.
func NewPersistConfig(cfg *Config) *PersistConfig {
	o := &PersistConfig{}
	o.SetClusterVersion(&cfg.ClusterVersion)
	o.scheduling.Store(&cfg.SchedulingConfig)
	return o
}

// GetClusterVersion returns the cluster version.
func (o *PersistConfig) GetClusterVersion() *semver.Version {
	return (*semver.Version)(atomic.LoadPointer(&o.clusterVersion))
}

// SetClusterVersion sets the cluster version.
func (o *PersistConfig) SetClusterVersion(v *semver.Version) {
	atomic.StorePointer(&o.clusterVersion, unsafe.Pointer(v))
}

// GetSchedulingConfig returns the scheduling configurations.
func (o *PersistConfig) GetSchedulingConfig() *SchedulingConfig {
	return o.scheduling.Load().(*SchedulingConfig)
}

// SetSchedulingConfig sets the scheduling configuration.
func (o *PersistConfig) SetSchedulingConfig(cfg *SchedulingConfig) {
	o.scheduling.Store(cfg)
}

// GetMaxReplicas returns the max replicas.
func (o *PersistConfig) GetMaxReplicas() int {
	return o.GetSchedulingConfig().MaxReplicas
}

// GetMaxSnapshotCount returns the max snapshot count.
func (o *PersistConfig) GetMaxSnapshotCount() uint64 {
	return o.GetSchedulingConfig().MaxSnapshotCount
}

// GetMaxPendingPeerCount returns the max pending peer count.
func (o *PersistConfig) GetMaxPendingPeerCount() uint64 {
	return o.GetSchedulingConfig().MaxPendingPeerCount
}

// IsPlacementRulesEnabled returns if the placement rules is enabled.
func (o *PersistConfig) IsPlacementRulesEnabled() bool {
	return o.GetSchedulingConfig().EnablePlacementRules
}

// GetLowSpaceRatio returns the low space ratio.
func (o *PersistConfig) GetLowSpaceRatio() float64 {
	return o.GetSchedulingConfig().LowSpaceRatio
}

// GetHighSpaceRatio returns the high space ratio.
func (o *PersistConfig) GetHighSpaceRatio() float64 {
	return o.GetSchedulingConfig().HighSpaceRatio
}

// GetMaxStoreDownTime returns the max store downtime.
func (o *PersistConfig) GetMaxStoreDownTime() time.Duration {
	return o.GetSchedulingConfig().MaxStoreDownTime.Duration
}

// GetLocationLabels returns the location labels.
func (o *PersistConfig) GetLocationLabels() []string {
	return o.GetSchedulingConfig().LocationLabels
}

// CheckLabelProperty checks if the label property is satisfied.
func (o *PersistConfig) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	return false
}

// IsUseJointConsensus returns if the joint consensus is enabled.
func (o *PersistConfig) IsUseJointConsensus() bool {
	return true
}

// GetKeyType returns the key type.
func (o *PersistConfig) GetKeyType() constant.KeyType {
	return constant.StringToKeyType("table")
}

// IsCrossTableMergeEnabled returns if the cross table merge is enabled.
func (o *PersistConfig) IsCrossTableMergeEnabled() bool {
	return o.GetSchedulingConfig().EnableCrossTableMerge
}

// IsOneWayMergeEnabled returns if the one way merge is enabled.
func (o *PersistConfig) IsOneWayMergeEnabled() bool {
	return o.GetSchedulingConfig().EnableOneWayMerge
}

// GetMergeScheduleLimit returns the merge schedule limit.
func (o *PersistConfig) GetMergeScheduleLimit() uint64 {
	return o.GetSchedulingConfig().MergeScheduleLimit
}

// GetRegionScoreFormulaVersion returns the region score formula version.
func (o *PersistConfig) GetRegionScoreFormulaVersion() string {
	return o.GetSchedulingConfig().RegionScoreFormulaVersion
}

// GetSchedulerMaxWaitingOperator returns the scheduler max waiting operator.
func (o *PersistConfig) GetSchedulerMaxWaitingOperator() uint64 {
	return o.GetSchedulingConfig().MaxWaitingOperator
}

// GetStoreLimitByType returns the store limit by type.
func (o *PersistConfig) GetStoreLimitByType(id uint64, typ storelimit.Type) float64 {
	return o.GetSchedulingConfig().StoreLimit[typ]
}

// IsWitnessAllowed returns if the witness is allowed.
func (o *PersistConfig) IsWitnessAllowed() bool {
	return false
}

// IsPlacementRulesCacheEnabled returns if the placement rules cache is enabled.
func (o *PersistConfig) IsPlacementRulesCacheEnabled() bool {
	return false
}

// SetPlacementRulesCacheEnabled sets if the placement rules cache is enabled.
func (o *PersistConfig) SetPlacementRulesCacheEnabled(b bool) {}

// SetEnableWitness sets if the witness is enabled.
func (o *PersistConfig) SetEnableWitness(b bool) {}
