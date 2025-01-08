// Copyright 2016 TiKV Project Authors.
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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

// Config is the pd server configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type Config struct {
	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	Name              string `toml:"name" json:"name"`
	DataDir           string `toml:"data-dir" json:"data-dir"`
	ForceNewCluster   bool   `json:"force-new-cluster"`
	EnableGRPCGateway bool   `json:"enable-grpc-gateway"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	InitialClusterToken string `toml:"initial-cluster-token" json:"initial-cluster-token"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd only supports seconds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log related config.
	Log log.Config `toml:"log" json:"log"`

	// Backward compatibility.
	LogFileDeprecated  string `toml:"log-file" json:"log-file,omitempty"`
	LogLevelDeprecated string `toml:"log-level" json:"log-level,omitempty"`

	// MaxConcurrentTSOProxyStreamings is the maximum number of concurrent TSO proxy streaming process routines allowed.
	// Exceeding this limit will result in an error being returned to the client when a new client starts a TSO streaming.
	// Set this to 0 will disable TSO Proxy.
	// Set this to the negative value to disable the limit.
	MaxConcurrentTSOProxyStreamings int `toml:"max-concurrent-tso-proxy-streamings" json:"max-concurrent-tso-proxy-streamings"`
	// TSOProxyRecvFromClientTimeout is the timeout for the TSO proxy to receive a tso request from a client via grpc TSO stream.
	// After the timeout, the TSO proxy will close the grpc TSO stream.
	TSOProxyRecvFromClientTimeout typeutil.Duration `toml:"tso-proxy-recv-from-client-timeout" json:"tso-proxy-recv-from-client-timeout"`

	// TSOSaveInterval is the interval to save timestamp.
	TSOSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// The interval to update physical part of timestamp. Usually, this config should not be set.
	// At most 1<<18 (262144) TSOs can be generated in the interval. The smaller the value, the
	// more TSOs provided, and at the same time consuming more CPU time.
	// This config is only valid in 1ms to 10s. If it's configured too long or too short, it will
	// be automatically clamped to the range.
	TSOUpdatePhysicalInterval typeutil.Duration `toml:"tso-update-physical-interval" json:"tso-update-physical-interval"`

	// Deprecated
	EnableLocalTSO bool `toml:"enable-local-tso" json:"enable-local-tso"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	Schedule sc.ScheduleConfig `toml:"schedule" json:"schedule"`

	Replication sc.ReplicationConfig `toml:"replication" json:"replication"`

	PDServerCfg PDServerConfig `toml:"pd-server" json:"pd-server"`

	ClusterVersion semver.Version `toml:"cluster-version" json:"cluster-version"`

	// Labels indicates the labels set for **this** PD server. The labels describe some specific properties
	// like `zone`/`rack`/`host`. Currently, labels won't affect the PD server except for some special
	// label keys. Now we have following special keys:
	Labels map[string]string `toml:"labels" json:"labels"`

	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes typeutil.ByteSize `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	// AutoCompactionMode is either 'periodic' or 'revision'. The default value is 'periodic'.
	AutoCompactionMode string `toml:"auto-compaction-mode" json:"auto-compaction-mode"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	// The default retention is 1 hour.
	// Before etcd v3.3.x, the type of retention is int. We add 'v2' suffix to make it backward compatible.
	AutoCompactionRetention string `toml:"auto-compaction-retention" json:"auto-compaction-retention-v2"`

	// TickInterval is the interval for etcd Raft tick.
	TickInterval typeutil.Duration `toml:"tick-interval" json:"tick-interval"`
	// ElectionInterval is the interval for etcd Raft election.
	ElectionInterval typeutil.Duration `toml:"election-interval" json:"election-interval"`
	// Prevote is true to enable Raft Pre-Vote.
	// If enabled, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool `toml:"enable-prevote" json:"enable-prevote"`

	MaxRequestBytes uint `toml:"max-request-bytes" json:"max-request-bytes"`

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	LabelProperty LabelPropertyConfig `toml:"label-property" json:"label-property"`

	// For all warnings during parsing.
	WarningMsgs []string `json:"-"`

	DisableStrictReconfigCheck bool `json:"-"`

	HeartbeatStreamBindInterval typeutil.Duration `json:"-"`
	LeaderPriorityCheckInterval typeutil.Duration `json:"-"`

	Logger   *zap.Logger        `json:"-"`
	LogProps *log.ZapProperties `json:"-"`

	Dashboard DashboardConfig `toml:"dashboard" json:"dashboard"`

	ReplicationMode ReplicationModeConfig `toml:"replication-mode" json:"replication-mode"`

	Keyspace KeyspaceConfig `toml:"keyspace" json:"keyspace"`

	Microservice MicroserviceConfig `toml:"micro-service" json:"micro-service"`

	Controller rm.ControllerConfig `toml:"controller" json:"controller"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
}

const (
	defaultLeaderLease             = int64(3)
	defaultCompactionMode          = "periodic"
	defaultAutoCompactionRetention = "1h"
	defaultQuotaBackendBytes       = typeutil.ByteSize(8 * units.GiB) // 8GB

	// The default max bytes for grpc message
	// Unsafe recovery report is included in store heartbeat, and assume that each peer report occupies about 500B at most,
	// then 150MB can fit for store reports that have about 300k regions which is something of a huge amount of region on one TiKV.
	defaultMaxRequestBytes = uint(150 * units.MiB) // 150MB

	defaultName                = "pd"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defaultInitialClusterState = embed.ClusterStateFlagNew
	defaultInitialClusterToken = "pd-cluster"

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickInterval = 500 * time.Millisecond
	// embed etcd has a check that `5 * tick > election`
	defaultElectionInterval = 3000 * time.Millisecond

	defaultMetricsPushInterval = 15 * time.Second

	defaultHeartbeatStreamRebindInterval = time.Minute

	defaultLeaderPriorityCheckInterval = time.Minute

	defaultUseRegionStorage  = true
	defaultFlowRoundByDigit  = 3 // KB
	maxTraceFlowRoundByDigit = 5 // 0.1 MB
	defaultMaxResetTSGap     = 24 * time.Hour
	defaultKeyType           = "table"

	// DefaultMinResolvedTSPersistenceInterval is the default value of min resolved ts persistent interval.
	DefaultMinResolvedTSPersistenceInterval = time.Second

	defaultEnableGRPCGateway   = true
	defaultDisableErrorVerbose = true
	defaultEnableWitness       = false
	defaultHaltScheduling      = false

	defaultDashboardAddress = "auto"

	defaultDRWaitStoreTimeout = time.Minute

	defaultMaxConcurrentTSOProxyStreamings = 5000
	defaultTSOProxyRecvFromClientTimeout   = 1 * time.Hour

	defaultTSOSaveInterval = time.Duration(defaultLeaderLease) * time.Second
	// defaultTSOUpdatePhysicalInterval is the default value of the config `TSOUpdatePhysicalInterval`.
	defaultTSOUpdatePhysicalInterval = 50 * time.Millisecond
	maxTSOUpdatePhysicalInterval     = 10 * time.Second
	minTSOUpdatePhysicalInterval     = 1 * time.Millisecond

	defaultLogFormat = "text"
	defaultLogLevel  = "info"

	defaultServerMemoryLimit          = 0
	minServerMemoryLimit              = 0
	maxServerMemoryLimit              = 0.99
	defaultServerMemoryLimitGCTrigger = 0.7
	minServerMemoryLimitGCTrigger     = 0.5
	maxServerMemoryLimitGCTrigger     = 0.99
	defaultEnableGOGCTuner            = false
	defaultGCTunerThreshold           = 0.6
	minGCTunerThreshold               = 0
	maxGCTunerThreshold               = 0.9

	defaultWaitRegionSplitTimeout   = 30 * time.Second
	defaultCheckRegionSplitInterval = 50 * time.Millisecond
	minCheckRegionSplitInterval     = 1 * time.Millisecond
	maxCheckRegionSplitInterval     = 100 * time.Millisecond

	defaultEnableSchedulingFallback  = true
	defaultEnableTSODynamicSwitching = false
)

var (
	defaultEnableTelemetry = false
	defaultRuntimeServices = []string{}
)

func init() {
	initByLDFlags(versioninfo.PDEdition)
}

func initByLDFlags(edition string) {
	if edition != versioninfo.CommunityEdition {
		defaultEnableTelemetry = false
	}
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

		// Backward compatibility for toml config
		if c.LogFileDeprecated != "" {
			msg := fmt.Sprintf("log-file in %s is deprecated, use [log.file] instead", configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
			if c.Log.File.Filename == "" {
				c.Log.File.Filename = c.LogFileDeprecated
			}
		}
		if c.LogLevelDeprecated != "" {
			msg := fmt.Sprintf("log-level in %s is deprecated, use [log] instead", configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
			if c.Log.Level == "" {
				c.Log.Level = c.LogLevelDeprecated
			}
		}
		if meta.IsDefined("schedule", "disable-raft-learner") {
			msg := fmt.Sprintf("disable-raft-learner in %s is deprecated", configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
	}

	// ignore the error check here
	configutil.AdjustCommandLineString(flagSet, &c.Log.Level, "log-level")
	configutil.AdjustCommandLineString(flagSet, &c.Log.File.Filename, "log-file")
	configutil.AdjustCommandLineString(flagSet, &c.Name, "name")
	configutil.AdjustCommandLineString(flagSet, &c.DataDir, "data-dir")
	configutil.AdjustCommandLineString(flagSet, &c.ClientUrls, "client-urls")
	configutil.AdjustCommandLineString(flagSet, &c.AdvertiseClientUrls, "advertise-client-urls")
	configutil.AdjustCommandLineString(flagSet, &c.PeerUrls, "peer-urls")
	configutil.AdjustCommandLineString(flagSet, &c.AdvertisePeerUrls, "advertise-peer-urls")
	configutil.AdjustCommandLineString(flagSet, &c.InitialCluster, "initial-cluster")
	configutil.AdjustCommandLineString(flagSet, &c.Join, "join")
	configutil.AdjustCommandLineString(flagSet, &c.Metric.PushAddress, "metrics-addr")
	configutil.AdjustCommandLineString(flagSet, &c.Security.CAPath, "cacert")
	configutil.AdjustCommandLineString(flagSet, &c.Security.CertPath, "cert")
	configutil.AdjustCommandLineString(flagSet, &c.Security.KeyPath, "key")
	configutil.AdjustCommandLineBool(flagSet, &c.ForceNewCluster, "force-new-cluster")

	return c.Adjust(meta, false)
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	if c.Join != "" && c.InitialCluster != "" {
		return errors.New("-initial-cluster and -join can not be provided at the same time")
	}
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

// Adjust is used to adjust the PD configurations.
func (c *Config) Adjust(meta *toml.MetaData, reloading bool) error {
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

	if err := c.Validate(); err != nil {
		return err
	}

	configutil.AdjustString(&c.ClientUrls, defaultClientUrls)
	configutil.AdjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	configutil.AdjustString(&c.PeerUrls, defaultPeerUrls)
	configutil.AdjustString(&c.AdvertisePeerUrls, c.PeerUrls)
	configutil.AdjustDuration(&c.Metric.PushInterval, defaultMetricsPushInterval)

	if len(c.InitialCluster) == 0 {
		// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
		// so the initial cluster is pd=http://127.0.0.1:2380,pd=http://127.0.0.1:2381
		items := strings.Split(c.AdvertisePeerUrls, ",")

		sep := ""
		for _, item := range items {
			c.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
			sep = ","
		}
	}

	configutil.AdjustString(&c.InitialClusterState, defaultInitialClusterState)
	configutil.AdjustString(&c.InitialClusterToken, defaultInitialClusterToken)

	if len(c.Join) > 0 {
		if _, err := url.Parse(c.Join); err != nil {
			return errors.Errorf("failed to parse join addr:%s, err:%v", c.Join, err)
		}
	}

	configutil.AdjustInt(&c.MaxConcurrentTSOProxyStreamings, defaultMaxConcurrentTSOProxyStreamings)
	configutil.AdjustDuration(&c.TSOProxyRecvFromClientTimeout, defaultTSOProxyRecvFromClientTimeout)

	configutil.AdjustInt64(&c.LeaderLease, defaultLeaderLease)
	configutil.AdjustDuration(&c.TSOSaveInterval, defaultTSOSaveInterval)
	configutil.AdjustDuration(&c.TSOUpdatePhysicalInterval, defaultTSOUpdatePhysicalInterval)

	if c.TSOUpdatePhysicalInterval.Duration > maxTSOUpdatePhysicalInterval {
		c.TSOUpdatePhysicalInterval.Duration = maxTSOUpdatePhysicalInterval
	} else if c.TSOUpdatePhysicalInterval.Duration < minTSOUpdatePhysicalInterval {
		c.TSOUpdatePhysicalInterval.Duration = minTSOUpdatePhysicalInterval
	}
	if c.TSOUpdatePhysicalInterval.Duration != defaultTSOUpdatePhysicalInterval {
		log.Warn("tso update physical interval is non-default",
			zap.Duration("update-physical-interval", c.TSOUpdatePhysicalInterval.Duration))
	}

	if c.Labels == nil {
		c.Labels = make(map[string]string)
	}

	configutil.AdjustString(&c.AutoCompactionMode, defaultCompactionMode)
	configutil.AdjustString(&c.AutoCompactionRetention, defaultAutoCompactionRetention)
	if !configMetaData.IsDefined("quota-backend-bytes") {
		c.QuotaBackendBytes = defaultQuotaBackendBytes
	}
	if !configMetaData.IsDefined("max-request-bytes") {
		c.MaxRequestBytes = defaultMaxRequestBytes
	}
	configutil.AdjustDuration(&c.TickInterval, defaultTickInterval)
	configutil.AdjustDuration(&c.ElectionInterval, defaultElectionInterval)

	configutil.AdjustString(&c.Metric.PushJob, c.Name)

	if err := c.Schedule.Adjust(configMetaData.Child("schedule"), reloading); err != nil {
		return err
	}
	if err := c.Replication.Adjust(configMetaData.Child("replication")); err != nil {
		return err
	}

	if err := c.PDServerCfg.adjust(configMetaData.Child("pd-server")); err != nil {
		return err
	}

	c.adjustLog(configMetaData.Child("log"))
	configutil.AdjustDuration(&c.HeartbeatStreamBindInterval, defaultHeartbeatStreamRebindInterval)

	configutil.AdjustDuration(&c.LeaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)

	if !configMetaData.IsDefined("enable-prevote") {
		c.PreVote = true
	}
	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = defaultEnableGRPCGateway
	}

	c.Dashboard.adjust(configMetaData.Child("dashboard"))

	c.ReplicationMode.adjust(configMetaData.Child("replication-mode"))

	c.Keyspace.adjust(configMetaData.Child("keyspace"))

	c.Microservice.adjust(configMetaData.Child("micro-service"))

	if err := c.Security.Encryption.Adjust(); err != nil {
		return err
	}

	c.Controller.Adjust(configMetaData.Child("controller"))

	return nil
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = defaultDisableErrorVerbose
	}
	configutil.AdjustString(&c.Log.Format, defaultLogFormat)
	configutil.AdjustString(&c.Log.Level, defaultLogLevel)
}

// Clone returns a cloned configuration.
func (c *Config) Clone() *Config {
	cfg := *c
	return &cfg
}

func (c *Config) String() string {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "<nil>"
	}
	return string(data)
}

// PDServerConfig is the configuration for pd server.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type PDServerConfig struct {
	// UseRegionStorage enables the independent region storage.
	UseRegionStorage bool `toml:"use-region-storage" json:"use-region-storage,string"`
	// MaxResetTSGap is the max gap to reset the TSO.
	MaxResetTSGap typeutil.Duration `toml:"max-gap-reset-ts" json:"max-gap-reset-ts"`
	// KeyType is option to specify the type of keys.
	// There are some types supported: ["table", "raw", "txn"], default: "table"
	KeyType string `toml:"key-type" json:"key-type"`
	// RuntimeServices is the running extension services.
	RuntimeServices typeutil.StringSlice `toml:"runtime-services" json:"runtime-services"`
	// MetricStorage is the cluster metric storage.
	// Currently, we use prometheus as metric storage, we may use PD/TiKV as metric storage later.
	MetricStorage string `toml:"metric-storage" json:"metric-storage"`
	// There are some values supported: "auto", "none", or a specific address, default: "auto"
	DashboardAddress string `toml:"dashboard-address" json:"dashboard-address"`
	// TraceRegionFlow the option to update flow information of regions.
	// WARN: TraceRegionFlow is deprecated.
	TraceRegionFlow bool `toml:"trace-region-flow" json:"trace-region-flow,string,omitempty"`
	// FlowRoundByDigit used to discretization processing flow information.
	FlowRoundByDigit int `toml:"flow-round-by-digit" json:"flow-round-by-digit"`
	// MinResolvedTSPersistenceInterval is the interval to save the min resolved ts.
	MinResolvedTSPersistenceInterval typeutil.Duration `toml:"min-resolved-ts-persistence-interval" json:"min-resolved-ts-persistence-interval"`
	// ServerMemoryLimit indicates the memory limit of current process.
	ServerMemoryLimit float64 `toml:"server-memory-limit" json:"server-memory-limit"`
	// ServerMemoryLimitGCTrigger indicates the gc percentage of the ServerMemoryLimit.
	ServerMemoryLimitGCTrigger float64 `toml:"server-memory-limit-gc-trigger" json:"server-memory-limit-gc-trigger"`
	// EnableGOGCTuner is to enable GOGC tuner. it can tuner GOGC.
	EnableGOGCTuner bool `toml:"enable-gogc-tuner" json:"enable-gogc-tuner,string"`
	// GCTunerThreshold is the threshold of GC tuner.
	GCTunerThreshold float64 `toml:"gc-tuner-threshold" json:"gc-tuner-threshold"`
	// BlockSafePointV1 is used to control gc safe point v1 and service safe point v1 can not be updated.
	BlockSafePointV1 bool `toml:"block-safe-point-v1" json:"block-safe-point-v1,string"`
}

func (c *PDServerConfig) adjust(meta *configutil.ConfigMetaData) error {
	configutil.AdjustDuration(&c.MaxResetTSGap, defaultMaxResetTSGap)
	if !meta.IsDefined("use-region-storage") {
		c.UseRegionStorage = defaultUseRegionStorage
	}
	if !meta.IsDefined("key-type") {
		c.KeyType = defaultKeyType
	}
	if !meta.IsDefined("runtime-services") {
		c.RuntimeServices = defaultRuntimeServices
	}
	if !meta.IsDefined("dashboard-address") {
		c.DashboardAddress = defaultDashboardAddress
	}
	if !meta.IsDefined("flow-round-by-digit") {
		configutil.AdjustInt(&c.FlowRoundByDigit, defaultFlowRoundByDigit)
	}
	if !meta.IsDefined("min-resolved-ts-persistence-interval") {
		configutil.AdjustDuration(&c.MinResolvedTSPersistenceInterval, DefaultMinResolvedTSPersistenceInterval)
	}
	if !meta.IsDefined("server-memory-limit") {
		configutil.AdjustFloat64(&c.ServerMemoryLimit, defaultServerMemoryLimit)
	}
	if c.ServerMemoryLimit < minServerMemoryLimit {
		c.ServerMemoryLimit = minServerMemoryLimit
	} else if c.ServerMemoryLimit > maxServerMemoryLimit {
		c.ServerMemoryLimit = maxServerMemoryLimit
	}
	if !meta.IsDefined("server-memory-limit-gc-trigger") {
		configutil.AdjustFloat64(&c.ServerMemoryLimitGCTrigger, defaultServerMemoryLimitGCTrigger)
	}
	if c.ServerMemoryLimitGCTrigger < minServerMemoryLimitGCTrigger {
		c.ServerMemoryLimitGCTrigger = minServerMemoryLimitGCTrigger
	} else if c.ServerMemoryLimitGCTrigger > maxServerMemoryLimitGCTrigger {
		c.ServerMemoryLimitGCTrigger = maxServerMemoryLimitGCTrigger
	}
	if !meta.IsDefined("enable-gogc-tuner") {
		c.EnableGOGCTuner = defaultEnableGOGCTuner
	}
	if !meta.IsDefined("gc-tuner-threshold") {
		configutil.AdjustFloat64(&c.GCTunerThreshold, defaultGCTunerThreshold)
	}
	if c.GCTunerThreshold < minGCTunerThreshold {
		c.GCTunerThreshold = minGCTunerThreshold
	} else if c.GCTunerThreshold > maxGCTunerThreshold {
		c.GCTunerThreshold = maxGCTunerThreshold
	}
	if err := c.migrateConfigurationFromFile(meta); err != nil {
		return err
	}
	return c.Validate()
}

func (c *PDServerConfig) migrateConfigurationFromFile(meta *configutil.ConfigMetaData) error {
	oldName, newName := "trace-region-flow", "flow-round-by-digit"
	defineOld, defineNew := meta.IsDefined(oldName), meta.IsDefined(newName)
	switch {
	case defineOld && defineNew:
		if c.TraceRegionFlow && (c.FlowRoundByDigit == defaultFlowRoundByDigit) {
			return errors.Errorf("config item %s and %s(deprecated) are conflict", newName, oldName)
		}
	case defineOld && !defineNew:
		if !c.TraceRegionFlow {
			c.FlowRoundByDigit = math.MaxInt8
		}
	}
	return nil
}

// MigrateDeprecatedFlags updates new flags according to deprecated flags.
func (c *PDServerConfig) MigrateDeprecatedFlags() {
	if !c.TraceRegionFlow {
		c.FlowRoundByDigit = math.MaxInt8
	}
	// json omity the false. next time will not persist to the kv.
	c.TraceRegionFlow = false
}

// Clone returns a cloned PD server config.
func (c *PDServerConfig) Clone() *PDServerConfig {
	runtimeServices := append(c.RuntimeServices[:0:0], c.RuntimeServices...)
	cfg := *c
	cfg.RuntimeServices = runtimeServices
	return &cfg
}

// Validate is used to validate if some pd-server configurations are right.
func (c *PDServerConfig) Validate() error {
	switch c.DashboardAddress {
	case "auto":
	case "none":
	default:
		if err := ValidateURLWithScheme(c.DashboardAddress); err != nil {
			return err
		}
	}
	if c.KeyType != "table" && c.KeyType != "raw" && c.KeyType != "txn" {
		return errors.Errorf("key-type %v is invalid", c.KeyType)
	}
	if c.FlowRoundByDigit < 0 {
		return errs.ErrConfigItem.GenWithStack("flow round by digit cannot be negative number")
	}
	if c.ServerMemoryLimit < minServerMemoryLimit || c.ServerMemoryLimit > maxServerMemoryLimit {
		return errors.New(fmt.Sprintf("server-memory-limit should between %v and %v", minServerMemoryLimit, maxServerMemoryLimit))
	}
	if c.ServerMemoryLimitGCTrigger < minServerMemoryLimitGCTrigger || c.ServerMemoryLimitGCTrigger > maxServerMemoryLimitGCTrigger {
		return errors.New(fmt.Sprintf("server-memory-limit-gc-trigger should between %v and %v",
			minServerMemoryLimitGCTrigger, maxServerMemoryLimitGCTrigger))
	}
	if c.GCTunerThreshold < minGCTunerThreshold || c.GCTunerThreshold > maxGCTunerThreshold {
		return errors.New(fmt.Sprintf("gc-tuner-threshold should between %v and %v", minGCTunerThreshold, maxGCTunerThreshold))
	}

	return nil
}

// StoreLabel is the config item of LabelPropertyConfig.
type StoreLabel struct {
	Key   string `toml:"key" json:"key"`
	Value string `toml:"value" json:"value"`
}

// LabelPropertyConfig is the config section to set properties to store labels.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type LabelPropertyConfig map[string][]StoreLabel

// Clone returns a cloned label property configuration.
func (c LabelPropertyConfig) Clone() LabelPropertyConfig {
	m := make(map[string][]StoreLabel, len(c))
	for k, sl := range c {
		sl2 := make([]StoreLabel, 0, len(sl))
		sl2 = append(sl2, sl...)
		m[k] = sl2
	}
	return m
}

// GetLeaderLease returns the leader lease.
func (c *Config) GetLeaderLease() int64 {
	return c.LeaderLease
}

// GetMaxConcurrentTSOProxyStreamings returns the max concurrent TSO proxy streamings.
// If the value is negative, there is no limit.
func (c *Config) GetMaxConcurrentTSOProxyStreamings() int {
	return c.MaxConcurrentTSOProxyStreamings
}

// GetTSOProxyRecvFromClientTimeout returns timeout value for TSO proxy receiving from the client.
func (c *Config) GetTSOProxyRecvFromClientTimeout() time.Duration {
	return c.TSOProxyRecvFromClientTimeout.Duration
}

// GetTSOUpdatePhysicalInterval returns TSO update physical interval.
func (c *Config) GetTSOUpdatePhysicalInterval() time.Duration {
	return c.TSOUpdatePhysicalInterval.Duration
}

// GetTSOSaveInterval returns TSO save interval.
func (c *Config) GetTSOSaveInterval() time.Duration {
	return c.TSOSaveInterval.Duration
}

// GetTLSConfig returns the TLS config.
func (c *Config) GetTLSConfig() *grpcutil.TLSConfig {
	return &c.Security.TLSConfig
}

// GenEmbedEtcdConfig generates a configuration for embedded etcd.
func (c *Config) GenEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.InitialClusterToken = c.InitialClusterToken
	cfg.EnablePprof = true
	cfg.PreVote = c.PreVote
	cfg.StrictReconfigCheck = !c.DisableStrictReconfigCheck
	cfg.TickMs = uint(c.TickInterval.Duration / time.Millisecond)
	cfg.ElectionMs = uint(c.ElectionInterval.Duration / time.Millisecond)
	cfg.AutoCompactionMode = c.AutoCompactionMode
	cfg.AutoCompactionRetention = c.AutoCompactionRetention
	cfg.QuotaBackendBytes = int64(c.QuotaBackendBytes)
	cfg.MaxRequestBytes = c.MaxRequestBytes

	cfg.ClientTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.ClientTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.ClientTLSInfo.CertFile = c.Security.CertPath
	cfg.ClientTLSInfo.KeyFile = c.Security.KeyPath
	// Keep compatibility with https://github.com/tikv/pd/pull/2305
	// Only check client cert when there are multiple CNs.
	if len(c.Security.CertAllowedCNs) > 1 {
		cfg.ClientTLSInfo.AllowedCNs = c.Security.CertAllowedCNs
	}
	cfg.PeerTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.PeerTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.PeerTLSInfo.CertFile = c.Security.CertPath
	cfg.PeerTLSInfo.KeyFile = c.Security.KeyPath
	cfg.PeerTLSInfo.AllowedCNs = c.Security.CertAllowedCNs
	cfg.ForceNewCluster = c.ForceNewCluster
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(c.Logger, c.Logger.Core(), c.LogProps.Syncer)
	cfg.EnableGRPCGateway = c.EnableGRPCGateway
	cfg.Logger = "zap"
	var err error

	cfg.ListenPeerUrls, err = parseUrls(c.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.AdvertisePeerUrls, err = parseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.ListenClientUrls, err = parseUrls(c.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.AdvertiseClientUrls, err = parseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// DashboardConfig is the configuration for tidb-dashboard.
type DashboardConfig struct {
	TiDBCAPath            string `toml:"tidb-cacert-path" json:"tidb-cacert-path"`
	TiDBCertPath          string `toml:"tidb-cert-path" json:"tidb-cert-path"`
	TiDBKeyPath           string `toml:"tidb-key-path" json:"tidb-key-path"`
	PublicPathPrefix      string `toml:"public-path-prefix" json:"public-path-prefix"`
	InternalProxy         bool   `toml:"internal-proxy" json:"internal-proxy"`
	EnableTelemetry       bool   `toml:"enable-telemetry" json:"enable-telemetry"`
	EnableExperimental    bool   `toml:"enable-experimental" json:"enable-experimental"`
	DisableCustomPromAddr bool   `toml:"disable-custom-prom-addr" json:"disable-custom-prom-addr"`
}

// ToTiDBTLSConfig generates tls config for connecting to TiDB, used by tidb-dashboard.
func (c *DashboardConfig) ToTiDBTLSConfig() (*tls.Config, error) {
	if (len(c.TiDBCertPath) != 0 && len(c.TiDBKeyPath) != 0) || len(c.TiDBCAPath) != 0 {
		tlsInfo := transport.TLSInfo{
			CertFile:      c.TiDBCertPath,
			KeyFile:       c.TiDBKeyPath,
			TrustedCAFile: c.TiDBCAPath,
		}
		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return tlsConfig, nil
	}
	return nil, nil
}

func (c *DashboardConfig) adjust(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("enable-telemetry") {
		c.EnableTelemetry = defaultEnableTelemetry
	}
}

// ReplicationModeConfig is the configuration for the replication policy.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReplicationModeConfig struct {
	ReplicationMode string                      `toml:"replication-mode" json:"replication-mode"` // can be 'dr-auto-sync' or 'majority', default value is 'majority'
	DRAutoSync      DRAutoSyncReplicationConfig `toml:"dr-auto-sync" json:"dr-auto-sync"`         // used when ReplicationMode is 'dr-auto-sync'
}

// Clone returns a copy of replication mode config.
func (c *ReplicationModeConfig) Clone() *ReplicationModeConfig {
	cfg := *c
	return &cfg
}

func (c *ReplicationModeConfig) adjust(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("replication-mode") || NormalizeReplicationMode(c.ReplicationMode) == "" {
		c.ReplicationMode = "majority"
	}
	c.DRAutoSync.adjust(meta.Child("dr-auto-sync"))
}

// NormalizeReplicationMode converts user's input mode to internal use.
// It returns "" if failed to convert.
func NormalizeReplicationMode(m string) string {
	s := strings.ReplaceAll(strings.ToLower(m), "_", "-")
	if s == "majority" || s == "dr-auto-sync" {
		return s
	}
	return ""
}

// DRAutoSyncReplicationConfig is the configuration for auto sync mode between 2 data centers.
type DRAutoSyncReplicationConfig struct {
	LabelKey           string            `toml:"label-key" json:"label-key"`
	Primary            string            `toml:"primary" json:"primary"`
	DR                 string            `toml:"dr" json:"dr"`
	PrimaryReplicas    int               `toml:"primary-replicas" json:"primary-replicas"`
	DRReplicas         int               `toml:"dr-replicas" json:"dr-replicas"`
	WaitStoreTimeout   typeutil.Duration `toml:"wait-store-timeout" json:"wait-store-timeout"`
	WaitRecoverTimeout typeutil.Duration `toml:"wait-recover-timeout" json:"wait-recover-timeout"`
	PauseRegionSplit   bool              `toml:"pause-region-split" json:"pause-region-split,string"`
}

func (c *DRAutoSyncReplicationConfig) adjust(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("wait-store-timeout") {
		c.WaitStoreTimeout = typeutil.NewDuration(defaultDRWaitStoreTimeout)
	}
}

// MicroserviceConfig is the configuration for microservice.
type MicroserviceConfig struct {
	EnableSchedulingFallback  bool `toml:"enable-scheduling-fallback" json:"enable-scheduling-fallback,string"`
	EnableTSODynamicSwitching bool `toml:"enable-tso-dynamic-switching" json:"enable-tso-dynamic-switching,string"`
}

func (c *MicroserviceConfig) adjust(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("enable-scheduling-fallback") {
		c.EnableSchedulingFallback = defaultEnableSchedulingFallback
	}
	if !meta.IsDefined("enable-tso-dynamic-switching") {
		c.EnableTSODynamicSwitching = defaultEnableTSODynamicSwitching
	}
}

// Clone returns a copy of microservice config.
func (c *MicroserviceConfig) Clone() *MicroserviceConfig {
	cfg := *c
	return &cfg
}

// IsSchedulingFallbackEnabled returns whether to enable scheduling service fallback to PD service.
func (c *MicroserviceConfig) IsSchedulingFallbackEnabled() bool {
	return c.EnableSchedulingFallback
}

// IsTSODynamicSwitchingEnabled returns whether to enable TSO dynamic switching.
func (c *MicroserviceConfig) IsTSODynamicSwitchingEnabled() bool {
	return c.EnableTSODynamicSwitching
}

// KeyspaceConfig is the configuration for keyspace management.
type KeyspaceConfig struct {
	// PreAlloc contains the keyspace to be allocated during keyspace manager initialization.
	PreAlloc []string `toml:"pre-alloc" json:"pre-alloc"`
	// WaitRegionSplit indicates whether to wait for the region split to complete
	WaitRegionSplit bool `toml:"wait-region-split" json:"wait-region-split"`
	// WaitRegionSplitTimeout indicates the max duration to wait region split.
	WaitRegionSplitTimeout typeutil.Duration `toml:"wait-region-split-timeout" json:"wait-region-split-timeout"`
	// CheckRegionSplitInterval indicates the interval to check whether the region split is complete
	CheckRegionSplitInterval typeutil.Duration `toml:"check-region-split-interval" json:"check-region-split-interval"`
}

// Validate checks if keyspace config falls within acceptable range.
func (c *KeyspaceConfig) Validate() error {
	if c.CheckRegionSplitInterval.Duration > maxCheckRegionSplitInterval || c.CheckRegionSplitInterval.Duration < minCheckRegionSplitInterval {
		return errors.New(fmt.Sprintf("[keyspace] check-region-split-interval should between %v and %v",
			minCheckRegionSplitInterval, maxCheckRegionSplitInterval))
	}
	if c.CheckRegionSplitInterval.Duration >= c.WaitRegionSplitTimeout.Duration {
		return errors.New("[keyspace] check-region-split-interval should be less than wait-region-split-timeout")
	}
	return nil
}

func (c *KeyspaceConfig) adjust(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("wait-region-split") {
		c.WaitRegionSplit = true
	}
	if !meta.IsDefined("wait-region-split-timeout") {
		c.WaitRegionSplitTimeout = typeutil.NewDuration(defaultWaitRegionSplitTimeout)
	}
	if !meta.IsDefined("check-region-split-interval") {
		c.CheckRegionSplitInterval = typeutil.NewDuration(defaultCheckRegionSplitInterval)
	}
}

// Clone makes a deep copy of the keyspace config.
func (c *KeyspaceConfig) Clone() *KeyspaceConfig {
	preAlloc := append(c.PreAlloc[:0:0], c.PreAlloc...)
	cfg := *c
	cfg.PreAlloc = preAlloc
	return &cfg
}

// GetPreAlloc returns the keyspace to be allocated during keyspace manager initialization.
func (c *KeyspaceConfig) GetPreAlloc() []string {
	return c.PreAlloc
}

// ToWaitRegionSplit returns whether to wait for the region split to complete.
func (c *KeyspaceConfig) ToWaitRegionSplit() bool {
	return c.WaitRegionSplit
}

// GetWaitRegionSplitTimeout returns the max duration to wait region split.
func (c *KeyspaceConfig) GetWaitRegionSplitTimeout() time.Duration {
	return c.WaitRegionSplitTimeout.Duration
}

// GetCheckRegionSplitInterval returns the interval to check whether the region split is complete.
func (c *KeyspaceConfig) GetCheckRegionSplitInterval() time.Duration {
	return c.CheckRegionSplitInterval.Duration
}
