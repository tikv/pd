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
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/metricutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/versioninfo"

	"github.com/BurntSushi/toml"
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config is the pd server configuration.
type Config struct {
	flagSet *flag.FlagSet

	Version bool `json:"-"`

	ConfigCheck bool `json:"-"`

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

	// TSOSaveInterval is the interval to save timestamp.
	TSOSaveInterval typeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// The interval to update physical part of timestamp. Usually, this config should not be set.
	// It's only useful for test purposes.
	// This config is only valid in 50ms to 10s. If it's configured too long or too short, it will
	// be automatically clamped to the range.
	TSOUpdatePhysicalInterval typeutil.Duration `toml:"tso-update-physical-interval" json:"tso-update-physical-interval"`

	// EnableLocalTSO is used to enable the Local TSO Allocator feature,
	// which allows the PD server to generate Local TSO for certain DC-level transactions.
	// To make this feature meaningful, user has to set the "zone" label for the PD server
	// to indicate which DC this PD belongs to.
	EnableLocalTSO bool `toml:"enable-local-tso" json:"enable-local-tso"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	Schedule ScheduleConfig `toml:"schedule" json:"schedule"`

	Replication ReplicationConfig `toml:"replication" json:"replication"`

	PDServerCfg PDServerConfig `toml:"pd-server" json:"pd-server"`

	ClusterVersion semver.Version `toml:"cluster-version" json:"cluster-version"`

	// Labels indicates the labels set for **this** PD server. The labels describe some specific properties
	// like `zone`/`rack`/`host`. Currently, labels won't affect the PD server except for some special
	// label keys. Now we have following special keys:
	// 1. 'zone' is a special key that indicates the DC location of this PD server. If it is set, the value for this
	// will be used to determine which DC's Local TSO service this PD will provide with if EnableLocalTSO is true.
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
	TickInterval typeutil.Duration `toml:"tick-interval"`
	// ElectionInterval is the interval for etcd Raft election.
	ElectionInterval typeutil.Duration `toml:"election-interval"`
	// Prevote is true to enable Raft Pre-Vote.
	// If enabled, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool `toml:"enable-prevote"`

	Security SecurityConfig `toml:"security" json:"security"`

	LabelProperty LabelPropertyConfig `toml:"label-property" json:"label-property"`

	configFile string

	// For all warnings during parsing.
	WarningMsgs []string

	// Only test can change them.
	nextRetryDelay             time.Duration
	DisableStrictReconfigCheck bool

	HeartbeatStreamBindInterval typeutil.Duration

	LeaderPriorityCheckInterval typeutil.Duration

	logger   *zap.Logger
	logProps *log.ZapProperties

	Dashboard DashboardConfig `toml:"dashboard" json:"dashboard"`

	ReplicationMode ReplicationModeConfig `toml:"replication-mode" json:"replication-mode"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.Version, "V", false, "print version information and exit")
	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.BoolVar(&cfg.ConfigCheck, "config-check", false, "check config file validity and exit")

	fs.StringVar(&cfg.Name, "name", "", "human-readable name for this pd member")

	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	fs.StringVar(&cfg.Join, "join", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")

	fs.StringVar(&cfg.Metric.PushAddress, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")

	fs.StringVar(&cfg.Log.Level, "L", "", "log level: debug, info, warn, error, fatal (default 'info')")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")

	fs.StringVar(&cfg.Security.CAPath, "cacert", "", "path of file that contains list of trusted TLS CAs")
	fs.StringVar(&cfg.Security.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.Security.KeyPath, "key", "", "path of file that contains X509 key in PEM format")
	fs.BoolVar(&cfg.ForceNewCluster, "force-new-cluster", false, "force to create a new one-member cluster")

	return cfg
}

const (
	defaultLeaderLease             = int64(3)
	defaultNextRetryDelay          = time.Second
	defaultCompactionMode          = "periodic"
	defaultAutoCompactionRetention = "1h"
	defaultQuotaBackendBytes       = typeutil.ByteSize(8 * 1024 * 1024 * 1024) // 8GB

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
	defaultTraceRegionFlow   = true
	defaultFlowRoundByDigit  = 3 // KB
	maxTraceFlowRoundByDigit = 5 // 0.1 MB
	defaultMaxResetTSGap     = 24 * time.Hour
	defaultKeyType           = "table"

	defaultStrictlyMatchLabel   = false
	defaultEnablePlacementRules = true
	defaultEnableGRPCGateway    = true
	defaultDisableErrorVerbose  = true

	defaultDashboardAddress = "auto"

	defaultDRWaitStoreTimeout = time.Minute
	defaultDRWaitSyncTimeout  = time.Minute
	defaultDRWaitAsyncTimeout = 2 * time.Minute

	defaultTSOSaveInterval = time.Duration(defaultLeaderLease) * time.Second
	// DefaultTSOUpdatePhysicalInterval is the default value of the config `TSOUpdatePhysicalInterval`.
	DefaultTSOUpdatePhysicalInterval = 50 * time.Millisecond
	maxTSOUpdatePhysicalInterval     = 10 * time.Second
	minTSOUpdatePhysicalInterval     = 50 * time.Millisecond
)

// Special keys for Labels
const (
	// ZoneLabel is the name of the key which indicates DC location of this PD server.
	ZoneLabel = "zone"
)

var (
	defaultEnableTelemetry = true
	defaultRuntimeServices = []string{}
	defaultLocationLabels  = []string{}
	// DefaultStoreLimit is the default store limit of add peer and remove peer.
	DefaultStoreLimit = StoreLimit{AddPeer: 15, RemovePeer: 15}
	// DefaultTiFlashStoreLimit is the default TiFlash store limit of add peer and remove peer.
	DefaultTiFlashStoreLimit = StoreLimit{AddPeer: 30, RemovePeer: 30}
)

func init() {
	initByLDFlags(versioninfo.PDEdition)
}

func initByLDFlags(edition string) {
	if edition != versioninfo.CommunityEdition {
		defaultEnableTelemetry = false
	}
}

// StoreLimit is the default limit of adding peer and removing peer when putting stores.
type StoreLimit struct {
	mu sync.RWMutex
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

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt(v *int, defValue int) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration <= 0 {
		v.Duration = defValue
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

func adjustPath(p *string) {
	absPath, err := filepath.Abs(*p)
	if err == nil {
		*p = absPath
	}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	var meta *toml.MetaData
	if c.configFile != "" {
		meta, err = c.configFromFile(c.configFile)
		if err != nil {
			return err
		}

		// Backward compatibility for toml config
		if c.LogFileDeprecated != "" {
			msg := fmt.Sprintf("log-file in %s is deprecated, use [log.file] instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
			if c.Log.File.Filename == "" {
				c.Log.File.Filename = c.LogFileDeprecated
			}
		}
		if c.LogLevelDeprecated != "" {
			msg := fmt.Sprintf("log-level in %s is deprecated, use [log] instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
			if c.Log.Level == "" {
				c.Log.Level = c.LogLevelDeprecated
			}
		}
		if meta.IsDefined("schedule", "disable-raft-learner") {
			msg := fmt.Sprintf("disable-raft-learner in %s is deprecated", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
		if meta.IsDefined("dashboard", "disable-telemetry") {
			msg := fmt.Sprintf("disable-telemetry in %s is deprecated, use enable-telemetry instead", c.configFile)
			c.WarningMsgs = append(c.WarningMsgs, msg)
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	err = c.Adjust(meta, false)
	return err
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

// Utility to test if a configuration is defined.
type configMetaData struct {
	meta *toml.MetaData
	path []string
}

func newConfigMetadata(meta *toml.MetaData) *configMetaData {
	return &configMetaData{meta: meta}
}

func (m *configMetaData) IsDefined(key string) bool {
	if m.meta == nil {
		return false
	}
	keys := append([]string(nil), m.path...)
	keys = append(keys, key)
	return m.meta.IsDefined(keys...)
}

func (m *configMetaData) Child(path ...string) *configMetaData {
	newPath := append([]string(nil), m.path...)
	newPath = append(newPath, path...)
	return &configMetaData{
		meta: m.meta,
		path: newPath,
	}
}

func (m *configMetaData) CheckUndecoded() error {
	if m.meta == nil {
		return nil
	}
	undecoded := m.meta.Undecoded()
	if len(undecoded) == 0 {
		return nil
	}
	errInfo := "Config contains undefined item: "
	for _, key := range undecoded {
		errInfo += key.String() + ", "
	}
	return errors.New(errInfo[:len(errInfo)-2])
}

// Adjust is used to adjust the PD configurations.
func (c *Config) Adjust(meta *toml.MetaData, reloading bool) error {
	configMetaData := newConfigMetadata(meta)
	if err := configMetaData.CheckUndecoded(); err != nil {
		c.WarningMsgs = append(c.WarningMsgs, err.Error())
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		adjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))
	adjustPath(&c.DataDir)

	if err := c.Validate(); err != nil {
		return err
	}

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)
	adjustDuration(&c.Metric.PushInterval, defaultMetricsPushInterval)

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

	adjustString(&c.InitialClusterState, defaultInitialClusterState)
	adjustString(&c.InitialClusterToken, defaultInitialClusterToken)

	if len(c.Join) > 0 {
		if _, err := url.Parse(c.Join); err != nil {
			return errors.Errorf("failed to parse join addr:%s, err:%v", c.Join, err)
		}
	}

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	adjustDuration(&c.TSOSaveInterval, defaultTSOSaveInterval)

	adjustDuration(&c.TSOUpdatePhysicalInterval, DefaultTSOUpdatePhysicalInterval)

	if c.TSOUpdatePhysicalInterval.Duration > maxTSOUpdatePhysicalInterval {
		c.TSOUpdatePhysicalInterval.Duration = maxTSOUpdatePhysicalInterval
	} else if c.TSOUpdatePhysicalInterval.Duration < minTSOUpdatePhysicalInterval {
		c.TSOUpdatePhysicalInterval.Duration = minTSOUpdatePhysicalInterval
	}

	if c.Labels == nil {
		c.Labels = make(map[string]string)
	}

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}

	adjustString(&c.AutoCompactionMode, defaultCompactionMode)
	adjustString(&c.AutoCompactionRetention, defaultAutoCompactionRetention)
	if !configMetaData.IsDefined("quota-backend-bytes") {
		c.QuotaBackendBytes = defaultQuotaBackendBytes
	}
	adjustDuration(&c.TickInterval, defaultTickInterval)
	adjustDuration(&c.ElectionInterval, defaultElectionInterval)

	adjustString(&c.Metric.PushJob, c.Name)

	if err := c.Schedule.adjust(configMetaData.Child("schedule"), reloading); err != nil {
		return err
	}
	if err := c.Replication.adjust(configMetaData.Child("replication")); err != nil {
		return err
	}

	if err := c.PDServerCfg.adjust(configMetaData.Child("pd-server")); err != nil {
		return err
	}

	c.adjustLog(configMetaData.Child("log"))
	adjustDuration(&c.HeartbeatStreamBindInterval, defaultHeartbeatStreamRebindInterval)

	adjustDuration(&c.LeaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)

	if !configMetaData.IsDefined("enable-prevote") {
		c.PreVote = true
	}
	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = defaultEnableGRPCGateway
	}

	c.Dashboard.adjust(configMetaData.Child("dashboard"))

	c.ReplicationMode.adjust(configMetaData.Child("replication-mode"))

	c.Security.Encryption.Adjust()

	return nil
}

func (c *Config) adjustLog(meta *configMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = defaultDisableErrorVerbose
	}
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

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) (*toml.MetaData, error) {
	meta, err := toml.DecodeFile(path, c)
	return &meta, errors.WithStack(err)
}

// ScheduleConfig is the schedule configuration.
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
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit" json:"leader-schedule-limit"`
	// LeaderSchedulePolicy is the option to balance leader, there are some policies supported: ["count", "size"], default: "count"
	LeaderSchedulePolicy string `toml:"leader-schedule-policy" json:"leader-schedule-policy"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `toml:"region-schedule-limit" json:"region-schedule-limit"`
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
	// EnableJointConsensus is the option to enable using joint consensus as a operator step.
	EnableJointConsensus bool `toml:"enable-joint-consensus" json:"enable-joint-consensus,string"`

	// Schedulers support for loading customized schedulers
	Schedulers SchedulerConfigs `toml:"schedulers" json:"schedulers-v2"` // json v2 is for the sake of compatible upgrade

	// Only used to display
	SchedulersPayload map[string]interface{} `toml:"schedulers-payload" json:"schedulers-payload"`

	// StoreLimitMode can be auto or manual, when set to auto,
	// PD tries to change the store limit values according to
	// the load state of the cluster dynamically. User can
	// overwrite the auto-tuned value by pd-ctl, when the value
	// is overwritten, the value is fixed until it is deleted.
	// Default: manual
	StoreLimitMode string `toml:"store-limit-mode" json:"store-limit-mode"`

	// Controls the time interval between write hot regions info into leveldb.
	HotRegionsWriteInterval typeutil.Duration `toml:"hot-regions-write-interval" json:"hot-regions-write-interval"`

	// The day of hot regions data to be reserved. 0 means close.
	HotRegionsReservedDays int64 `toml:"hot-regions-reserved-days" json:"hot-regions-reserved-days"`
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

const (
	defaultMaxReplicas               = 3
	defaultMaxSnapshotCount          = 64
	defaultMaxPendingPeerCount       = 64
	defaultMaxMergeRegionSize        = 20
	defaultMaxMergeRegionKeys        = 200000
	defaultSplitMergeInterval        = 1 * time.Hour
	defaultPatrolRegionInterval      = 10 * time.Millisecond
	defaultMaxStoreDownTime          = 30 * time.Minute
	defaultLeaderScheduleLimit       = 4
	defaultRegionScheduleLimit       = 2048
	defaultReplicaScheduleLimit      = 64
	defaultMergeScheduleLimit        = 8
	defaultHotRegionScheduleLimit    = 4
	defaultTolerantSizeRatio         = 0
	defaultLowSpaceRatio             = 0.8
	defaultHighSpaceRatio            = 0.7
	defaultRegionScoreFormulaVersion = "v2"
	// defaultHotRegionCacheHitsThreshold is the low hit number threshold of the
	// hot region.
	defaultHotRegionCacheHitsThreshold = 3
	defaultSchedulerMaxWaitingOperator = 5
	defaultLeaderSchedulePolicy        = "count"
	defaultStoreLimitMode              = "manual"
	defaultEnableJointConsensus        = true
	defaultEnableCrossTableMerge       = true
	defaultHotRegionsWriteInterval     = 10 * time.Minute
	defaultHotRegionsResevervedDays    = 0
)

func (c *ScheduleConfig) adjust(meta *configMetaData, reloading bool) error {
	if !meta.IsDefined("max-snapshot-count") {
		adjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	}
	if !meta.IsDefined("max-pending-peer-count") {
		adjustUint64(&c.MaxPendingPeerCount, defaultMaxPendingPeerCount)
	}
	if !meta.IsDefined("max-merge-region-size") {
		adjustUint64(&c.MaxMergeRegionSize, defaultMaxMergeRegionSize)
	}
	if !meta.IsDefined("max-merge-region-keys") {
		adjustUint64(&c.MaxMergeRegionKeys, defaultMaxMergeRegionKeys)
	}
	adjustDuration(&c.SplitMergeInterval, defaultSplitMergeInterval)
	adjustDuration(&c.PatrolRegionInterval, defaultPatrolRegionInterval)
	adjustDuration(&c.MaxStoreDownTime, defaultMaxStoreDownTime)
	if !meta.IsDefined("leader-schedule-limit") {
		adjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	}
	if !meta.IsDefined("region-schedule-limit") {
		adjustUint64(&c.RegionScheduleLimit, defaultRegionScheduleLimit)
	}
	if !meta.IsDefined("replica-schedule-limit") {
		adjustUint64(&c.ReplicaScheduleLimit, defaultReplicaScheduleLimit)
	}
	if !meta.IsDefined("merge-schedule-limit") {
		adjustUint64(&c.MergeScheduleLimit, defaultMergeScheduleLimit)
	}
	if !meta.IsDefined("hot-region-schedule-limit") {
		adjustUint64(&c.HotRegionScheduleLimit, defaultHotRegionScheduleLimit)
	}
	if !meta.IsDefined("hot-region-cache-hits-threshold") {
		adjustUint64(&c.HotRegionCacheHitsThreshold, defaultHotRegionCacheHitsThreshold)
	}
	if !meta.IsDefined("tolerant-size-ratio") {
		adjustFloat64(&c.TolerantSizeRatio, defaultTolerantSizeRatio)
	}
	if !meta.IsDefined("scheduler-max-waiting-operator") {
		adjustUint64(&c.SchedulerMaxWaitingOperator, defaultSchedulerMaxWaitingOperator)
	}
	if !meta.IsDefined("leader-schedule-policy") {
		adjustString(&c.LeaderSchedulePolicy, defaultLeaderSchedulePolicy)
	}
	if !meta.IsDefined("store-limit-mode") {
		adjustString(&c.StoreLimitMode, defaultStoreLimitMode)
	}
	if !meta.IsDefined("enable-joint-consensus") {
		c.EnableJointConsensus = defaultEnableJointConsensus
	}
	if !meta.IsDefined("enable-cross-table-merge") {
		c.EnableCrossTableMerge = defaultEnableCrossTableMerge
	}
	adjustFloat64(&c.LowSpaceRatio, defaultLowSpaceRatio)
	adjustFloat64(&c.HighSpaceRatio, defaultHighSpaceRatio)

	// new cluster:v2, old cluster:v1
	if !meta.IsDefined("region-score-formula-version") && !reloading {
		adjustString(&c.RegionScoreFormulaVersion, defaultRegionScoreFormulaVersion)
	}

	adjustSchedulers(&c.Schedulers, DefaultSchedulers)

	for k, b := range c.migrateConfigurationMap() {
		v, err := c.parseDeprecatedFlag(meta, k, *b[0], *b[1])
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

	if !meta.IsDefined("hot-regions-write-interval") {
		adjustDuration(&c.HotRegionsWriteInterval, defaultHotRegionsWriteInterval)
	}

	if !meta.IsDefined("hot-regions-reserved-days") {
		adjustInt64(&c.HotRegionsReservedDays, defaultHotRegionsResevervedDays)
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

func (c *ScheduleConfig) parseDeprecatedFlag(meta *configMetaData, name string, old, new bool) (bool, error) {
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
		return errors.New("tolerant-size-ratio should be nonnegative")
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
	for _, scheduleConfig := range c.Schedulers {
		if !IsSchedulerRegistered(scheduleConfig.Type) {
			return errors.Errorf("create func of %v is not registered, maybe misspelled", scheduleConfig.Type)
		}
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
}

// IsDefaultScheduler checks whether the scheduler is enable by default.
func IsDefaultScheduler(typ string) bool {
	for _, c := range DefaultSchedulers {
		if typ == c.Type {
			return true
		}
	}
	return false
}

// ReplicationConfig is the replication configuration.
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

func (c *ReplicationConfig) adjust(meta *configMetaData) error {
	adjustUint64(&c.MaxReplicas, defaultMaxReplicas)
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

// PDServerConfig is the configuration for pd server.
type PDServerConfig struct {
	// UseRegionStorage enables the independent region storage.
	UseRegionStorage bool `toml:"use-region-storage" json:"use-region-storage,string"`
	// MaxResetTSGap is the max gap to reset the TSO.
	MaxResetTSGap typeutil.Duration `toml:"max-gap-reset-ts" json:"max-gap-reset-ts"`
	// KeyType is option to specify the type of keys.
	// There are some types supported: ["table", "raw", "txn"], default: "table"
	KeyType string `toml:"key-type" json:"key-type"`
	// RuntimeServices is the running the running extension services.
	RuntimeServices typeutil.StringSlice `toml:"runtime-services" json:"runtime-services"`
	// MetricStorage is the cluster metric storage.
	// Currently we use prometheus as metric storage, we may use PD/TiKV as metric storage later.
	MetricStorage string `toml:"metric-storage" json:"metric-storage"`
	// There are some values supported: "auto", "none", or a specific address, default: "auto"
	DashboardAddress string `toml:"dashboard-address" json:"dashboard-address"`
	// TraceRegionFlow the option to update flow information of regions.
	// WARN: TraceRegionFlow is deprecated.
	TraceRegionFlow bool `toml:"trace-region-flow" json:"trace-region-flow,string,omitempty"`
	// FlowRoundByDigit used to discretization processing flow information.
	FlowRoundByDigit int `toml:"flow-round-by-digit" json:"flow-round-by-digit"`
}

func (c *PDServerConfig) adjust(meta *configMetaData) error {
	adjustDuration(&c.MaxResetTSGap, defaultMaxResetTSGap)
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
	if !meta.IsDefined("trace-region-flow") {
		c.TraceRegionFlow = defaultTraceRegionFlow
	}
	if !meta.IsDefined("flow-round-by-digit") {
		adjustInt(&c.FlowRoundByDigit, defaultFlowRoundByDigit)
	}
	c.migrateConfigurationFromFile(meta)
	return c.Validate()
}

func (c *PDServerConfig) migrateConfigurationFromFile(meta *configMetaData) error {
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
	if c.FlowRoundByDigit < 0 {
		return errs.ErrConfigItem.GenWithStack("flow round by digit cannot be negative number")
	}

	return nil
}

// StoreLabel is the config item of LabelPropertyConfig.
type StoreLabel struct {
	Key   string `toml:"key" json:"key"`
	Value string `toml:"value" json:"value"`
}

// RejectLeader is the label property type that suggests a store should not
// have any region leaders.
const RejectLeader = "reject-leader"

// LabelPropertyConfig is the config section to set properties to store labels.
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

// ParseUrls parse a string into multiple urls.
// Export for api.
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errs.ErrURLParse.Wrap(err).GenWithStackByCause()
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

// SetupLogger setup the logger.
func (c *Config) SetupLogger() error {
	lg, p, err := log.InitLogger(&c.Log, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errs.ErrInitLogger.Wrap(err).FastGenWithCause()
	}
	c.logger = lg
	c.logProps = p
	logutil.SetRedactLog(c.Security.RedactInfoLog)
	return nil
}

// GetZapLogger gets the created zap logger.
func (c *Config) GetZapLogger() *zap.Logger {
	return c.logger
}

// GetZapLogProperties gets properties of the zap logger.
func (c *Config) GetZapLogProperties() *log.ZapProperties {
	return c.logProps
}

// GetConfigFile gets the config file.
func (c *Config) GetConfigFile() string {
	return c.configFile
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

	allowedCN, serr := c.Security.GetOneAllowedCN()
	if serr != nil {
		return nil, serr
	}
	cfg.ClientTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.ClientTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.ClientTLSInfo.CertFile = c.Security.CertPath
	cfg.ClientTLSInfo.KeyFile = c.Security.KeyPath
	// Client no need to set the CN. (cfg.ClientTLSInfo.AllowedCN = allowedCN)
	cfg.PeerTLSInfo.ClientCertAuth = len(c.Security.CAPath) != 0
	cfg.PeerTLSInfo.TrustedCAFile = c.Security.CAPath
	cfg.PeerTLSInfo.CertFile = c.Security.CertPath
	cfg.PeerTLSInfo.KeyFile = c.Security.KeyPath
	cfg.PeerTLSInfo.AllowedCN = allowedCN
	cfg.ForceNewCluster = c.ForceNewCluster
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(c.logger, c.logger.Core(), c.logProps.Syncer)
	cfg.EnableGRPCGateway = c.EnableGRPCGateway
	cfg.EnableV2 = true
	cfg.Logger = "zap"
	var err error

	cfg.LPUrls, err = ParseUrls(c.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = ParseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = ParseUrls(c.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = ParseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// DashboardConfig is the configuration for tidb-dashboard.
type DashboardConfig struct {
	TiDBCAPath         string `toml:"tidb-cacert-path" json:"tidb-cacert-path"`
	TiDBCertPath       string `toml:"tidb-cert-path" json:"tidb-cert-path"`
	TiDBKeyPath        string `toml:"tidb-key-path" json:"tidb-key-path"`
	PublicPathPrefix   string `toml:"public-path-prefix" json:"public-path-prefix"`
	InternalProxy      bool   `toml:"internal-proxy" json:"internal-proxy"`
	EnableTelemetry    bool   `toml:"enable-telemetry" json:"enable-telemetry"`
	EnableExperimental bool   `toml:"enable-experimental" json:"enable-experimental"`
	// WARN: DisableTelemetry is deprecated.
	DisableTelemetry bool `toml:"disable-telemetry" json:"disable-telemetry,omitempty"`
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

func (c *DashboardConfig) adjust(meta *configMetaData) {
	if !meta.IsDefined("enable-telemetry") {
		c.EnableTelemetry = defaultEnableTelemetry
	}
	c.EnableTelemetry = c.EnableTelemetry && !c.DisableTelemetry
}

// ReplicationModeConfig is the configuration for the replication policy.
type ReplicationModeConfig struct {
	ReplicationMode string                      `toml:"replication-mode" json:"replication-mode"` // can be 'dr-auto-sync' or 'majority', default value is 'majority'
	DRAutoSync      DRAutoSyncReplicationConfig `toml:"dr-auto-sync" json:"dr-auto-sync"`         // used when ReplicationMode is 'dr-auto-sync'
}

// Clone returns a copy of replication mode config.
func (c *ReplicationModeConfig) Clone() *ReplicationModeConfig {
	cfg := *c
	return &cfg
}

func (c *ReplicationModeConfig) adjust(meta *configMetaData) {
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
	LabelKey         string            `toml:"label-key" json:"label-key"`
	Primary          string            `toml:"primary" json:"primary"`
	DR               string            `toml:"dr" json:"dr"`
	PrimaryReplicas  int               `toml:"primary-replicas" json:"primary-replicas"`
	DRReplicas       int               `toml:"dr-replicas" json:"dr-replicas"`
	WaitStoreTimeout typeutil.Duration `toml:"wait-store-timeout" json:"wait-store-timeout"`
	WaitSyncTimeout  typeutil.Duration `toml:"wait-sync-timeout" json:"wait-sync-timeout"`
	WaitAsyncTimeout typeutil.Duration `toml:"wait-async-timeout" json:"wait-async-timeout"`
}

func (c *DRAutoSyncReplicationConfig) adjust(meta *configMetaData) {
	if !meta.IsDefined("wait-store-timeout") {
		c.WaitStoreTimeout = typeutil.NewDuration(defaultDRWaitStoreTimeout)
	}
	if !meta.IsDefined("wait-sync-timeout") {
		c.WaitSyncTimeout = typeutil.NewDuration(defaultDRWaitSyncTimeout)
	}
	if !meta.IsDefined("wait-async-timeout") {
		c.WaitAsyncTimeout = typeutil.NewDuration(defaultDRWaitAsyncTimeout)
	}
}

// SecurityConfig indicates the security configuration for pd server
type SecurityConfig struct {
	grpcutil.TLSConfig
	// RedactInfoLog indicates that whether enabling redact log
	RedactInfoLog bool              `toml:"redact-info-log" json:"redact-info-log"`
	Encryption    encryption.Config `toml:"encryption" json:"encryption"`
}
