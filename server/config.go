// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/timeutil"
)

// Config is the pd server configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	Version bool

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	Name    string `toml:"name" json:"name"`
	DataDir string `toml:"data-dir" json:"data-dir"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log level.
	LogLevel string `toml:"log-level" json:"log-level"`
	// Log file.
	LogFile string `toml:"log-file" json:"log-file"`

	// TsoSaveInterval is the interval to save timestamp.
	TsoSaveInterval timeutil.Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// ClusterID is the cluster ID communicating with other services.
	ClusterID uint64 `toml:"cluster-id" json:"cluster-id"`

	// MaxPeerCount for a region. default is 3.
	MaxPeerCount uint64 `toml:"max-peer-count" json:"max-peer-count"`

	BalanceCfg BalanceConfig `toml:"balance" json:"balance"`

	MetricCfg MetricConfig `toml:"metric" json:"metric"`

	// Only test can change them.
	nextRetryDelay             time.Duration
	disableStrictReconfigCheck bool

	configFile string
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.Version, "v", false, "print version information and exit")
	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.Uint64Var(&cfg.ClusterID, "cluster-id", 0, "initial cluster ID for the pd cluster [deprecated]")
	fs.StringVar(&cfg.Name, "name", defaultName, "human-readable name for this pd member")

	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	fs.StringVar(&cfg.Join, "join", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")

	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")

	return cfg
}

const (
	defaultLeaderLease    = int64(3)
	defaultMaxPeerCount   = uint64(3)
	defaultNextRetryDelay = time.Second

	defaultName                = "pd"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defualtInitialClusterState = embed.ClusterStateFlagNew
)

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

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *timeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	c.randomClusterID()
	return nil
}

func (c *Config) randomClusterID() {
	hasClusterID := false
	c.FlagSet.Visit(func(f *flag.Flag) {
		if f.Name == "cluster-id" {
			hasClusterID = true
		}
	})
	if hasClusterID {
		return
	}

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	c.ClusterID = (ts << 32) + uint64(rand.Uint32())
}

func (c *Config) validate() error {
	if c.Join != "" && c.InitialCluster != "" {
		return errors.New("-initial-cluster and -join can not be provided at the same time")
	}
	return nil
}

func (c *Config) adjust() error {
	if err := c.validate(); err != nil {
		return errors.Trace(err)
	}

	adjustString(&c.Name, defaultName)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	if c.Join != "" {
		initialCluster, state, err := prepareJoinCluster(c)
		if err != nil {
			return errors.Trace(err)
		}
		c.InitialCluster = initialCluster
		c.InitialClusterState = state
	}

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

	adjustString(&c.InitialClusterState, defualtInitialClusterState)

	adjustUint64(&c.MaxPeerCount, defaultMaxPeerCount)

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	adjustDuration(&c.TsoSaveInterval, time.Duration(defaultLeaderLease)*time.Second)

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}

	c.BalanceCfg.adjust()
	return nil
}

func (c *Config) clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

func (c *Config) setBalanceConfig(cfg BalanceConfig) {
	// TODO: add more check for cfg set.
	cfg.adjust()

	c.BalanceCfg = cfg
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// BalanceConfig is the balance configuration.
type BalanceConfig struct {
	// For capacity balance.
	// If the used ratio of one store is less than this value,
	// it will never be used as a from store.
	MinCapacityUsedRatio float64 `toml:"min-capacity-used-ratio" json:"min-capacity-used-ratio"`
	// If the used ratio of one store is greater than this value,
	// it will never be used as a to store.
	MaxCapacityUsedRatio float64 `toml:"max-capacity-used-ratio" json:"max-capacity-used-ratio"`

	// For leader balance.
	// If the leader region count of one store is less than this value,
	// it will never be used as a from store.
	MaxLeaderCount uint64 `toml:"max-leader-count" json:"max-leader-count"`

	// For capacity balance.
	// If the sending snapshot count of one storage is greater than this value,
	// it will never be used as a from store.
	MaxSendingSnapCount uint64 `toml:"max-sending-snap-count" json:"max-sending-snap-count"`
	// If the receiving snapshot count of one storage is greater than this value,
	// it will never be used as a to store.
	MaxReceivingSnapCount uint64 `toml:"max-receiving-snap-count" json:"max-receiving-snap-count"`

	// If the new store and old store's diff scores are not beyond this value,
	// the balancer will do nothing.
	MaxDiffScoreFraction float64 `toml:"max-diff-score-fraction" json:"max-diff-score-fraction"`

	// Balance loop interval time (seconds).
	BalanceInterval uint64 `toml:"balance-interval" json:"balance-interval"`

	// MaxBalanceCount is the max region count to balance at the same time.
	MaxBalanceCount uint64 `toml:"max-balance-count" json:"max-balance-count"`

	// MaxBalanceRetryPerLoop is the max retry count to balance in a balance schedule.
	MaxBalanceRetryPerLoop uint64 `toml:"max-balance-retry-per-loop" json:"max-balance-retry-per-loop"`

	// MaxBalanceCountPerLoop is the max region count to balance in a balance schedule.
	MaxBalanceCountPerLoop uint64 `toml:"max-balance-count-per-loop" json:"max-balance-count-per-loop"`

	// MaxTransferWaitCount is the max heartbeat count to wait leader transfer to finish.
	MaxTransferWaitCount uint64 `toml:"max-transfer-wait-count" json:"max-transfer-wait-count"`

	// MaxPeerDownDuration is the max duration at which
	// a peer will be considered to be down if its leader reports it.
	MaxPeerDownDuration timeutil.Duration `toml:"max-peer-down-duration" json:"max-peer-down-duration"`

	// MaxStoreDownDuration is the max duration at which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownDuration timeutil.Duration `toml:"max-store-down-duration" json:"max-store-down-duration"`
}

func newBalanceConfig() *BalanceConfig {
	return &BalanceConfig{}
}

const (
	defaultMinCapacityUsedRatio   = float64(0.1)
	defaultMaxCapacityUsedRatio   = float64(0.9)
	defaultMaxLeaderCount         = uint64(10)
	defaultMaxSendingSnapCount    = uint64(3)
	defaultMaxReceivingSnapCount  = uint64(3)
	defaultMaxDiffScoreFraction   = float64(0.1)
	defaultMaxBalanceCount        = uint64(16)
	defaultBalanceInterval        = uint64(30)
	defaultMaxBalanceRetryPerLoop = uint64(10)
	defaultMaxBalanceCountPerLoop = uint64(3)
	defaultMaxTransferWaitCount   = uint64(3)
	defaultMaxPeerDownDuration    = 30 * time.Minute
	defaultMaxStoreDownDuration   = 10 * time.Minute
)

func (c *BalanceConfig) adjust() {
	adjustFloat64(&c.MinCapacityUsedRatio, defaultMinCapacityUsedRatio)
	adjustFloat64(&c.MaxCapacityUsedRatio, defaultMaxCapacityUsedRatio)

	adjustUint64(&c.MaxLeaderCount, defaultMaxLeaderCount)
	adjustUint64(&c.MaxSendingSnapCount, defaultMaxSendingSnapCount)
	adjustUint64(&c.MaxReceivingSnapCount, defaultMaxReceivingSnapCount)

	adjustFloat64(&c.MaxDiffScoreFraction, defaultMaxDiffScoreFraction)

	adjustUint64(&c.BalanceInterval, defaultBalanceInterval)
	adjustUint64(&c.MaxBalanceCount, defaultMaxBalanceCount)
	adjustUint64(&c.MaxBalanceRetryPerLoop, defaultMaxBalanceRetryPerLoop)
	adjustUint64(&c.MaxBalanceCountPerLoop, defaultMaxBalanceCountPerLoop)

	adjustUint64(&c.MaxTransferWaitCount, defaultMaxTransferWaitCount)

	adjustDuration(&c.MaxPeerDownDuration, defaultMaxPeerDownDuration)
	adjustDuration(&c.MaxStoreDownDuration, defaultMaxStoreDownDuration)
}

func (c *BalanceConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("BalanceConfig(%+v)", *c)
}

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval timeutil.Duration `toml:"interval" json:"interval"`
}

// ParseUrls parse a string into multiple urls.
// Export for api.
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Trace(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

// generates a configuration for embedded etcd.
func (c *Config) genEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	// Use unique cluster id as the etcd cluster token too.
	cfg.InitialClusterToken = fmt.Sprintf("pd-%d", c.ClusterID)
	cfg.ClusterState = c.InitialClusterState
	cfg.EnablePprof = true
	cfg.StrictReconfigCheck = !c.disableStrictReconfigCheck

	var err error

	cfg.LPUrls, err = ParseUrls(c.PeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls, err = ParseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls, err = ParseUrls(c.ClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls, err = ParseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}

var unixURLCount uint64

// unixURL returns a unique unix socket url, used for test only.
func unixURL() string {
	return fmt.Sprintf("unix://localhost:%d.%d.sock", os.Getpid(), atomic.AddUint64(&unixURLCount, 1))
}

// NewTestSingleConfig is only for test to create one pd.
// Because pd-client also needs this, so export here.
func NewTestSingleConfig() *Config {
	cfg := &Config{
		// We use cluster 0 for all tests.
		ClusterID:  0,
		Name:       "pd",
		ClientUrls: unixURL(),
		PeerUrls:   unixURL(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     1,
		TsoSaveInterval: timeutil.NewDuration(200 * time.Millisecond),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.disableStrictReconfigCheck = true

	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because pd-client also needs this, so export here.
func NewTestMultiConfig(count int) []*Config {
	cfgs := make([]*Config, count)

	clusters := []string{}
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleConfig()
		cfg.Name = fmt.Sprintf("pd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}
