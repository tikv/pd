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
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
)

// Config is the pd server configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	Name    string `toml:"name" json:"name"`
	DataDir string `toml:"data-dir" json:"data-dir"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log level.
	LogLevel string `toml:"log-level" json:"log-level"`

	// TsoSaveInterval is the interval time (ms) to save timestamp.
	// When the leader begins to run, it first loads the saved timestamp from etcd, e.g, T1,
	// and the leader must guarantee that the next timestamp must be > T1 + 2 * TsoSaveInterval.
	TsoSaveInterval int64 `toml:"tso-save-interval" json:"tso-save-interval"`

	// ClusterID is the cluster ID communicating with other services.
	ClusterID uint64 `toml:"cluster-id" json:"cluster-id"`

	// MaxPeerCount for a region. default is 3.
	MaxPeerCount uint64 `toml:"max-peer-count" json:"max-peer-count"`

	BalanceCfg BalanceConfig `toml:"balance" json:"balance"`

	// Only test can change it.
	nextRetryDelay time.Duration

	configFile string

	// deprecated later
	Addr          string `toml:"addr" json:"addr"`
	HTTPAddr      string `toml:"http-addr" json:"http-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.Uint64Var(&cfg.ClusterID, "cluster-id", 0, "initial cluster ID for the pd cluster")
	fs.StringVar(&cfg.Name, "name", defaultName, "human-readable name for this pd member")

	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.Addr, "addr", defaultAddr, "server listening address (deprecate later)")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", "advertise server address (deprecate later) (default '${addr}')")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", defaultHTTPAddr, "http address (deprecate later)")
	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	fs.StringVar(&cfg.InitialClusterState, "initial-cluster-state", defualtInitialClusterState, "initial cluster state ('new' or 'existing')")

	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")

	return cfg
}

const (
	defaultLeaderLease     = int64(3)
	defaultTsoSaveInterval = int64(2000)
	defaultMaxPeerCount    = uint64(3)
	defaultNextRetryDelay  = time.Second

	defaultName                = "pd"
	defaultDataDir             = "default.pd"
	defaultAddr                = "127.0.0.1:1234"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defaultHTTPAddr            = "0.0.0.0:9090"
	defualtInitialClusterState = "new"
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

	return nil
}

func (c *Config) adjust() {
	adjustString(&c.Name, defaultName)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	adjustString(&c.Addr, defaultAddr)
	adjustString(&c.AdvertiseAddr, c.Addr)
	adjustString(&c.HTTPAddr, defaultHTTPAddr)

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

	if c.LeaderLease <= 0 {
		c.LeaderLease = defaultLeaderLease
	}

	if c.TsoSaveInterval <= 0 {
		c.TsoSaveInterval = defaultTsoSaveInterval
	}

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}

	c.BalanceCfg.adjust()
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
	MaxPeerDownDuration time.Duration `toml:"max-peer-down-duration" json:"max-peer-down-duration"`

	// MaxStoreDownDuration is the max duration at which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownDuration time.Duration `toml:"max-store-down-duration" json:"max-store-down-duration"`
}

func newBalanceConfig() *BalanceConfig {
	return &BalanceConfig{}
}

const (
	defaultMinCapacityUsedRatio   = float64(0.3)
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
	defaultMaxStoreDownDuration   = 60 * time.Second
)

func (c *BalanceConfig) adjust() {
	if c.MinCapacityUsedRatio == 0 {
		c.MinCapacityUsedRatio = defaultMinCapacityUsedRatio
	}

	if c.MaxCapacityUsedRatio == 0 {
		c.MaxCapacityUsedRatio = defaultMaxCapacityUsedRatio
	}

	if c.MaxLeaderCount == 0 {
		c.MaxLeaderCount = defaultMaxLeaderCount
	}

	if c.MaxSendingSnapCount == 0 {
		c.MaxSendingSnapCount = defaultMaxSendingSnapCount
	}

	if c.MaxReceivingSnapCount == 0 {
		c.MaxReceivingSnapCount = defaultMaxReceivingSnapCount
	}

	if c.MaxDiffScoreFraction == 0 {
		c.MaxDiffScoreFraction = defaultMaxDiffScoreFraction
	}

	if c.BalanceInterval == 0 {
		c.BalanceInterval = defaultBalanceInterval
	}

	if c.MaxBalanceCount == 0 {
		c.MaxBalanceCount = defaultMaxBalanceCount
	}

	if c.MaxBalanceRetryPerLoop == 0 {
		c.MaxBalanceRetryPerLoop = defaultMaxBalanceRetryPerLoop
	}

	if c.MaxBalanceCountPerLoop == 0 {
		c.MaxBalanceCountPerLoop = defaultMaxBalanceCountPerLoop
	}

	if c.MaxTransferWaitCount == 0 {
		c.MaxTransferWaitCount = defaultMaxTransferWaitCount
	}

	if c.MaxPeerDownDuration == 0 {
		c.MaxPeerDownDuration = defaultMaxPeerDownDuration
	}

	if c.MaxStoreDownDuration == 0 {
		c.MaxStoreDownDuration = defaultMaxStoreDownDuration
	}
}

func (c *BalanceConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("BalanceConfig(%+v)", *c)
}

func parseUrls(s string) ([]url.URL, error) {
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
	cfg.ClusterState = defualtInitialClusterState
	cfg.EnablePprof = true

	var err error

	cfg.LPUrls, err = parseUrls(c.PeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls, err = parseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls, err = parseUrls(c.ClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls, err = parseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}

// Generate a unique port for etcd usage. This is only used for test.
// Because initializing etcd must assign certain address.
func freePort() uint64 {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	port := uint64(l.Addr().(*net.TCPAddr).Port)
	l.Close()

	// wait a little to avoid using binding address.
	time.Sleep(50 * time.Millisecond)

	return port
}

// NewTestSingleConfig is only for test to create one pd.
// Because pd-client also needs this, so export here.
func NewTestSingleConfig() *Config {
	cfg := &Config{
		// We use cluster 0 for all tests.
		ClusterID:  0,
		Name:       "pd",
		Addr:       fmt.Sprintf("127.0.0.1:%d", freePort()),
		ClientUrls: fmt.Sprintf("http://127.0.0.1:%d", freePort()),
		PeerUrls:   fmt.Sprintf("http://127.0.0.1:%d", freePort()),

		InitialClusterState: "new",

		LeaderLease:     1,
		TsoSaveInterval: 500,
	}

	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)

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
