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

	Host       string `toml:"host" json:"host"`
	Port       uint64 `toml:"port" json:"port"`
	ClientPort uint64 `toml:"client-port" json:"client-port"`
	PeerPort   uint64 `toml:"peer-port" json:"peer-port"`
	HTTPPort   uint64 `toml:"http-port" json:"http-port"`

	AdvertisePort       uint64 `toml:"advertise-port" json:"advertise-port"`
	AdvertiseClientPort uint64 `toml:"advertise-client-port" json:"advertise-client-port"`
	AdvertisePeerPort   uint64 `toml:"advertise-peer-port" json:"advertise-peer-port"`

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

	// TsoSaveInterval is the interval time (ms) to save timestamp.
	// When the leader begins to run, it first loads the saved timestamp from etcd, e.g, T1,
	// and the leader must guarantee that the next timestamp must be > T1 + 2 * TsoSaveInterval.
	TsoSaveInterval int64 `toml:"tso-save-interval" json:"tso-save-interval"`

	// ClusterID is the cluster ID communicating with other services.
	ClusterID uint64 `toml:"cluster-id" json:"cluster-id"`

	// MaxPeerCount for a region. default is 3.
	MaxPeerCount uint64 `toml:"max-peer-count" json:"max-peer-count"`

	BalanceCfg BalanceConfig `toml:"balance" json:"balance"`

	// Server advertise listening address for outer client communication.
	// Host:Port
	AdvertiseAddr string `json:"-"`

	// Only test can change it.
	nextRetryDelay time.Duration

	configFile string
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.Uint64Var(&cfg.ClusterID, "cluster-id", 0, "initial cluster ID for the pd cluster")
	fs.StringVar(&cfg.Name, "name", defaultName, "human-readable name for this pd member")
	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "path to the data directory (default 'default.{name}')")
	fs.StringVar(&cfg.Host, "host", defaultHost, "host for outer traffic")
	fs.Uint64Var(&cfg.Port, "port", defaultPort, "server port (deprecate later)")
	fs.Uint64Var(&cfg.AdvertisePort, "advertise-port", 0, "advertise server port (deprecate later) (default '{port}')")
	fs.Uint64Var(&cfg.HTTPPort, "http-port", defaultHTTPPort, "http port (deprecate later)")
	fs.Uint64Var(&cfg.ClientPort, "client-port", defaultClientPort, "port for client traffic")
	fs.Uint64Var(&cfg.AdvertiseClientPort, "advertise-client-port", 0, "advertise port for client traffic (default '{client-port}')")
	fs.Uint64Var(&cfg.PeerPort, "peer-port", defaultPeerPort, "port for peer traffic")
	fs.Uint64Var(&cfg.AdvertisePeerPort, "advertise-peer-port", 0, "advertise port for peer traffic (default '{peer-port}')")
	fs.StringVar(&cfg.Join, "join", "", "join to an existing cluster (usage: http://host:peer-port)")

	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")

	return cfg
}

// PdRootPath for all pd servers.
const PdRootPath = "/pd"

const (
	defaultLeaderLease     = int64(3)
	defaultTsoSaveInterval = int64(2000)
	defaultMaxPeerCount    = uint64(3)
	defaultNextRetryDelay  = time.Second

	defaultName                = "pd"
	defaultDataDir             = "default.pd"
	defaultHost                = "127.0.0.1"
	defaultPort                = uint64(1234)
	defaultClientPort          = uint64(2379)
	defaultPeerPort            = uint64(2380)
	defaultHTTPPort            = uint64(9090)
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
	flagArgs := make(map[string]*flag.Flag)
	flag.Visit(func(flag *flag.Flag) {
		flagArgs[flag.Name] = flag
	})

	var err error
	if confFlag, ok := flagArgs["config"]; ok {
		c.configFile = confFlag.Value.String()
		if c.configFile != "" {
			err = c.configFromFile(c.configFile)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

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
	adjustString(&c.Host, defaultHost)
	adjustString(&c.Name, defaultName)
	adjustUint64(&c.Port, defaultPort)
	adjustUint64(&c.ClientPort, defaultClientPort)
	adjustUint64(&c.PeerPort, defaultPeerPort)
	adjustUint64(&c.HTTPPort, defaultHTTPPort)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustUint64(&c.AdvertisePort, c.Port)
	adjustUint64(&c.AdvertiseClientPort, c.ClientPort)
	adjustUint64(&c.AdvertisePeerPort, c.PeerPort)

	c.AdvertiseAddr = fmt.Sprintf("%s:%d", c.Host, c.AdvertisePort)

	adjustString(&c.InitialCluster, fmt.Sprintf("%s=http://%s:%d", c.Name, c.Host, c.AdvertisePeerPort))
	adjustString(&c.InitialClusterState, embed.ClusterStateFlagNew)

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

	(&c.BalanceCfg).adjust()
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

	// MaxStoreDownDuration is the max duration at which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownDuration uint64 `toml:"max-store-down-duration" json:"max-store-down-duration"`
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
	defaultMaxStoreDownDuration   = uint64(60)
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

	// TODO: check SSL configuration later if possible
	scheme := "http"

	u, err := url.Parse(fmt.Sprintf("%s://0.0.0.0:%d", scheme, c.PeerPort))
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LPUrls = []url.URL{*u}

	u, err = url.Parse(fmt.Sprintf("%s://0.0.0.0:%d", scheme, c.ClientPort))
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls = []url.URL{*u}

	u, err = url.Parse(fmt.Sprintf("%s://%s:%d", scheme, c.Host, c.AdvertisePeerPort))
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls = []url.URL{*u}

	u, err = url.Parse(fmt.Sprintf("%s://%s:%d", scheme, c.Host, c.AdvertiseClientPort))
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls = []url.URL{*u}

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
		Host:       "127.0.0.1",
		Port:       freePort(),
		ClientPort: freePort(),
		PeerPort:   freePort(),

		InitialClusterState: "new",

		LeaderLease:     1,
		TsoSaveInterval: 500,
	}

	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=http://127.0.0.1:%d", cfg.PeerPort)

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

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, fmt.Sprintf("http://127.0.0.1:%d", cfg.PeerPort)))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}
