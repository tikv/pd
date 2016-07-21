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
	// Server listening address.
	Addr string `toml:"addr" json:"addr"`

	// Server advertise listening address for outer client communication.
	// If not set, using default Addr instead.
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	// HTTP server listening address.
	HTTPAddr string `toml:"http-addr" json:"http-addr"`

	// Pprof listening address.
	PprofAddr string `toml:"pprof-addr" json:"pprof-addr"`

	// RootPath in Etcd as the prefix for all keys. If not set, use default "pd".
	// Deprecated and will be removed later.
	RootPath string `toml:"root" json:"root"`

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

	// Remote metric address for StatsD.
	MetricAddr string `toml:"metric-addr" json:"metric-addr"`

	BalanceCfg *BalanceConfig `toml:"balance" json:"balance"`

	// Only test can change it.
	nextRetryDelay time.Duration

	EtcdCfg *EtcdConfig `toml:"etcd" json:"etcd"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{
		BalanceCfg: newBalanceConfig(),
		EtcdCfg:    newEtcdConfig(),
	}
}

const (
	defaultRootPath        = "/pd"
	defaultLeaderLease     = int64(3)
	defaultTsoSaveInterval = int64(2000)
	defaultMaxPeerCount    = uint64(3)
	defaultNextRetryDelay  = time.Second
)

func (c *Config) adjust() {
	if len(c.RootPath) == 0 {
		c.RootPath = defaultRootPath
	}
	// TODO: Maybe we should do more check for root path, only allow letters?

	if c.LeaderLease <= 0 {
		c.LeaderLease = defaultLeaderLease
	}

	if c.TsoSaveInterval <= 0 {
		c.TsoSaveInterval = defaultTsoSaveInterval
	}

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}

	if c.MaxPeerCount == 0 {
		c.MaxPeerCount = defaultMaxPeerCount
	}

	if c.BalanceCfg == nil {
		c.BalanceCfg = &BalanceConfig{}
	}

	c.BalanceCfg.adjust()
}

func (c *Config) clone() *Config {
	cfg := &Config{}
	*cfg = *c
	cfg.BalanceCfg = cfg.BalanceCfg.clone()
	cfg.EtcdCfg = cfg.EtcdCfg.clone()
	return cfg
}

func (c *Config) setCfg(cfg *Config) {
	// TODO: add more check for cfg set.
	cfg.adjust()

	bc := c.BalanceCfg
	ec := c.EtcdCfg
	*c = *cfg
	c.BalanceCfg = bc
	*c.BalanceCfg = *cfg.BalanceCfg

	c.EtcdCfg = ec
	*c.EtcdCfg = *cfg.EtcdCfg
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// LoadFromFile loads config from file.
func (c *Config) LoadFromFile(path string) error {
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

	// For leader count balance.
	// If the leader region count of one store is greater than this value,
	// it will be used as a from store to do leader balance.
	MaxLeaderCount uint64 `toml:"max-leader-count" json:"max-leader-count"`

	// For snapshot balance filter.
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

	// LeaderScoreWeight is the leader score weight to calculate the store score.
	LeaderScoreWeight float64 `toml:"leader-score-weight" json:"leader-score-weight"`

	// CapacityScoreWeight is the capacity score weight to calculate the store score.
	CapacityScoreWeight float64 `toml:"capacity-score-weight" json:"capacity-score-weight"`
}

func newBalanceConfig() *BalanceConfig {
	return &BalanceConfig{}
}

const (
	defaultMinCapacityUsedRatio   = float64(0.4)
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
	defaultLeaderScoreWeight      = float64(0.4)
	defaultCapacityScoreWeight    = float64(0.6)
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

	if c.LeaderScoreWeight == 0 {
		c.LeaderScoreWeight = defaultLeaderScoreWeight
	}

	if c.CapacityScoreWeight == 0 {
		c.CapacityScoreWeight = defaultCapacityScoreWeight
	}
}

func (c *BalanceConfig) clone() *BalanceConfig {
	if c == nil {
		return nil
	}

	bc := &BalanceConfig{}
	*bc = *c
	return bc
}

func (c *BalanceConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("BalanceConfig(%+v)", *c)
}

// EtcdConfig is for etcd configuration.
type EtcdConfig struct {
	Name                string `toml:"name" json:"name"`
	DataDir             string `toml:"data-dir" json:"data-dir"`
	WalDir              string `toml:"wal-dir" json:"wal-dir"`
	ListenPeerURL       string `toml:"listen-peer-url" json:"listen-peer-url"`
	ListenClientURL     string `toml:"listen-client-url" json:"listen-client-url"`
	AdvertisePeerURL    string `toml:"advertise-peer-url" json:"advertise-peer-url"`
	AdvertiseClientURL  string `toml:"advertise-client-url" json:"advertise-client-url"`
	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
}

const (
	defaultEtcdName               = "default"
	defaultEtcdDataDir            = "default.pd"
	defaultEtcdWalDir             = ""
	defaultEtcdListenPeerURL      = "http://localhost:2380"
	defaultEtcdListenClientURL    = "http://localhost:2379"
	defaultEtcdAdvertisePeerURL   = "http://localhost:2380"
	defaultEtcdAdvertiseClientURL = "http://localhost:2379"
	defaultEtcdInitialCluster     = "default=http://localhost:2380"
)

func newEtcdConfig() *EtcdConfig {
	return &EtcdConfig{
		Name:                defaultEtcdName,
		DataDir:             defaultEtcdDataDir,
		WalDir:              defaultEtcdWalDir,
		ListenPeerURL:       defaultEtcdListenPeerURL,
		ListenClientURL:     defaultEtcdListenClientURL,
		AdvertisePeerURL:    defaultEtcdAdvertisePeerURL,
		AdvertiseClientURL:  defaultEtcdAdvertiseClientURL,
		InitialCluster:      defaultEtcdInitialCluster,
		InitialClusterState: embed.ClusterStateFlagNew,
	}
}

func (c *EtcdConfig) clone() *EtcdConfig {
	if c == nil {
		return nil
	}

	ec := &EtcdConfig{}
	*ec = *c
	return ec
}

// String implements fmt.Stringer interface.
func (c *EtcdConfig) String() string {
	if c == nil {
		return "<nil>"
	}

	return fmt.Sprintf("EtcdConfig(%+v)", *c)
}

// generates a configuration for embedded etcd.
func (c *Config) genEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.EtcdCfg.Name
	cfg.Dir = c.EtcdCfg.DataDir
	cfg.WalDir = c.EtcdCfg.WalDir
	cfg.InitialCluster = c.EtcdCfg.InitialCluster
	// Use unique cluster id as the etcd cluster token too.
	cfg.InitialClusterToken = fmt.Sprintf("pd-%d", c.ClusterID)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.EnablePprof = true

	u, err := url.Parse(c.EtcdCfg.ListenPeerURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LPUrls = []url.URL{*u}

	u, err = url.Parse(c.EtcdCfg.ListenClientURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls = []url.URL{*u}

	u, err = url.Parse(c.EtcdCfg.AdvertisePeerURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls = []url.URL{*u}

	u, err = url.Parse(c.EtcdCfg.AdvertiseClientURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls = []url.URL{*u}

	return cfg, nil
}

// Generate a unique port for etcd usage. This is only used for test.
// Because initializing etcd must assign certain address.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

// NewTestSingleEtcdConfig is only for test to create one etcd configuration.
// Because pd-client also needs this, so export here.
func NewTestSingleEtcdConfig() *EtcdConfig {
	cfg := newEtcdConfig()
	cfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	cfg.Name = "default"

	clientPort := freePort()
	peerPort := freePort()

	cfg.ListenClientURL = fmt.Sprintf("http://localhost:%d", clientPort)
	cfg.ListenPeerURL = fmt.Sprintf("http://localhost:%d", peerPort)

	cfg.AdvertiseClientURL = cfg.ListenClientURL
	cfg.AdvertisePeerURL = cfg.ListenPeerURL
	cfg.InitialCluster = fmt.Sprintf("default=http://localhost:%d", peerPort)

	return cfg
}

// NewTestMultiEtcdConfig is only for test to create multiple etcd configurations.
// Because pd-client also needs this, so export here.
func NewTestMultiEtcdConfig(count int) []*EtcdConfig {
	cfgs := make([]*EtcdConfig, count)

	clusters := []string{}
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleEtcdConfig()
		cfg.Name = fmt.Sprintf("etcd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Name, cfg.ListenPeerURL))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.InitialCluster = initialCluster
	}

	return cfgs
}
