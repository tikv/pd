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
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// Config is the pd server configuration.
type Config struct {
	// Server listening address.
	Addr string `toml:"addr"`

	// Server advertise listening address for outer client communication.
	// If not set, using default Addr instead.
	AdvertiseAddr string `toml:"advertise-addr"`

	// Etcd endpoints
	EtcdAddrs []string `toml:"etcd-addrs"`

	// HTTP server listening address.
	HTTPAddr string `toml:"http-addr"`

	// Pprof listening address.
	PprofAddr string `toml:"pprof-addr"`

	// RootPath in Etcd as the prefix for all keys. If not set, use default "pd".
	RootPath string `toml:"root"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease"`

	// Log level.
	LogLevel string `toml:"log-level"`

	// TsoSaveInterval is the interval time (ms) to save timestamp.
	// When the leader begins to run, it first loads the saved timestamp from etcd, e.g, T1,
	// and the leader must guarantee that the next timestamp must be > T1 + 2 * TsoSaveInterval.
	TsoSaveInterval int64 `toml:"tso-save-interval"`

	// ClusterID is the cluster ID communicating with other services.
	ClusterID uint64 `toml:"cluster-id"`

	// MaxPeerCount for a region. default is 3.
	MaxPeerCount uint64 `toml:"max-peer-count"`

	// Remote metric address for StatsD.
	MetricAddr string `toml:"metric-addr"`

	// Metric prefix.
	MetricPrefix string `toml:"metric-prefix"`

	BCfg *BalanceConfig `toml:"balance"`

	// Only test can change it.
	nextRetryDelay time.Duration

	args map[string]bool
}

func (c *Config) VisitArgs(flag *flag.Flag) {
	if c.args == nil {
		c.args = make(map[string]bool)
	}

	c.args[flag.Name] = true
}

func (c *Config) adjust() {
	if len(c.RootPath) == 0 {
		c.RootPath = "pd"
	}
	// TODO: Maybe we should do more check for root path, only allow letters?

	if c.LeaderLease <= 0 {
		c.LeaderLease = 3
	}

	if c.TsoSaveInterval <= 0 {
		c.TsoSaveInterval = 2000
	}

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = time.Second
	}

	if c.MaxPeerCount == 0 {
		c.MaxPeerCount = 3
	}

	if len(c.MetricPrefix) == 0 {
		c.MetricPrefix = "pd"
	}

	c.BCfg.adjust()
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// SetStringFlagConfig sets string flag value to config.
func (c *Config) SetStringFlagConfig(dest *string, name string, value string) {
	if c.args[name] {
		*dest = value
	}
}

// SetStringSliceFlagConfig sets string slice value to config.
func (c *Config) SetStringSliceFlagConfig(dest *[]string, name string, value string) {
	if c.args[name] {
		*dest = append([]string{}, strings.Split(value, ",")...)
	}
}

// SetIntFlagConfig sets int value to config.
func (c *Config) SetIntFlagConfig(dest *int64, name string, value int64) {
	if c.args[name] {
		*dest = value
	}
}

// SetUintFlagConfig sets uint value to config.
func (c *Config) SetUintFlagConfig(dest *uint64, name string, value uint64) {
	if c.args[name] {
		*dest = value
	}
}

// SetFloatFlagConfig sets float value to config.
func (c *Config) SetFloatFlagConfig(dest *float64, name string, value float64) {
	if c.args[name] {
		*dest = value
	}
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
	MinCapacityUsedRatio float64 `toml:"min-capacity-used-ratio"`
	// If the used ratio of one store is greater than this value,
	// it will never be used as a to store.
	MaxCapacityUsedRatio float64 `toml:"max-capacity-used-ratio"`

	// For snapshot balance filter.
	// If the sending snapshot count of one storage is greater than this value,
	// it will never be used as a from store.
	MaxSnapSendingCount uint64 `toml:"max-snap-sending-count"`
	// If the receiving snapshot count of one storage is greater than this value,
	// it will never be used as a to store.
	MaxSnapReceivingCount uint64 `toml:"max-snap-receiving-count"`

	// If the new store and old store's diff scores are not beyond this value,
	// the balancer will do nothing.
	MaxDiffScoreFraction float64 `toml:"max-diff-score-fraction"`

	// Balance loop interval time (seconds).
	BalanceInterval uint64 `toml:"balance-interval"`

	// MaxBalanceCount is the max region count to balance at the same time.
	MaxBalanceCount uint64 `toml:"max-balance-count"`

	// MaxBalanceRetryPerLoop is the max retry count to balance in a balance schedule.
	MaxBalanceRetryPerLoop uint64 `toml:"max-balance-retry-per-loop"`

	// MaxBalanceCountPerLoop is the max region count to balance in a balance schedule.
	MaxBalanceCountPerLoop uint64 `toml:"max-balance-count-per-loop"`
}

func (c *BalanceConfig) adjust() {
	if c.MinCapacityUsedRatio == 0 {
		c.MinCapacityUsedRatio = 0.4
	}

	if c.MaxCapacityUsedRatio == 0 {
		c.MaxCapacityUsedRatio = 0.9
	}

	if c.MaxSnapSendingCount == 0 {
		c.MaxSnapSendingCount = 3
	}

	if c.MaxSnapReceivingCount == 0 {
		c.MaxSnapReceivingCount = 3
	}

	if c.MaxDiffScoreFraction == 0 {
		c.MaxDiffScoreFraction = 0.1
	}

	if c.BalanceInterval == 0 {
		c.BalanceInterval = 30
	}

	if c.MaxBalanceCount == 0 {
		c.MaxBalanceCount = 16
	}

	if c.MaxBalanceRetryPerLoop == 0 {
		c.MaxBalanceRetryPerLoop = 10
	}

	if c.MaxBalanceCountPerLoop == 0 {
		c.MaxBalanceCountPerLoop = 3
	}
}

func (c *BalanceConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("BalanceConfig(%+v)", *c)
}
