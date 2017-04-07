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

package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/metricutil"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/pkg/typeutil"
)

// Config is the pd server configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	Version bool `json:"-"`

	Server ServerConfig `toml:"server" json:"server"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	Schedule ScheduleConfig `toml:"schedule" json:"schedule"`

	Replication ReplicationConfig `toml:"replication" json:"replication"`

	configFile string
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("pd", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.Version, "V", false, "print version information and exit")
	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")
	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.StringVar(&cfg.Server.Name, "name", defaultName, "human-readable name for this pd member")

	fs.StringVar(&cfg.Server.DataDir, "data-dir", "", "path to the data directory (default 'default.${name}')")
	fs.StringVar(&cfg.Server.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.Server.AdvertiseClientUrls, "advertise-client-urls", "", "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.Server.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.Server.AdvertisePeerUrls, "advertise-peer-urls", "", "advertise url for peer traffic (default '${peer-urls}')")
	fs.StringVar(&cfg.Server.InitialCluster, "initial-cluster", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	fs.StringVar(&cfg.Server.Join, "join", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")

	fs.StringVar(&cfg.Server.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.Server.LogFile, "log-file", "", "log file path")

	return cfg
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

	err = c.adjust()
	return errors.Trace(err)
}

func (c *Config) validate() error {
	c.Server.validate()
	return nil
}

// Adjust fills default variable into Config
func (c *Config) Adjust() error {
	return c.adjust()
}

func (c *Config) adjust() error {
	if err := c.validate(); err != nil {
		return errors.Trace(err)
	}

	c.Server.adjust()

	adjustString(&c.Metric.PushJob, c.Server.Name)

	c.Schedule.adjust()
	c.Replication.adjust()
	return nil
}

// Clone is used to copy Config
func (c *Config) Clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
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

// GenEmbedEtcdConfig generates a configuration for embedded etcd.
func (c *Config) GenEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Server.Name
	cfg.Dir = c.Server.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.Server.InitialCluster
	cfg.ClusterState = c.Server.InitialClusterState
	cfg.EnablePprof = true
	cfg.StrictReconfigCheck = !c.Server.DisableStrictReconfigCheck
	cfg.TickMs = uint(c.Server.tickMs)
	cfg.ElectionMs = uint(c.Server.electionMs)
	cfg.AutoCompactionRetention = c.Server.AutoCompactionRetention
	cfg.QuotaBackendBytes = int64(c.Server.QuotaBackendBytes)

	var err error

	cfg.LPUrls, err = ParseUrls(c.Server.PeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls, err = ParseUrls(c.Server.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls, err = ParseUrls(c.Server.ClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls, err = ParseUrls(c.Server.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}

// NewTestSingleConfig is only for test to create one pd.
// Because pd-client also needs this, so export here.
func NewTestSingleConfig() *Config {
	svrCfg := ServerConfig{
		Name:       "pd",
		ClientUrls: testutil.UnixURL(),
		PeerUrls:   testutil.UnixURL(),

		InitialClusterState: embed.ClusterStateFlagNew,

		LeaderLease:     1,
		TsoSaveInterval: typeutil.NewDuration(200 * time.Millisecond),
	}
	svrCfg.AdvertiseClientUrls = svrCfg.ClientUrls
	svrCfg.AdvertisePeerUrls = svrCfg.PeerUrls
	svrCfg.DataDir, _ = ioutil.TempDir("/tmp", "test_pd")
	svrCfg.InitialCluster = fmt.Sprintf("pd=%s", svrCfg.PeerUrls)
	svrCfg.DisableStrictReconfigCheck = true
	svrCfg.tickMs = 100
	svrCfg.electionMs = 1000

	svrCfg.adjust()

	cfg := &Config{Server: svrCfg}
	return cfg
}

// NewTestMultiConfig is only for test to create multiple pd configurations.
// Because pd-client also needs this, so export here.
func NewTestMultiConfig(count int) []*Config {
	cfgs := make([]*Config, count)

	clusters := []string{}
	for i := 1; i <= count; i++ {
		cfg := NewTestSingleConfig()
		cfg.Server.Name = fmt.Sprintf("pd%d", i)

		clusters = append(clusters, fmt.Sprintf("%s=%s", cfg.Server.Name, cfg.Server.PeerUrls))

		cfgs[i-1] = cfg
	}

	initialCluster := strings.Join(clusters, ",")
	for _, cfg := range cfgs {
		cfg.Server.InitialCluster = initialCluster
	}

	return cfgs
}
