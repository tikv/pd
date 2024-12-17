// Copyright 2018 TiKV Project Authors.
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

package tests

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"

	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/server/config"
)

type serverConfig struct {
	Name                string
	DataDir             string
	ClientURLs          string
	AdvertiseClientURLs string
	PeerURLs            string
	AdvertisePeerURLs   string
	ClusterConfig       *clusterConfig
	Join                bool
}

func newServerConfig(name string, cc *clusterConfig, join bool) *serverConfig {
	tempDir, _ := os.MkdirTemp("", "pd_tests")
	return &serverConfig{
		Name:          name,
		DataDir:       tempDir,
		ClientURLs:    tempurl.Alloc(),
		PeerURLs:      tempurl.Alloc(),
		ClusterConfig: cc,
		Join:          join,
	}
}

// Generate generates a config for the server.
func (c *serverConfig) Generate(opts ...ConfigOption) (*config.Config, error) {
	arguments := []string{
		"--name=" + c.Name,
		"--data-dir=" + c.DataDir,
		"--client-urls=" + c.ClientURLs,
		"--advertise-client-urls=" + c.AdvertiseClientURLs,
		"--peer-urls=" + c.PeerURLs,
		"--advertise-peer-urls=" + c.AdvertisePeerURLs,
	}
	if c.Join {
		arguments = append(arguments, "--join="+c.ClusterConfig.getJoinAddr())
	} else {
		arguments = append(arguments, "--initial-cluster="+c.ClusterConfig.getServerAddrs())
	}

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flagSet.BoolP("version", "V", false, "print version information and exit")
	flagSet.StringP("config", "", "", "config file")
	flagSet.BoolP("config-check", "", false, "check config file validity and exit")
	flagSet.StringP("name", "", "", "human-readable name for this pd member")
	flagSet.StringP("data-dir", "", "", "path to the data directory (default 'default.${name}')")
	flagSet.StringP("client-urls", "", "http://127.0.0.1:2379", "url for client traffic")
	flagSet.StringP("advertise-client-urls", "", "", "advertise url for client traffic (default '${client-urls}')")
	flagSet.StringP("peer-urls", "", "http://127.0.0.1:2379", "url for peer traffic")
	flagSet.StringP("advertise-peer-urls", "", "", "advertise url for peer traffic (default '${peer-urls}')")
	flagSet.StringP("initial-cluster", "", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	flagSet.StringP("join", "", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")
	flagSet.StringP("metrics-addr", "", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	flagSet.StringP("log-level", "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	flagSet.StringP("log-file", "", "", "log file path")
	flagSet.StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	flagSet.StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	flagSet.StringP("key", "", "", "path of file that contains X509 key in PEM format")
	flagSet.BoolP("force-new-cluster", "", false, "force to create a new one-member cluster")
	err := flagSet.Parse(arguments)
	if err != nil {
		return nil, err
	}
	cfg := config.NewConfig()
	err = cfg.Parse(flagSet)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(cfg, c.Name)
	}
	return cfg, nil
}

type clusterConfig struct {
	InitialServers []*serverConfig
	JoinServers    []*serverConfig
}

func newClusterConfig(n int) *clusterConfig {
	var cc clusterConfig
	for range n {
		c := newServerConfig(cc.nextServerName(), &cc, false)
		cc.InitialServers = append(cc.InitialServers, c)
	}
	return &cc
}

func (c *clusterConfig) join() *serverConfig {
	sc := newServerConfig(c.nextServerName(), c, true)
	c.JoinServers = append(c.JoinServers, sc)
	return sc
}

func (c *clusterConfig) nextServerName() string {
	return fmt.Sprintf("pd%d", len(c.InitialServers)+len(c.JoinServers)+1)
}

func (c *clusterConfig) getServerAddrs() string {
	addrs := make([]string, 0, len(c.InitialServers))
	for _, s := range c.InitialServers {
		addrs = append(addrs, fmt.Sprintf("%s=%s", s.Name, s.PeerURLs))
	}
	return strings.Join(addrs, ",")
}

func (c *clusterConfig) getJoinAddr() string {
	return c.InitialServers[0].PeerURLs
}

// GetClientURL returns the client URL of the cluster.
func (c *clusterConfig) GetClientURL() string {
	return c.InitialServers[0].ClientURLs
}

// GetClientURLs returns all client URLs of the cluster.
func (c *clusterConfig) GetClientURLs() []string {
	urls := make([]string, 0, len(c.InitialServers))
	for _, svr := range c.InitialServers {
		urls = append(urls, svr.ClientURLs)
	}
	return urls
}
