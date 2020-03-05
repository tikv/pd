// Copyright 2018 PingCAP, Inc.
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

package client_test

import (
	"context"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/configpb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/pd/v4/pkg/testutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/tests"
	"go.etcd.io/etcd/clientv3"
)

var _ = Suite(&configClientTestSuite{})

type configClientTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *configClientTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *configClientTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *configClientTestSuite) TestUpdateWrongEntry(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1, func(cfg *config.Config) { cfg.EnableDynamicConfig = true })
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewConfigClientWithContext(s.ctx, endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)

	cfgData := `[aaa]
  xxx-yyy-zzz = 1
  [aaa.bbb]
    xxx-yyy = "1KB"
    xxx-zzz = false
    yyy-zzz = ["aa", "bb"]
    [aaa.bbb.ccc]
      yyy-xxx = 0.00005
`

	// create config
	status, version, config, err := cli.Create(s.ctx, &configpb.Version{Global: 0, Local: 0}, "component", "component1", cfgData)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(err, IsNil)

	// update wrong config
	status, version, err = cli.Update(s.ctx,
		&configpb.Version{Global: 0, Local: 0},
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: "component"}}},
		[]*configpb.ConfigEntry{{Name: "aaa.xxx-xxx", Value: "2"}},
	)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_UNKNOWN)
	c.Assert(strings.Contains(status.GetMessage(), "cannot find the config item"), IsTrue)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(err, IsNil)

	// update right config
	status, version, err = cli.Update(s.ctx,
		&configpb.Version{Global: 0, Local: 0},
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: "component"}}},
		[]*configpb.ConfigEntry{{Name: "aaa.xxx-yyy-zzz", Value: "2"}},
	)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)
}

func (s *configClientTestSuite) TestClientLeaderChange(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3, func(cfg *config.Config) { cfg.EnableDynamicConfig = true })
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewConfigClientWithContext(s.ctx, endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)

	cfgData := `[aaa]
  xxx-yyy-zzz = 1
  [aaa.bbb]
    xxx-yyy = "1KB"
    xxx-zzz = false
    yyy-zzz = ["aa", "bb"]
    [aaa.bbb.ccc]
      yyy-xxx = 0.00005
`

	// create config
	status, version, config, err := cli.Create(s.ctx, &configpb.Version{Global: 0, Local: 0}, "component", "component1", cfgData)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(err, IsNil)

	// get config
	status, version, config, err = cli.Get(s.ctx, &configpb.Version{Global: 0, Local: 0}, "component", "component1")
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(err, IsNil)

	// update config
	status, version, err = cli.Update(s.ctx,
		&configpb.Version{Global: 0, Local: 0},
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: "component"}}},
		[]*configpb.ConfigEntry{{Name: "aaa.xxx-yyy-zzz", Value: "2"}},
	)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)
	cfgData1 := `[aaa]
  xxx-yyy-zzz = 2
  [aaa.bbb]
    xxx-yyy = "1KB"
    xxx-zzz = false
    yyy-zzz = ["aa", "bb"]
    [aaa.bbb.ccc]
      yyy-xxx = 0.00005
`
	// get config
	status, version, config, err = cli.Get(s.ctx, &configpb.Version{Global: 1, Local: 0}, "component", "component1")
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData1)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)

	leader := cluster.GetLeader()
	s.waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	leader = cluster.WaitLeader()
	c.Assert(leader, Not(Equals), "")
	s.waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	// get config
	status, version, config, err = cli.Get(s.ctx, &configpb.Version{Global: 1, Local: 0}, "component", "component1")
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData1)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)

	// Check URL list.
	cli.Close()
	urls := cli.(client).GetURLs()
	sort.Strings(urls)
	sort.Strings(endpoints)
	c.Assert(urls, DeepEquals, endpoints)
}

func (s *configClientTestSuite) TestLeaderTransfer(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2, func(cfg *config.Config) { cfg.EnableDynamicConfig = true })
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewConfigClientWithContext(s.ctx, endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)
	cfgData := `[aaa]
  xxx-yyy-zzz = 1
  [aaa.bbb]
    xxx-yyy = "1KB"
    xxx-zzz = false
    yyy-zzz = ["aa", "bb"]
    [aaa.bbb.ccc]
      yyy-xxx = 0.00005
`
	// create config
	status, version, config, err := cli.Create(s.ctx, &configpb.Version{Global: 0, Local: 0}, "component", "component1", cfgData)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(err, IsNil)

	// get config
	status, version, config, err = cli.Get(s.ctx, &configpb.Version{Global: 0, Local: 0}, "component", "component1")
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 0, Local: 0})
	c.Assert(err, IsNil)

	// update config
	status, version, err = cli.Update(s.ctx,
		&configpb.Version{Global: 0, Local: 0},
		&configpb.ConfigKind{Kind: &configpb.ConfigKind_Global{Global: &configpb.Global{Component: "component"}}},
		[]*configpb.ConfigEntry{{Name: "aaa.bbb.xxx-yyy", Value: "2KB"}},
	)
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)
	cfgData1 := `[aaa]
  xxx-yyy-zzz = 1
  [aaa.bbb]
    xxx-yyy = "2KB"
    xxx-zzz = false
    yyy-zzz = ["aa", "bb"]
    [aaa.bbb.ccc]
      yyy-xxx = 0.00005
`
	// get config
	status, version, config, err = cli.Get(s.ctx, &configpb.Version{Global: 1, Local: 0}, "component", "component1")
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData1)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)

	// Transfer leader.
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	})
	c.Assert(err, IsNil)
	leaderPath := filepath.Join("/pd", strconv.FormatUint(cli.GetClusterID(context.Background()), 10), "leader")
	for i := 0; i < 10; i++ {
		cluster.WaitLeader()
		_, err = etcdCli.Delete(context.TODO(), leaderPath)
		c.Assert(err, IsNil)
		// Sleep to make sure all servers are notified and starts campaign.
		time.Sleep(time.Second)
	}

	// get config
	status, version, config, err = cli.Get(s.ctx, &configpb.Version{Global: 1, Local: 0}, "component", "component1")
	c.Assert(status.GetCode(), Equals, configpb.StatusCode_OK)
	c.Assert(config, Equals, cfgData1)
	c.Assert(version, DeepEquals, &configpb.Version{Global: 1, Local: 0})
	c.Assert(err, IsNil)
}

func (s *configClientTestSuite) waitLeader(c *C, cli client, leader string) {
	testutil.WaitUntil(c, func(c *C) bool {
		cli.ScheduleCheckLeader()
		return cli.GetLeaderAddr() == leader
	})
}
