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
// See the License for the specific language governing permissions and
// limitations under the License.

package server_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"

	// Register schedulers.
	_ "github.com/tikv/pd/server/schedulers"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *serverTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *serverTestSuite) TestUpdateAdvertiseUrls(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// AdvertisePeerUrls should equals to PeerUrls.
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		c.Assert(serverConf.AdvertisePeerUrls, Equals, conf.PeerURLs)
		c.Assert(serverConf.AdvertiseClientUrls, Equals, conf.ClientURLs)
	}

	err = cluster.StopAll()
	c.Assert(err, IsNil)

	// Change config will not affect peer urls.
	// Recreate servers with new peer URLs.
	for _, conf := range cluster.GetConfig().InitialServers {
		conf.AdvertisePeerURLs = conf.PeerURLs + "," + tempurl.Alloc()
	}
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf, e := conf.Generate()
		c.Assert(e, IsNil)
		s, e := tests.NewTestServer(s.ctx, serverConf)
		c.Assert(e, IsNil)
		cluster.GetServers()[conf.Name] = s
	}
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		c.Assert(serverConf.AdvertisePeerUrls, Equals, conf.PeerURLs)
	}
}

func (s *serverTestSuite) TestClusterID(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	clusterID := cluster.GetServer("pd1").GetClusterID()
	for _, s := range cluster.GetServers() {
		c.Assert(s.GetClusterID(), Equals, clusterID)
	}

	// Restart all PDs.
	err = cluster.StopAll()
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// All PDs should have the same cluster ID as before.
	for _, s := range cluster.GetServers() {
		c.Assert(s.GetClusterID(), Equals, clusterID)
	}

	cluster2, err := tests.NewTestCluster(s.ctx, 3, func(conf *config.Config, serverName string) { conf.InitialClusterToken = "foobar" })
	defer cluster2.Destroy()
	c.Assert(err, IsNil)
	err = cluster2.RunInitialServers()
	c.Assert(err, IsNil)
	clusterID2 := cluster2.GetServer("pd1").GetClusterID()
	c.Assert(clusterID2, Not(Equals), clusterID)
}

func (s *serverTestSuite) TestLeader(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	leader1 := cluster.WaitLeader()
	c.Assert(leader1, Not(Equals), "")

	err = cluster.GetServer(leader1).Stop()
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func(c *C) bool {
		leader := cluster.GetLeader()
		return leader != leader1
	})
}
