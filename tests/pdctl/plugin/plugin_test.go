// Copyright 2019 PingCAP, Inc.
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

package plugin_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&pluginTestSuite{})

type pluginTestSuite struct{}

func (s *pluginTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *pluginTestSuite) TestPlugin(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	// plugin load
	args := []string{"-u", pdAddr, "plugin", "load", "/home/dc/pd/plugin/scheduler_example/grant_leader/grantLeaderPlugin.so"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// plugin unload
	args2 := []string{"-u", pdAddr, "plugin", "unload", "/home/dc/pd/plugin/scheduler_example/grant_leader/grantLeaderPlugin.so"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args2...)
	c.Assert(err, IsNil)

	// plugin update
	args3 := []string{"-u", pdAddr, "plugin", "update", "/home/dc/pd/plugin/scheduler_example/grant_leader/grantLeaderPlugin.so"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args3...)
	c.Assert(err, IsNil)
}
