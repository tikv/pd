// Copyright 2020 PingCAP, Inc.
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

package componenttest

import (
	"context"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&componentTestSuite{})

type componentTestSuite struct{}

func (s *componentTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
	server.ConfigCheckInterval = 10 * time.Millisecond
}

func (s *componentTestSuite) TestComponent(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddrs := cluster.GetConfig().GetClientURLs()
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

	// component ids
	args := []string{"-u", pdAddrs[0], "component", "ids", "pd"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	obtain := string(output)
	c.Assert(strings.Contains(obtain, pdAddrs[0][7:]), IsTrue)
	c.Assert(strings.Contains(obtain, pdAddrs[1][7:]), IsTrue)

	// component show
	for i := 0; i < len(pdAddrs); i++ {
		args = []string{"-u", pdAddrs[0], "component", "show", pdAddrs[i][7:]}
		_, output, err = pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(err, IsNil)
		obtain := string(output)
		c.Assert(strings.Contains(obtain, "region-schedule-limit = 2048"), IsTrue)
		c.Assert(strings.Contains(obtain, "location-labels = []"), IsTrue)
		c.Assert(strings.Contains(obtain, `level = ""`), IsTrue)
	}

	// component set
	args = []string{"-u", pdAddrs[0], "component", "set", "pd", "schedule.region-schedule-limit", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddrs[0], "component", "set", "pd", "replication.location-labels", "zone,rack"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddrs[0], "component", "set", "pd", "log.level", "warn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	time.Sleep(20 * time.Millisecond)

	// component show
	for i := 0; i < len(pdAddrs); i++ {
		args = []string{"-u", pdAddrs[0], "component", "show", pdAddrs[i][7:]}
		_, output, err = pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(err, IsNil)
		obtain := string(output)
		c.Assert(strings.Contains(obtain, "region-schedule-limit = 1"), IsTrue)
		c.Assert(strings.Contains(obtain, `location-labels = ["zone", "rack"]`), IsTrue)
		c.Assert(strings.Contains(obtain, `level = "warn"`), IsTrue)
	}
}
