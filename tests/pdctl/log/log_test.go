// Copyright 2019 TiKV Project Authors.
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

package log_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&logTestSuite{})

type logTestSuite struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	svr     *server.Server
	pdAddrs []string
}

func (s *logTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
	s.ctx, s.cancel = context.WithCancel(context.Background())
	var err error
	s.cluster, err = tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	err = s.cluster.RunInitialServers()
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()
	s.pdAddrs = s.cluster.GetConfig().GetClientURLs()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := s.cluster.GetServer(s.cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	s.svr = leaderServer.GetServer()
	pdctl.MustPutStore(c, s.svr, store)
}

func (s *logTestSuite) TearDownSuite(c *C) {
	s.cluster.Destroy()
	s.cancel()
}

func (s *logTestSuite) TestLog(c *C) {
	cmd := pdctlCmd.GetRootCmd()
	var testCases = []struct {
		cmd    []string
		expect string
	}{
		// log [fatal|error|warn|info|debug]
		{
			cmd:    []string{"-u", s.pdAddrs[0], "log", "fatal"},
			expect: "fatal",
		},
		{
			cmd:    []string{"-u", s.pdAddrs[0], "log", "error"},
			expect: "error",
		},
		{
			cmd:    []string{"-u", s.pdAddrs[0], "log", "warn"},
			expect: "warn",
		},
		{
			cmd:    []string{"-u", s.pdAddrs[0], "log", "info"},
			expect: "info",
		},
		{
			cmd:    []string{"-u", s.pdAddrs[0], "log", "debug"},
			expect: "debug",
		},
	}

	for _, testCase := range testCases {
		_, err := pdctl.ExecuteCommand(cmd, testCase.cmd...)
		c.Assert(err, IsNil)
		c.Assert(s.svr.GetConfig().Log.Level, Equals, testCase.expect)
	}
}

func (s *logTestSuite) TestInstanceLog(c *C) {
	cmd := pdctlCmd.GetRootCmd()
	var testCases = []struct {
		cmd      []string
		instance string
		expect   string
	}{
		// log [fatal|error|warn|info|debug] [address]
		{
			cmd:      []string{"-u", s.pdAddrs[0], "log", "debug", s.pdAddrs[0]},
			instance: s.pdAddrs[0],
			expect:   "debug",
		},
		{
			cmd:      []string{"-u", s.pdAddrs[0], "log", "error", s.pdAddrs[1]},
			instance: s.pdAddrs[1],
			expect:   "error",
		},
		{
			cmd:      []string{"-u", s.pdAddrs[0], "log", "warn", s.pdAddrs[2]},
			instance: s.pdAddrs[2],
			expect:   "warn",
		},
	}

	for _, testCase := range testCases {
		_, err := pdctl.ExecuteCommand(cmd, testCase.cmd...)
		c.Assert(err, IsNil)
		svrs := s.cluster.GetServers()
		for _, svr := range svrs {
			if svr.GetAddr() == testCase.instance {
				c.Assert(svr.GetConfig().Log.Level, Equals, testCase.expect)
			}
		}
	}
}
