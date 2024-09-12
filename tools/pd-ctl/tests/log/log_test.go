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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type logTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *pdTests.TestCluster
	pdAddrs []string
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(logTestSuite))
}

func (suite *logTestSuite) SetupSuite() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	var err error
	suite.cluster, err = pdTests.NewTestCluster(suite.ctx, 3)
	re.NoError(err)
	re.NoError(suite.cluster.RunInitialServers())
	re.NotEmpty(suite.cluster.WaitLeader())
	suite.pdAddrs = suite.cluster.GetConfig().GetClientURLs()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := suite.cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdTests.MustPutStore(re, suite.cluster, store)
}

func (suite *logTestSuite) TearDownSuite() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *logTestSuite) TestLog() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	var testCases = []struct {
		cmd    []string
		expect string
	}{
		// log [fatal|error|warn|info|debug]
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "fatal"},
			expect: "fatal",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "error"},
			expect: "error",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "warn"},
			expect: "warn",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "info"},
			expect: "info",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "debug"},
			expect: "debug",
		},
	}

	for _, testCase := range testCases {
		_, err := tests.ExecuteCommand(cmd, testCase.cmd...)
		re.NoError(err)
		re.Equal(testCase.expect, suite.cluster.GetLeaderServer().GetConfig().Log.Level)
	}
}

func (suite *logTestSuite) TestInstanceLog() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	var testCases = []struct {
		cmd      []string
		instance string
		expect   string
	}{
		// log [fatal|error|warn|info|debug] [address]
		{
			cmd:      []string{"-u", suite.pdAddrs[0], "log", "debug", suite.pdAddrs[0]},
			instance: suite.pdAddrs[0],
			expect:   "debug",
		},
		{
			cmd:      []string{"-u", suite.pdAddrs[0], "log", "error", suite.pdAddrs[1]},
			instance: suite.pdAddrs[1],
			expect:   "error",
		},
		{
			cmd:      []string{"-u", suite.pdAddrs[0], "log", "warn", suite.pdAddrs[2]},
			instance: suite.pdAddrs[2],
			expect:   "warn",
		},
	}

	for _, testCase := range testCases {
		_, err := tests.ExecuteCommand(cmd, testCase.cmd...)
		re.NoError(err)
		svrs := suite.cluster.GetServers()
		for _, svr := range svrs {
			if svr.GetAddr() == testCase.instance {
				re.Equal(testCase.expect, svr.GetConfig().Log.Level)
			}
		}
	}
}
