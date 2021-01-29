// Copyright 2020 TiKV Project Authors.
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

package pdbackup

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
)

var _ = SerialSuites(&testLocalTSOSerialSuite{})

type testLocalTSOSerialSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testLocalTSOSerialSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testLocalTSOSerialSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testLocalTSOSerialSuite) TestTransferTSOLocalAllocator(c *C) {
	tso.PriorityCheck = 5 * time.Second
	defer func() {
		tso.PriorityCheck = 1 * time.Minute
	}()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-1",
	}
	serverNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, serverNum, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/injectNextLeaderKey", "return(true)"), IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader(tests.WithWaitInterval(5*time.Second), tests.WithRetryTimes(3))
	// To speed up the test, we force to do the check
	cluster.CheckClusterDCLocation()
	originName := cluster.WaitAllocatorLeader("dc-1", tests.WithRetryTimes(5), tests.WithWaitInterval(5*time.Second))
	c.Assert(originName, Equals, "")
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/tso/injectNextLeaderKey"), IsNil)
	cluster.CheckClusterDCLocation()
	originName = cluster.WaitAllocatorLeader("dc-1")
	c.Assert(len(originName), Greater, 0)
	for name, server := range cluster.GetServers() {
		if name == originName {
			continue
		}
		err := server.GetTSOAllocatorManager().TransferAllocatorForDCLocation("dc-1", server.GetServer().GetMember().ID())
		c.Assert(err, IsNil)
		testutil.WaitUntil(c, func(c *C) bool {
			cluster.CheckClusterDCLocation()
			currName := cluster.WaitAllocatorLeader("dc-1")
			return currName == name
		}, testutil.WithSleepInterval(1*time.Second))
		return
	}
}
