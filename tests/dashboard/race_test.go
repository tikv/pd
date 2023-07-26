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

package dashboard_test

import (
	"context"
	"time"

	. "github.com/pingcap/check"

	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"

	// Register schedulers.
	_ "github.com/tikv/pd/server/schedulers"
)

var _ = Suite(&raceTestSuite{})

type raceTestSuite struct{}

func (s *raceTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
	dashboard.SetCheckInterval(50 * time.Millisecond)
	tests.WaitLeaderReturnDelay = 0
	tests.WaitLeaderCheckInterval = 20 * time.Millisecond
}

func (s *raceTestSuite) TearDownSuite(c *C) {
	server.EnableZap = false
	dashboard.SetCheckInterval(time.Second)
	tests.WaitLeaderReturnDelay = 20 * time.Millisecond
	tests.WaitLeaderCheckInterval = 500 * time.Millisecond
}

func (s *raceTestSuite) TestCancelDuringStarting(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	time.Sleep(60 * time.Millisecond)
	cancel()
}
