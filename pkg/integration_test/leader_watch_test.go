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

package integration

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	gofail "github.com/pingcap/gofail/runtime"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server"
)

func (s *integrationTestSuite) TestWatcher(c *C) {
	c.Parallel()
	cluster, err := newTestCluster(1, func(conf *server.Config) { conf.AutoCompactionRetention = "1s" })
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pd1 := cluster.GetServer(cluster.GetLeader())
	c.Assert(pd1, NotNil)

	pd2, err := cluster.Join()
	c.Assert(err, IsNil)
	err = pd2.Run(context.TODO())
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	time.Sleep(5 * time.Second)
	pd3, err := cluster.Join()
	c.Assert(err, IsNil)
	gofail.Enable("github.com/pingcap/pd/server/delayWatcher", `pause`)
	err = pd3.Run(context.Background())
	c.Assert(err, IsNil)
	time.Sleep(200 * time.Millisecond)
	c.Assert(pd3.GetLeader().GetName(), Equals, pd1.GetConfig().Name)
	pd1.Stop()
	cluster.WaitLeader()
	c.Assert(pd2.GetLeader().GetName(), Equals, pd2.GetConfig().Name)
	gofail.Disable("github.com/pingcap/pd/server/delayWatcher")
	testutil.WaitUntil(c, func(c *C) bool {
		return c.Check(pd3.GetLeader().GetName(), Equals, pd2.GetConfig().Name)
	})
	c.Succeed()
}

func (s *integrationTestSuite) TestWatcherCompacted(c *C) {
	c.Parallel()
	cluster, err := newTestCluster(1, func(conf *server.Config) { conf.AutoCompactionRetention = "1s" })
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pd1 := cluster.GetServer(cluster.GetLeader())
	c.Assert(pd1, NotNil)
	client := pd1.GetEtcdClient()
	client.Put(context.Background(), "test", "v")
	// wait compaction
	time.Sleep(2 * time.Second)
	pd2, err := cluster.Join()
	c.Assert(err, IsNil)
	err = pd2.Run(context.Background())
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func(c *C) bool {
		return c.Check(pd2.GetLeader().GetName(), Equals, pd1.GetConfig().Name)
	})
	c.Succeed()
}
