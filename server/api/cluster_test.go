// Copyright 2017 TiKV Project Authors.
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

package api

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterSuite) TestCluster(c *C) {
	url := fmt.Sprintf("%s/cluster", s.urlPrefix)
	c1 := &metapb.Cluster{}
	err := readJSON(testDialClient, url, c1)
	c.Assert(err, IsNil)

	c2 := &metapb.Cluster{}
	r := config.ReplicationConfig{MaxReplicas: 6}
	c.Assert(s.svr.SetReplicationConfig(r), IsNil)
	err = readJSON(testDialClient, url, c2)
	c.Assert(err, IsNil)

	c1.MaxPeerCount = 6
	c.Assert(c1, DeepEquals, c2)
}

func (s *testClusterSuite) TestGetClusterStatus(c *C) {
	url := fmt.Sprintf("%s/cluster/status", s.urlPrefix)
	status := cluster.Status{}
	err := readJSON(testDialClient, url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.IsZero(), IsTrue)
	c.Assert(status.IsInitialized, IsFalse)
	now := time.Now()
	mustBootstrapCluster(c, s.svr)
	err = readJSON(testDialClient, url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.After(now), IsTrue)
	c.Assert(status.IsInitialized, IsFalse)
	s.svr.SetReplicationConfig(config.ReplicationConfig{MaxReplicas: 1})
	err = readJSON(testDialClient, url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.After(now), IsTrue)
	c.Assert(status.IsInitialized, IsTrue)
}
