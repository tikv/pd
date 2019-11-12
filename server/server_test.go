// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"context"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
)

func TestServer(t *testing.T) {
	EnableZap = true
	TestingT(t)
}

func mustRunTestServer(c *C) (*Server, CleanupFunc) {
	var err error
	_, server, cleanup, err := NewTestServer(c)
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{server})
	return server, cleanup
}

func mustWaitLeader(c *C, svrs []*Server) *Server {
	var leader *Server
	testutil.WaitUntil(c, func(c *C) bool {
		for _, s := range svrs {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	return leader
}

var _ = Suite(&testServerSuite{})

type testServerSuite struct{}

func newTestServersWithCfgs(c *C, cfgs []*Config) ([]*Server, CleanupFunc) {
	svrs := make([]*Server, 0, len(cfgs))

	ch := make(chan *Server)
	for _, cfg := range cfgs {
		go func(cfg *Config) {
			svr, err := CreateServer(cfg, nil)
			c.Assert(err, IsNil)
			c.Assert(svr, NotNil)
			err = svr.Run(context.TODO())
			c.Assert(err, IsNil)
			ch <- svr
		}(cfg)
	}

	for i := 0; i < len(cfgs); i++ {
		svrs = append(svrs, <-ch)
	}
	mustWaitLeader(c, svrs)

	cleanup := func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}

	return svrs, cleanup
}

func (s *testServerSuite) TestCheckClusterID(c *C) {
	cfgs := NewTestMultiConfig(c, 2)
	for i, cfg := range cfgs {
		cfg.DataDir = fmt.Sprintf("/tmp/test_pd_check_clusterID_%d", i)
		// Clean up before testing.
		cleanServer(cfg)
	}
	originInitial := cfgs[0].InitialCluster
	for _, cfg := range cfgs {
		cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls)
	}

	cfgA, cfgB := cfgs[0], cfgs[1]
	// Start a standalone cluster
	// TODO: clean up. For now tests failed because:
	//    etcdserver: failed to purge snap file ...
	svrsA, _ := newTestServersWithCfgs(c, []*Config{cfgA})
	// Close it.
	for _, svr := range svrsA {
		svr.Close()
	}

	// Start another cluster.
	_, cleanB := newTestServersWithCfgs(c, []*Config{cfgB})
	defer cleanB()

	// Start previous cluster, expect an error.
	cfgA.InitialCluster = originInitial
	svr, err := CreateServer(cfgA, nil)
	c.Assert(err, IsNil)
	err = svr.Run(context.TODO())
	c.Assert(err, NotNil)
}
