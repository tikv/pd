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
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

func newTestServer(c *C, rootPath string) *Server {
	cfg := &Config{
		Addr:            "127.0.0.1:0",
		RootPath:        rootPath,
		LeaderLease:     1,
		TsoSaveInterval: 500,
		// We use cluster 0 for all tests.
		ClusterID: 0,
		EtcdCfg:   NewTestSingleEtcdConfig(),
	}

	svr, err := NewServer(cfg)
	c.Assert(err, IsNil)

	return svr
}

var _ = Suite(&testLeaderServerSuite{})

type testLeaderServerSuite struct {
	client     *clientv3.Client
	svrs       map[string]*Server
	leaderPath string
}

func (s *testLeaderServerSuite) getRootPath() string {
	return "test_leader"
}

func (s *testLeaderServerSuite) SetUpSuite(c *C) {
	s.svrs = make(map[string]*Server)

	etcdCfgs := NewTestMultiEtcdConfig(3)

	ch := make(chan *Server, 3)
	for i := 0; i < 3; i++ {
		cfg := &Config{
			Addr:            "127.0.0.1:0",
			RootPath:        s.getRootPath(),
			LeaderLease:     1,
			TsoSaveInterval: 500,
			ClusterID:       0,
			EtcdCfg:         etcdCfgs[i],
		}

		go func() {
			svr, err := NewServer(cfg)
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	for i := 0; i < 3; i++ {
		svr := <-ch
		s.svrs[svr.cfg.AdvertiseAddr] = svr
		s.leaderPath = svr.getLeaderPath()
	}

	s.setUpClient(c)
}

func (s *testLeaderServerSuite) TearDownSuite(c *C) {
	for _, svr := range s.svrs {
		svr.Close()
	}
	s.client.Close()
}

func (s *testLeaderServerSuite) setUpClient(c *C) {
	endpoints := make([]string, 0, 3)

	for _, svr := range s.svrs {
		endpoints = append(endpoints, svr.GetEndpoints()...)
	}

	var err error
	s.client, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, IsNil)
}

func (s *testLeaderServerSuite) TestLeader(c *C) {
	for _, svr := range s.svrs {
		go svr.Run()
	}

	leader1 := mustGetLeader(c, s.client, s.leaderPath)
	svr, ok := s.svrs[leader1.GetAddr()]
	c.Assert(ok, IsTrue)
	svr.Close()
	delete(s.svrs, leader1.GetAddr())

	// now, another two servers must select a leader
	s.setUpClient(c)

	// wait leader changes
	for i := 0; i < 50; i++ {
		leader, _ := getLeader(s.client, s.leaderPath)
		if leader != nil && leader.GetAddr() != leader1.GetAddr() {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	leader2 := mustGetLeader(c, s.client, s.leaderPath)
	c.Assert(leader1.GetAddr(), Not(Equals), leader2.GetAddr())
}
