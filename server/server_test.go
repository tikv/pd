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
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

// We can put more test utilities below.

const (
	maxWaitCount = 50
	waitInterval = time.Millisecond * 200
)

func mustGetLeader(c *C, s *Server) *pdpb.Leader {
	for i := 0; i < maxWaitCount; i++ {
		leader, err := s.GetLeader()
		if err == nil {
			return leader
		}
		time.Sleep(waitInterval)
	}
	c.Fatal("failed to get leader")
	return nil
}

func mustGetLeaderServer(c *C, servers map[string]*Server) *Server {
	for i := 0; i < maxWaitCount; i++ {
		for _, s := range servers {
			if s.IsLeader() {
				return s
			}
		}
		time.Sleep(waitInterval)
	}
	c.Fatal("failed to get leader server")
	return nil
}

type cleanupFunc func()

func newTestServer(c *C) (*Server, cleanUpFunc) {
	cfg := NewTestSingleConfig()

	svr, err := NewServer(cfg)
	c.Assert(err, IsNil)

	cleanup := func() {
		svr.Close()
		cleanServer(svr.cfg)
	}

	return svr, cleanup
}

var stripUnix = strings.NewReplacer("unix://", "")

func cleanServer(cfg *Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)

	// Clean unix sockets
	os.Remove(stripUnix.Replace(cfg.PeerUrls))
	os.Remove(stripUnix.Replace(cfg.ClientUrls))
	os.Remove(stripUnix.Replace(cfg.AdvertisePeerUrls))
	os.Remove(stripUnix.Replace(cfg.AdvertiseClientUrls))
}

func newMultiTestServers(c *C, count int) ([]*Server, cleanupFunc) {
	svrs := make([]*Server, 0, count)
	cfgs := NewTestMultiConfig(count)

	ch := make(chan *Server, count)
	for i := 0; i < count; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := NewServer(cfg)
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	for i := 0; i < count; i++ {
		svr := <-ch
		go svr.Run()
		svrs = append(svrs, svr)
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

func mustWaitLeader(c *C, svrs []*Server) *Server {
	for i := 0; i < 500; i++ {
		for _, s := range svrs {
			if s.IsLeader() {
				return s
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Fatal("no leader")
	return nil
}

func mustRPCCall(c *C, conn net.Conn, req *pdpb.Request) *pdpb.Response {
	resp, err := rpcCall(conn, uint64(rand.Int63()), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	return resp
}

var _ = Suite(&testLeaderServerSuite{})

type testLeaderServerSuite struct {
	svrs map[string]*Server
}

func (s *testLeaderServerSuite) SetUpSuite(c *C) {
	s.svrs = make(map[string]*Server)

	cfgs := NewTestMultiConfig(3)

	ch := make(chan *Server, 3)
	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := NewServer(cfg)
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	for i := 0; i < 3; i++ {
		svr := <-ch
		s.svrs[svr.GetAddr()] = svr
	}
}

func (s *testLeaderServerSuite) TearDownSuite(c *C) {
	for _, svr := range s.svrs {
		svr.Close()
		cleanServer(svr.cfg)
	}
}

func (s *testLeaderServerSuite) TestLeader(c *C) {
	for _, svr := range s.svrs {
		go svr.Run()
	}

	leader := mustGetLeaderServer(c, s.svrs)

	op := clientv3.OpPut("hello", "world")

	_, err := leader.txn().Then(op).Commit()
	c.Assert(err, IsNil)

	for _, l := range s.svrs {
		if l != leader {
			_, err = l.txn().Then(op).Commit()
			c.Assert(err, NotNil)
			c.Assert(errors.Cause(err), Equals, errNotLeader)
		}
	}

	leader.Close()
	delete(s.svrs, leader.GetAddr())

	newLeader := mustGetLeaderServer(c, s.svrs)
	c.Assert(leader.GetAddr(), Not(Equals), newLeader.GetAddr())
}
