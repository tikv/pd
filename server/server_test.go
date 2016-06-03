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
	"flag"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

var (
	testEtcd = flag.String("etcd", "127.0.0.1:2379", "Etcd endpoints, separated by comma")
)

func newTestServer(c *C, rootPath string) *Server {
	cfg := &Config{
		Addr:            "127.0.0.1:0",
		EtcdAddrs:       strings.Split(*testEtcd, ","),
		RootPath:        rootPath,
		LeaderLease:     1,
		TsoSaveInterval: 500,
		// We use cluster 0 for all tests.
		ClusterID: 0,
	}

	svr, err := NewServer(cfg)
	c.Assert(err, IsNil)

	return svr
}

func newEtcdClient(c *C) *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*testEtcd, ","),
		DialTimeout: time.Second,
	})

	c.Assert(err, IsNil)
	return client
}

func deleteRoot(c *C, client *clientv3.Client, rootPath string) {
	kv := clientv3.NewKV(client)

	_, err := kv.Delete(context.Background(), rootPath+"/", clientv3.WithPrefix())
	c.Assert(err, IsNil)

	_, err = kv.Delete(context.Background(), rootPath)
	c.Assert(err, IsNil)
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

	for i := 0; i < 3; i++ {
		svr := newTestServer(c, s.getRootPath())
		s.svrs[svr.cfg.AdvertiseAddr] = svr
		s.leaderPath = svr.getLeaderPath()
	}

	s.client = newEtcdClient(c)

	deleteRoot(c, s.client, s.getRootPath())
}

func (s *testLeaderServerSuite) TearDownSuite(c *C) {
	for _, svr := range s.svrs {
		svr.Close()
	}
	s.client.Close()
}

func (s *testLeaderServerSuite) TestLeader(c *C) {
	for _, svr := range s.svrs {
		go svr.Run()
	}

	for i := 0; i < 100 && len(s.svrs) > 0; i++ {
		leader, err := GetLeader(s.client, s.leaderPath)
		c.Assert(err, IsNil)

		if leader == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// The leader key is not expired, retry again.
		svr, ok := s.svrs[leader.GetAddr()]
		if !ok {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		delete(s.svrs, leader.GetAddr())
		svr.Close()
	}

	c.Assert(s.svrs, HasLen, 0)
}
