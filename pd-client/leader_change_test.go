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

package pd

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

var _ = Suite(&testLeaderChangeSuite{})

type testLeaderChangeSuite struct{}

func mustGetEtcdClient(c *C, svrs map[string]*server.Server) *clientv3.Client {
	for _, svr := range svrs {
		return svr.GetClient()
	}
	c.Fatal("etcd client none available")
	return nil
}

func (s *testLeaderChangeSuite) TestLeaderChange(c *C) {
	cfgs := server.NewTestMultiConfig(3)

	ch := make(chan *server.Server, 3)

	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := server.CreateServer(cfg)
			c.Assert(err, IsNil)
			err = svr.StartEtcd(api.NewHandler(svr))
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	svrs := make(map[string]*server.Server, 3)
	for i := 0; i < 3; i++ {
		svr := <-ch
		svrs[svr.GetAddr()] = svr
	}

	endpoints := make([]string, 0, 3)
	for _, svr := range svrs {
		go svr.Run()
		endpoints = append(endpoints, svr.GetEndpoints()...)
	}

	mustWaitLeader(c, svrs)

	defer func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}()

	cli, err := NewClient(endpoints, 0)
	c.Assert(err, IsNil)
	defer cli.Close()

	p1, l1, err := cli.GetTS()
	c.Assert(err, IsNil)

	leader, err := getLeader(endpoints)
	c.Assert(err, IsNil)
	mustConnectLeader(c, endpoints, leader.GetAddr())

	svrs[leader.GetAddr()].Close()
	delete(svrs, leader.GetAddr())

	// wait leader changes
	changed := false
	for i := 0; i < 20; i++ {
		newLeader, _ := getLeader(endpoints)
		if newLeader != nil && newLeader.GetAddr() != leader.GetAddr() {
			mustConnectLeader(c, endpoints, newLeader.GetAddr())
			changed = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Assert(changed, IsTrue)

	for i := 0; i < 20; i++ {
		p2, l2, err := cli.GetTS()
		if err == nil {
			c.Assert(p1<<18+l1, Less, p2<<18+l2)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Error("failed getTS from new leader after 10 seconds")
}

func mustConnectLeader(c *C, urls []string, leaderAddr string) {
	connCh := make(chan *conn)
	go func() {
		conn := mustNewConn(urls, nil)
		connCh <- conn
	}()

	var conn *conn
	select {
	case conn = <-connCh:
		addr := conn.RemoteAddr()
		c.Assert(addr.Network()+"://"+addr.String(), Equals, leaderAddr)
	case <-time.After(time.Second * 10):
		c.Fatal("failed to connect to pd")
	}
	defer conn.Close()

	conn.wg.Add(1)
	go conn.connectLeader(urls, time.Second)

	select {
	case leaderConn := <-conn.ConnChan:
		addr := leaderConn.RemoteAddr()
		c.Assert(addr.Network()+"://"+addr.String(), Equals, leaderAddr)
	case <-time.After(time.Second * 10):
		c.Fatal("failed to connect to leader")
	}

	// Create another goroutine and return to close the connection.
	// Make sure it will not block forever.
	conn.wg.Add(1)
	go conn.connectLeader(urls, time.Second)
	time.Sleep(time.Second * 3)
}
