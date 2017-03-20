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
	"path/filepath"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"golang.org/x/net/context"
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

func (s *testLeaderChangeSuite) prepareClusterN(c *C, n int) (svrs map[string]*server.Server, endpoints []string, closeFunc func()) {
	cfgs := server.NewTestMultiConfig(n)

	ch := make(chan *server.Server, n)

	for i := 0; i < n; i++ {
		cfg := cfgs[i]

		go func() {
			svr := server.CreateServer(cfg)
			err := svr.StartEtcd(api.NewHandler(svr))
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	svrs = make(map[string]*server.Server, n)
	for i := 0; i < n; i++ {
		svr := <-ch
		svrs[svr.GetAddr()] = svr
	}

	endpoints = make([]string, 0, n)
	for _, svr := range svrs {
		go svr.Run()
		endpoints = append(endpoints, svr.GetEndpoints()...)
	}

	mustWaitLeader(c, svrs)

	closeFunc = func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}
	return
}

func (s *testLeaderChangeSuite) TestLeaderChange(c *C) {
	svrs, endpoints, closeFunc := s.prepareClusterN(c, 3)
	defer closeFunc()

	cli, err := NewClient(endpoints)
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

func (s *testLeaderChangeSuite) TestLeaderTransfer(c *C) {
	servers, endpoints, closeFunc := s.prepareClusterN(c, 2)
	defer closeFunc()

	cli, err := NewClient(endpoints)
	c.Assert(err, IsNil)
	defer cli.Close()

	quit := make(chan struct{})
	lastPhysical, lastLogical, err := cli.GetTS()
	c.Assert(err, IsNil)
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
			}

			physical, logical, err1 := cli.GetTS()
			if err1 == nil {
				c.Assert(lastPhysical<<18+lastLogical, Less, physical<<18+logical)
				lastPhysical, lastLogical = physical, logical
			}
			time.Sleep(time.Millisecond)
		}
	}()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	})
	c.Assert(err, IsNil)
	leaderPath := filepath.Join("/pd", strconv.FormatUint(cli.GetClusterID(), 10), "leader")
	for i := 0; i < 10; i++ {
		mustWaitLeader(c, servers)
		_, err = etcdCli.Delete(context.TODO(), leaderPath)
		c.Assert(err, IsNil)
		// Sleep to make sure all servers are notified and starts campaign.
		time.Sleep(time.Second)
	}
	close(quit)
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
	// Ensure the leader connection will be closed if we don't use it.
	c.Assert(len(conn.ConnChan), Equals, 1)
	conn.Close()
	c.Assert(len(conn.ConnChan), Equals, 0)
}
