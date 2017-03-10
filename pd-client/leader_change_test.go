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

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"golang.org/x/net/context"
)

var _ = Suite(&testLeaderChangeSuite{})

type testLeaderChangeSuite struct{}

func (s *testLeaderChangeSuite) TestLeaderChange(c *C) {
	cfgs := server.NewTestMultiConfig(3)

	ch := make(chan *server.Server, 3)

	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			svr := server.CreateServer(cfg)
			err := svr.StartEtcd(api.NewHandler(svr))
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

	defer func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}()

	leaderPeer := mustWaitLeader(c, svrs)
	grpcClient := mustNewGrpcClient(c, leaderPeer.GetAddr())
	bootstrapServer(c, newHeader(leaderPeer), grpcClient)

	cli, err := NewClient(endpoints)
	c.Assert(err, IsNil)
	defer cli.Close()

	p1, l1, err := cli.GetTS(context.Background())
	c.Assert(err, IsNil)

	leader := s.mustGetLeader(c, endpoints)
	s.verifyLeader(c, cli.(*client), leader)

	svrs[leader].Close()
	delete(svrs, leader)

	mustWaitLeader(c, svrs)
	newLeader := s.mustGetLeader(c, endpoints)
	c.Assert(newLeader, Not(Equals), leader)
	s.verifyLeader(c, cli.(*client), newLeader)

	for i := 0; i < 20; i++ {
		p2, l2, err := cli.GetTS(context.Background())
		if err == nil {
			c.Assert(p1<<18+l1, Less, p2<<18+l2)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Error("failed getTS from new leader after 10 seconds")
}

func (s *testLeaderChangeSuite) mustGetLeader(c *C, urls []string) string {
	for _, u := range urls {
		client, err := apiutil.NewClient(u, pdTimeout)
		if err != nil {
			continue
		}
		leader, err := client.GetLeader()
		if err != nil {
			continue
		}
		return leader.GetClientUrls()[0]
	}
	c.Fatal("failed get leader")
	return ""
}

func (s *testLeaderChangeSuite) verifyLeader(c *C, cli *client, leader string) {
	cli.scheduleCheckLeader()
	time.Sleep(time.Millisecond * 500)

	cli.connMu.RLock()
	defer cli.connMu.RUnlock()
	c.Assert(cli.connMu.leader, Equals, leader)
}
