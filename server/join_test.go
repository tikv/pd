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
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testJoinServerSuite{})

type testJoinServerSuite struct{}

func newTestMultiJoinConfig(count int) []*Config {
	cfgs := NewTestMultiConfig(count)
	for i := 0; i < count; i++ {
		cfgs[i].InitialCluster = ""
		if i == 0 {
			continue
		}
		cfgs[i].Join = cfgs[i-1].ClientUrls
	}
	return cfgs
}

func waitMembers(svr *Server, c int) error {
	// maxRetryTime * waitInterval = 5s
	maxRetryCount := 10
	waitInterval := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	client := svr.GetClient()
	for ; maxRetryCount != 0; maxRetryCount-- {
		listResp, err := client.MemberList(ctx)
		if err != nil {
			continue
		}

		count := 0
		for _, memb := range listResp.Members {
			if len(memb.Name) == 0 {
				// unstarted, see:
				// https://github.com/coreos/etcd/blob/master/etcdctl/ctlv3/command/printer.go#L60
				// https://coreos.com/etcd/docs/latest/runtime-configuration.html#add-a-new-member
				continue
			}
			count++
		}

		if count >= c {
			return nil
		}

		time.Sleep(waitInterval)
	}
	return errors.New("waitMembers Timeout")
}

type cleanUpFunc func()

func mustNewJoinCluster(c *C, num int) ([]*Config, []*Server, cleanUpFunc) {
	dirs := make([]string, 0, num)
	svrs := make([]*Server, 0, num)
	cfgs := newTestMultiJoinConfig(num)

	ch := make(chan *Server, num)
	for _, cfg := range cfgs {
		dirs = append(dirs, cfg.DataDir)

		go func(cfg *Config) {
			s, e := CreateServer(cfg)
			c.Assert(e, IsNil)
			e = s.StartEtcd(nil)
			c.Assert(e, IsNil)
			ch <- s
			s.Run()
		}(cfg)
	}

	for i := 0; i < num; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)

	waitMembers(svrs[rand.Intn(num)], num)

	// clean up
	clean := func() {
		for _, s := range svrs {
			s.Close()
		}
	}

	return cfgs, svrs, clean
}

func (s *testJoinServerSuite) TestRegularJoin(c *C) {
	cfgs, _, clean := mustNewJoinCluster(c, 3)
	defer clean()

	endpoints := strings.Split(cfgs[rand.Intn(3)].ClientUrls, ",")
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, IsNil)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	listResp, err := client.MemberList(ctx)
	c.Assert(err, IsNil)
	c.Assert(len(listResp.Members), Equals, len(cfgs))
}

func (s *testJoinServerSuite) TestJoinSelf(c *C) {
	cfgs := newTestMultiJoinConfig(1)
	cfgs[0].Join = cfgs[0].AdvertiseClientUrls

	svr, err := CreateServer(cfgs[0])
	c.Assert(err, IsNil)
	err = svr.StartEtcd(nil)
	c.Assert(err, IsNil)
	defer svr.Close()
	go svr.Run()

	err = waitMembers(svr, 1)
	c.Assert(err, IsNil)
}

func (s *testJoinServerSuite) TestReJoin(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := rand.Intn(len(cfgs))
	svrs[target].Close()
	time.Sleep(100 * time.Millisecond)

	ch := make(chan *Server)
	go func() {
		cfgs[target].InitialCluster = ""
		svr, err := CreateServer(cfgs[target])
		c.Assert(err, IsNil)
		err = svr.StartEtcd(nil)
		c.Assert(err, IsNil)
		defer svr.Close()
		ch <- svr
		svr.Run()
	}()

	re := <-ch
	err := waitMembers(re, len(cfgs))
	c.Assert(err, IsNil)
}
