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
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
)

func TestJoin(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testJoinServerSuite{})

type testJoinServerSuite struct {
	cfgs []*Config
}

func (s *testJoinServerSuite) SetUpSuite(c *C) {
	s.cfgs = newTestMultiJoinConfig(3)
}

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

func (s *testJoinServerSuite) TestJoin(c *C) {
	svrs := make([]*Server, 0, len(s.cfgs))
	for i, cfg := range s.cfgs {
		log.Info("NewServer: ", i)
		svr, err := NewServer(cfg)
		c.Assert(err, IsNil)
		defer svr.Close()
		svrs = append(svrs, svr)

		go svr.Run()

		// Make sure new pd is started.
		time.Sleep(5 * time.Second)
	}

	endpoints := strings.Split(s.cfgs[rand.Intn(3)].ClientUrls, ",")
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
	c.Assert(len(listResp.Members), Equals, len(s.cfgs))
}
