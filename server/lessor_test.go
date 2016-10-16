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
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testLessorSuite{})

type testLessorSuite struct {
	etcd   *embed.Etcd
	client *clientv3.Client
}

func (s *testLessorSuite) SetUpSuite(c *C) {
	cfg, err := NewTestSingleConfig().genEmbedEtcdConfig()
	c.Assert(err, IsNil)

	etcd, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)

	endpoints := []string{cfg.LCUrls[0].String()}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	c.Assert(err, IsNil)

	<-etcd.Server.ReadyNotify()

	s.etcd = etcd
	s.client = client
}

func (s *testLessorSuite) TearDownSuite(c *C) {
	s.etcd.Close()
	s.client.Close()
}

const (
	leaderPrefix = "/pd/leader"
)

func (s *testLessorSuite) TestLessor(c *C) {
	ch := make(chan *Lessor)
	lessors := make(map[int]*Lessor)

	for i := 0; i < 3; i++ {
		lessor, err := NewLessor(s.client, 3, leaderPrefix)
		c.Assert(err, IsNil)

		go func(id int, lessor *Lessor) {
			leader := &pdpb.Leader{
				Pid: int64(id),
			}
			err := lessor.Campaign(leader)
			c.Assert(err, IsNil)
			ch <- lessor
		}(i, lessor)

		lessors[i] = lessor
	}

	for len(lessors) != 0 {
		lessor := <-ch

		leader, err := GetLeader(s.client, leaderPrefix)
		c.Assert(err, IsNil)

		id := int(leader.GetPid())
		c.Assert(lessors[id], Equals, lessor)

		op := clientv3.OpPut("hello", "world")

		_, err = lessor.Txn().Then(op).Commit()
		c.Assert(err, IsNil)

		lessor.Close()
		delete(lessors, id)

		_, err = lessor.Txn().Then(op).Commit()
		c.Assert(err, NotNil)
		c.Assert(errors.Cause(err), Equals, errTxnFailed)
	}
}
