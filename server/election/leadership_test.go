// Copyright 2020 TiKV Project Authors.
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

package election

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLeadershipSuite{})

type testLeadershipSuite struct{}

const defaultTestLeaderLease = 3

func (s *testLeadershipSuite) TestLeadership(c *C) {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
		etcdutil.CleanConfig(cfg)
	}()
	c.Assert(err, IsNil)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)

	<-etcd.Server.ReadyNotify()

	// Campaign the same leadership
	leadership1 := NewLeadership(client, "/test_leader", "test_leader_1")
	leadership2 := NewLeadership(client, "/test_leader", "test_leader_2")

	// leadership1 starts first and get the leadership
	err = leadership1.Campaign(defaultTestLeaderLease, "test_leader_1")
	c.Assert(err, IsNil)
	// leadership2 starts then and can not get the leadership
	err = leadership2.Campaign(defaultTestLeaderLease, "test_leader_2")
	c.Assert(err, NotNil)

	c.Assert(leadership1.Check(), IsTrue)
	// leadership2 failed, so the check should return false
	c.Assert(leadership2.Check(), IsFalse)

	// Wait for the lease expires
	time.Sleep(defaultTestLeaderLease * time.Second)

	c.Assert(leadership1.Check(), IsFalse)
	c.Assert(leadership2.Check(), IsFalse)

	err = leadership1.DeleteLeaderKey()
	c.Assert(err, IsNil)
	err = leadership1.Campaign(defaultTestLeaderLease, "test_leader_1")
	c.Assert(err, IsNil)
	c.Assert(leadership1.Check(), IsTrue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leadership1.Keep(ctx)

	time.Sleep(defaultTestLeaderLease * time.Second)

	c.Assert(leadership1.Check(), IsTrue)
}
