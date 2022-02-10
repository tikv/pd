// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var _ = Suite(&testLeaseSuite{})

type testLeaseSuite struct{}

func (s *testLeaseSuite) TestLease(c *C) {
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

	// Create the lease.
	lease1 := &lease{
		Purpose: "test_lease_1",
		client:  client,
		lease:   clientv3.NewLease(client),
	}
	lease2 := &lease{
		Purpose: "test_lease_2",
		client:  client,
		lease:   clientv3.NewLease(client),
	}
	c.Check(lease1.IsExpired(), IsTrue)
	c.Check(lease2.IsExpired(), IsTrue)
	c.Check(lease1.Close(), IsNil)
	c.Check(lease2.Close(), IsNil)

	// Grant the two leases with the same timeout.
	c.Check(lease1.Grant(defaultLeaseTimeout), IsNil)
	c.Check(lease2.Grant(defaultLeaseTimeout), IsNil)
	c.Check(lease1.IsExpired(), IsFalse)
	c.Check(lease2.IsExpired(), IsFalse)

	// Wait for a while to make both two leases timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	c.Check(lease1.IsExpired(), IsTrue)
	c.Check(lease2.IsExpired(), IsTrue)

	// Grant the two leases with different timeouts.
	c.Check(lease1.Grant(defaultLeaseTimeout), IsNil)
	c.Check(lease2.Grant(defaultLeaseTimeout*4), IsNil)
	c.Check(lease1.IsExpired(), IsFalse)
	c.Check(lease2.IsExpired(), IsFalse)

	// Wait for a while to make one of the lease timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	c.Check(lease1.IsExpired(), IsTrue)
	c.Check(lease2.IsExpired(), IsFalse)

	// Close both of the two leases.
	c.Check(lease1.Close(), IsNil)
	c.Check(lease2.Close(), IsNil)
	c.Check(lease1.IsExpired(), IsTrue)
	c.Check(lease2.IsExpired(), IsTrue)

	// Grant the lease1 and keep it alive.
	c.Check(lease1.Grant(defaultLeaseTimeout), IsNil)
	c.Check(lease1.IsExpired(), IsFalse)
	ctx, cancel := context.WithCancel(context.Background())
	go lease1.KeepAlive(ctx)
	defer cancel()

	// Wait for a timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	c.Check(lease1.IsExpired(), IsFalse)
	// Close and wait for a timeout.
	c.Check(lease1.Close(), IsNil)
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	c.Check(lease1.IsExpired(), IsTrue)
}
