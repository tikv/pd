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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

const defaultLeaseTimeout = 1

func TestLeadership(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.ListenClientUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	// Campaign the same leadership
	leadership1 := NewLeadership(client, "/test_leader", "test_leader_1")
	leadership2 := NewLeadership(client, "/test_leader", "test_leader_2")

	// leadership1 starts first and get the leadership
	err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	// leadership2 starts then and can not get the leadership
	err = leadership2.Campaign(defaultLeaseTimeout, "test_leader_2")
	re.Error(err)

	re.True(leadership1.Check())
	// leadership2 failed, so the check should return false
	re.False(leadership2.Check())

	// Sleep longer than the defaultLeaseTimeout to wait for the lease expires
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.False(leadership1.Check())
	re.False(leadership2.Check())

	// Delete the leader key and campaign for leadership1
	err = leadership1.DeleteLeaderKey()
	re.NoError(err)
	err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	re.True(leadership1.Check())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leadership1.Keep(ctx)

	// Sleep longer than the defaultLeaseTimeout
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.True(leadership1.Check())
	re.False(leadership2.Check())

	// Delete the leader key and re-campaign for leadership2
	err = leadership1.DeleteLeaderKey()
	re.NoError(err)
	err = leadership2.Campaign(defaultLeaseTimeout, "test_leader_2")
	re.NoError(err)
	re.True(leadership2.Check())
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go leadership2.Keep(ctx)

	// Sleep longer than the defaultLeaseTimeout
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.False(leadership1.Check())
	re.True(leadership2.Check())

	// Test resetting the leadership.
	leadership1.Reset()
	leadership2.Reset()
	re.False(leadership1.Check())
	re.False(leadership2.Check())

	// Try to keep the reset leadership.
	leadership1.Keep(ctx)
	leadership2.Keep(ctx)

	// Check the lease.
	lease1 := leadership1.getLease()
	re.NotNil(lease1)
	lease2 := leadership1.getLease()
	re.NotNil(lease2)

	re.True(lease1.IsExpired())
	re.True(lease2.IsExpired())
	re.NoError(lease1.Close())
	re.NoError(lease2.Close())
}
