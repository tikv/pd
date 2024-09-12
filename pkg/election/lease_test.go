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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestLease(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()

	// Create the lease.
	lease1 := NewLease(client, "test_lease_1")
	lease2 := NewLease(client, "test_lease_2")
	re.True(lease1.IsExpired())
	re.True(lease2.IsExpired())
	re.NoError(lease1.Close())
	re.NoError(lease2.Close())

	// Grant the two leases with the same timeout.
	re.NoError(lease1.Grant(defaultLeaseTimeout))
	re.NoError(lease2.Grant(defaultLeaseTimeout))
	re.False(lease1.IsExpired())
	re.False(lease2.IsExpired())

	// Wait for a while to make both two leases timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	re.True(lease1.IsExpired())
	re.True(lease2.IsExpired())

	// Grant the two leases with different timeouts.
	re.NoError(lease1.Grant(defaultLeaseTimeout))
	re.NoError(lease2.Grant(defaultLeaseTimeout * 4))
	re.False(lease1.IsExpired())
	re.False(lease2.IsExpired())

	// Wait for a while to make one of the lease timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	re.True(lease1.IsExpired())
	re.False(lease2.IsExpired())

	// Close both of the two leases.
	re.NoError(lease1.Close())
	re.NoError(lease2.Close())
	re.True(lease1.IsExpired())
	re.True(lease2.IsExpired())

	// Grant the lease1 and keep it alive.
	re.NoError(lease1.Grant(defaultLeaseTimeout))
	re.False(lease1.IsExpired())
	ctx, cancel := context.WithCancel(context.Background())
	go lease1.KeepAlive(ctx)
	defer cancel()

	// Wait for a timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	re.False(lease1.IsExpired())
	// Close and wait for a timeout.
	re.NoError(lease1.Close())
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	re.True(lease1.IsExpired())
}

func TestLeaseKeepAlive(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()

	// Create the lease.
	lease := NewLease(client, "test_lease")

	re.NoError(lease.Grant(defaultLeaseTimeout))
	ch := lease.keepAliveWorker(context.Background(), 2*time.Second)
	time.Sleep(2 * time.Second)
	<-ch
	re.NoError(lease.Close())
}
