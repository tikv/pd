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
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestLoadExpireTime(t *testing.T) {
	re := require.New(t)

	var nilLease *Lease
	re.Equal(typeutil.ZeroTime, nilLease.loadExpireTime())

	emptyLease := &Lease{}
	re.Equal(typeutil.ZeroTime, emptyLease.loadExpireTime())

	invalidLease := &Lease{}
	invalidLease.expireTime.Store("invalid expire time")
	re.Equal(typeutil.ZeroTime, invalidLease.loadExpireTime())

	expireTime := time.Now()
	validLease := &Lease{}
	validLease.expireTime.Store(expireTime)
	re.Equal(expireTime, validLease.loadExpireTime())
}

func TestLease(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	// Create the lease.
	lease := NewLease(client, "test_lease")

	re.NoError(lease.Grant(defaultLeaseTimeout))
	ch := lease.keepAliveWorker(ctx, 2*time.Second)
	time.Sleep(2 * time.Second)
	<-ch
	re.NoError(lease.Close())
}

func TestLeaseKeepAliveRefreshesExpireTime(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	sendKeepAliveResponse(t, keepAliveCh, 30)
	re.Eventually(func() bool {
		expire := lease.expireTime.Load().(time.Time)
		return time.Until(expire) > 20*time.Second
	}, time.Second, 10*time.Millisecond)

	cancel()
	re.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestLeaseKeepAliveExitsWhenWorkerChannelCloses(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	close(keepAliveCh)
	re.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestLeaseKeepAliveWorkerExitsOnInvalidTTL(t *testing.T) {
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timeCh := lease.keepAliveWorker(ctx, time.Second)

	sendKeepAliveResponse(t, keepAliveCh, 0)
	requireClosedTimeCh(t, timeCh)
}

func TestLeaseKeepAliveWorkerKeepsLatestExpireWhenConsumerIsSlow(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timeCh := lease.keepAliveWorker(ctx, time.Second)

	sendKeepAliveResponse(t, keepAliveCh, 1)
	sendKeepAliveResponse(t, keepAliveCh, 5)
	sendKeepAliveResponse(t, keepAliveCh, 30)

	select {
	case expire, ok := <-timeCh:
		re.True(ok)
		re.Greater(time.Until(expire), 20*time.Second)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for keepalive expire time")
	}
}

func newTestLeaseWithKeepAliveCh(keepAliveCh chan *clientv3.LeaseKeepAliveResponse, leaseTimeout time.Duration) *Lease {
	lease := &Lease{
		Purpose:      "test_lease",
		lease:        &fakeLease{keepAliveCh: keepAliveCh},
		leaseTimeout: leaseTimeout,
	}
	lease.ID.Store(clientv3.LeaseID(1))
	lease.expireTime.Store(time.Now().Add(-time.Second))
	return lease
}

func sendKeepAliveResponse(t *testing.T, keepAliveCh chan<- *clientv3.LeaseKeepAliveResponse, ttl int64) {
	t.Helper()
	select {
	case keepAliveCh <- &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(1), TTL: ttl}:
	case <-time.After(time.Second):
		t.Fatal("timed out sending keepalive response")
	}
}

func requireClosedTimeCh(t *testing.T, timeCh <-chan time.Time) {
	t.Helper()
	select {
	case _, ok := <-timeCh:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for keepalive worker to exit")
	}
}

type fakeLease struct {
	keepAliveCh  chan *clientv3.LeaseKeepAliveResponse
	keepAliveErr error
}

func (*fakeLease) Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
	return nil, nil
}

func (*fakeLease) Revoke(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return nil, nil
}

func (*fakeLease) TimeToLive(context.Context, clientv3.LeaseID, ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return nil, nil
}

func (*fakeLease) Leases(context.Context) (*clientv3.LeaseLeasesResponse, error) {
	return nil, nil
}

func (l *fakeLease) KeepAlive(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return l.keepAliveCh, l.keepAliveErr
}

func (*fakeLease) KeepAliveOnce(context.Context, clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (*fakeLease) Close() error {
	return nil
}
