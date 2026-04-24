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
	"github.com/tikv/pd/pkg/utils/testutil"
)

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
	go lease1.KeepAlive(t.Context())

	// Wait for a timeout.
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	re.False(lease1.IsExpired())
	// Close and wait for a timeout.
	re.NoError(lease1.Close())
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)
	re.True(lease1.IsExpired())
}

// TestLeaseKeepAlive runs KeepAlive against a real etcd cluster and verifies
// that expireTime keeps advancing while the loop is active.
func TestLeaseKeepAlive(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	lease := NewLease(client, "test_lease")
	re.NoError(lease.Grant(defaultLeaseTimeout))

	go lease.KeepAlive(t.Context())

	origExpire := lease.getExpireTime()
	testutil.Eventually(re, func() bool {
		return lease.getExpireTime().After(origExpire)
	})

	re.NoError(lease.Close())
}

func TestLeaseKeepAliveRefreshesExpireTime(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, 5*time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	sendKeepAliveResponse(t, keepAliveCh, 30)
	testutil.Eventually(re, func() bool {
		expire := lease.getExpireTime()
		return time.Until(expire) > 20*time.Second
	})

	cancel()
	testutil.Eventually(re, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestLeaseKeepAliveExitsWhenWorkerChannelCloses(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, time.Hour)
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(t.Context())
		close(done)
	}()

	close(keepAliveCh)
	testutil.Eventually(re, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

// TestLeaseKeepAliveExitsOnInvalidTTL verifies that a keepalive response with
// non-positive TTL (etcd signalling that the lease is no longer valid) causes
// KeepAlive to return so the election owner can step down.
func TestLeaseKeepAliveExitsOnInvalidTTL(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, 5*time.Second)

	done := make(chan struct{})
	go func() {
		lease.KeepAlive(t.Context())
		close(done)
	}()

	sendKeepAliveResponse(t, keepAliveCh, 0)
	testutil.Eventually(re, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

// TestLeaseKeepAliveKeepsExpireMonotonic verifies that when a response with a
// smaller TTL arrives after a larger one, expireTime is not pulled backwards.
// This protects against out-of-order or stale renewals overriding a good
// estimate.
func TestLeaseKeepAliveKeepsExpireMonotonic(t *testing.T) {
	re := require.New(t)
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	lease := newTestLeaseWithKeepAliveCh(keepAliveCh, time.Hour)
	go lease.KeepAlive(t.Context())

	// The large TTL lands first and should push expireTime well into the future.
	sendKeepAliveResponse(t, keepAliveCh, 30)
	testutil.Eventually(re, func() bool {
		return time.Until(lease.getExpireTime()) > 20*time.Second
	})
	beforeSmall := lease.getExpireTime()

	// A subsequent smaller TTL must not regress expireTime, because its absolute
	// deadline is earlier than the stored maxExpire.
	sendKeepAliveResponse(t, keepAliveCh, 1)
	// Give the loop a beat to process and (incorrectly) store the smaller expire
	// if the monotonicity guard were missing.
	time.Sleep(100 * time.Millisecond)
	afterSmall := lease.getExpireTime()
	re.False(afterSmall.Before(beforeSmall))
}

func newTestLeaseWithKeepAliveCh(keepAliveCh chan *clientv3.LeaseKeepAliveResponse, leaseTimeout time.Duration) *Lease {
	lease := &Lease{
		Purpose:      "test_lease",
		lease:        &fakeLease{keepAliveCh: keepAliveCh},
		leaseTimeout: leaseTimeout,
		metrics:      newLeaseMetrics("test_lease"),
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
