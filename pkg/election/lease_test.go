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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

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
	ch := lease.keepAliveWorker(ctx, 2*time.Second, nil)
	time.Sleep(2 * time.Second)
	<-ch
	re.NoError(lease.Close())
}

func TestLeaseKeepAliveGuard(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientLease := &keepAliveOnceCountingLease{}
	lease := &Lease{
		purpose:      "test_lease_guard",
		lease:        clientLease,
		leaseTimeout: time.Second,
	}
	lease.setID(1)
	lease.expireTime.Store(time.Now().Add(time.Second))
	re.False(lease.IsExpired())

	var guardCalls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		lease.runKeepAlive(ctx, func() bool {
			guardCalls.Add(1)
			return false
		})
	}()
	re.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	re.True(lease.IsExpired())
	re.Positive(guardCalls.Load())
	re.Zero(clientLease.calls.Load())
}

func TestLeaseKeepAliveGuardStopsAfterRenewal(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientLease := &keepAliveOnceCountingLease{}
	lease := &Lease{
		purpose:      "test_lease_guard_transition",
		lease:        clientLease,
		leaseTimeout: 900 * time.Millisecond,
		metrics:      newLeaseMetrics("test_lease_guard_transition"),
	}
	lease.setID(1)
	initialExpireTime := time.Now().Add(100 * time.Millisecond)
	lease.expireTime.Store(initialExpireTime)

	var guardCalls atomic.Int32
	rejectRenewal := make(chan struct{})
	var rejectOnce sync.Once
	reject := func() {
		rejectOnce.Do(func() {
			close(rejectRenewal)
		})
	}
	defer reject()

	done := make(chan struct{})
	go func() {
		defer close(done)
		lease.runKeepAlive(ctx, func() bool {
			if guardCalls.Add(1) == 1 {
				return true
			}
			<-rejectRenewal
			return false
		})
	}()

	// Confirm that the lease was renewed before changing the guard result.
	re.Eventually(func() bool {
		return clientLease.calls.Load() == 1 && lease.loadExpireTime().After(initialExpireTime)
	}, time.Second, 10*time.Millisecond)
	reject()
	re.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	re.True(lease.IsExpired())
	re.True(lease.loadExpireTime().IsZero())
	re.GreaterOrEqual(guardCalls.Load(), int32(2))
	re.Equal(int32(1), clientLease.calls.Load())
}

func TestKeepAliveResponseDoesNotReviveResetLease(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientLease := &keepAliveOnceCountingLease{}
	lease := &Lease{
		purpose:      "test_keepalive_response_after_reset",
		lease:        clientLease,
		leaseTimeout: 30 * time.Second,
		metrics:      newLeaseMetrics("test_keepalive_response_after_reset"),
	}
	lease.setID(1)
	lease.expireTime.Store(time.Now().Add(time.Minute))

	loadedOldExpireTime := make(chan struct{})
	resumeStore := make(chan struct{})
	var injectOnce sync.Once
	var resumeOnce sync.Once
	resume := func() {
		resumeOnce.Do(func() {
			close(resumeStore)
		})
	}
	defer resume()

	failpointName := "github.com/tikv/pd/pkg/election/beforeCompareAndSwapExpireTime"
	re.NoError(failpoint.EnableCall(failpointName, func() {
		injectOnce.Do(func() {
			close(loadedOldExpireTime)
			<-resumeStore
		})
	}))
	defer func() {
		re.NoError(failpoint.Disable(failpointName))
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		lease.runKeepAlive(ctx, nil)
	}()

	re.Eventually(func() bool {
		select {
		case <-loadedOldExpireTime:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	lease.expireTime.Store(typeutil.ZeroTime)
	resume()
	re.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	re.True(lease.IsExpired())
	re.True(lease.loadExpireTime().IsZero())
	re.Equal(int32(1), clientLease.calls.Load())
}

type keepAliveOnceCountingLease struct {
	clientv3.Lease
	calls atomic.Int32
}

func (l *keepAliveOnceCountingLease) KeepAliveOnce(context.Context, clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	l.calls.Add(1)
	return &clientv3.LeaseKeepAliveResponse{TTL: 1}, nil
}
