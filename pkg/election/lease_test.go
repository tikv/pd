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
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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

func TestKeepAliveResponseIntervalRecorded(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	lease := newTestLeaseWithResponses(purpose, 300*time.Millisecond, 30, 30)
	beforeCount := keepAliveResponseIntervalCount(t, purpose)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	testutil.Eventually(re, func() bool {
		return keepAliveResponseIntervalCount(t, purpose) >= beforeCount+1
	})
	cancel()
	testutil.Eventually(re, func() bool {
		return isClosed(done)
	})
}

func TestRenewalFailureInvalidTTL(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	lease := newTestLeaseWithResponses(purpose, time.Hour, 0)
	beforeFailure := renewalFailureValue(purpose, reasonInvalidTTL)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	testutil.Eventually(re, func() bool {
		return renewalFailureValue(purpose, reasonInvalidTTL) >= beforeFailure+1
	})
	cancel()
	testutil.Eventually(re, func() bool {
		return isClosed(done)
	})
}

func TestRenewalFailureLeaseExpired(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	lease := newTestLeaseWithResponses(purpose, 20*time.Millisecond)
	beforeFailure := renewalFailureValue(purpose, reasonLeaseExpired)

	done := make(chan struct{})
	go func() {
		lease.KeepAlive(t.Context())
		close(done)
	}()

	testutil.Eventually(re, func() bool {
		return isClosed(done)
	})
	re.Equal(beforeFailure+1, renewalFailureValue(purpose, reasonLeaseExpired))
	re.LessOrEqual(localTTLRemainingValue(purpose), float64(0))
}

func TestNoFailureOnContextCancel(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	lease := newTestLeaseWithResponses(purpose, time.Hour)
	beforeFailures := map[string]float64{
		reasonInvalidTTL:   renewalFailureValue(purpose, reasonInvalidTTL),
		reasonLeaseExpired: renewalFailureValue(purpose, reasonLeaseExpired),
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	cancel()
	testutil.Eventually(re, func() bool {
		return isClosed(done)
	})
	for reason, value := range beforeFailures {
		re.Equal(value, renewalFailureValue(purpose, reason))
	}
}

func TestLocalTTLGaugeUpdates(t *testing.T) {
	re := require.New(t)
	purpose := t.Name()
	lease := newTestLeaseWithResponses(purpose, time.Hour, 30)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		lease.KeepAlive(ctx)
		close(done)
	}()

	testutil.Eventually(re, func() bool {
		return localTTLRemainingValue(purpose) > 0
	})
	cancel()
	testutil.Eventually(re, func() bool {
		return isClosed(done)
	})
}

func newTestLeaseWithResponses(purpose string, leaseTimeout time.Duration, ttls ...int64) *Lease {
	lease := &Lease{
		Purpose:      purpose,
		lease:        &fakeLease{ttls: append([]int64(nil), ttls...)},
		leaseTimeout: leaseTimeout,
	}
	lease.ID.Store(clientv3.LeaseID(1))
	lease.expireTime.Store(time.Now().Add(-time.Second))
	lease.initMetrics()
	return lease
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func renewalFailureValue(purpose, reason string) float64 {
	return promtestutil.ToFloat64(renewalFailureTotal.WithLabelValues(purpose, reason))
}

func localTTLRemainingValue(purpose string) float64 {
	return promtestutil.ToFloat64(localTTLRemaining.WithLabelValues(purpose))
}

func keepAliveResponseIntervalCount(t *testing.T, purpose string) uint64 {
	t.Helper()
	re := require.New(t)
	observer := keepAliveResponseInterval.WithLabelValues(purpose)
	metric, ok := observer.(prometheus.Metric)
	re.True(ok)
	metricPB := &dto.Metric{}
	re.NoError(metric.Write(metricPB))
	return metricPB.GetHistogram().GetSampleCount()
}

type fakeLease struct {
	ttls []int64
	err  error
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

func (*fakeLease) KeepAlive(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (l *fakeLease) KeepAliveOnce(context.Context, clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	if l.err != nil {
		return nil, l.err
	}
	if len(l.ttls) == 0 {
		return nil, errors.New("no fake keepalive response")
	}
	ttl := l.ttls[0]
	l.ttls = l.ttls[1:]
	return &clientv3.LeaseKeepAliveResponse{ID: clientv3.LeaseID(1), TTL: ttl}, nil
}

func (*fakeLease) Close() error {
	return nil
}
