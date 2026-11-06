// Copyright 2026 TiKV Project Authors.
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

package tso

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

type staleUpdateElection struct {
	mockElection
	serving  atomic.Bool
	resigned chan struct{}
}

func (m *staleUpdateElection) IsServing() bool { return m.serving.Load() }
func (m *staleUpdateElection) PromoteSelf()    { m.serving.Store(true) }
func (m *staleUpdateElection) Resign() {
	m.serving.Store(false)
	close(m.resigned)
}

type staleUpdateStorage struct {
	failing atomic.Bool
	calls   atomic.Int32

	mu                  sync.Mutex
	finalFailureReached chan struct{}
	releaseFinalFailure <-chan struct{}
	nextSuccessfulSave  chan struct{}
}

func (*staleUpdateStorage) LoadTimestamp(uint32) (time.Time, error) {
	return typeutil.ZeroTime, nil
}

func (s *staleUpdateStorage) SaveTimestamp(context.Context, uint32, time.Time, *election.Leadership) error {
	if !s.failing.Load() {
		s.mu.Lock()
		nextSuccessfulSave := s.nextSuccessfulSave
		s.nextSuccessfulSave = nil
		s.mu.Unlock()
		if nextSuccessfulSave != nil {
			close(nextSuccessfulSave)
		}
		return nil
	}
	if s.calls.Add(1) == maxUpdateTSORetryCount {
		s.mu.Lock()
		finalFailureReached := s.finalFailureReached
		releaseFinalFailure := s.releaseFinalFailure
		s.mu.Unlock()
		close(finalFailureReached)
		if releaseFinalFailure != nil {
			<-releaseFinalFailure
		}
	}
	return errors.New("tso update failed")
}

func (*staleUpdateStorage) DeleteTimestamp(context.Context, uint32) error { return nil }

func (s *staleUpdateStorage) startFailures(releaseFinalFailure <-chan struct{}) <-chan struct{} {
	s.mu.Lock()
	s.finalFailureReached = make(chan struct{})
	s.releaseFinalFailure = releaseFinalFailure
	finalFailureReached := s.finalFailureReached
	s.mu.Unlock()
	s.calls.Store(0)
	s.failing.Store(true)
	return finalFailureReached
}

func (s *staleUpdateStorage) stopFailures() {
	s.failing.Store(false)
}

func (s *staleUpdateStorage) notifyOnNextSuccessfulSave() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSuccessfulSave = make(chan struct{})
	return s.nextSuccessfulSave
}

func isSignaled(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

// TestAllocatorIgnoresStaleUpdateFailure is a regression test for #11086.
func TestAllocatorIgnoresStaleUpdateFailure(t *testing.T) {
	re := require.New(t)

	member := &staleUpdateElection{
		resigned: make(chan struct{}),
	}
	member.serving.Store(true)
	storage := &staleUpdateStorage{}
	allocator := NewAllocator(context.Background(), 11086, member, storage, &TestServiceConfig{
		TSOUpdatePhysicalInterval: time.Millisecond,
		TSOSaveInterval:           2 * time.Millisecond,
		MaxResetTSGap:             time.Hour,
	})
	defer allocator.Close()

	re.NoError(allocator.Initialize())
	releaseStaleFailure := make(chan struct{}, 1)
	defer close(releaseStaleFailure)
	staleFailure := storage.startFailures(releaseStaleFailure)
	testutil.Eventually(re, func() bool { return isSignaled(staleFailure) })

	// Reinitialize and promote before the old UpdateTSO returns its final error.
	member.serving.Store(false)
	storage.stopFailures()
	re.NoError(allocator.Initialize())
	nextSuccessfulSave := storage.notifyOnNextSuccessfulSave()
	member.PromoteSelf()
	releaseStaleFailure <- struct{}{}
	// The updater must handle the stale error before it can perform this save.
	testutil.Eventually(re, func() bool {
		return isSignaled(nextSuccessfulSave) || isSignaled(member.resigned)
	})
	re.False(isSignaled(member.resigned), "a stale update failure resigned the reinitialized allocator")
	re.True(allocator.IsInitialize(), "a stale update failure reset the reinitialized timestamp")

	_, err := allocator.GenerateTSO(context.Background(), 1)
	re.NoError(err)

	currentFailure := storage.startFailures(nil)
	testutil.Eventually(re, func() bool { return isSignaled(currentFailure) })
	testutil.Eventually(re, func() bool { return isSignaled(member.resigned) })
	re.False(allocator.IsInitialize())
}

// TestClosingAllocatorReturnsNotLeader checks that a closing allocator, which resets its timestamp
// before it resigns, fails the requests with the leader-change error, whether it starts to close
// before or while a request generates the TSO, and refuses to be initialized again.
func TestClosingAllocatorReturnsNotLeader(t *testing.T) {
	re := require.New(t)
	newAllocator := func() (*Allocator, *staleUpdateElection) {
		member := &staleUpdateElection{resigned: make(chan struct{})}
		member.serving.Store(true)
		allocator := NewAllocator(context.Background(), 1, member, &staleUpdateStorage{}, &TestServiceConfig{
			TSOUpdatePhysicalInterval: time.Millisecond,
			TSOSaveInterval:           2 * time.Millisecond,
			MaxResetTSGap:             time.Hour,
		})
		re.NoError(allocator.Initialize())
		_, err := allocator.GenerateTSO(context.Background(), 1)
		re.NoError(err)
		return allocator, member
	}

	// Closed before the request: the timestamp is reset while the member is still the primary.
	allocator, _ := newAllocator()
	allocator.Close()
	re.False(allocator.IsInitialize())
	start := time.Now()
	_, err := allocator.GenerateTSO(context.Background(), 1)
	re.ErrorContains(err, "not leader")
	// It fails at once instead of waiting for the timestamp to be initialized again.
	re.Less(time.Since(start), 200*time.Millisecond)
	// The first request of a keyspace group initializes the allocator, which a closed
	// allocator must refuse even though the member is still the primary.
	re.ErrorContains(allocator.Initialize(), "not leader")
	re.False(allocator.IsInitialize())
	re.Zero(promtestutil.ToFloat64(allocator.tsoAllocatorRoleGauge))

	// Closed and resigned after the serving check of the request.
	allocator, member := newAllocator()
	defer allocator.Close()
	fp := "github.com/tikv/pd/pkg/tso/afterGenerateTSOServingCheck"
	re.NoError(failpoint.EnableCall(fp, func() {
		allocator.Close()
		member.Resign()
	}))
	defer func() { re.NoError(failpoint.Disable(fp)) }()
	_, err = allocator.GenerateTSO(context.Background(), 1)
	re.ErrorContains(err, "not leader")
}
