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
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/election"
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
	failing     atomic.Bool
	calls       atomic.Int32
	thirdFailed chan struct{}
}

func (*staleUpdateStorage) LoadTimestamp(uint32) (time.Time, error) {
	return typeutil.ZeroTime, nil
}

func (s *staleUpdateStorage) SaveTimestamp(context.Context, uint32, time.Time, *election.Leadership) error {
	if !s.failing.Load() {
		return nil
	}
	if s.calls.Add(1) == maxUpdateTSORetryCount {
		close(s.thirdFailed)
	}
	return errors.New("tso update failed")
}

func (*staleUpdateStorage) DeleteTimestamp(context.Context, uint32) error { return nil }

func (s *staleUpdateStorage) startFailures() <-chan struct{} {
	s.calls.Store(0)
	s.thirdFailed = make(chan struct{})
	s.failing.Store(true)
	return s.thirdFailed
}

func (s *staleUpdateStorage) stopFailures() {
	s.failing.Store(false)
}

// TestAllocatorIgnoresStaleUpdateFailure is a regression test for #11086.
func TestAllocatorIgnoresStaleUpdateFailure(t *testing.T) {
	re := require.New(t)
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

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
	staleFailure := storage.startFailures()
	select {
	case <-staleFailure:
	case <-time.After(2 * time.Second):
		t.Fatal("old UpdateTSO did not reach its final failure")
	}

	member.serving.Store(false)
	storage.stopFailures()
	re.NoError(allocator.Initialize())
	time.Sleep(2 * updateTSORetryInterval)
	re.True(allocator.IsInitialize(), "a stale update failure reset the reinitialized timestamp")

	member.PromoteSelf()
	_, err := allocator.GenerateTSO(context.Background(), 1)
	re.NoError(err)

	currentFailure := storage.startFailures()
	select {
	case <-currentFailure:
	case <-time.After(2 * time.Second):
		t.Fatal("current UpdateTSO did not reach its final failure")
	}
	select {
	case <-member.resigned:
	case <-time.After(2 * time.Second):
		t.Fatal("current lease was not resigned after UpdateTSO failed")
	}
	re.False(allocator.IsInitialize())
}
