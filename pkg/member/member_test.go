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

package member

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/errs"
)

func TestIsEtcdPeerConnected(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		re.Equal("/raft/probing", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()
	client := peer.Client()
	defer client.CloseIdleConnections()
	client.Timeout = time.Second
	re.True(isEtcdPeerConnected(t.Context(), client, []string{"invalid://peer", peer.URL}))
	re.False(isEtcdPeerConnected(t.Context(), client, nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	re.False(isEtcdPeerConnected(ctx, client, []string{peer.URL}))
}

func TestWaitForEtcdLeader(t *testing.T) {
	defer goleak.VerifyNone(t)
	for _, tc := range []struct {
		name   string
		term   uint64
		leader uint64
		err    error
	}{
		{"election completes", 2, 0, nil},
		{"leader clears before term applies", 1, 0, nil},
		{"election canceled", 2, 0, context.Canceled},
		{"election times out", 2, 0, context.DeadlineExceeded},
		{"target already elected", 2, 2, nil},
		{"different successor", 1, 3, errs.ErrNotLeader},
		{"ownership changed term", 2, 1, errs.ErrNotLeader},
		{"local leader waits", 1, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			status := &testRaftStatus{term: tc.term, observed: make(chan struct{}, 1)}
			status.leader.Store(tc.leader)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- waitForEtcdLeader(ctx, status, 1, 2, time.Millisecond) }()
			select {
			case <-status.observed:
			case <-ctx.Done():
				t.Fatal("waiter did not observe the initial leader state")
			}
			switch tc.err {
			case nil:
				status.leader.Store(2)
			case context.Canceled:
				cancel()
			}
			select {
			case err := <-result:
				if tc.err == nil {
					re.NoError(err)
				} else {
					re.ErrorIs(err, tc.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("waiter did not finish")
			}
		})
	}
}

type testRaftStatus struct {
	term     uint64
	leader   atomic.Uint64
	observed chan struct{}
}

func (*testRaftStatus) ID() types.ID           { return 1 }
func (*testRaftStatus) CommittedIndex() uint64 { return 0 }
func (*testRaftStatus) AppliedIndex() uint64   { return 0 }
func (s *testRaftStatus) Term() uint64         { return s.term }

func (s *testRaftStatus) Leader() types.ID {
	leader := s.leader.Load()
	// Capture the initial observation before the test changes the state.
	select {
	case s.observed <- struct{}{}:
	default:
	}
	return types.ID(leader)
}
