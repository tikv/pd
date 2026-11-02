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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
)

type recoveringTSOServiceProvider struct {
	*mockTSOServiceProvider
	update func(context.Context) bool
}

func (p *recoveringTSOServiceProvider) updateConnectionCtxs(ctx context.Context) bool {
	return p.update(ctx)
}

func startRecoveryDispatcher(t *testing.T, provider tsoServiceProvider) (*tsoDispatcher, *Request) {
	t.Helper()
	td := newTSODispatcher(t.Context(), provider)
	var wg sync.WaitGroup
	wg.Add(1)
	go td.handleDispatcher(&wg)
	t.Cleanup(func() {
		td.close()
		wg.Wait()
	})
	req := &Request{
		requestCtx: td.ctx,
		clientCtx:  td.ctx,
		done:       make(chan error, 1),
		start:      time.Now(),
		pool:       &sync.Pool{},
	}
	td.push(req)
	return td, req
}

func TestDispatcherUsesBackgroundStream(t *testing.T) {
	for _, duringRefresh := range []bool{true, false} {
		name := "after-refresh"
		if duringRefresh {
			name = "during-refresh"
		}
		t.Run(name, func(t *testing.T) {
			refreshStarted := make(chan struct{})
			refreshFinished := make(chan struct{})
			releaseRefresh := make(chan struct{})
			var once sync.Once
			provider := &recoveringTSOServiceProvider{
				mockTSOServiceProvider: newMockTSOServiceProvider(opt.NewOption(), nil),
				update: func(ctx context.Context) bool {
					once.Do(func() { close(refreshStarted) })
					select {
					case <-ctx.Done():
					case <-releaseRefresh:
					}
					select {
					case <-refreshFinished:
					default:
						close(refreshFinished)
					}
					return false
				},
			}
			td, req := startRecoveryDispatcher(t, provider)
			select {
			case <-refreshStarted:
			case <-time.After(time.Second):
				t.Fatal("dispatcher did not attempt stream recovery")
			}
			if !duringRefresh {
				close(releaseRefresh)
				select {
				case <-refreshFinished:
				case <-time.After(time.Second):
					t.Fatal("stream recovery did not return")
				}
			}

			// A concurrent background updater has established the new leader's stream.
			// The foreground attempt still reports failure, as it used the old leader.
			ctx, cancel := context.WithCancel(td.ctx)
			inner := newMockTSOStreamImpl(ctx, resultModeGenerated)
			stream := newTSOStream(ctx, mockStreamURL, inner)
			require.True(t, provider.conCtxMgr.CleanAllAndStore(ctx, cancel, mockStreamURL, stream))
			if duringRefresh {
				close(releaseRefresh)
			}

			// Readiness must resume this request before the 500ms polling fallback.
			physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
			require.NoError(t, err)
			require.Positive(t, physical)
		})
	}
}

func TestDispatcherFindsStreamPublishedBeforeSubscription(t *testing.T) {
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: newMockTSOServiceProvider(opt.NewOption(), nil),
		update:                 func(context.Context) bool { return false },
	}
	published := make(chan bool, 1)
	var once sync.Once
	const injection = "github.com/tikv/pd/client/clients/tso/beforeSubscribeTSOStreamUpdates"
	require.NoError(t, failpoint.EnableCall(injection, func() {
		once.Do(func() {
			// Publish after the first empty lookup but before the subscription. There
			// is no waiter yet, so only rechecking availability can observe this store.
			ctx, cancel := context.WithCancel(t.Context())
			stream := newTSOStream(ctx, mockStreamURL, newMockTSOStreamImpl(ctx, resultModeGenerated))
			stored := provider.conCtxMgr.CleanAllAndStore(ctx, cancel, mockStreamURL, stream)
			if !stored {
				cancel()
			}
			published <- stored
		})
	}))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(injection)) })
	_, req := startRecoveryDispatcher(t, provider)
	physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
	require.NoError(t, err)
	require.True(t, <-published)
	require.Positive(t, physical)
}

func TestDispatcherStreamRecoveryPollingFallback(t *testing.T) {
	var attempts atomic.Int32
	mock := newMockTSOServiceProvider(opt.NewOption(), nil)
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: mock,
		update: func(ctx context.Context) bool {
			if attempts.Add(1) == 1 {
				return false
			}
			return mock.updateConnectionCtxs(ctx)
		},
	}
	_, req := startRecoveryDispatcher(t, provider)
	physical, _, err := req.waitTimeout(2 * time.Second)
	require.NoError(t, err)
	require.Positive(t, physical)
	require.Equal(t, int32(2), attempts.Load())
}

func TestDispatcherStreamRecoveryShutdown(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: newMockTSOServiceProvider(opt.NewOption(), nil),
		update: func(context.Context) bool {
			once.Do(func() { close(started) })
			return false
		},
	}
	td, req := startRecoveryDispatcher(t, provider)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not attempt stream recovery")
	}
	td.close()
	_, _, err := req.waitTimeout(time.Second)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDispatcherStreamRecoveryTimeout(t *testing.T) {
	option := opt.NewOption()
	option.Timeout = 50 * time.Millisecond
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: newMockTSOServiceProvider(option, nil),
		update:                 func(context.Context) bool { return false },
	}
	td, req := startRecoveryDispatcher(t, provider)
	_, _, err := req.waitTimeout(time.Second)
	require.Error(t, err)
	require.True(t, errs.ErrClientCreateTSOStream.Equal(err), "%v", err)
	require.NoError(t, td.ctx.Err())
}
