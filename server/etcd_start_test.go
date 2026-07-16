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

package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

const (
	testCheckInterval     = 5 * time.Millisecond
	testNoProgressTimeout = 50 * time.Millisecond
)

// constApplied returns an appliedIndex getter that never advances, simulating a
// hung etcd that makes no apply progress.
func constApplied(v uint64) func() uint64 {
	return func() uint64 { return v }
}

// advancingApplied returns an appliedIndex getter that advances on every call,
// simulating a healthy etcd that keeps applying its raft log.
func advancingApplied() func() uint64 {
	var idx atomic.Uint64
	return func() uint64 { return idx.Add(1) }
}

func TestWaitEtcdReadyProgress(t *testing.T) {
	t.Run("ready immediately", func(t *testing.T) {
		re := require.New(t)
		ready := make(chan struct{})
		close(ready)
		err := waitEtcdReadyProgress(context.Background(), ready, constApplied(0),
			testCheckInterval, testNoProgressTimeout)
		re.NoError(err)
	})

	t.Run("keeps waiting while applied index advances", func(t *testing.T) {
		re := require.New(t)
		// ready only fires well after the no-progress timeout would have elapsed;
		// a fixed deadline would have killed this, but steady apply progress must
		// keep the wait alive.
		ready := make(chan struct{})
		go func() {
			time.Sleep(4 * testNoProgressTimeout)
			close(ready)
		}()
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), ready, advancingApplied(),
			testCheckInterval, testNoProgressTimeout)
		re.NoError(err)
		// It must have outlived the no-progress window instead of bailing at it.
		re.GreaterOrEqual(time.Since(start), 3*testNoProgressTimeout)
	})

	t.Run("gives up when no apply progress", func(t *testing.T) {
		re := require.New(t)
		// ready never fires and applied index is stuck: a genuine hang.
		start := time.Now()
		err := waitEtcdReadyProgress(context.Background(), make(chan struct{}), constApplied(7),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		re.True(errors.ErrorEqual(err, errs.ErrCancelStartEtcd))
		// It should wait roughly the whole no-progress window before giving up.
		re.GreaterOrEqual(time.Since(start), testNoProgressTimeout-testCheckInterval)
	})

	t.Run("honors ctx cancellation", func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(2 * testCheckInterval)
			cancel()
		}()
		start := time.Now()
		// applied index advances, so only ctx cancellation can end the wait.
		err := waitEtcdReadyProgress(ctx, make(chan struct{}), advancingApplied(),
			testCheckInterval, testNoProgressTimeout)
		re.Error(err)
		re.True(errors.ErrorEqual(err, errs.ErrCancelStartEtcd))
		// Cancellation returns before the no-progress timeout would trigger.
		re.Less(time.Since(start), testNoProgressTimeout)
	})
}
