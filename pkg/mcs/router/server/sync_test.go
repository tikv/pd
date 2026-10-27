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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestRegionSyncerRetryPreservesPendingLeaderChange(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	syncer := NewRegionSyncer(t.Context(), nil, nil, nil, "test", "")
	defer syncer.cancelFunc()
	// Leader discovery can enqueue a forced reconnect while a no-leader retry waits.
	syncer.reconnectCh <- true
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.startSyncWithLeader()
	}()
	select {
	case <-done:
	case <-time.After(3 * retryInterval):
		// Release the old implementation's blocked send before failing the test.
		<-syncer.reconnectCh
		<-done
		t.Fatal("retry blocked on a pending leader-change notification")
	}
	select {
	case force := <-syncer.reconnectCh:
		re.True(force)
	default:
		t.Fatal("retry lost the pending leader-change notification")
	}
}

func TestRegionSyncerCanceledRetryDoesNotEnqueue(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	syncer := NewRegionSyncer(t.Context(), nil, nil, nil, "test", "")
	syncer.cancelFunc()
	syncer.startSyncWithLeader()
	re.Empty(syncer.reconnectCh)
}
