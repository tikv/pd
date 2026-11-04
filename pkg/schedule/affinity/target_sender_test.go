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

package affinity

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
)

// TestTargetSenderBacksOffUntilPDRecovers checks that the Regions of a group send
// one target at a time while PD fails the writes, that the group backs off after
// each failure, and that a Region observed after PD recovers writes the target.
func TestTargetSenderBacksOffUntilPDRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		// The stores stay available while the test advances time.
		manager := newTargetTestManagerWithContext(t, t.Context())
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		var available atomic.Bool
		writer := &fakeTargetWriter{reply: func(target GroupTarget) ([]string, error) {
			if !available.Load() {
				return nil, errors.New("PD is unavailable")
			}
			return []string{target.Expected.ID}, nil
		}}
		manager.SetTargetWriter(writer)
		// observe checks three healthy Regions of the group with different leaders.
		observe := func() {
			for i, leader := range []uint64{1, 2, 3} {
				voters := []uint64{leader}
				for _, id := range []uint64{1, 2, 3} {
					if id != leader {
						voters = append(voters, id)
					}
				}
				region := generateRegionForTest(uint64(10+i), voters, nonOverlappingRange)
				manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
			}
			synctest.Wait()
		}
		failures := testutil.ToFloat64(targetWriteFailureCounter)

		// The first Region sends the target; the others are dropped.
		observe()
		re.Len(writer.sent(), 1)
		re.Equal(uint64(1), writer.sent()[0].LeaderStoreID)
		re.Equal(failures+1, testutil.ToFloat64(targetWriteFailureCounter))
		// The group backs off for 1s, 2s, 4s, 8s and then 10s after each failure.
		for i, backoff := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second} {
			time.Sleep(backoff - time.Millisecond)
			observe()
			re.Len(writer.sent(), i+1, "no target is sent during the backoff")
			time.Sleep(time.Millisecond)
			observe()
			re.Len(writer.sent(), i+2)
		}
		re.Equal(failures+7, testutil.ToFloat64(targetWriteFailureCounter))
		state := manager.GetAffinityGroupState("g")
		re.Zero(state.LeaderStoreID)
		re.False(state.AffinitySchedulingAllowed)

		// PD recovers. After the backoff, a Region writes the target, and the
		// watcher syncs it.
		available.Store(true)
		time.Sleep(10 * time.Second)
		observe()
		sent := writer.sent()
		re.Len(sent, 8)
		re.Equal(failures+7, testutil.ToFloat64(targetWriteFailureCounter))
		manager.SyncGroupFromEtcd(sent[7].result())
		state = manager.GetAffinityGroupState("g")
		re.Equal(*sent[7].result(), state.Group)
		re.True(state.AffinitySchedulingAllowed)
	})
}

// TestTargetSenderResetsBackoffAfterWrite checks that a write resets the backoff
// and that the group still waits after it, so that Regions observed before the
// watcher syncs the target do not send it again at once.
func TestTargetSenderResetsBackoffAfterWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		var fail atomic.Bool
		writer := &fakeTargetWriter{reply: func(target GroupTarget) ([]string, error) {
			if fail.Load() {
				return nil, errors.New("PD is unavailable")
			}
			return []string{target.Expected.ID}, nil
		}}
		sender := newTargetSender(writer)
		go sender.run(t.Context())
		target := GroupTarget{Expected: Group{ID: "g"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}
		send := func() bool {
			queued := sender.send(target)
			synctest.Wait()
			return queued
		}

		fail.Store(true)
		re.True(send())
		time.Sleep(time.Second)
		re.True(send())
		// Two failures in a row: the group waits 2s.
		fail.Store(false)
		time.Sleep(2 * time.Second)
		re.True(send())
		// After a write the group waits 1s, not 4s.
		re.False(send())
		time.Sleep(time.Second)
		re.True(send())
		re.Len(writer.sent(), 4)
	})
}

// TestTargetSenderBoundsGroups checks that a sender tracks at most
// maxSenderGroups groups without blocking, and forgets the groups that may send
// again when it needs room.
func TestTargetSenderBoundsGroups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		release := make(chan struct{})
		var calls atomic.Int32
		writer := &blockingTargetWriter{release: release, calls: &calls}
		sender := newTargetSender(writer)
		go sender.run(t.Context())
		target := func(i int) GroupTarget {
			return GroupTarget{Expected: Group{ID: fmt.Sprintf("g%d", i)}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}
		}

		// PD does not answer. Every group is queued or in flight, without blocking.
		for i := range maxSenderGroups {
			re.True(sender.send(target(i)))
		}
		re.False(sender.send(target(maxSenderGroups)), "no room for another group")
		synctest.Wait()
		re.Equal(int32(1), calls.Load(), "one target is in flight at a time")

		// PD answers. The groups wait after their writes, so there is still no room.
		close(release)
		synctest.Wait()
		re.Equal(int32(maxSenderGroups), calls.Load())
		re.False(sender.send(target(maxSenderGroups)))
		// Once they may send again, the sender forgets them to make room.
		time.Sleep(minTargetSendBackoff)
		re.True(sender.send(target(maxSenderGroups)))
		synctest.Wait()
		sender.mu.Lock()
		re.Len(sender.groups, 1)
		sender.mu.Unlock()
	})
}

type blockingTargetWriter struct {
	release chan struct{}
	calls   *atomic.Int32
}

func (w *blockingTargetWriter) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	w.calls.Add(1)
	select {
	case <-w.release:
		return []string{targets[0].Expected.ID}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestTargetSenderStopsWithManager checks that the sender goroutine and its write
// in flight end with the context of the Manager, without counting a failure.
// synctest.Test fails if a goroutine started in the bubble is still blocked.
func TestTargetSenderStopsWithManager(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		manager := newTargetTestManagerWithContext(t, ctx)
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		var calls atomic.Int32
		manager.SetTargetWriter(&blockingTargetWriter{release: make(chan struct{}), calls: &calls})
		manager.ObserveAvailableRegion(generateRegionForTest(10, []uint64{1, 2, 3}, nonOverlappingRange), manager.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(int32(1), calls.Load())

		failures := testutil.ToFloat64(targetWriteFailureCounter)
		cancel()
		synctest.Wait()
		re.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))
		// Observing after the Manager stops neither blocks nor sends.
		manager.ObserveAvailableRegion(generateRegionForTest(11, []uint64{2, 1, 3}, nonOverlappingRange), manager.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(int32(1), calls.Load())
	})
}
