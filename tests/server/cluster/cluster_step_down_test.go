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

package cluster_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

// TestPDLeaderCancelsClusterJobsBeforeBlockingCleanup asserts the ordering
// inside the step-down path that concerns the RaftCluster: its background jobs
// are told to stop before anything that can block for an unbounded time.
//
// Previously the jobs used the server context, and RaftCluster.Stop was the
// first operation to cancel them, from a defer after the lease teardown in
// Member.Resign. Five of those jobs write to etcd with no leader guard through
// the health-checked client, so a member whose step-down was blocked kept
// writing into the healthy quorum for as long as the block lasted.
func TestPDLeaderCancelsClusterJobsBeforeBlockingCleanup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// One member is enough: everything asserted below is this member's own
	// in-memory state, and no peer has to take over for it to mean what it says.
	tc, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer tc.Destroy()
	re.NoError(tc.RunInitialServers())

	leaderName := tc.WaitLeader()
	re.NotEmpty(leaderName)
	// A cluster that is not bootstrapped has no background jobs to speak of.
	re.NoError(tc.GetServer(leaderName).BootstrapCluster())
	leaderServer := tc.GetServer(leaderName).GetServer()
	re.True(leaderServer.IsServing())
	// Take the cluster now. GetRaftCluster returns nil once the cluster is out
	// of service, and being out of service is the state asserted below.
	rc := leaderServer.GetRaftCluster()
	re.NotNil(rc)
	re.True(rc.IsRunning())
	// Without region heartbeats the coordinator never leaves its preparation
	// loop, and the scheduler paths this is about are never live. Mark it
	// prepared and wait for the schedulers to come up.
	rc.SetPrepared()
	testutil.Eventually(re, func() bool {
		return len(rc.GetSchedulers()) > 0
	})

	// The lease this member currently holds. Its ID pins the assertion below to
	// the term being given up here rather than to a campaign in flight.
	oldLease := leaderServer.GetMember().GetLeadership().GetLease()
	re.NotNil(oldLease)
	oldLeaseID := oldLease.GetID()
	re.NotEqual(clientv3.NoLease, oldLeaseID)

	// Stand in for the part of the cleanup that can block. Everything in
	// Lease.Close past this point either talks to the local etcd or logs, and on
	// a stalled volume neither of those returns.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/blockLeaseClose",
		fmt.Sprintf("return(\"leader election@%s\")", leaderName)))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/blockLeaseClose"))
	}()
	// Fail renewals so the local deadline is what ends the term.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/keepAliveFailed",
		fmt.Sprintf("return(\"leader election@%s\")", leaderName)))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/keepAliveFailed"))
	}()

	// Wait for the instant inside the blocked Lease.Close. The conjunction is
	// the one TestPDLeaderClearsIdentityBeforeBlockingCleanup spells out: the
	// leader is nil, the leader value is still set (Leadership.Reset clears it
	// only after Lease.Close returns, so the cleanup is provably still
	// blocked), the lease reads as expired, and it is still the same lease.
	testutil.Eventually(re, func() bool {
		m := leaderServer.GetMember()
		return m.GetLeader() == nil &&
			m.GetLeadership().GetLeaderValue() != "" &&
			!m.GetLeadership().Check() &&
			m.GetLeadership().GetLease().GetID() == oldLeaseID
	}, testutil.WithWaitFor(30*time.Second))

	// The cluster must be out of service while the close is still blocked. An
	// Eventually rather than an immediate assertion only because Member.Resign
	// has a second caller - the TSO allocator resigns on an update failure -
	// which could in principle reach the conjunction above a leader tick before
	// resetLeader does; the two checks repeated inside keep the close provably
	// blocked throughout. A cancel placed after Member.Resign fails here.
	testutil.Eventually(re, func() bool {
		m := leaderServer.GetMember()
		re.NotEmpty(m.GetLeadership().GetLeaderValue())
		re.Equal(oldLeaseID, m.GetLeadership().GetLease().GetID())
		return !rc.IsRunning() && rc.Context() == nil
	}, testutil.WithWaitFor(time.Second))
	// And the jobs must actually have exited, not merely been flagged: the
	// eleven top-level jobs behind Wait, then the coordinator and schedulers,
	// which are where the scheduler-config writes and operator dispatch live.
	// The block lasts long enough for all of them to. A cancel that flips the
	// flag but leaves the context alive fails here.
	exited := make(chan struct{})
	go func() {
		rc.Wait()
		rc.GetCoordinator().GetWaitGroup().Wait()
		rc.GetCoordinator().GetSchedulersController().Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		re.FailNow("background jobs still running while the step-down is blocked")
	}
}

// TestBootstrapOverlapsStepDown exercises the Bootstrap RPC and real campaign
// teardown, both before Start and while Start holds the cluster lock.
func TestBootstrapOverlapsStepDown(t *testing.T) {
	for _, beforeStart := range []bool{true, false} {
		name := "during-start"
		pausePoint := "github.com/tikv/pd/server/cluster/bootstrapBeforeClusterStarted"
		if beforeStart {
			name = "before-start"
			pausePoint = "github.com/tikv/pd/server/bootstrapBeforeClusterStart"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tc, err := tests.NewTestCluster(ctx, 1)
			re.NoError(err)
			defer tc.Destroy()
			re.NoError(tc.RunInitialServers())
			leaderName := tc.WaitLeader()
			re.NotEmpty(leaderName)
			ts := tc.GetServer(leaderName)
			s := ts.GetServer()
			rc := s.DirectlyGetRaftCluster()
			re.False(rc.IsRunning())

			paused := make(chan context.Context, 1)
			resume := make(chan struct{})
			stopEntered := make(chan struct{})
			stopped := make(chan struct{})
			finishStop := make(chan struct{})
			var pauseOnce, stopOnce, stoppedOnce sync.Once
			resumeStart := sync.OnceFunc(func() { close(resume) })
			resumeStop := sync.OnceFunc(func() { close(finishStop) })
			// Release paused goroutines and cancel their parent even on failure.
			defer func() {
				cancel()
				resumeStart()
				resumeStop()
			}()
			enableHook := func(name string, fn any) {
				re.NoError(failpoint.EnableCall(name, fn))
				t.Cleanup(func() { re.NoError(failpoint.Disable(name)) })
			}
			enableHook(pausePoint, func(jobCtx context.Context) {
				pauseOnce.Do(func() {
					paused <- jobCtx
					<-resume
				})
			})
			enableHook("github.com/tikv/pd/server/cluster/stopClusterAfterCancel", func() {
				stopOnce.Do(func() { close(stopEntered) })
			})
			enableHook("github.com/tikv/pd/server/raftClusterStopped", func() {
				stoppedOnce.Do(func() {
					close(stopped)
					<-finishStop
				})
			})
			bootstrapDone := make(chan error, 1)
			go func() { bootstrapDone <- ts.BootstrapCluster() }()
			var jobCtx context.Context
			select {
			case jobCtx = <-paused:
			case <-time.After(20 * time.Second):
				t.Fatal("bootstrap did not reach the pause point")
			}
			const exitCampaign = "github.com/tikv/pd/server/exitCampaignLeader"
			re.NoError(failpoint.Enable(exitCampaign, fmt.Sprintf("return(\"%d\")", s.GetMember().ID())))
			t.Cleanup(func() { re.NoError(failpoint.Disable(exitCampaign)) })
			select {
			case <-stopEntered:
			case <-time.After(20 * time.Second):
				t.Fatal("step-down did not reach Stop")
			}
			// Cancellation must reach even an unpublished cluster while Start
			// still holds the lock. Waiting for Stop's lock is too late.
			re.ErrorIs(jobCtx.Err(), context.Canceled)
			if !beforeStart {
				resumeStart()
			}
			select {
			case <-stopped:
			case <-time.After(20 * time.Second):
				t.Fatal("Stop did not finish reclaiming cluster jobs")
			}
			re.False(s.IsServing())
			re.False(rc.IsRunning())
			re.Nil(rc.Context())
			re.False(rc.IsSchedulingControllerRunning())
			re.False(s.GetTSOAllocator().IsInitialize())

			// The next campaign loads the metadata already persisted by the
			// bootstrap request and must get a fresh, usable context.
			re.NoError(failpoint.Disable(exitCampaign))
			resumeStop()
			re.Equal(leaderName, tc.WaitLeader())
			testutil.Eventually(re, rc.IsRunning)
			newCtx := rc.Context()
			re.NotNil(newCtx)
			re.NoError(newCtx.Err())
			// A request delayed across the whole step-down must still use its
			// old, cancelled term rather than restart or cancel the new term.
			resumeStart()
			select {
			case err := <-bootstrapDone:
				re.ErrorContains(err, context.Canceled.Error())
			case <-time.After(20 * time.Second):
				t.Fatal("bootstrap did not return after its term ended")
			}
			re.Equal(newCtx, rc.Context())
			re.NoError(newCtx.Err())
			re.True(rc.IsRunning())
		})
	}
}
