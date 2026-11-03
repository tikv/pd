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

package member_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func newEarlyTSOCluster(t *testing.T, count int) *tests.TestCluster {
	t.Helper()
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), count, func(conf *config.Config, _ string) {
		// Bound the future timestamp inherited at a handoff so the test can
		// observe periodic physical updates without overflowing the logical part.
		conf.TSOSaveInterval = typeutil.Duration{Duration: time.Second}
	})
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	re.NoError(cluster.GetLeaderServer().BootstrapCluster())
	return cluster
}

func pauseAfterTSOInitialization(t *testing.T, memberID uint64) (wait, resume func()) {
	t.Helper()
	return pauseAtTSOCheckpoint(t, memberID, "afterTSOInitialized")
}

func pauseAtTSOCheckpoint(t *testing.T, memberID uint64, checkpoint string) (wait, resume func()) {
	t.Helper()
	hook := "github.com/tikv/pd/server/cluster/" + checkpoint
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	resume = sync.OnceFunc(func() { close(release) })
	require.NoError(t, failpoint.EnableCall(hook, func(id uint64) {
		if id == memberID {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	}))
	t.Cleanup(func() {
		resume()
		require.NoError(t, failpoint.Disable(hook))
	})
	return func() {
		t.Helper()
		select {
		case <-reached:
		case <-time.After(30 * time.Second):
			t.Fatalf("leader initialization did not reach %s", checkpoint)
		}
	}, resume
}

func earlyTSOClient(t *testing.T, server *tests.TestServer) pdpb.PDClient {
	t.Helper()
	client, conn := testutil.MustNewGrpcClient(require.New(t), server.GetAddr())
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return client
}

func earlyTSOStream(t *testing.T, client pdpb.PDClient, forwardedHost string) pdpb.PD_TsoClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	if forwardedHost != "" {
		ctx = grpcutil.BuildForwardContext(ctx, forwardedHost)
	}
	stream, err := client.Tso(ctx)
	require.NoError(t, err)
	return stream
}

func requestEarlyTSO(t *testing.T, stream pdpb.PD_TsoClient, clusterID uint64) *pdpb.Timestamp {
	t.Helper()
	re := require.New(t)
	re.NoError(stream.Send(&pdpb.TsoRequest{Header: testutil.NewRequestHeader(clusterID), Count: 1}))
	response, err := stream.Recv()
	re.NoError(err)
	re.Nil(response.GetHeader().GetError())
	re.Equal(uint32(1), response.GetCount())
	ts := response.GetTimestamp()
	re.Positive(ts.GetPhysical())
	re.Positive(ts.GetLogical())
	return ts
}

func requireEarlyTSORejected(t *testing.T, stream pdpb.PD_TsoClient, clusterID uint64) {
	t.Helper()
	re := require.New(t)
	// The same stream previously served requests in this term. Its next request
	// must still check leadership after the lease is lost.
	re.NoError(stream.Send(&pdpb.TsoRequest{Header: testutil.NewRequestHeader(clusterID), Count: 1}))
	_, err := stream.Recv()
	re.ErrorContains(err, errs.NotLeaderErr)
}

func TestEarlyTSORejectsBeforeInitialization(t *testing.T) {
	re := require.New(t)
	cluster := newEarlyTSOCluster(t, 1)
	leader := cluster.GetLeaderServer()
	client := earlyTSOClient(t, leader)
	previous := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	wait, resume := pauseAtTSOCheckpoint(t, leader.GetServerID(), "beforeTSOInitialized")
	leader.ResetPDLeader()
	wait()
	re.False(leader.GetServer().IsServing())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	members, err := client.GetMembers(ctx, &pdpb.GetMembersRequest{})
	re.NoError(err)
	re.Nil(members.GetLeader())
	stream, err := client.Tso(ctx)
	re.NoError(err)
	re.NoError(stream.Send(&pdpb.TsoRequest{Header: testutil.NewRequestHeader(leader.GetClusterID()), Count: 1}))
	_, err = stream.Recv()
	// A valid lease alone must not send requests into getTS's two-second
	// initialization retry loop. Return the leader-change error immediately.
	re.ErrorContains(err, errs.NotLeaderErr)

	resume()
	testutil.Eventually(re, leader.GetServer().IsServing)
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}

func TestTSOServesDuringPDInitialization(t *testing.T) {
	re := require.New(t)
	cluster := newEarlyTSOCluster(t, 3)
	oldLeader := cluster.GetLeaderServer()
	target := cluster.GetServer(cluster.GetFollower())
	re.NotNil(target)
	clusterID := target.GetClusterID()
	oldClient := earlyTSOClient(t, oldLeader)
	beforeHandoff := requestEarlyTSO(t, earlyTSOStream(t, oldClient, ""), clusterID)

	wait, resume := pauseAfterTSOInitialization(t, target.GetServerID())
	re.NoError(oldLeader.MoveEtcdLeader(oldLeader.GetServerID(), target.GetServerID()))
	wait()
	re.False(target.GetServer().IsServing())
	client := earlyTSOClient(t, target)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	members, err := client.GetMembers(ctx, &pdpb.GetMembersRequest{})
	re.NoError(err)
	re.Equal(target.GetServerID(), members.GetLeader().GetMemberId())

	// Followers discover the new owner through the election key even while
	// the new leader is still loading metadata.
	testutil.Eventually(re, func() bool {
		return oldLeader.GetLeader().GetMemberId() == target.GetServerID()
	})
	direct := earlyTSOStream(t, client, "")
	localForward := earlyTSOStream(t, client, target.GetAddr())
	proxy := earlyTSOStream(t, oldClient, target.GetAddr())
	previous := beforeHandoff
	for _, stream := range []pdpb.PD_TsoClient{direct, localForward, proxy} {
		next := requestEarlyTSO(t, stream, clusterID)
		re.Negative(tsoutil.CompareTimestamp(previous, next))
		previous = next
	}

	header := testutil.NewRequestHeader(clusterID)
	_, err = client.GetAllStores(ctx, &pdpb.GetAllStoresRequest{Header: header})
	re.EqualError(err, errs.ErrNotLeader.Error())
	_, err = client.AllocID(ctx, &pdpb.AllocIDRequest{Header: header})
	re.EqualError(err, errs.ErrNotLeader.Error())

	// A request alone cannot advance physical time at this request volume.
	// This checks that the periodic updater also runs before full PD readiness.
	initialPhysical := previous.GetPhysical()
	testutil.Eventually(re, func() bool {
		next := requestEarlyTSO(t, direct, clusterID)
		re.Negative(tsoutil.CompareTimestamp(previous, next))
		previous = next
		return next.GetPhysical() > initialPhysical
	}, testutil.WithWaitFor(5*time.Second))
	re.False(target.GetServer().IsServing())

	resume()
	testutil.Eventually(re, target.GetServer().IsServing)
	stores, err := client.GetAllStores(ctx, &pdpb.GetAllStoresRequest{Header: header})
	re.NoError(err)
	re.Nil(stores.GetHeader().GetError())
	re.Len(stores.GetStores(), 1)
	next := requestEarlyTSO(t, direct, clusterID)
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}

func TestEarlyTSOStopsWhenLeaseExpires(t *testing.T) {
	re := require.New(t)
	cluster := newEarlyTSOCluster(t, 1)
	leader := cluster.GetLeaderServer()
	wait, resume := pauseAfterTSOInitialization(t, leader.GetServerID())
	leader.ResetPDLeader()
	wait()
	client := earlyTSOClient(t, leader)
	stream := earlyTSOStream(t, client, "")
	previous := requestEarlyTSO(t, stream, leader.GetClusterID())
	lease := leader.GetServer().GetMember().GetLeadership().GetLease()
	re.NotNil(lease)

	const keepaliveHook = "github.com/tikv/pd/pkg/election/keepAliveFailed"
	re.NoError(failpoint.Enable(keepaliveHook, fmt.Sprintf(`return("leader election@%s")`, leader.GetConfig().Name)))
	t.Cleanup(func() { re.NoError(failpoint.Disable(keepaliveHook)) })
	testutil.Eventually(re, lease.IsExpired, testutil.WithWaitFor(15*time.Second))
	requireEarlyTSORejected(t, stream, leader.GetClusterID())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	members, err := client.GetMembers(ctx, &pdpb.GetMembersRequest{})
	re.NoError(err)
	re.Nil(members.GetLeader())
	re.False(leader.GetServer().IsServing())

	// Finishing the obsolete initialization must not make its expired term
	// serve again. A fresh campaign must initialize from the persisted window.
	const campaignHook = "github.com/tikv/pd/server/leaderLoopCheckAgain"
	re.NoError(failpoint.Enable(campaignHook, fmt.Sprintf(`return("%d")`, leader.GetServerID())))
	t.Cleanup(func() { re.NoError(failpoint.Disable(campaignHook)) })
	resume()
	testutil.Eventually(re, func() bool {
		return leader.GetServer().GetMember().GetLeadership().GetLease() == nil &&
			!leader.GetServer().GetTSOAllocator().IsInitialize()
	})
	re.NoError(failpoint.Disable(keepaliveHook))
	re.NoError(failpoint.Disable(campaignHook))
	testutil.Eventually(re, func() bool {
		current := leader.GetServer().GetMember().GetLeadership().GetLease()
		return current != nil && current.GetID() != lease.GetID() && leader.GetServer().IsServing()
	})
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}

func TestEarlyTSOInitializationFailureResignsBeforeCleanup(t *testing.T) {
	for _, tc := range []struct {
		name string
		hook string
	}{
		{name: "cluster initialization", hook: "github.com/tikv/pd/server/cluster/raftClusterReturn"},
		{name: "ID rebasing", hook: "github.com/tikv/pd/server/rebaseErr"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkInitializationFailureCleanup(t, tc.hook)
		})
	}
}

func checkInitializationFailureCleanup(t *testing.T, failureHook string) {
	t.Helper()
	re := require.New(t)
	cluster := newEarlyTSOCluster(t, 1)
	leader := cluster.GetLeaderServer()
	wait, resume := pauseAfterTSOInitialization(t, leader.GetServerID())
	leader.ResetPDLeader()
	wait()
	client := earlyTSOClient(t, leader)
	stream := earlyTSOStream(t, client, "")
	previous := requestEarlyTSO(t, stream, leader.GetClusterID())
	svr := leader.GetServer()
	lease := svr.GetMember().GetLeadership().GetLease()
	re.NotNil(lease)
	waitClose, resumeClose := pauseLeaseClose(t, lease)
	t.Cleanup(resumeClose)

	const campaignHook = "github.com/tikv/pd/server/leaderLoopCheckAgain"
	re.NoError(failpoint.Enable(failureHook, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(failureHook)) })
	re.NoError(failpoint.Enable(campaignHook, fmt.Sprintf(`return("%d")`, leader.GetServerID())))
	t.Cleanup(func() { re.NoError(failpoint.Disable(campaignHook)) })
	resume()
	waitClose()
	re.False(svr.IsServing())
	re.Nil(svr.GetMember().GetLeadership().GetLease())
	requireEarlyTSORejected(t, stream, leader.GetClusterID())
	resumeClose()
	testutil.Eventually(re, func() bool { return !svr.GetTSOAllocator().IsInitialize() })

	re.NoError(failpoint.Disable(failureHook))
	re.NoError(failpoint.Disable(campaignHook))
	testutil.Eventually(re, svr.IsServing)
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}
