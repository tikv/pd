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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	etcdtypes "go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
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
	return pauseMemberHook(t, memberID, "github.com/tikv/pd/server/cluster/"+checkpoint)
}

func pauseMemberHook(t *testing.T, memberID uint64, hook string) (wait, resume func()) {
	t.Helper()
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
			t.Fatalf("leader did not reach %s", hook)
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
	lease := leader.GetServer().GetMember().GetLeadership().GetLease()
	re.NotNil(lease)

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

	const saveHook = "github.com/tikv/pd/pkg/tso/failedToSaveTimestamp"
	re.NoError(failpoint.Enable(saveHook, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(saveHook)) })
	resume()
	testutil.Eventually(re, leader.GetServer().IsServing)
	// Full PD readiness proves the first initialization attempt returned.
	// A live lease must still not authorize an uninitialized allocator.
	re.False(leader.GetServer().GetTSOAllocator().IsInitialize())
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.NoError(failpoint.Disable(saveHook))
	testutil.Eventually(re, leader.GetServer().GetTSOAllocator().IsInitialize)
	re.Same(lease, leader.GetServer().GetMember().GetLeadership().GetLease())
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

func TestEarlyTSOStopsAfterPeerPartition(t *testing.T) {
	re := require.New(t)
	cluster := newEarlyTSOCluster(t, 3)
	oldLeader := cluster.GetLeaderServer()
	target := cluster.GetServer(cluster.GetFollower())
	re.NotNil(target)
	wait, resume := pauseAfterTSOInitialization(t, target.GetServerID())
	re.NoError(oldLeader.MoveEtcdLeader(oldLeader.GetServerID(), target.GetServerID()))
	wait()
	re.False(target.GetServer().IsServing())
	client := earlyTSOClient(t, target)
	stream := earlyTSOStream(t, client, "")
	previous := requestEarlyTSO(t, stream, target.GetClusterID())
	lease := target.GetServer().GetMember().GetLeadership().GetLease()
	re.NotNil(lease)

	// Isolate raft traffic while leaving the old owner's client endpoint open.
	for _, peer := range cluster.GetServers() {
		if peer != target {
			target.GetServer().GetMember().Etcd().Server.CutPeer(etcdtypes.ID(peer.GetServerID()))
			peer.GetServer().GetMember().Etcd().Server.CutPeer(etcdtypes.ID(target.GetServerID()))
		}
	}
	heal := sync.OnceFunc(func() {
		for _, peer := range cluster.GetServers() {
			if peer != target {
				target.GetServer().GetMember().Etcd().Server.MendPeer(etcdtypes.ID(peer.GetServerID()))
				peer.GetServer().GetMember().Etcd().Server.MendPeer(etcdtypes.ID(target.GetServerID()))
			}
		}
	})
	t.Cleanup(heal)
	var successor *tests.TestServer
	testutil.Eventually(re, func() bool {
		for _, server := range cluster.GetServers() {
			if server != target && server.IsLeader() {
				successor = server
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(30*time.Second))
	re.True(lease.IsExpired())
	requireEarlyTSORejected(t, stream, target.GetClusterID())
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), target.GetClusterID())
	successorClient := earlyTSOClient(t, successor)
	next := requestEarlyTSO(t, earlyTSOStream(t, successorClient, ""), successor.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))

	// Restoring connectivity must not revive the obsolete initialization.
	heal()
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), target.GetClusterID())
	resume()
	testutil.Eventually(re, func() bool {
		return target.GetLeader().GetMemberId() == successor.GetServerID() && !target.IsLeader()
	})
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), target.GetClusterID())
	final := requestEarlyTSO(t, earlyTSOStream(t, successorClient, ""), successor.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(next, final))
}

func TestEarlyTSOInitializationFailureResignsBeforeCleanup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hook       string
		key        func() string
		failureLog string
	}{
		{name: "cluster initialization", hook: "github.com/tikv/pd/server/cluster/raftClusterReturn"},
		{name: "cluster metadata", key: keypath.ClusterPath, failureLog: "failed to load cluster info"},
		{name: "ID rebasing", key: keypath.AllocIDPath, failureLog: "failed to sync id from etcd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkInitializationFailureCleanup(t, tc.hook, tc.key, tc.failureLog)
		})
	}
}

func checkInitializationFailureCleanup(t *testing.T, failureHook string, corruptKey func() string, failureLog string) {
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
	waitWithdrawal, resumeWithdrawal := pauseMemberHook(t, leader.GetServerID(), "github.com/tikv/pd/pkg/member/afterMemberLeaderWithdrawn")
	fname := testutil.InitTempFileLogger(t, "info")

	var clearFailure func()
	if corruptKey != nil {
		// A malformed persisted value makes the real initialization call fail,
		// including its warning log, rather than returning from a failpoint.
		key := corruptKey()
		stored, err := svr.GetClient().Get(t.Context(), key)
		re.NoError(err)
		re.Len(stored.Kvs, 1)
		clearFailure = sync.OnceFunc(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := svr.GetClient().Put(ctx, key, string(stored.Kvs[0].Value))
			re.NoError(err)
		})
		t.Cleanup(clearFailure)
		_, err = svr.GetClient().Put(t.Context(), key, "invalid")
		re.NoError(err)
	} else {
		re.NoError(failpoint.Enable(failureHook, "return(true)"))
		clearFailure = sync.OnceFunc(func() { re.NoError(failpoint.Disable(failureHook)) })
		t.Cleanup(clearFailure)
	}

	const campaignHook = "github.com/tikv/pd/server/leaderLoopCheckAgain"
	re.NoError(failpoint.Enable(campaignHook, fmt.Sprintf(`return("%d")`, leader.GetServerID())))
	t.Cleanup(func() { re.NoError(failpoint.Disable(campaignHook)) })
	resume()
	waitWithdrawal()
	re.Nil(svr.GetMember().GetLeader())
	re.Same(lease, svr.GetMember().GetLeadership().GetLease())
	re.False(lease.IsExpired())
	members, err := client.GetMembers(t.Context(), &pdpb.GetMembersRequest{})
	re.NoError(err)
	re.Nil(members.GetLeader())
	requireEarlyTSORejected(t, stream, leader.GetClusterID())
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	resumeWithdrawal()
	waitClose()
	re.False(svr.IsServing())
	re.Nil(svr.GetMember().GetLeadership().GetLease())
	re.True(svr.GetTSOAllocator().IsInitialize())
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	if failureLog != "" {
		content, err := os.ReadFile(fname)
		re.NoError(err)
		re.NotContains(string(content), failureLog)
	}
	resumeClose()
	testutil.Eventually(re, func() bool { return !svr.GetTSOAllocator().IsInitialize() })
	if failureLog != "" {
		testutil.Eventually(re, func() bool {
			content, err := os.ReadFile(fname)
			re.NoError(err)
			return strings.Contains(string(content), failureLog)
		})
	}

	clearFailure()
	re.NoError(failpoint.Disable(campaignHook))
	testutil.Eventually(re, svr.IsServing)
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}

func TestEarlyTSOInitializationRetryAfterWithdrawal(t *testing.T) {
	re := require.New(t)
	cluster := newEarlyTSOCluster(t, 1)
	leader := cluster.GetLeaderServer()
	svr := leader.GetServer()
	client := earlyTSOClient(t, leader)
	previous := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())

	const saveHook = "github.com/tikv/pd/pkg/tso/failedToSaveTimestamp"
	re.NoError(failpoint.Enable(saveHook, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(saveHook)) })
	oldLease := svr.GetMember().GetLeadership().GetLease()
	leader.ResetPDLeader()
	testutil.Eventually(re, func() bool {
		return svr.IsServing() && svr.GetMember().GetLeadership().GetLease() != oldLease
	})
	re.False(svr.GetTSOAllocator().IsInitialize())
	lease := svr.GetMember().GetLeadership().GetLease()

	// Let the retry persist its window, then finish it only after the campaign
	// has withdrawn readiness and PD identity, with the same lease still live.
	waitRetry, resumeRetry := pauseAtTSOCheckpoint(t, leader.GetServerID(), "afterTSOInitializationBeforeReady")
	re.NoError(failpoint.Disable(saveHook))
	waitRetry()
	waitWithdrawal, resumeWithdrawal := pauseMemberHook(t, leader.GetServerID(), "github.com/tikv/pd/pkg/member/afterMemberLeaderWithdrawn")
	const exitHook = "github.com/tikv/pd/server/exitCampaignLeader"
	re.NoError(failpoint.Enable(exitHook, fmt.Sprintf(`return("%d")`, leader.GetServerID())))
	t.Cleanup(func() { re.NoError(failpoint.Disable(exitHook)) })
	waitWithdrawal()
	re.Nil(svr.GetMember().GetLeader())
	re.Same(lease, svr.GetMember().GetLeadership().GetLease())
	re.False(lease.IsExpired())
	waitCheck, resumeCheck := pauseAtTSOCheckpoint(t, leader.GetServerID(), "afterTSOServiceCheck")
	resumeRetry()
	waitCheck()
	re.True(svr.GetTSOAllocator().IsInitialize())
	members, err := client.GetMembers(t.Context(), &pdpb.GetMembersRequest{})
	re.NoError(err)
	re.Nil(members.GetLeader())
	requireEarlyTSORejected(t, earlyTSOStream(t, client, ""), leader.GetClusterID())

	re.NoError(failpoint.Disable(exitHook))
	resumeCheck()
	resumeWithdrawal()
	testutil.Eventually(re, func() bool {
		return svr.IsServing() && svr.GetMember().GetLeadership().GetLease() != lease
	})
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}

func TestEarlyTSOInitializationWritesAfterLeadershipLoss(t *testing.T) {
	for _, atCommit := range []bool{false, true} {
		name := "before metadata loading"
		if atCommit {
			name = "during metadata commit"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			cluster := newEarlyTSOCluster(t, 1)
			leader := cluster.GetLeaderServer()
			svr := leader.GetServer()
			client := earlyTSOClient(t, leader)
			previous := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
			wait, resume := pauseAfterTSOInitialization(t, leader.GetServerID())
			leader.ResetPDLeader()
			wait()
			lease := svr.GetMember().GetLeadership().GetLease()
			// Missing rules make initialization create the default placement rule.
			_, err := svr.GetClient().Delete(t.Context(), keypath.RulesPathPrefix(), clientv3.WithPrefix())
			re.NoError(err)
			waitNext, resumeNext := pauseAtTSOCheckpoint(t, leader.GetServerID(), "beforeTSOInitialized")

			resumeCommit := func() {}
			if atCommit {
				const hook = "github.com/tikv/pd/pkg/storage/kv/beforeTxnCommit"
				reached := make(chan struct{})
				release := make(chan struct{})
				var once sync.Once
				resumeCommit = sync.OnceFunc(func() { close(release) })
				re.NoError(failpoint.EnableCall(hook, func(ops []clientv3.Op) {
					for _, op := range ops {
						if op.IsPut() && strings.HasPrefix(string(op.KeyBytes()), keypath.RulesPathPrefix()) {
							once.Do(func() { close(reached) })
							<-release
							return
						}
					}
				}))
				t.Cleanup(func() {
					resumeCommit()
					re.NoError(failpoint.Disable(hook))
				})
				resume()
				select {
				case <-reached:
				case <-time.After(30 * time.Second):
					t.Fatal("initialization did not attempt to persist placement rules")
				}
			}

			const updateHook = "github.com/tikv/pd/pkg/tso/failedToUpdateTimestamp"
			re.NoError(failpoint.Enable(updateHook, "return(true)"))
			t.Cleanup(func() { re.NoError(failpoint.Disable(updateHook)) })
			testutil.Eventually(re, func() bool {
				resp, err := svr.GetClient().Get(t.Context(), svr.GetMember().GetElectionPath())
				re.NoError(err)
				return len(resp.Kvs) == 0 && !svr.GetTSOAllocator().IsInitialize()
			})
			resume()
			resumeCommit()
			// The next campaign is paused before it can initialize anything. Any
			// persisted rule here would therefore come from the withdrawn term.
			waitNext()
			re.NotSame(lease, svr.GetMember().GetLeadership().GetLease())
			rules, err := svr.GetClient().Get(t.Context(), keypath.RulesPathPrefix(), clientv3.WithPrefix())
			re.NoError(err)
			re.Empty(rules.Kvs)
			re.NoError(failpoint.Disable(updateHook))
			resumeNext()
			testutil.Eventually(re, svr.IsServing)
			rules, err = svr.GetClient().Get(t.Context(), keypath.RulesPathPrefix(), clientv3.WithPrefix())
			re.NoError(err)
			re.NotEmpty(rules.Kvs)
			next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
			re.Negative(tsoutil.CompareTimestamp(previous, next))
		})
	}
}

func TestEarlyTSORegionCleanupAfterLeadershipLoss(t *testing.T) {
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), 1, func(conf *config.Config, _ string) {
		conf.PDServerCfg.UseRegionStorage = false
		conf.TSOSaveInterval = typeutil.Duration{Duration: time.Second}
	})
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	svr := leader.GetServer()
	client := earlyTSOClient(t, leader)
	previous := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	waitInit, resumeInit := pauseAfterTSOInitialization(t, leader.GetServerID())
	leader.ResetPDLeader()
	waitInit()
	parent := &metapb.Region{Id: 100, StartKey: []byte("a"), EndKey: []byte("z"), RegionEpoch: &metapb.RegionEpoch{Version: 2}}
	child := &metapb.Region{Id: 101, StartKey: []byte("a"), EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{Version: 3}}
	re.NoError(svr.GetStorage().SaveRegion(parent))
	re.NoError(svr.GetStorage().SaveRegion(child))
	waitDelete, resumeDelete := pauseMemberHook(t, parent.GetId(), "github.com/tikv/pd/pkg/storage/endpoint/beforeRegionOverlapDelete")
	waitNext, resumeNext := pauseAtTSOCheckpoint(t, leader.GetServerID(), "beforeTSOInitialized")
	resumeInit()
	waitDelete()

	const updateHook = "github.com/tikv/pd/pkg/tso/failedToUpdateTimestamp"
	re.NoError(failpoint.Enable(updateHook, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(updateHook)) })
	testutil.Eventually(re, func() bool {
		resp, err := svr.GetClient().Get(t.Context(), svr.GetMember().GetElectionPath())
		re.NoError(err)
		return len(resp.Kvs) == 0 && !svr.GetTSOAllocator().IsInitialize()
	})
	// A successor's heartbeat can replace the old parent while its deletion is
	// paused. The withdrawn loader must leave this newer persisted record intact.
	parent.StartKey = []byte("m")
	parent.RegionEpoch.Version = 4
	re.NoError(svr.GetStorage().SaveRegion(parent))
	resumeDelete()
	waitNext()
	loaded := &metapb.Region{}
	ok, err := svr.GetStorage().LoadRegion(parent.GetId(), loaded)
	re.NoError(err)
	re.True(ok, "withdrawn loader deleted the replacement region")
	re.Equal(parent, loaded)
	re.NoError(failpoint.Disable(updateHook))
	resumeNext()
	testutil.Eventually(re, svr.IsServing)
	region := svr.DirectlyGetRaftCluster().GetRegion(parent.GetId())
	re.NotNil(region, "recovered leader skipped the successor's replacement region")
	re.Equal(parent, region.GetMeta())
	region = svr.DirectlyGetRaftCluster().GetRegion(child.GetId())
	re.NotNil(region)
	re.Equal(child, region.GetMeta())
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}

func TestEarlyTSOCampaignFailureStopsBootstrapCluster(t *testing.T) {
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), 1)
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	svr := leader.GetServer()
	client := earlyTSOClient(t, leader)
	previous := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	waitBootstrap, resumeBootstrap := pauseMemberHook(t, leader.GetServerID(), "github.com/tikv/pd/server/beforeBootstrapClusterStart")
	bootstrapResult := make(chan error, 1)
	go func() { bootstrapResult <- leader.BootstrapCluster() }()
	waitBootstrap()

	const reloadHook = "github.com/tikv/pd/server/beforeLeaderReloadConfig"
	reached := make(chan int, 2)
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	resumes := [2]func(){sync.OnceFunc(func() { close(release[0]) }), sync.OnceFunc(func() { close(release[1]) })}
	var attempts int
	re.NoError(failpoint.EnableCall(reloadHook, func(id uint64) {
		if id == leader.GetServerID() && attempts < len(release) {
			i := attempts
			attempts++
			reached <- i
			<-release[i]
		}
	}))
	t.Cleanup(func() {
		resumes[0]()
		resumes[1]()
		re.NoError(failpoint.Disable(reloadHook))
	})
	waitReload := func(want int) {
		t.Helper()
		select {
		case got := <-reached:
			re.Equal(want, got)
		case <-time.After(30 * time.Second):
			t.Fatal("campaign did not reach config reload")
		}
	}
	leader.ResetPDLeader()
	waitReload(0)
	failedLease := svr.GetMember().GetLeadership().GetLease()
	resumeBootstrap()
	select {
	case err := <-bootstrapResult:
		re.NoError(err)
	case <-time.After(30 * time.Second):
		t.Fatal("bootstrap did not finish in the new campaign")
	}
	re.True(svr.DirectlyGetRaftCluster().IsRunning())
	configKey := keypath.ConfigPath()
	saved, err := svr.GetClient().Get(t.Context(), configKey)
	re.NoError(err)
	_, err = svr.GetClient().Put(t.Context(), configKey, "invalid config")
	re.NoError(err)
	resumes[0]()
	waitReload(1)
	re.NotSame(failedLease, svr.GetMember().GetLeadership().GetLease())
	re.False(svr.DirectlyGetRaftCluster().IsRunning(), "failed campaign left the bootstrap cluster running")
	if len(saved.Kvs) == 0 {
		_, err = svr.GetClient().Delete(t.Context(), configKey)
	} else {
		_, err = svr.GetClient().Put(t.Context(), configKey, string(saved.Kvs[0].Value))
	}
	re.NoError(err)

	resumes[1]()
	testutil.Eventually(re, svr.IsServing)
	re.True(svr.DirectlyGetRaftCluster().IsRunning())
	next := requestEarlyTSO(t, earlyTSOStream(t, client, ""), leader.GetClusterID())
	re.Negative(tsoutil.CompareTimestamp(previous, next))
}
