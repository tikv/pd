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

package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// TestDeleteExpectedPrimaryFlagRevokesLease reconstructs the corner case from issue
// #10875. The expected primary flag is bound to an etcd lease; if the key were
// deleted while its lease lingered, a later campaign would read an empty flag, skip
// the affinity guard, and then fail with ErrEtcdTxnConflict against the still-present
// leader key. DeleteExpectedPrimaryFlag must therefore remove both the key and its
// lease, so the "key deleted but lease persists" state can never exist.
func TestDeleteExpectedPrimaryFlagRevokesLease(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)
	const value = "http://127.0.0.1:2379"

	// Mark the flag bound to a fresh lease, exactly as TransferPrimary does.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	leaseID := grantResp.ID
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: value, output: value}, leaseID))

	// Sanity: the key exists and is bound to the lease.
	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Len(getResp.Kvs, 1)
	re.Equal(int64(leaseID), getResp.Kvs[0].Lease)

	// The newly elected primary cleans up the flag once it wins.
	re.False(DeleteExpectedPrimaryFlag(client, msParam, value, nil))

	// The key is gone...
	getResp, err = client.Get(ctx, path)
	re.NoError(err)
	re.Empty(getResp.Kvs)
	// ...and so is its lease: TimeToLive returns -1 for a revoked/expired lease, which
	// is the guarantee that the #10875 "key deleted but lease persists" state is gone.
	ttlResp, err := client.TimeToLive(ctx, leaseID)
	re.NoError(err)
	re.Equal(int64(-1), ttlResp.TTL)
}

// TestDeleteExpectedPrimaryFlagSkipsOnValueMismatch ensures the conditional delete
// does not clobber a newer transfer. If a second transfer has already rewritten the
// flag (with its own lease) to another member while this primary was winning, the
// stale winner must leave both the key and the newer lease intact and report that
// it has been superseded, so it steps down instead of defeating that transfer.
func TestDeleteExpectedPrimaryFlagSkipsOnValueMismatch(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)

	// A newer transfer points the flag at "newer" with its own lease.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	leaseID := grantResp.ID
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "newer", output: "newer"}, leaseID))

	// A primary that campaigned for the older value tries to clean up; it must not
	// delete the newer marker, and it must learn that it has been superseded.
	re.True(DeleteExpectedPrimaryFlag(client, msParam, "older", nil))

	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Len(getResp.Kvs, 1)
	re.Equal("newer", string(getResp.Kvs[0].Value))
	ttlResp, err := client.TimeToLive(ctx, leaseID)
	re.NoError(err)
	re.Positive(ttlResp.TTL)
}

// TestDeleteExpectedPrimaryFlagReconciliation covers the post-campaign reconcile
// paths of DeleteExpectedPrimaryFlag beyond the plain delete/mismatch cases:
//   - a winner of a free election (no marker observed) keeps serving when the
//     marker is still absent, but steps down when a newer transfer installed a
//     marker naming another member while it was winning;
//   - when the newer marker targets the winner itself, the winner keeps serving
//     and the marker is cleaned up together with its lease.
func TestDeleteExpectedPrimaryFlagReconciliation(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)
	const selfURL = "http://127.0.0.1:2379"

	self := member.NewParticipant(client, *msParam)
	self.InitInfo(&schedulingpb.Participant{
		Name:       "self",
		Id:         1,
		ListenUrls: []string{selfURL},
	}, "primary election")

	// Free election, no marker: nothing to reconcile, keep serving.
	re.False(DeleteExpectedPrimaryFlag(client, msParam, "", self))

	// A transfer installs a marker naming another member after the free election
	// was won: the winner must step down, leaving the marker and its lease intact.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "http://other:2379", output: "other"}, grantResp.ID))
	re.True(DeleteExpectedPrimaryFlag(client, msParam, "", self))
	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Len(getResp.Kvs, 1)

	// The marker is rewritten to target the winner itself: keep serving, and the
	// marker plus its lease are cleaned up.
	grantResp2, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: selfURL, output: "self"}, grantResp2.ID))
	re.False(DeleteExpectedPrimaryFlag(client, msParam, "", self))
	getResp, err = client.Get(ctx, path)
	re.NoError(err)
	re.Empty(getResp.Kvs)
	ttlResp, err := client.TimeToLive(ctx, grantResp2.ID)
	re.NoError(err)
	re.Equal(int64(-1), ttlResp.TTL)
}

// TestExpectedPrimaryCmp verifies the campaign guard returned by ExpectedPrimaryCmp.
// The empty-value case must assert the marker is still absent: this closes the race
// where a campaigner that observed no transfer resumes after a transfer installed a
// target marker and released the leader key, and would otherwise win the campaign
// while bypassing the transfer affinity guard.
func TestExpectedPrimaryCmp(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)

	// runGuarded runs a txn gated only by the campaign guard and reports whether it
	// committed, mirroring how CampaignWithCmps folds the guard into the campaign txn.
	runGuarded := func(expectedValue string) bool {
		resp, err := client.Txn(ctx).
			If(ExpectedPrimaryCmp(msParam, expectedValue)).
			Then(clientv3.OpGet(path)).
			Commit()
		re.NoError(err)
		return resp.Succeeded
	}

	// No transfer in progress: the empty-value guard (marker still absent) holds.
	re.True(runGuarded(""))

	// A transfer installs a marker naming "target".
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "target", output: "target"}, grantResp.ID))

	// The empty-value guard must now fail: a campaigner that observed "no transfer"
	// can no longer win once a marker exists.
	re.False(runGuarded(""))
	// The transfer target's own guard (value match) still holds, so it can campaign.
	re.True(runGuarded("target"))
	// A stale or non-target value does not match the installed marker.
	re.False(runGuarded("other"))
}

// TestMarkExpectedPrimaryFlagGuardsAgainstLeadershipChange reconstructs the race
// flagged in review: TransferPrimary checks IsServing() before doing discovery and
// lease-grant work, but only writes the expected-primary marker afterwards. If this
// instance loses leadership in that window - because it resigned, its lease
// expired, or anything else vacated the leader key - a rival can win a fresh
// campaign and start serving before the stale caller's marker write lands. That
// marker write must not silently succeed once a different primary is already
// serving: the guarded Put must be rejected by the same leader-key comparison
// election.Leadership uses to guard its own writes, and the marker must never
// appear in etcd.
func TestMarkExpectedPrimaryFlagGuardsAgainstLeadershipChange(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)
	const selfURL = "http://127.0.0.1:2379"
	const rivalURL = "http://127.0.0.1:2380"

	self := member.NewParticipant(client, *msParam)
	self.InitInfo(&schedulingpb.Participant{
		Name:       "self",
		Id:         1,
		ListenUrls: []string{selfURL},
	}, "primary election")

	// self wins the campaign and starts serving, exactly like the real election
	// loop: CampaignWithCmps writes the leader key, then PromoteSelf marks it locally.
	re.NoError(self.CampaignWithCmps(ctx, constant.DefaultLease))
	self.PromoteSelf()
	re.True(self.IsServing())

	// Snapshot the guard TransferPrimary would build right after its IsServing()
	// check, while self still legitimately owns the leader key.
	guard := clientv3.Compare(clientv3.Value(self.GetLeadership().GetLeaderKey()), "=", self.MemberValue())

	// self loses leadership in the window between the check and the marker write
	// (resign releases the leader key and revokes its lease)...
	self.Resign()

	// ...and a rival wins a fresh campaign on the same election, becoming the new
	// primary that will never look at a marker addressed by the stale request below.
	rival := member.NewParticipant(client, *msParam)
	rival.InitInfo(&schedulingpb.Participant{
		Name:       "rival",
		Id:         2,
		ListenUrls: []string{rivalURL},
	}, "primary election")
	re.NoError(rival.CampaignWithCmps(ctx, constant.DefaultLease))
	rival.PromoteSelf()
	re.True(rival.IsServing())

	// The stale request proceeds as TransferPrimary does after its initial check:
	// grant a lease and attempt the guarded marker write with the snapshotted guard.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	err = markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "http://third:2379", output: "third"}, grantResp.ID, guard)
	re.Error(err)

	// The marker must never have been published: the new primary is already serving
	// and would never react to it.
	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Empty(getResp.Kvs)

	// Mirror TransferPrimary's own cleanup on this failure path so the granted lease
	// doesn't leak past the test.
	_, err = client.Revoke(ctx, grantResp.ID)
	re.NoError(err)
}

// TestMarkExpectedPrimaryFlagGuardFencesElectionTerm covers the gap flagged in
// review on top of TestMarkExpectedPrimaryFlagGuardsAgainstLeadershipChange:
// Participant.MemberValue() is fixed for the participant's lifetime, so a guard
// built from Value(leaderKey) == MemberValue() cannot tell "the same leadership
// term IsServing() observed" apart from "this member lost its lease and won a
// fresh campaign with the same MemberValue()" while a transfer request was stalled
// in discovery or the lease grant. TransferPrimary guards on the leader key's
// CreateRevision instead, which changes on every fresh campaign (Campaign requires
// CreateRevision(leaderKey) == 0 to win), so it fences to the exact etcd write
// backing the observed term. This test reconstructs that race directly against
// markExpectedPrimaryFlag: the same participant loses and regains leadership
// (same MemberValue, new term), and a guard captured before the regain must be
// rejected, while a guard captured after it must succeed.
func TestMarkExpectedPrimaryFlagGuardFencesElectionTerm(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	msParam := &keypath.MsParam{ServiceName: constant.SchedulingServiceName}
	path := keypath.ExpectedPrimaryPath(msParam)
	const selfURL = "http://127.0.0.1:2379"

	self := member.NewParticipant(client, *msParam)
	self.InitInfo(&schedulingpb.Participant{
		Name:       "self",
		Id:         1,
		ListenUrls: []string{selfURL},
	}, "primary election")
	leaderKeyPath := self.GetLeadership().GetLeaderKey()

	// self wins the first term and starts serving.
	re.NoError(self.CampaignWithCmps(ctx, constant.DefaultLease))
	self.PromoteSelf()
	re.True(self.IsServing())

	// Snapshot the guard the way TransferPrimary does: read the leader key live and
	// fence on its CreateRevision, right after the IsServing() check would have run.
	staleResp, err := client.Get(ctx, leaderKeyPath)
	re.NoError(err)
	re.Len(staleResp.Kvs, 1)
	re.Equal(self.MemberValue(), string(staleResp.Kvs[0].Value))
	staleGuard := clientv3.Compare(clientv3.CreateRevision(leaderKeyPath), "=", staleResp.Kvs[0].CreateRevision)

	// self loses leadership (lease expiry, an unrelated resign, etc.) and then wins a
	// fresh campaign - a new term, same MemberValue since it never changes after
	// InitInfo. This is exactly the window a value-only guard cannot see through.
	self.Resign()
	re.NoError(self.CampaignWithCmps(ctx, constant.DefaultLease))
	self.PromoteSelf()
	re.True(self.IsServing())

	freshResp, err := client.Get(ctx, leaderKeyPath)
	re.NoError(err)
	re.Len(freshResp.Kvs, 1)
	re.Equal(self.MemberValue(), string(freshResp.Kvs[0].Value))
	re.NotEqual(staleResp.Kvs[0].CreateRevision, freshResp.Kvs[0].CreateRevision, "re-campaigning must produce a new CreateRevision")

	// The stale, pre-regain guard must be rejected even though the value matches.
	grantResp, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	re.Error(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "http://third:2379", output: "third"}, grantResp.ID, staleGuard))
	getResp, err := client.Get(ctx, path)
	re.NoError(err)
	re.Empty(getResp.Kvs)
	_, err = client.Revoke(ctx, grantResp.ID)
	re.NoError(err)

	// A guard captured after the regain, against the current term, succeeds.
	freshGuard := clientv3.Compare(clientv3.CreateRevision(leaderKeyPath), "=", freshResp.Kvs[0].CreateRevision)
	grantResp2, err := client.Grant(ctx, constant.TransferPrimaryLeaseMultiplier*constant.DefaultLease)
	re.NoError(err)
	re.NoError(markExpectedPrimaryFlag(client, msParam, &primaryData{raw: "http://third:2379", output: "third"}, grantResp2.ID, freshGuard))
	getResp, err = client.Get(ctx, path)
	re.NoError(err)
	re.Len(getResp.Kvs, 1)
}

// TestVerifyLeaderKeyClearedDistinguishesFailureFromSupersede exercises the decision
// table verifyLeaderKeyCleared uses after Resign(): the old leader key genuinely gone
// (revoke succeeded), the old leader key rewritten with a fresh CreateRevision (a
// competing campaign already won it), and the old leader key unchanged (the revoke
// failed and the old key is still there).
func TestVerifyLeaderKeyClearedDistinguishesFailureFromSupersede(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	const leaderKeyPath = "/pd/test-transfer-primary/leader"

	// No key at all (never existed) must never be mistaken for a failure to clear.
	re.NoError(verifyLeaderKeyCleared(client, leaderKeyPath, 0))

	// Install the "old" leader key and capture its CreateRevision, mirroring the
	// revision TransferPrimary captures before Resign().
	_, err := client.Put(ctx, leaderKeyPath, "old-primary")
	re.NoError(err)
	oldResp, err := client.Get(ctx, leaderKeyPath)
	re.NoError(err)
	re.Len(oldResp.Kvs, 1)
	oldRevision := oldResp.Kvs[0].CreateRevision

	// The revoke failed and the old key stays untouched for the whole poll budget:
	// must be reported as a failure. This case pays the full leaderKeyClearPollTimeout.
	re.Error(verifyLeaderKeyCleared(client, leaderKeyPath, oldRevision))

	// The revoke succeeded and the key is now gone: success.
	_, err = client.Delete(ctx, leaderKeyPath)
	re.NoError(err)
	re.NoError(verifyLeaderKeyCleared(client, leaderKeyPath, oldRevision))

	// A fresh campaign already won and rewrote the key (necessarily the transfer
	// target, since the still-valid marker guard blocks any other candidate's
	// campaign transaction atomically alongside this same CreateRevision check - see
	// ExpectedPrimaryCmp) - a different CreateRevision must be treated as success, not
	// a failure to clear.
	_, err = client.Put(ctx, leaderKeyPath, "new-primary")
	re.NoError(err)
	newResp, err := client.Get(ctx, leaderKeyPath)
	re.NoError(err)
	re.NotEqual(oldRevision, newResp.Kvs[0].CreateRevision)
	re.NoError(verifyLeaderKeyCleared(client, leaderKeyPath, oldRevision))
}

// TestVerifyLeaderKeyClearedPollsForConcurrentRevoke reconstructs the race flagged in
// review: Lease.Close is idempotent (a CompareAndSwap guard), so a second, concurrent
// Resign() on the same participant (e.g. TSO's primaryPriorityCheckLoop racing an HTTP
// transfer) returns immediately without waiting for whichever caller's revoke is
// actually in flight. A single check right after Resign() would see the old leader key
// still present with the same CreateRevision and wrongly report a failure even though
// that concurrent revoke is genuinely about to succeed. verifyLeaderKeyCleared must
// poll long enough to observe the delayed clear instead of failing on the first look.
func TestVerifyLeaderKeyClearedPollsForConcurrentRevoke(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	ctx := context.Background()
	const leaderKeyPath = "/pd/test-transfer-primary/leader"

	_, err := client.Put(ctx, leaderKeyPath, "old-primary")
	re.NoError(err)
	oldResp, err := client.Get(ctx, leaderKeyPath)
	re.NoError(err)
	oldRevision := oldResp.Kvs[0].CreateRevision

	// Simulate a concurrent caller's in-flight revoke landing partway through the poll
	// window, well within leaderKeyClearPollTimeout.
	go func() {
		time.Sleep(leaderKeyClearPollInterval * 3)
		_, _ = client.Delete(ctx, leaderKeyPath)
	}()

	re.NoError(verifyLeaderKeyCleared(client, leaderKeyPath, oldRevision))
}

// TestIsSamePrimary covers the matching used by TransferPrimary to skip a
// self-transfer (#10970): a member matches by either its name or its service
// address, and an empty target never matches.
func TestReadFailureBackoff(t *testing.T) {
	re := require.New(t)
	// Non-positive streaks are treated like the first failure.
	re.Equal(constant.InitialReadFailureBackoff, ReadFailureBackoff(0))
	re.Equal(constant.InitialReadFailureBackoff, ReadFailureBackoff(-1))
	re.Equal(constant.InitialReadFailureBackoff, ReadFailureBackoff(1))
	// Doubles with each additional consecutive failure.
	re.Equal(2*constant.InitialReadFailureBackoff, ReadFailureBackoff(2))
	re.Equal(4*constant.InitialReadFailureBackoff, ReadFailureBackoff(3))
	re.Equal(8*constant.InitialReadFailureBackoff, ReadFailureBackoff(4))
	// Caps at the max instead of continuing to grow or overflowing, including for a
	// pathologically large streak.
	re.Equal(constant.MaxReadFailureBackoff, ReadFailureBackoff(6))
	re.Equal(constant.MaxReadFailureBackoff, ReadFailureBackoff(7))
	re.Equal(constant.MaxReadFailureBackoff, ReadFailureBackoff(1<<30))
}

func TestIsSamePrimary(t *testing.T) {
	re := require.New(t)
	entry := discovery.ServiceRegistryEntry{Name: "tso-1", ServiceAddr: "http://127.0.0.1:2379"}
	re.True(isSamePrimary(entry, "tso-1"))                  // match by name
	re.True(isSamePrimary(entry, "http://127.0.0.1:2379"))  // match by service address
	re.False(isSamePrimary(entry, "tso-2"))                 // different name
	re.False(isSamePrimary(entry, "http://127.0.0.1:2380")) // different address
	re.False(isSamePrimary(entry, ""))                      // empty target never matches
}
