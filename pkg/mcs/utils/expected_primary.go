// Copyright 2024 TiKV Project Authors.
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
	"math/rand/v2"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// GetExpectedPrimaryFlag gets the expected primary flag. A read failure is
// returned as an error rather than collapsed into an empty flag: callers treat an
// empty flag as "no transfer in progress" and campaign without the affinity guard,
// so a failed read must NOT be mistaken for "no marker" — otherwise a non-target
// member could win while a transfer marker actually exists.
func GetExpectedPrimaryFlag(client *clientv3.Client, msParam *keypath.MsParam) (string, error) {
	path := keypath.ExpectedPrimaryPath(msParam)
	primary, err := etcdutil.GetValue(client, path)
	if err != nil {
		log.Error("get expected primary flag error", errs.ZapError(err), zap.String("primary-path", path))
		return "", err
	}

	return string(primary), nil
}

// ReadFailureBackoff returns how long an election loop should sleep after the nth
// (1-indexed) consecutive GetExpectedPrimaryFlag read failure. It doubles from
// constant.InitialReadFailureBackoff and caps at constant.MaxReadFailureBackoff, so a
// sustained run of failures backs off instead of retrying at a fixed rate against an
// etcd that is already struggling. consecutiveFailures <= 1 returns the initial value.
func ReadFailureBackoff(consecutiveFailures int) time.Duration {
	shift := consecutiveFailures - 1
	if shift < 0 {
		shift = 0
	}
	// Cap the shift itself (not just the result) so the left shift below never
	// overflows for a pathologically large streak.
	const maxShift = 5 // constant.InitialReadFailureBackoff << 5 == constant.MaxReadFailureBackoff
	if shift > maxShift {
		shift = maxShift
	}
	backoff := constant.InitialReadFailureBackoff << shift
	if backoff > constant.MaxReadFailureBackoff {
		return constant.MaxReadFailureBackoff
	}
	return backoff
}

// primaryData is used to store the primary data.
// The raw value is used to write to etcd, while the output string is used for logging and debugging purposes.
type primaryData struct {
	raw    string
	output string
}

// markExpectedPrimaryFlag marks the expected primary flag when the primary is
// specified. Extra cmps are folded into the same transaction as the Put, so a
// leader-key ownership guard built by the caller makes the marker write atomic
// with still holding leadership: if the caller lost leadership between its own
// IsServing() check and this call, the guard no longer holds and the Put is
// rejected instead of silently publishing a marker that the new, already-serving
// primary will never look at.
func markExpectedPrimaryFlag(client *clientv3.Client, msParam *keypath.MsParam, primary *primaryData, leaseID clientv3.LeaseID, cmps ...clientv3.Cmp) error {
	path := keypath.ExpectedPrimaryPath(msParam)
	log.Info("set expected primary flag", zap.String("primary-path", path), zap.String("primary", primary.output))
	// write a flag to indicate the expected primary.
	resp, err := kv.NewSlowLogTxn(client).
		If(cmps...).
		Then(clientv3.OpPut(path, primary.raw, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		log.Error("mark expected primary error", errs.ZapError(err), zap.String("primary-path", path))
		return err
	}
	if !resp.Succeeded {
		log.Error("mark expected primary error", zap.String("primary-path", path))
		return errors.New("mark expected primary txn did not succeed")
	}
	return nil
}

// DeleteExpectedPrimaryFlag reconciles the expected primary flag after this member
// has won the campaign. In the common case the flag still holds expectedValue (or
// is already gone) and is deleted together with its lease, so steady state has no
// marker and later failures re-elect immediately instead of waiting for the marker
// TTL. The flag can also have been rewritten by a newer transfer while this member
// was winning; since a serving primary no longer watches the marker, that transfer
// would otherwise return success without ever taking effect. In that case:
//   - if the newer marker still targets this member, it is deleted as well and the
//     member keeps serving;
//   - if it targets another member, DeleteExpectedPrimaryFlag returns true and the
//     caller must step down so the re-election routes leadership to that target.
func DeleteExpectedPrimaryFlag(client *clientv3.Client, msParam *keypath.MsParam, expectedValue string, p *member.Participant) (superseded bool) {
	path := keypath.ExpectedPrimaryPath(msParam)
	current, deleted, err := deleteMarkerIfEquals(client, path, expectedValue)
	if deleted {
		log.Info("delete expected primary flag", zap.String("primary-path", path))
		return false
	}
	if err != nil {
		// The reconciliation transaction itself failed (a real RPC/etcd error, not a
		// value mismatch), so it is unknown whether a newer transfer rewrote the
		// marker while this member was winning. A serving primary never watches the
		// marker again (see the doc comment above), so silently assuming "no rewrite"
		// here could strand that newer transfer forever. Fail closed instead: step
		// down so the caller re-enters the election loop and this gets re-checked
		// fresh on the next campaign attempt - a spurious step-down on a transient
		// blip is far cheaper than a transfer that silently never takes effect.
		log.Warn("failed to reconcile expected primary flag, stepping down to be safe",
			zap.String("primary-path", path), errs.ZapError(err))
		return true
	}
	if current == "" {
		// The marker is confirmed absent: deleted, expired, or it never existed.
		return false
	}
	// The marker was rewritten by a newer transfer while this member was winning.
	if p != nil && p.IsExpectedPrimary(current) {
		// The newer transfer also targets this member, which is already serving.
		// Clean the marker up so it does not linger until its TTL. Best effort:
		// on failure the TTL applies.
		if _, deleted, _ := deleteMarkerIfEquals(client, path, current); deleted {
			log.Info("delete expected primary flag rewritten by a newer transfer to this member",
				zap.String("primary-path", path))
		}
		return false
	}
	log.Info("expected primary flag was rewritten by a newer transfer while campaigning, step down",
		zap.String("primary-path", path), zap.String("expected-value", expectedValue),
		zap.String("current-value", current))
	return true
}

// deleteMarkerIfEquals atomically deletes the expected primary marker and revokes
// its lease when its value equals want. It returns the marker value observed by the
// transaction ("" when the marker does not exist), whether the marker was deleted,
// and a non-nil err when the transaction itself failed (as opposed to succeeding
// with a value mismatch) - callers must not conflate the two: an err means the
// marker's actual state is unknown, not that it is absent. Note that etcd value
// comparisons always fail on a missing key, so want == "" never deletes anything
// and reports the current value.
func deleteMarkerIfEquals(client *clientv3.Client, path, want string) (current string, deleted bool, err error) {
	resp, err := kv.NewSlowLogTxn(client).
		If(clientv3.Compare(clientv3.Value(path), "=", want)).
		Then(clientv3.OpGet(path), clientv3.OpDelete(path)).
		Else(clientv3.OpGet(path)).
		Commit()
	if err != nil {
		log.Warn("failed to delete expected primary flag", zap.String("primary-path", path), errs.ZapError(err))
		return "", false, err
	}
	kvs := resp.Responses[0].GetResponseRange().GetKvs()
	if !resp.Succeeded {
		if len(kvs) == 0 {
			return "", false, nil
		}
		return string(kvs[0].Value), false, nil
	}
	// Deleted. Revoke the lease the marker was bound to, so the "key deleted but
	// lease persists" state can never exist (#10875).
	if len(kvs) > 0 && kvs[0].Lease != 0 {
		leaseID := clientv3.LeaseID(kvs[0].Lease)
		// Bound the revoke: this runs on the campaign path before the primary is
		// promoted to serving, and the cleanup is best-effort, so a hung RPC must not
		// block serving.
		ctx, cancel := context.WithTimeout(client.Ctx(), etcdutil.DefaultRequestTimeout)
		defer cancel()
		if _, err := client.Revoke(ctx, leaseID); err != nil {
			log.Warn("failed to revoke expected primary flag lease",
				zap.String("primary-path", path), zap.Int64("lease-id", int64(leaseID)), errs.ZapError(err))
		}
	}
	return want, true, nil
}

// ExpectedPrimaryCmp returns an etcd comparison that guards a primary campaign
// against a concurrent transfer.
//   - When expectedValue is non-empty, a transfer installed a marker naming this
//     member as the target; the comparison asserts the marker still holds that
//     value.
//   - When expectedValue is empty, the campaigner observed no transfer in
//     progress; the comparison asserts the marker is still absent. Without this
//     a campaigner that paused after the read could resume after a transfer
//     installed a target marker and released the leader key, then win the
//     campaign and bypass the transfer affinity guard.
func ExpectedPrimaryCmp(msParam *keypath.MsParam, expectedValue string) clientv3.Cmp {
	path := keypath.ExpectedPrimaryPath(msParam)
	if expectedValue == "" {
		return clientv3.Compare(clientv3.CreateRevision(path), "=", 0)
	}
	return clientv3.Compare(clientv3.Value(path), "=", expectedValue)
}

// leaderKeyClearPollTimeout bounds how long verifyLeaderKeyCleared waits for the old
// leader key to clear before concluding the revoke failed. A concurrent Resign() on
// the same participant (e.g. TSO's primaryPriorityCheckLoop racing an HTTP transfer)
// returns immediately without waiting, because Lease.Close is idempotent via a
// CompareAndSwap guard - so a single check right after Resign() can see a stale "not
// yet cleared" state from a revoke that some other, concurrent caller triggered and is
// genuinely still in flight. This must be at least as long as that revoke's own bound
// (pkg/election/lease.go's unexported revokeLeaseTimeout, currently 1s) so a
// legitimately in-flight concurrent revoke has time to land.
const (
	leaderKeyClearPollTimeout  = 2 * time.Second
	leaderKeyClearPollInterval = 100 * time.Millisecond
)

// verifyLeaderKeyCleared checks whether the old leader key actually cleared after
// Resign(). Resign() revokes the leader key's lease but is best-effort and swallows
// its own error internally (see Leadership.Reset), so this is the only way the caller
// can tell whether that revoke actually happened.
//
// The check compares CreateRevision, not mere presence: a leader key present with a
// CreateRevision different from oldRevision means a fresh campaign already won it -
// which can only be the transfer target, since the still-valid marker's
// ExpectedPrimaryCmp guard makes any other candidate's campaign transaction fail
// atomically alongside the very same CreateRevision(leaderKey)==0 check (see
// ExpectedPrimaryCmp and Leadership.Campaign) - so that case is success, not failure.
// A leader key present with the same CreateRevision means the revoke has not
// completed yet - either this call's own, or (per leaderKeyClearPollTimeout's doc
// comment) a concurrent caller's still in flight - so this polls instead of failing
// on the first observation, and only reports failure once the poll budget is spent.
//
// This matters because the marker's own TTL clock starts at the lease Grant, before
// Resign() even runs, while the old leader key - if its revoke fails - only starts
// decaying towards its own natural expiry once Resign() cancels its keepalive; that
// decay can take up to a full leader lease. A slow or failed revoke could therefore
// let the marker expire before the old leader key does, silently losing the transfer's
// affinity guarantee even though the caller already reported success.
func verifyLeaderKeyCleared(client *clientv3.Client, leaderKeyPath string, oldRevision int64) error {
	deadline := time.Now().Add(leaderKeyClearPollTimeout)
	for {
		ctx, cancel := context.WithTimeout(client.Ctx(), etcdutil.DefaultRequestTimeout)
		resp, err := client.Get(ctx, leaderKeyPath)
		cancel()
		if err != nil {
			return errors.Annotate(err, "failed to verify the old leader key cleared after resign")
		}
		if len(resp.Kvs) == 0 || resp.Kvs[0].CreateRevision != oldRevision {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("the old leader key did not clear after resign, etcd may be degraded; please retry the transfer")
		}
		time.Sleep(leaderKeyClearPollInterval)
	}
}

// TransferPrimary transfers the primary of the specified service to a target member.
//
// It writes the expected primary flag pointing at the target (with a TTL of
// constant.TransferPrimaryLeaseMultiplier leader leases) and then resigns the
// current primary by revoking its leader lease, so the re-election picks up the
// target. The flag write happens before the resignation on purpose: it guarantees
// the affinity guard is in place before the leader key is released, so no other
// member can win the gap.
//
// TransferPrimary reports success as soon as the flag is written and the current
// primary has resigned - it does NOT wait for the target to actually win the
// campaign or finish initializing. The worst-case unavailability this can leave
// behind, if the target never wins a single campaign (down, unreachable, or stuck),
// is bounded by the flag's TTL: TransferPrimaryLeaseMultiplier * leaderLease, i.e.
// one leader lease. Once any member wins a campaign - the target or, after the TTL,
// any other live member via free election - the flag is cleared immediately
// (DeleteExpectedPrimaryFlag) regardless of whether that member goes on to
// initialize successfully, so a failure after winning does not extend this window;
// see DeleteExpectedPrimaryFlag and campaignPrimary's ordering. This is a deliberate
// trade-off: reporting success without waiting keeps the API call itself fast and
// independent of the caller's own timeout, at the cost of not confirming the
// transfer actually reached the requested target before returning.
//
// keyspaceGroupID is optional, only used for TSO service. p must be the participant
// of the current serving primary (the API ensures the request runs on the primary).
func TransferPrimary(client *clientv3.Client, p *member.Participant, serviceName,
	oldPrimary, newPrimary string, keyspaceGroupID uint32, tsoMembersMap map[string]bool) error {
	if p == nil || !p.IsServing() {
		return errors.New("current member is not serving as primary, please check leadership")
	}

	// Capture the leader key's CreateRevision right after the IsServing() check, before
	// discovery or the lease grant below - both are network round trips during which p
	// could lose its lease and win a fresh campaign with the same MemberValue() (which
	// never changes for the lifetime of the participant). CreateRevision changes on
	// every fresh campaign (Leadership.Campaign requires CreateRevision(leaderKey) == 0
	// to win), so fencing the eventual marker write on the revision observed here - not
	// on a value comparison, and not on a revision re-read later, closer to the write -
	// ties the write to the exact leadership session IsServing() just checked for the
	// whole duration of this function, not just its tail end.
	leaderKeyPath := p.GetLeadership().GetLeaderKey()
	getCtx, getCancel := context.WithTimeout(client.Ctx(), etcdutil.DefaultRequestTimeout)
	leaderResp, err := client.Get(getCtx, leaderKeyPath)
	getCancel()
	if err != nil {
		return errors.Annotate(err, "failed to read leader key for transfer guard")
	}
	if len(leaderResp.Kvs) == 0 || string(leaderResp.Kvs[0].Value) != p.MemberValue() {
		return errors.New("current member is not serving as primary, please check leadership")
	}
	leaderKeyGuard := clientv3.Compare(clientv3.CreateRevision(leaderKeyPath), "=", leaderResp.Kvs[0].CreateRevision)

	log.Info("try to transfer primary", zap.String("service", serviceName), zap.String("from", oldPrimary), zap.String("to", newPrimary))
	entries, err := discovery.GetMSMembers(serviceName, client)
	if err != nil {
		return err
	}

	if newPrimary != "" {
		for _, member := range entries {
			if tsoMembersMap != nil && !tsoMembersMap[member.ServiceAddr] {
				continue
			}
			if isSamePrimary(member, newPrimary) && isSamePrimary(member, oldPrimary) {
				log.Info("skip transferring primary to itself",
					zap.String("service", serviceName),
					zap.String("primary", oldPrimary))
				return nil
			}
		}
	}

	// Do nothing when I am the only member of cluster.
	if len(entries) == 1 {
		return errors.Errorf("no valid secondary to transfer primary, the only member is %s", entries[0].Name)
	}

	var primaryIDs []string
	for _, member := range entries {
		// only members of specific group are valid primary candidates for TSO service.
		if tsoMembersMap != nil && !tsoMembersMap[member.ServiceAddr] {
			continue
		}
		if (newPrimary == "" && !isSamePrimary(member, oldPrimary)) || isSamePrimary(member, newPrimary) {
			primaryIDs = append(primaryIDs, member.ServiceAddr)
		}
	}
	if len(primaryIDs) == 0 {
		return errors.Errorf("no valid secondary to transfer primary, from %s to %s", oldPrimary, newPrimary)
	}

	nextPrimaryID := rand.IntN(len(primaryIDs))

	// Grant a fresh lease for the expected primary flag, sized to a few leader
	// leases so it outlives the re-election window. It is not kept alive by anyone:
	// the target deletes the flag once it wins, otherwise the TTL expires and the
	// cluster falls back to a free election.
	leaderLease := p.GetLeadership().GetLease().GetTimeoutSeconds()
	if leaderLease <= 0 {
		leaderLease = constant.DefaultLease
	}
	expectedLease := constant.TransferPrimaryLeaseMultiplier * leaderLease
	// Bound the grant: client.Ctx() is the shared etcd client's lifetime context, with
	// no deadline of its own and not tied to this call. A hung Grant on an unbounded
	// context would block for the client's whole lifetime instead of just failing this
	// transfer - and since some callers (e.g. TSO's primaryPriorityCheckLoop) invoke
	// TransferPrimary synchronously and are themselves waited on during shutdown, that
	// hang can prevent the server from ever closing its etcd client.
	grantCtx, grantCancel := context.WithTimeout(client.Ctx(), etcdutil.DefaultRequestTimeout)
	grantResp, err := client.Grant(grantCtx, expectedLease)
	grantCancel()
	if err != nil {
		return errors.Errorf("failed to grant lease for expected primary, err: %v", err)
	}

	primaryID := primaryIDs[nextPrimaryID]
	msParam := &keypath.MsParam{
		ServiceName: serviceName,
		GroupID:     keyspaceGroupID,
	}
	primary := &primaryData{
		raw:    primaryID,
		output: primaryID,
	}
	// Mark the expected primary first so the affinity guard is in place before the
	// current primary releases the leader key below. leaderKeyGuard was built from the
	// CreateRevision captured right after the IsServing() check above, before discovery
	// and this lease grant, so it fences the write to that exact leadership session for
	// the whole function, not just the gap between here and the commit. It is evaluated
	// atomically with the Put inside the same etcd transaction, so anything that
	// changed the leader key since - a fresh campaign included, even by this same
	// participant with the same MemberValue() - is caught at commit time.
	if err = markExpectedPrimaryFlag(client, msParam, primary, grantResp.ID, leaderKeyGuard); err != nil {
		revokeExpectedPrimaryLease(client, grantResp.ID)
		return errors.Errorf("failed to mark expected primary flag for %s, err: %v", serviceName, err)
	}

	// Resign the current primary by revoking its leader lease. This makes the local
	// IsServing() flip to false immediately, so the primary election loop steps down
	// and re-campaigns, where the affinity guard routes the leadership to the target.
	p.Resign()

	// Confirm the resign actually cleared the old leader key rather than trusting it
	// silently - see verifyLeaderKeyCleared's doc comment for why this check exists and
	// what it distinguishes.
	if err := verifyLeaderKeyCleared(client, leaderKeyPath, leaderResp.Kvs[0].CreateRevision); err != nil {
		return err
	}

	// Report success here without waiting for primaryID to actually win the campaign
	// or finish initializing. This means the worst-case unavailability window this
	// call can leave behind - if primaryID never wins a single campaign at all (down,
	// unreachable, or stuck) - is bounded by expectedLease, i.e. exactly one leader
	// lease (TransferPrimaryLeaseMultiplier == 1): that is how long the marker written
	// above stays valid and keeps every other candidate skipping campaigns in favor of
	// primaryID (see the affinity check next to GetExpectedPrimaryFlag in each
	// service's election loop). Once the TTL lapses the marker disappears and any live
	// member is free to win instead, so a completely absent target costs at most one
	// lease of downtime, never longer. A target that does win - even if it
	// subsequently fails to initialize and steps back down - clears the marker
	// immediately on winning (see DeleteExpectedPrimaryFlag, called before
	// initialization in campaignPrimary), so that path recovers well before the TTL
	// and does not compound with this bound.
	return nil
}

// revokeExpectedPrimaryLease revokes a lease granted for an expected-primary
// marker whose write failed or was rejected by the leadership guard, so a
// failed/aborted transfer never leaks a lease. Best effort: a failure here is
// logged, not propagated - the caller already has the original error to report,
// and the lease's own TTL bounds the residual leak if revoke also fails.
func revokeExpectedPrimaryLease(client *clientv3.Client, leaseID clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(client.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	if _, err := client.Revoke(ctx, leaseID); err != nil {
		log.Warn("failed to revoke expected primary lease after failed marker write",
			zap.Int64("lease-id", int64(leaseID)), errs.ZapError(err))
	}
}

func isSamePrimary(member discovery.ServiceRegistryEntry, primary string) bool {
	return primary != "" && (member.Name == primary || member.ServiceAddr == primary)
}
