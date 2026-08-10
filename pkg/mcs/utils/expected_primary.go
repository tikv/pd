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
	"slices"
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
	current, deleted := deleteMarkerIfEquals(client, path, expectedValue)
	if deleted {
		log.Info("delete expected primary flag", zap.String("primary-path", path))
		return false
	}
	if current == "" {
		// The marker is already gone (deleted, expired, or it never existed), or the
		// transaction failed; in the latter case the marker TTL bounds the staleness.
		return false
	}
	// The marker was rewritten by a newer transfer while this member was winning.
	if p != nil && p.IsExpectedPrimary(current) {
		// The newer transfer also targets this member, which is already serving.
		// Clean the marker up so it does not linger until its TTL. Best effort:
		// on failure the TTL applies.
		if _, deleted := deleteMarkerIfEquals(client, path, current); deleted {
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
// transaction ("" when the marker does not exist or the transaction failed) and
// whether the marker was deleted. Note that etcd value comparisons always fail on a
// missing key, so want == "" never deletes anything and reports the current value.
func deleteMarkerIfEquals(client *clientv3.Client, path, want string) (current string, deleted bool) {
	resp, err := kv.NewSlowLogTxn(client).
		If(clientv3.Compare(clientv3.Value(path), "=", want)).
		Then(clientv3.OpGet(path), clientv3.OpDelete(path)).
		Else(clientv3.OpGet(path)).
		Commit()
	if err != nil {
		log.Warn("failed to delete expected primary flag", zap.String("primary-path", path), errs.ZapError(err))
		return "", false
	}
	kvs := resp.Responses[0].GetResponseRange().GetKvs()
	if !resp.Succeeded {
		if len(kvs) == 0 {
			return "", false
		}
		return string(kvs[0].Value), false
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
	return want, true
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

// TransferPrimary transfers the primary of the specified service to a target member.
//
// It writes the expected primary flag pointing at the target (with a TTL of a few
// leader leases, see constant.TransferPrimaryLeaseMultiplier) and then resigns the
// current primary by revoking its leader lease, so the re-election picks up the
// target. The flag write
// happens before the resignation on purpose: it guarantees the affinity guard is in
// place before the leader key is released, so no other member can win the gap.
//
// keyspaceGroupID is optional, only used for TSO service. p must be the participant
// of the current serving primary (the API ensures the request runs on the primary).
// ctx bounds the post-transfer verification below (see waitForPrimaryTransfer):
// callers driven by an HTTP request should pass the request's context so a client
// that gives up does not leave the verification polling for the full timeout
// regardless.
func TransferPrimary(ctx context.Context, client *clientv3.Client, p *member.Participant, serviceName,
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
	leaderResp, err := client.Get(client.Ctx(), leaderKeyPath)
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
	grantResp, err := client.Grant(client.Ctx(), expectedLease)
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

	// Verify the transfer actually landed on primaryID before reporting success. A
	// member that predates this marker mechanism (e.g. a not-yet-upgraded replica
	// during a rolling upgrade) does not read the marker and can win the now-vacated
	// leader key through its own unguarded campaign, so a caller-specified target is
	// not otherwise guaranteed. newPrimary == "" (pick any valid secondary) has no
	// fixed target to verify against, so it is skipped.
	//
	// primaryID - not newPrimary - is what gets compared: newPrimary is whatever the
	// caller passed (registry name or address; the default config gives services a
	// name distinct from their advertise address), but primaryID is always the
	// resolved ServiceAddr, the one identity the marker mechanism itself already
	// relies on (Participant.IsExpectedPrimary matches a marker value against this
	// same member's ListenUrls). Comparing against newPrimary directly would silently
	// never match whenever it was supplied as a name, since a target's own proto Name
	// (e.g. TSO's "address-groupID") isn't the registry name either.
	//
	// The timeout matches the marker's own TTL (expectedLease), not a short fixed
	// value: such a pre-marker member never cleans the marker up on winning, so if it
	// is itself cycled out again within this window - e.g. as the rolling upgrade
	// continues - the still-valid marker lets primaryID's own guarded campaign
	// recover the transfer. Failure is only reported once that whole recovery window
	// has actually elapsed.
	if newPrimary != "" {
		verifyCtx, cancel := context.WithTimeout(ctx, time.Duration(expectedLease)*time.Second)
		defer cancel()
		return waitForPrimaryTransfer(verifyCtx, client, leaderKeyPath, serviceName, newPrimary, primaryID)
	}
	return nil
}

// primaryTransferStableWindow is how long the leader key must continuously show the
// target's identity before waitForPrimaryTransfer reports success. A single matching
// read - or even a couple of them a few hundred milliseconds apart - is not enough:
// the leader key is written as soon as the campaign transaction commits, well before
// the winner is durably promoted. Between those two points it still runs its own
// post-campaign steps - reconciling the expected-primary marker, running
// primaryCallbacks, and for TSO initializing the allocator (which syncs the
// timestamp oracle and does real I/O) - and a failure in any of them makes it step
// back down again. Requiring the match to hold continuously for this long narrows -
// it cannot eliminate - the window where a transient, soon-to-be-reverted win would
// otherwise be reported as a confirmed success: it only helps if the failure surfaces
// (and is reflected back to the leader key via the loser's own cleanup) within this
// window. There's no empirical measurement behind this specific value; it's a
// judgment call sized to comfortably outlast the post-campaign steps above under
// normal conditions, not a proof they always finish by then.
const primaryTransferStableWindow = 2 * time.Second

// waitForPrimaryTransfer polls the leader key until its holder's identity has
// continuously matched expectedIdentity, on the same election term, for at least
// primaryTransferStableWindow, or ctx is done. requestedPrimary is only used for the
// error messages below - it's
// whatever identity form the caller originally supplied (registry name or address),
// kept around purely so a failure message reads naturally to whoever issued the
// request; expectedIdentity (see TransferPrimary) is what's actually compared.
//
// This reports success purely by identity, with no notion of which member version
// won: the leader key's value is the same participant proto regardless of which
// binary wrote it, so a pre-marker member's win is read exactly like any other and
// simply fails to match unless it happens to be expectedIdentity itself - which is a
// legitimate outcome, not a special case to detect.
//
// ctx bounds when this returns: it carries the caller's own timeout (see
// TransferPrimary) and, when derived from an HTTP request's context, also lets an
// abandoned request stop this from polling for the rest of that timeout regardless.
func waitForPrimaryTransfer(ctx context.Context, client *clientv3.Client, leaderKeyPath, serviceName, requestedPrimary, expectedIdentity string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var currentPrimary string
	var matchSince time.Time
	// matchedRevision pins the stability window to a single election term. The
	// leader key is deleted on every step-down/expiry and recreated on the next
	// win (Leadership.Campaign requires CreateRevision(leaderKey) == 0), so its
	// ModRevision changes on every fresh term. Without this, a target that loses
	// and re-wins the key between two polls - invisible to us if both polls land
	// on a "matched" state - would keep the timer running across that gap and
	// could report success on a term that has barely started, still mid
	// primaryCallbacks/TSO initialization.
	var matchedRevision int64
	for {
		target := member.NewParticipantByService(serviceName)
		if ok, modRevision, err := etcdutil.GetProtoMsgWithModRev(client, leaderKeyPath, target); err == nil && ok {
			if slices.Contains(target.GetListenUrls(), expectedIdentity) {
				if matchSince.IsZero() || modRevision != matchedRevision {
					matchSince = time.Now()
					matchedRevision = modRevision
				} else if time.Since(matchSince) >= primaryTransferStableWindow {
					return nil
				}
			} else {
				matchSince = time.Time{}
			}
			currentPrimary = target.GetName()
		} else {
			matchSince = time.Time{}
			currentPrimary = ""
		}
		select {
		case <-ctx.Done():
			if currentPrimary == "" {
				return errors.Errorf("transfer requested to %s, but no primary was elected: %v", requestedPrimary, ctx.Err())
			}
			return errors.Errorf("transfer requested to %s, but %s is currently primary: %v", requestedPrimary, currentPrimary, ctx.Err())
		case <-ticker.C:
		}
	}
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
