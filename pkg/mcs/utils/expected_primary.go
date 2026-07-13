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

// markExpectedPrimaryFlag marks the expected primary flag when the primary is specified.
func markExpectedPrimaryFlag(client *clientv3.Client, msParam *keypath.MsParam, primary *primaryData, leaseID clientv3.LeaseID) error {
	path := keypath.ExpectedPrimaryPath(msParam)
	log.Info("set expected primary flag", zap.String("primary-path", path), zap.String("primary", primary.output))
	// write a flag to indicate the expected primary.
	resp, err := kv.NewSlowLogTxn(client).
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

// DeleteExpectedPrimaryFlag deletes the expected primary flag once the target has
// won the campaign, so that in steady state the flag is absent and a subsequent
// failure of the new primary triggers a free re-election immediately instead of
// waiting for the flag's TTL to expire.
//
// The delete is conditional on the flag still holding `expectedValue` (the value
// the winner campaigned with). This prevents clobbering a newer transfer that may
// have already rewritten the flag to a different target while this primary was
// winning. It is best-effort: the flag's TTL is the backstop, so a failure here is
// only logged and never blocks serving.
//
// The flag is bound to an etcd lease, so after deleting the key we also revoke that
// lease. Otherwise we would leave the key gone but the lease alive — exactly the
// inconsistent state that issue #10875 is about — and leak a lease until its TTL
// expires.
func DeleteExpectedPrimaryFlag(client *clientv3.Client, msParam *keypath.MsParam, expectedValue string) {
	if expectedValue == "" {
		// Normal election without a transfer in progress, nothing to clean up.
		return
	}
	path := keypath.ExpectedPrimaryPath(msParam)
	// Read the key (to capture its lease) and delete it in the same conditional txn.
	resp, err := kv.NewSlowLogTxn(client).
		If(clientv3.Compare(clientv3.Value(path), "=", expectedValue)).
		Then(clientv3.OpGet(path), clientv3.OpDelete(path)).
		Commit()
	if err != nil {
		log.Warn("failed to delete expected primary flag", zap.String("primary-path", path), errs.ZapError(err))
		return
	}
	if !resp.Succeeded {
		log.Info("skip deleting expected primary flag, it has been changed or already gone",
			zap.String("primary-path", path), zap.String("expected-value", expectedValue))
		return
	}
	log.Info("delete expected primary flag", zap.String("primary-path", path))
	// Revoke the lease the flag was bound to, if any, so no lease is leaked.
	kvs := resp.Responses[0].GetResponseRange().GetKvs()
	if len(kvs) == 0 || kvs[0].Lease == 0 {
		return
	}
	leaseID := clientv3.LeaseID(kvs[0].Lease)
	// Bound the revoke: this runs on the campaign path before the primary is promoted
	// to serving, and the cleanup is best-effort, so a hung RPC must not block serving.
	ctx, cancel := context.WithTimeout(client.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	if _, err := client.Revoke(ctx, leaseID); err != nil {
		log.Warn("failed to revoke expected primary flag lease",
			zap.String("primary-path", path), zap.Int64("lease-id", int64(leaseID)), errs.ZapError(err))
	}
}

// ExpectedPrimaryCmp returns an etcd comparison asserting the expected primary flag
// still equals `expectedValue`. The caller appends it to the campaign transaction so
// that winning the leader key and "I am still the expected primary" become atomic:
// if a concurrent transfer rewrote the flag after it was read, the campaign txn
// fails and this member does not become primary. It returns nil when expectedValue
// is empty (normal election, no transfer in progress), in which case the campaign
// must not be constrained.
func ExpectedPrimaryCmp(msParam *keypath.MsParam, expectedValue string) *clientv3.Cmp {
	if expectedValue == "" {
		return nil
	}
	cmp := clientv3.Compare(clientv3.Value(keypath.ExpectedPrimaryPath(msParam)), "=", expectedValue)
	return &cmp
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
func TransferPrimary(client *clientv3.Client, p *member.Participant, serviceName,
	oldPrimary, newPrimary string, keyspaceGroupID uint32, tsoMembersMap map[string]bool) error {
	if p == nil || !p.IsServing() {
		return errors.New("current member is not serving as primary, please check leadership")
	}
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

	msParam := &keypath.MsParam{
		ServiceName: serviceName,
		GroupID:     keyspaceGroupID,
	}
	primary := &primaryData{
		raw:    primaryIDs[nextPrimaryID],
		output: primaryIDs[nextPrimaryID],
	}
	// Mark the expected primary first so the affinity guard is in place before the
	// current primary releases the leader key below.
	if err = markExpectedPrimaryFlag(client, msParam, primary, grantResp.ID); err != nil {
		return errors.Errorf("failed to mark expected primary flag for %s, err: %v", serviceName, err)
	}

	// Resign the current primary by revoking its leader lease. This makes the local
	// IsServing() flip to false immediately, so the primary election loop steps down
	// and re-campaigns, where the affinity guard routes the leadership to the target.
	p.Resign()
	return nil
}

func isSamePrimary(member discovery.ServiceRegistryEntry, primary string) bool {
	return primary != "" && (member.Name == primary || member.ServiceAddr == primary)
}
