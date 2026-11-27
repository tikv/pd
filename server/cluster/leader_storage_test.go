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

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestLeaderKVTermFence(t *testing.T) {
	servers, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	const key = "/test-leader-metadata"
	for _, operation := range []string{"save", "remove", "transaction"} {
		for _, state := range []string{"live", "withdrawn", "lease replaced at commit"} {
			t.Run(operation+"/"+state, func(t *testing.T) {
				re := require.New(t)
				m := member.NewMember(servers[0], client, uint64(servers[0].Server.ID()))
				m.InitMemberInfo("", "", "leader")
				re.NoError(m.Campaign(t.Context(), 30))
				t.Cleanup(m.Resign)
				term, ok := m.GetLeadership().CaptureTerm()
				re.True(ok)
				base := kv.NewEtcdKVBase(client)
				fenced := &leaderKV{Base: base, runner: base, member: m, lease: m.GetLeadership().GetLease(), term: term}
				re.NoError(base.Save(key, "current"))
				if state == "withdrawn" {
					m.Resign()
				}
				if state == "lease replaced at commit" {
					// Keep the local lease valid and the election value unchanged. Only the
					// comparison against the captured etcd lease can reject this commit.
					const hook = "github.com/tikv/pd/pkg/storage/kv/beforeTxnCommit"
					re.NoError(failpoint.EnableCall(hook, func(ops []clientv3.Op) {
						if len(ops) == 0 || string(ops[0].KeyBytes()) != key {
							return
						}
						lease, err := client.Grant(t.Context(), 30)
						re.NoError(err)
						t.Cleanup(func() { _, err := client.Revoke(context.Background(), lease.ID); re.NoError(err) })
						_, err = client.Put(t.Context(), m.GetElectionPath(), m.MemberValue(), clientv3.WithLease(lease.ID))
						re.NoError(err)
						re.NoError(base.Save(key, "successor"))
						re.True(m.IsLeaseValid(fenced.lease))
					}))
					t.Cleanup(func() { re.NoError(failpoint.Disable(hook)) })
				}
				var err error
				switch operation {
				case "save":
					err = fenced.Save(key, "old")
				case "remove":
					err = fenced.Remove(key)
				case "transaction":
					err = fenced.RunInTxn(t.Context(), func(txn kv.Txn) error { return txn.Save(key, "old") })
				}
				expected := "current"
				switch state {
				case "live":
					re.NoError(err)
					expected = "old"
					if operation == "remove" {
						expected = ""
					}
				case "withdrawn":
					re.ErrorIs(err, errs.ErrNotLeader)
				default:
					re.ErrorIs(err, errs.ErrEtcdTxnConflict)
					expected = "successor"
				}
				value, err := base.Load(key)
				re.NoError(err)
				re.Equal(expected, value)
			})
		}
	}
}

func TestLeaderContextCanceledBeforeLeaseReset(t *testing.T) {
	re := require.New(t)
	servers, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	m := member.NewMember(servers[0], client, uint64(servers[0].Server.ID()))
	m.InitMemberInfo("", "", "leader")
	re.NoError(m.Campaign(t.Context(), 30))
	defer m.Resign()
	lease := m.GetLeadership().GetLease()
	previous, cancelPrevious := context.WithCancel(t.Context())
	re.True(m.SetLeadershipCancel(lease, cancelPrevious))
	current, cancelCurrent := context.WithCancel(t.Context())
	re.True(m.SetLeadershipCancel(lease, cancelCurrent))
	re.ErrorIs(previous.Err(), context.Canceled)
	re.NoError(current.Err())
	const hook = "github.com/tikv/pd/pkg/member/afterMemberLeaderWithdrawn"
	re.NoError(failpoint.EnableCall(hook, func(uint64) {
		re.Same(lease, m.GetLeadership().GetLease())
		re.ErrorIs(current.Err(), context.Canceled)
		late, cancelLate := context.WithCancel(t.Context())
		re.False(m.SetLeadershipCancel(lease, cancelLate))
		re.ErrorIs(late.Err(), context.Canceled)
	}))
	defer func() { re.NoError(failpoint.Disable(hook)) }()
	m.Resign()
}
