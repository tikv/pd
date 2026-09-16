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

package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestEtcdUnguardedRawTxnBuilder(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	txn := NewEtcdKVBase(client).CreateRawTxn()
	// Keep the etcd builder's immediate validation for ordinary transactions.
	txn.If(RawTxnCondition{Key: "value", CmpType: RawTxnCmpNotExists})
	re.Panics(func() { txn.If() })
	txn.Then(RawTxnOp{OpType: RawTxnOpPut, Key: "value", Value: "new"})
	re.Panics(func() { txn.Then() })
	txn.Else()
	re.Panics(func() { txn.Else() })
	resp, err := txn.Commit()
	re.NoError(err)
	re.True(resp.Succeeded)
	// The original builder also permits committing the same transaction again.
	resp, err = txn.Commit()
	re.NoError(err)
	re.False(resp.Succeeded)
}

func TestEtcdWriteConditions(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	ctx := context.Background()
	const leaderKey = "/test-leader"
	campaign := func(value string) clientv3.LeaseID {
		lease, err := client.Grant(ctx, 60)
		re.NoError(err)
		_, err = client.Put(ctx, leaderKey, value, clientv3.WithLease(lease.ID))
		re.NoError(err)
		return lease.ID
	}
	leaseA := campaign("member-a")
	conditions := []clientv3.Cmp{clientv3.Compare(clientv3.LeaseValue(leaderKey), "=", int64(leaseA))}
	old := NewEtcdKVBase(client, conditions...)
	// Mutating the caller's slice must not change a retained storage view.
	conditions[0] = clientv3.Compare(clientv3.Value(leaderKey), "=", "wrong-member")
	testReadWrite(re, old)
	testSaveMultiple(re, old, 7)
	testLoadConflict(re, old)
	testRawTxn(re, old)
	re.NoError(old.Save("value", "old"))

	// Build transactions while A still owns the key, then change ownership
	// before Commit. Both Then and Else must be fenced independently of the
	// user's condition result.
	pending := make([]RawTxn, 0, 2)
	for _, expected := range []string{"new", "missing"} {
		pending = append(pending, old.CreateRawTxn().
			If(RawTxnCondition{Key: "value", CmpType: RawTxnCmpEqual, Value: expected}).
			Then(RawTxnOp{OpType: RawTxnOpPut, Key: "value", Value: "stale-then"}).
			Else(RawTxnOp{OpType: RawTxnOpPut, Key: "value", Value: "stale-else"}))
	}
	var current *etcdKVBase
	err := old.RunInTxn(ctx, func(txn Txn) error {
		re.NoError(txn.Save("value", "stale-transaction"))
		leaseB := campaign("member-b")
		current = NewEtcdKVBase(client, clientv3.Compare(clientv3.LeaseValue(leaderKey), "=", int64(leaseB)))
		return current.Save("value", "new")
	})
	re.ErrorIs(err, errs.ErrEtcdTxnConflict)
	for _, txn := range pending {
		_, err := txn.Commit()
		re.ErrorIs(err, errs.ErrEtcdTxnConflict)
	}
	assertStale := func() {
		re.ErrorIs(old.Save("value", "stale"), errs.ErrEtcdTxnConflict)
		re.ErrorIs(old.Remove("value"), errs.ErrEtcdTxnConflict)
		re.ErrorIs(old.RunInTxn(ctx, func(txn Txn) error {
			return txn.Remove("value")
		}), errs.ErrEtcdTxnConflict)
		value, err := old.Load("value")
		re.NoError(err)
		re.Equal("new", value)
	}
	assertStale()

	// The same member value in a later campaign must not revive old writes.
	leaseA2 := campaign("member-a")
	re.NotEqual(leaseA, leaseA2)
	assertStale()
	current = NewEtcdKVBase(client, clientv3.Compare(clientv3.LeaseValue(leaderKey), "=", int64(leaseA2)))
	re.NoError(current.Save("value", "new"))
	_, err = client.Revoke(ctx, leaseA2)
	re.NoError(err)
	re.ErrorIs(current.Save("value", "no-leader"), errs.ErrEtcdTxnConflict)
	assertStale()
	// Server-wide storage remains usable without a leader, e.g. for member data.
	re.NoError(NewEtcdKVBase(client).Save("value", "server-wide"))
}
