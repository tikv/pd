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

func TestCommittedRevision(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(cleanup)
	backend := NewEtcdKVBase(client)
	ctx := context.Background()
	var committed CommittedRevisionTxn
	require.NoError(t, backend.RunInTxn(ctx, func(txn Txn) error {
		committed = txn.(CommittedRevisionTxn)
		require.Zero(t, committed.CommitRevision())
		if err := txn.Save("bucket", "bits"); err != nil {
			return err
		}
		return txn.Save("owner", "tenant")
	}))
	revision := committed.CommitRevision()
	require.Positive(t, revision)
	resp, err := client.Get(ctx, "bucket")
	require.NoError(t, err)
	require.Equal(t, revision, resp.Kvs[0].ModRevision)
	resp, err = client.Get(ctx, "owner")
	require.NoError(t, err)
	require.Equal(t, revision, resp.Kvs[0].ModRevision)

	// Existing conditional transactions can now publish a cached revision after
	// commit, without another point read. A stale revision must reject all writes.
	require.NoError(t, backend.Save("bucket", "bits"))
	err = backend.RunInTxnWithConditions(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision("bucket"), "=", revision),
	}, func(txn Txn) error {
		committed = txn.(CommittedRevisionTxn)
		return txn.Save("owner", "wrong")
	})
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.Zero(t, committed.CommitRevision())
	owner, err := backend.Load("owner")
	require.NoError(t, err)
	require.Equal(t, "tenant", owner)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	err = backend.RunInTxn(cancelCtx, func(txn Txn) error {
		committed = txn.(CommittedRevisionTxn)
		return txn.Save("owner", "cancelled")
	})
	require.Error(t, err)
	require.Zero(t, committed.CommitRevision())
}
