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

package endpoint

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestAffinityRevisionBatchAndTermFence(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	se := NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
	ctx := t.Context()
	lease, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = client.Put(ctx, "affinity-test-primary", "primary", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	conditions := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value("affinity-test-primary"), "=", "primary"),
		clientv3.Compare(clientv3.LeaseValue("affinity-test-primary"), "=", lease.ID),
	}
	write := func(txn kv.Txn) error {
		for _, id := range []string{"_tidb_pt_10_p1", "_tidb_pt_10_p2"} {
			if err := se.SaveAffinityGroup(txn, id, map[string]string{"id": id}); err != nil {
				return err
			}
		}
		return nil
	}
	require.NoError(t, se.RunInTxnWithConditions(ctx, conditions, write), "a batch must write its shared revision key once")
	token, err := se.Load(AffinityTableRevisionKey("_tidb_pt_10_p1"))
	require.NoError(t, err)
	require.NotEmpty(t, token)
	// Same identity, new lease: an old-primary commit must still fail.
	newLease, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = client.Put(ctx, "affinity-test-primary", "primary", clientv3.WithLease(newLease.ID))
	require.NoError(t, err)
	require.Error(t, se.RunInTxnWithConditions(ctx, conditions, write))
	unchanged, err := se.Load(AffinityTableRevisionKey("_tidb_pt_10_p1"))
	require.NoError(t, err)
	require.Equal(t, token, unchanged)

	require.NoError(t, se.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := se.SaveRule(txn, "rule1", map[string]string{"id": "rule1"}); err != nil {
			return err
		}
		return se.SaveRegionRule(txn, "range1", map[string]string{"id": "range1"})
	}))
	metadata, err := se.Load(AffinityMetadataRevisionKey())
	require.NoError(t, err)
	require.NotEmpty(t, metadata)
	require.NoError(t, se.RunInTxn(ctx, func(txn kv.Txn) error { return se.DeleteRegionRule(txn, "range1") }))
	updated, err := se.Load(AffinityMetadataRevisionKey())
	require.NoError(t, err)
	require.NotEqual(t, metadata, updated)

	batch := make([]func(kv.Txn) error, 130)
	for i := range batch {
		id := fmt.Sprintf("custom-batch-%03d", i)
		batch[i] = func(txn kv.Txn) error {
			return se.SaveAffinityGroup(txn, id, map[string]string{"id": id})
		}
	}
	require.NoError(t, RunBatchOpInTxnWithLimit(ctx, se, batch, etcdutil.MaxEtcdTxnOps/2), "custom groups need capacity for one generation token per group")
}
