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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

func TestRunInTxnWithConditionsFallback(t *testing.T) {
	re := require.New(t)
	store := NewStorageEndpoint(kv.NewMemoryKV(), nil)
	called := false
	runTxn := func(kv.Txn) error {
		called = true
		return nil
	}

	re.NoError(store.RunInTxnWithConditions(context.Background(), nil, runTxn))
	re.True(called)

	called = false
	err := store.RunInTxnWithConditions(
		context.Background(),
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value("leader"), "=", "current")},
		runTxn,
	)
	re.ErrorContains(err, "does not support conditional transactions")
	re.False(called)
}

func TestLoadKeyspaceGroupRevisionComparisonKeepsRequestBounded(t *testing.T) {
	re := require.New(t)
	options := &etcdutil.TestEtcdClusterOptions{
		ServerCfgModifier: func(cfg *embed.Config) {
			cfg.MaxRequestBytes = 6 * 1024
		},
	}
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, options)
	defer clean()

	ctx := context.Background()
	store := NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
	group := &KeyspaceGroup{
		ID:        1,
		UserKind:  Basic.String(),
		Keyspaces: make([]uint32, 1000),
	}
	for i := range group.Keyspaces {
		group.Keyspaces[i] = uint32(i)
	}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, group)
	}))

	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		loaded, err := store.LoadKeyspaceGroup(txn, group.ID)
		if err != nil {
			return err
		}
		return store.SaveKeyspaceGroup(txn, loaded)
	}))
}
