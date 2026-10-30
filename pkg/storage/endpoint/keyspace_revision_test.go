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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestLoadKeyspaceMetaWithRevision(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	var corrupt func(*etcdserverpb.TxnResponse)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, &etcdutil.TestEtcdClusterOptions{
		ClientCfgModifier: func(config *clientv3.Config) {
			config.DialOptions = append(config.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				err := invoker(ctx, method, req, reply, cc, opts...)
				if response, ok := reply.(*etcdserverpb.TxnResponse); ok && err == nil && corrupt != nil {
					corrupt(response)
				}
				return err
			}))
		},
	})
	defer clean()
	store := NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, Name: "revision-keyspace"}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error { return store.SaveKeyspaceMeta(txn, meta) }))
	re.NoError(store.Save("unrelated-keyspace-write", "newer"))
	stored, err := client.Get(ctx, keypath.KeyspaceMetaPath(1))
	re.NoError(err)
	re.Less(stored.Kvs[0].ModRevision, stored.Header.Revision)
	loaded, revision, err := store.LoadKeyspaceMetaWithRevision(ctx, 1)
	re.NoError(err)
	re.Equal(meta, loaded)
	re.Equal(stored.Kvs[0].ModRevision, revision)

	for _, invalid := range []string{"wrong-key", "nonpositive-revision", "multiple-values", "unsuccessful", "missing-response"} {
		t.Run(invalid, func(t *testing.T) {
			re := require.New(t)
			corrupt = func(response *etcdserverpb.TxnResponse) {
				values := response.Responses[0].GetResponseRange()
				switch invalid {
				case "wrong-key":
					values.Kvs[0].Key = []byte("another-key")
				case "nonpositive-revision":
					values.Kvs[0].ModRevision = 0
				case "multiple-values":
					values.Kvs = append(values.Kvs, values.Kvs[0])
				case "unsuccessful":
					response.Succeeded = false
				case "missing-response":
					response.Responses = nil
				}
			}
			loaded, _, err := store.LoadKeyspaceMetaWithRevision(ctx, 1)
			corrupt = nil
			re.Error(err)
			re.Nil(loaded)
		})
	}
	for _, missing := range []string{"missing", "empty"} {
		t.Run(missing, func(t *testing.T) {
			re := require.New(t)
			if missing == "empty" {
				re.NoError(store.Save(keypath.KeyspaceMetaPath(2), ""))
			}
			loaded, _, err := store.LoadKeyspaceMetaWithRevision(ctx, 2)
			re.NoError(err)
			re.Nil(loaded)
		})
	}
	re.NoError(store.Save(keypath.KeyspaceMetaPath(2), "invalid-protobuf"))
	loaded, _, err = store.LoadKeyspaceMetaWithRevision(ctx, 2)
	re.Error(err)
	re.Nil(loaded)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = store.LoadKeyspaceMetaWithRevision(canceled, 1)
	re.ErrorIs(err, context.Canceled)
	_, _, err = NewStorageEndpoint(kv.NewMemoryKV(), nil).LoadKeyspaceMetaWithRevision(ctx, 1)
	re.ErrorContains(err, "does not support raw transaction with context")
}
