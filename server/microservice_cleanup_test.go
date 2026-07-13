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

package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestCleanupMicroserviceMetadataInPDMode(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12345)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Keyspaces: []uint32{1},
		}); err != nil {
			return err
		}
		if err := store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
			Id:   1,
			Name: "keyspace-1",
			Config: map[string]string{
				keyspace.TSOKeyspaceGroupIDKey: strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10),
				"gc_life_time":                 "10m",
			},
		}); err != nil {
			return err
		}
		return store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
			Id:   2,
			Name: "keyspace-2",
			Config: map[string]string{
				"custom": "value",
			},
		})
	}))
	_, err := client.Put(ctx, keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379"), "tso")
	re.NoError(err)
	_, err = client.Put(ctx, keypath.RegistryPath(mcs.SchedulingServiceName, "127.0.0.1:3379"), "scheduling")
	re.NoError(err)
	_, err = client.Put(ctx, keypath.ElectionPath(&keypath.MsParam{
		ServiceName: mcs.TSOServiceName,
		GroupID:     constant.DefaultKeyspaceGroupID,
	}), "primary")
	re.NoError(err)
	_, err = client.Put(ctx, keypath.TimestampPath(constant.DefaultKeyspaceGroupID), "timestamp")
	re.NoError(err)

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))
	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))

	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Empty(groups)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		re.NoError(err)
		re.NotNil(meta)
		re.NotContains(meta.GetConfig(), keyspace.TSOKeyspaceGroupIDKey)
		re.Equal("10m", meta.GetConfig()["gc_life_time"])

		meta, err = store.LoadKeyspaceMeta(txn, 2)
		re.NoError(err)
		re.NotNil(meta)
		re.Equal("value", meta.GetConfig()["custom"])
		return nil
	}))
	resp, err := etcdutil.EtcdKVGet(client, microserviceEtcdPrefix(), clientv3.WithPrefix())
	re.NoError(err)
	re.Empty(resp.Kvs)
	resp, err = etcdutil.EtcdKVGet(client, keypath.TimestampPath(constant.DefaultKeyspaceGroupID))
	re.NoError(err)
	re.Len(resp.Kvs, 1)
	re.Equal("timestamp", string(resp.Kvs[0].Value))
}

func TestScheduleMicroserviceMetadataCleanupReturnsImmediately(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12348)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
		})
	}))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/SlowEtcdKVGet", "return(1)"))
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/SlowEtcdKVGet")
	})

	start := time.Now()
	svr.scheduleMicroserviceMetadataCleanup(ctx)
	re.Less(time.Since(start), 200*time.Millisecond)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/SlowEtcdKVGet"))
	svr.serverLoopWg.Wait()
}

func TestCleanupMicroserviceMetadataInPDModeRejectsNonDefaultGroup(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12346)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
		}); err != nil {
			return err
		}
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       1,
			UserKind: endpoint.Standard.String(),
		})
	}))
	_, err := client.Put(ctx, keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379"), "tso")
	re.NoError(err)

	err = svr.cleanupMicroserviceMetadataInPDMode(ctx)
	re.ErrorContains(err, "non-default TSO keyspace group 1")
	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 2)
	resp, err := etcdutil.EtcdKVGet(client, microserviceEtcdPrefix(), clientv3.WithPrefix())
	re.NoError(err)
	re.Len(resp.Kvs, 1)
}

func TestCleanupMicroserviceMetadataInPDModeRejectsNonDefaultKeyspaceAssignment(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12347)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
		})
	}))
	metas := make([]*keyspacepb.KeyspaceMeta, 0, etcdutil.MaxEtcdTxnOps+1)
	for id := uint32(1); id <= etcdutil.MaxEtcdTxnOps; id++ {
		metas = append(metas, newKeyspaceMetaWithTSOGroup(id, "0"))
	}
	invalidID := uint32(etcdutil.MaxEtcdTxnOps + 1)
	metas = append(metas, newKeyspaceMetaWithTSOGroup(invalidID, "1"))
	saveKeyspaceMetas(ctx, re, store, metas)
	_, err := client.Put(ctx, keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379"), "tso")
	re.NoError(err)

	err = svr.cleanupMicroserviceMetadataInPDMode(ctx)
	re.ErrorContains(err, "keyspace "+strconv.FormatUint(uint64(invalidID), 10)+" is assigned to non-default TSO keyspace group 1")
	var group *endpoint.KeyspaceGroup
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		var err error
		group, err = store.LoadKeyspaceGroup(txn, constant.DefaultKeyspaceGroupID)
		if err != nil {
			return err
		}
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		re.NoError(err)
		re.Equal("0", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
		return nil
	}))
	re.NotNil(group)
	resp, err := etcdutil.EtcdKVGet(client, microserviceEtcdPrefix(), clientv3.WithPrefix())
	re.NoError(err)
	re.Len(resp.Kvs, 1)
}

func TestCleanupMicroserviceMetadataInPDModeWithLargeKeyspaceBatch(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12349)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
		})
	}))
	metas := make([]*keyspacepb.KeyspaceMeta, 0, etcdutil.MaxEtcdTxnOps)
	for id := uint32(1); id <= etcdutil.MaxEtcdTxnOps; id++ {
		metas = append(metas, newKeyspaceMetaWithTSOGroup(id, "0"))
	}
	saveKeyspaceMetas(ctx, re, store, metas)

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		loaded, err := store.LoadRangeKeyspace(txn, constant.StartKeyspaceID, 0)
		if err != nil {
			return err
		}
		re.Len(loaded, etcdutil.MaxEtcdTxnOps)
		for _, meta := range loaded {
			re.NotContains(meta.GetConfig(), keyspace.TSOKeyspaceGroupIDKey)
		}
		return nil
	}))
}

func newKeyspaceMetaWithTSOGroup(id uint32, groupID string) *keyspacepb.KeyspaceMeta {
	return &keyspacepb.KeyspaceMeta{
		Id:   id,
		Name: "keyspace-" + strconv.FormatUint(uint64(id), 10),
		Config: map[string]string{
			keyspace.TSOKeyspaceGroupIDKey: groupID,
		},
	}
}

func saveKeyspaceMetas(
	ctx context.Context,
	re *require.Assertions,
	store storage.Storage,
	metas []*keyspacepb.KeyspaceMeta,
) {
	for start := 0; start < len(metas); start += microserviceMetadataCleanupBatchSize {
		end := min(start+microserviceMetadataCleanupBatchSize, len(metas))
		re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
			for _, meta := range metas[start:end] {
				if err := store.SaveKeyspaceMeta(txn, meta); err != nil {
					return err
				}
			}
			return nil
		}))
	}
}
