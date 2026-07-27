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
		return store.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{
			Id:   1,
			Name: "keyspace-1",
			Config: map[string]string{
				keyspace.TSOKeyspaceGroupIDKey: strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10),
				"gc_life_time":                 "10m",
			},
		})
	}))

	registryPath := keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379")
	electionPath := keypath.ElectionPath(&keypath.MsParam{
		ServiceName: mcs.TSOServiceName,
		GroupID:     constant.DefaultKeyspaceGroupID,
	})
	timestampPath := keypath.TimestampPath(constant.DefaultKeyspaceGroupID)
	for path, value := range map[string]string{
		registryPath:  "tso",
		electionPath:  "primary",
		timestampPath: "timestamp",
	} {
		_, err := client.Put(ctx, path, value)
		re.NoError(err)
	}

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))
	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))

	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Empty(groups)
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		re.NoError(err)
		re.NotNil(meta)
		re.Equal("0", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
		re.Equal("10m", meta.GetConfig()["gc_life_time"])
		return nil
	}))
	for path, value := range map[string]string{
		registryPath:  "tso",
		electionPath:  "primary",
		timestampPath: "timestamp",
	} {
		resp, err := etcdutil.EtcdKVGet(client, path)
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		re.Equal(value, string(resp.Kvs[0].Value))
	}
}

func TestCleanupMicroserviceMetadataIgnoresLeasedRegistryKeys(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12351)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:        constant.DefaultKeyspaceGroupID,
			UserKind:  endpoint.Basic.String(),
			Keyspaces: []uint32{1},
		})
	}))

	registryPath := keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379")
	leaseID, err := etcdutil.EtcdKVPutWithTTL(ctx, client, registryPath, "tso", 60)
	re.NoError(err)
	re.NotZero(leaseID)
	defer func() {
		_, _ = client.Revoke(ctx, leaseID)
	}()

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))
	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Empty(groups)
	resp, err := etcdutil.EtcdKVGet(client, registryPath)
	re.NoError(err)
	re.Len(resp.Kvs, 1)
	re.Equal(leaseID, clientv3.LeaseID(resp.Kvs[0].Lease))
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

func TestScheduleMicroserviceMetadataCleanupStopsOnRejectedState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12353)
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

	svr.scheduleMicroserviceMetadataCleanup(ctx)
	done := make(chan struct{})
	go func() {
		svr.serverLoopWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("cleanup task kept retrying a rejected metadata state")
	}
}

func TestMicroserviceMetadataCleanupTransitionDetection(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12350)
	defer keypath.ResetClusterID()

	store := storage.NewStorageWithEtcdBackend(client)
	svr := &Server{storage: store, client: client}
	needsCleanup, err := svr.validateMicroserviceMetadataCleanup()
	re.NoError(err)
	re.False(needsCleanup)

	_, err = client.Put(ctx, keypath.RegistryPath(mcs.TSOServiceName, "127.0.0.1:3379"), "tso")
	re.NoError(err)
	needsCleanup, err = svr.validateMicroserviceMetadataCleanup()
	re.NoError(err)
	re.False(needsCleanup)

	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		return store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID:       constant.DefaultKeyspaceGroupID,
			UserKind: endpoint.Basic.String(),
		})
	}))
	needsCleanup, err = svr.validateMicroserviceMetadataCleanup()
	re.NoError(err)
	re.True(needsCleanup)
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

	err := svr.cleanupMicroserviceMetadataInPDMode(ctx)
	re.ErrorContains(err, "non-default TSO keyspace group 1")
	groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
	re.NoError(err)
	re.Len(groups, 2)
}

func TestCleanupMicroserviceMetadataInPDModePreservesAssignmentMarkers(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	keypath.SetClusterID(12347)
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
		return store.SaveKeyspaceMeta(txn, newKeyspaceMetaWithTSOGroup(1, "1"))
	}))

	re.NoError(svr.cleanupMicroserviceMetadataInPDMode(ctx))
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := store.LoadKeyspaceMeta(txn, 1)
		re.NoError(err)
		re.Equal("1", meta.GetConfig()[keyspace.TSOKeyspaceGroupIDKey])
		return nil
	}))
}

func TestCleanupMicroserviceMetadataInPDModeRejectsGroupTransition(t *testing.T) {
	testCases := []struct {
		name       string
		transition func(*endpoint.KeyspaceGroup)
		errText    string
	}{
		{
			name: "splitting",
			transition: func(group *endpoint.KeyspaceGroup) {
				group.SplitState = &endpoint.SplitState{SplitSource: group.ID}
			},
			errText: "splitting",
		},
		{
			name: "merging",
			transition: func(group *endpoint.KeyspaceGroup) {
				group.MergeState = &endpoint.MergeState{MergeList: []uint32{1}}
			},
			errText: "merging",
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
			defer clean()
			keypath.SetClusterID(uint64(12400 + i))
			defer keypath.ResetClusterID()

			store := storage.NewStorageWithEtcdBackend(client)
			svr := &Server{storage: store, client: client}
			group := &endpoint.KeyspaceGroup{
				ID:       constant.DefaultKeyspaceGroupID,
				UserKind: endpoint.Basic.String(),
			}
			testCase.transition(group)
			re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
				return store.SaveKeyspaceGroup(txn, group)
			}))

			err := svr.cleanupMicroserviceMetadataInPDMode(ctx)
			re.ErrorContains(err, testCase.errText)
			groups, err := store.LoadKeyspaceGroups(constant.DefaultKeyspaceGroupID, 0)
			re.NoError(err)
			re.Len(groups, 1)
		})
	}
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
