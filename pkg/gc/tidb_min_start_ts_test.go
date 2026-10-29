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

package gc

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func newMetaServiceGroupGCFixture(t *testing.T) (*endpoint.StorageEndpoint, *GCStateManager, *clientv3.Client, func()) {
	t.Helper()
	re := require.New(t)
	servers, groupClient, cleanGroup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(cleanGroup)
	opt := newGCStateManagerForTestOptions{metaServiceGroups: map[string]string{"group1": strings.Join(groupClient.Endpoints(), ",")}}
	opt.generateKeyspacesByCount(2)
	storage, _, manager, clean, cancel := newGCStateManagerForTest(t, opt)
	t.Cleanup(clean)
	t.Cleanup(cancel)
	meta, err := manager.keyspaceManager.LoadKeyspaceByID(1)
	re.NoError(err)
	re.Equal("group1", meta.Config[keyspace.MetaServiceGroupIDKey])
	return storage, manager, groupClient, servers[0].Close
}

func TestMetaServiceGroupMinStartTS(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	storage, manager, groupClient, _ := newMetaServiceGroupGCFixture(t)
	provider := storage.GetGCStateProvider()

	lease, err := groupClient.Grant(ctx, 60)
	re.NoError(err)
	reportKey := keypath.CompatibleTiDBMinStartTSPrefix(1) + "tidb-group"
	_, err = groupClient.Put(ctx, reportKey, "100", clientv3.WithLease(lease.ID))
	re.NoError(err)
	result, err := manager.AdvanceTxnSafePoint(1, 200, time.Now())
	re.NoError(err)
	re.Equal(uint64(100), result.NewTxnSafePoint)
	re.Contains(result.BlockerDescription, "tidb-group")
	persisted, err := provider.LoadTxnSafePoint(1)
	re.NoError(err)
	re.Equal(uint64(100), persisted)
	revision, err := storage.Load(keypath.GCStateRevisionPath())
	re.NoError(err)
	re.NotEmpty(revision)
	for _, path := range []string{keypath.TxnSafePointPath(1), keypath.GCStateRevisionPath()} {
		response, err := groupClient.Get(ctx, path)
		re.NoError(err)
		re.Empty(response.Kvs)
	}
	_, err = groupClient.Revoke(ctx, lease.ID)
	re.NoError(err)
	response, err := groupClient.Get(ctx, reportKey)
	re.NoError(err)
	re.Empty(response.Kvs)
	result, err = manager.AdvanceTxnSafePoint(1, 200, time.Now())
	re.NoError(err)
	re.Equal(uint64(200), result.NewTxnSafePoint)
}

func TestMetaServiceGroupMinStartTSIsolation(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	storage, manager, groupClient, _ := newMetaServiceGroupGCFixture(t)
	// Reports on PD and under another keyspace must not substitute for the assigned group's report.
	re.NoError(storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(1)+"wrong-backend", "30"))
	_, err := groupClient.Put(ctx, keypath.CompatibleTiDBMinStartTSPrefix(2)+"other-keyspace", "20")
	re.NoError(err)
	reportKey := keypath.CompatibleTiDBMinStartTSPrefix(1) + "tidb-group"
	_, err = groupClient.Put(ctx, reportKey, "100")
	re.NoError(err)
	disabled := false
	re.NoError(manager.metaServiceGroupManager.PatchStatus(ctx, "group1", &keyspace.MetaServiceGroupStatusPatch{Enabled: &disabled}))
	now := time.Now()
	_, err = manager.SetGCBarrier(1, "backup", 80, time.Hour, now)
	re.NoError(err)
	result, err := manager.AdvanceTxnSafePoint(1, 200, now)
	re.NoError(err)
	re.Equal(uint64(80), result.NewTxnSafePoint)
	re.Contains(result.BlockerDescription, "backup")
	_, err = manager.DeleteGCBarrier(1, "backup")
	re.NoError(err)
	_, err = manager.SetGlobalGCBarrier(ctx, "global-backup", 90, time.Hour, now)
	re.NoError(err)
	result, err = manager.AdvanceTxnSafePoint(1, 200, now)
	re.NoError(err)
	re.Equal(uint64(90), result.NewTxnSafePoint)
	re.Contains(result.BlockerDescription, "global-backup")
	_, err = manager.DeleteGlobalGCBarrier(ctx, "global-backup")
	re.NoError(err)
	minimum, updated, err := manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
	re.NoError(err)
	re.True(updated)
	re.Equal(uint64(100), minimum.SafePoint)
	re.Equal("tidb_min_start_ts_tidb-group", minimum.ServiceID)

	// Reading a changed report must not reuse a cached result or decrease an existing safe point.
	_, err = groupClient.Put(ctx, reportKey, "50")
	re.NoError(err)
	result, err = manager.AdvanceTxnSafePoint(1, 200, now)
	re.NoError(err)
	re.Equal(uint64(100), result.NewTxnSafePoint)
	_, err = groupClient.Put(ctx, reportKey, "150")
	re.NoError(err)
	result, err = manager.AdvanceTxnSafePoint(1, 200, now)
	re.NoError(err)
	re.Equal(uint64(150), result.NewTxnSafePoint)
}

func TestMetaServiceGroupMinStartTSLocalFallback(t *testing.T) {
	re := require.New(t)
	storage, manager, groupClient, _ := newMetaServiceGroupGCFixture(t)
	for _, id := range []uint32{1, 2, constant.NullKeyspaceID} {
		if id != constant.NullKeyspaceID {
			op := keyspace.OpDel
			if id == 2 {
				op = keyspace.OpPut
			}
			meta, err := manager.keyspaceManager.LoadKeyspaceByID(id)
			re.NoError(err)
			_, err = manager.keyspaceManager.UpdateKeyspaceConfig(meta.Name, []*keyspace.Mutation{{Op: op, Key: keyspace.MetaServiceGroupIDKey, Value: ""}})
			re.NoError(err)
		}
		re.NoError(storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(id)+"tidb-local", "100"))
		_, err := groupClient.Put(context.Background(), keypath.CompatibleTiDBMinStartTSPrefix(id)+"wrong-backend", "20")
		re.NoError(err)
		result, err := manager.AdvanceTxnSafePoint(id, 200, time.Now())
		re.NoError(err)
		re.Equal(uint64(100), result.NewTxnSafePoint)
		re.Contains(result.BlockerDescription, "tidb-local")
	}
}

func TestMetaServiceGroupMinStartTSFailure(t *testing.T) {
	for _, failure := range []string{"missing-group", "invalid-group-id", "empty-endpoints", "invalid-endpoints", "missing-manager", "TLS", "unavailable-backend", "malformed-report"} {
		t.Run(failure, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			storage, manager, groupClient, stopGroup := newMetaServiceGroupGCFixture(t)
			now := time.Now()
			_, err := manager.AdvanceTxnSafePoint(1, 40, now)
			re.NoError(err)
			_, err = manager.SetGCBarrier(1, "expired", 60, time.Second, now.Add(-time.Minute))
			re.NoError(err)
			// The report failure happens after lazy deletion has been staged in the write batch.
			barrierPath := keypath.GCBarrierPath(1, "expired")
			barrier, err := storage.Load(barrierPath)
			re.NoError(err)
			re.NotEmpty(barrier)
			revision, err := storage.Load(keypath.GCStateRevisionPath())
			re.NoError(err)
			groupID := "group1"
			addresses := strings.Join(groupClient.Endpoints(), ",")
			var tlsLoader func() (*tls.Config, error)
			switch failure {
			case "missing-group", "invalid-group-id":
				groupID = "missing"
				if failure == "invalid-group-id" {
					groupID = "invalid/id"
				}
				// Simulate inconsistent persisted metadata that the normal config API rejects.
				re.NoError(storage.RunInTxn(ctx, func(txn kv.Txn) error {
					meta, err := storage.LoadKeyspaceMeta(txn, 1)
					if err != nil {
						return err
					}
					meta.Config[keyspace.MetaServiceGroupIDKey] = groupID
					return storage.SaveKeyspaceMeta(txn, meta)
				}))
			case "empty-endpoints":
				addresses = ""
			case "invalid-endpoints":
				addresses = "http://"
			case "TLS":
				tlsLoader = func() (*tls.Config, error) { return nil, errors.New("TLS unavailable") }
			case "unavailable-backend":
				stopGroup()
			case "malformed-report":
				_, err = groupClient.Put(ctx, keypath.CompatibleTiDBMinStartTSPrefix(1)+"broken", "not-a-timestamp")
				re.NoError(err)
			}
			groups := keyspace.NewMetaServiceGroupManager(storage, map[string]string{"group1": addresses}, tlsLoader)
			t.Cleanup(func() { re.NoError(groups.Close()) })
			manager.metaServiceGroupManager = groups
			if failure == "missing-manager" {
				manager.metaServiceGroupManager = nil
			}
			for _, legacy := range []bool{false, true} {
				if legacy {
					_, _, err = manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
				} else {
					_, err = manager.AdvanceTxnSafePoint(1, 200, now)
				}
				re.Error(err)
				persisted, err := storage.GetGCStateProvider().LoadTxnSafePoint(1)
				re.NoError(err)
				re.Equal(uint64(40), persisted)
				afterBarrier, err := storage.Load(barrierPath)
				re.NoError(err)
				re.Equal(barrier, afterBarrier)
				afterRevision, err := storage.Load(keypath.GCStateRevisionPath())
				re.NoError(err)
				re.Equal(revision, afterRevision)
			}
		})
	}
}
