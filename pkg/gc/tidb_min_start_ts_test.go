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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	pingcaperrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
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

// advanceRoutingTestSafePoint advances keyspace 1 toward 200 through the selected API.
func advanceRoutingTestSafePoint(t *testing.T, manager *GCStateManager, legacy bool, now time.Time) (uint64, error) {
	t.Helper()
	if legacy {
		minimum, updated, err := manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
		if err != nil {
			require.False(t, updated)
			require.Nil(t, minimum)
			return 0, err
		}
		return minimum.SafePoint, nil
	}
	result, err := manager.AdvanceTxnSafePoint(1, 200, now)
	return result.NewTxnSafePoint, err
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

func TestMetaServiceGroupReportReadDoesNotBlockOtherScopes(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "modern"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			_, manager, groupClient, _ := newMetaServiceGroupGCFixture(t)
			_, err := groupClient.Put(ctx, keypath.CompatibleTiDBMinStartTSPrefix(1)+"tidb-group", "160")
			re.NoError(err)
			now := time.Now()
			reached, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			var paused atomic.Bool
			hook := "github.com/tikv/pd/pkg/gc/txnSafePointMinStartTSLoaded"
			re.NoError(failpoint.EnableCall(hook, func() {
				if paused.CompareAndSwap(false, true) {
					close(reached)
					<-resume
				}
			}))
			t.Cleanup(func() { re.NoError(failpoint.Disable(hook)) })
			blockedDone := make(chan struct{})
			var blockedSafePoint uint64
			var blockedErr error
			go func() {
				defer close(blockedDone)
				if legacy {
					var minimum *endpoint.ServiceSafePoint
					minimum, _, blockedErr = manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
					if blockedErr == nil {
						blockedSafePoint = minimum.SafePoint
					}
				} else {
					var result AdvanceTxnSafePointResult
					result, blockedErr = manager.AdvanceTxnSafePoint(1, 200, now)
					blockedSafePoint = result.NewTxnSafePoint
				}
			}()
			t.Cleanup(func() {
				release()
				<-blockedDone
			})
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("group report read did not reach the pause")
			}

			healthyDone := make(chan struct{})
			var healthyResult AdvanceTxnSafePointResult
			var healthyErr error
			go func() {
				defer close(healthyDone)
				_, healthyErr = manager.SetGCBarrier(constant.NullKeyspaceID, "healthy-local", 120, time.Hour, now)
				if healthyErr != nil {
					return
				}
				_, healthyErr = manager.SetGlobalGCBarrier(ctx, "healthy-global", 140, time.Hour, now)
				if healthyErr != nil {
					return
				}
				healthyResult, healthyErr = manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 200, now)
			}()
			t.Cleanup(func() {
				release()
				<-healthyDone
			})
			select {
			case <-healthyDone:
				re.NoError(healthyErr)
				re.Equal(uint64(120), healthyResult.NewTxnSafePoint)
			case <-time.After(5 * time.Second):
				t.Fatal("group report read blocked unrelated GC barrier updates and advancement")
			}
			release()
			<-blockedDone
			re.NoError(blockedErr)
			re.Equal(uint64(140), blockedSafePoint, "the global barrier installed during preparation must block group A")
		})
	}
}

func TestTxnSafePointRejectsStalePreparedReport(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		legacy, unified, noOp bool
		initial               uint64
	}{
		{name: "modern-after-legacy-advance", initial: 50},
		{name: "legacy-after-modern-advance", legacy: true, initial: 50},
		{name: "modern-after-legacy-no-op", noOp: true, initial: 50},
		{name: "legacy-after-modern-no-op", legacy: true, noOp: true, initial: 50},
		{name: "unified-after-modern-advance", legacy: true, unified: true, initial: 50},
		{name: "unified-after-modern-no-op", legacy: true, unified: true, noOp: true, initial: 50},
		{name: "missing-safe-point"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			opt := newGCStateManagerForTestOptions{}
			opt.generateKeyspacesByCount(1)
			scope := uint32(1)
			if tc.unified {
				opt.specifyInitialKeyspaces[0].Config[keyspace.GCManagementType] = "unified"
				scope = constant.NullKeyspaceID
			}
			storage, provider, manager, clean, cancel := newGCStateManagerForTest(t, opt)
			t.Cleanup(clean)
			t.Cleanup(cancel)
			now := time.Now()
			if tc.initial != 0 {
				_, err := manager.AdvanceTxnSafePoint(scope, tc.initial, now)
				re.NoError(err)
			}
			reportKey := keypath.CompatibleTiDBMinStartTSPrefix(scope) + "tidb-local"
			re.NoError(storage.Save(reportKey, "200"))
			watcher, err := manager.WatchGCStates(ctx, true)
			re.NoError(err)
			t.Cleanup(watcher.Close)
			reached, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			var paused atomic.Bool
			hook := "github.com/tikv/pd/pkg/gc/txnSafePointMinStartTSLoaded"
			re.NoError(failpoint.EnableCall(hook, func() {
				if paused.CompareAndSwap(false, true) {
					close(reached)
					<-resume
				}
			}))
			t.Cleanup(func() { re.NoError(failpoint.Disable(hook)) })
			done := make(chan struct{})
			var staleErr error
			go func() {
				defer close(done)
				if tc.legacy {
					_, _, staleErr = manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
				} else {
					_, staleErr = manager.AdvanceTxnSafePoint(1, 200, now)
				}
			}()
			t.Cleanup(func() {
				release()
				<-done
			})
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("stale report read did not reach the pause")
			}

			// A newer report and a successful advancement invalidate the pending report,
			// even if the advancement writes the same txn safe point.
			re.NoError(storage.Save(reportKey, "100"))
			current := uint64(100)
			if tc.noOp {
				current = tc.initial
			}
			competingDone := make(chan struct{})
			var competingSafePoint uint64
			var competingUpdated bool
			var competingErr error
			go func() {
				defer close(competingDone)
				if tc.legacy {
					var result AdvanceTxnSafePointResult
					result, competingErr = manager.AdvanceTxnSafePoint(scope, current, now)
					competingSafePoint = result.NewTxnSafePoint
					competingUpdated = result.OldTxnSafePoint != result.NewTxnSafePoint
				} else {
					var minimum *endpoint.ServiceSafePoint
					minimum, competingUpdated, competingErr = manager.CompatibleUpdateServiceGCSafePoint(scope, keypath.GCWorkerServiceSafePointID, current, math.MaxInt64, now)
					if competingErr == nil {
						competingSafePoint = minimum.SafePoint
					}
				}
			}()
			t.Cleanup(func() {
				release()
				<-competingDone
			})
			select {
			case <-competingDone:
			case <-time.After(5 * time.Second):
				t.Fatal("pending report blocked the competing advancement")
			}
			re.NoError(competingErr)
			re.Equal(current, competingSafePoint)
			re.Equal(!tc.noOp, competingUpdated)
			_, published, err := watcher.receiveOne(false)
			re.NoError(err)
			re.True(published, "the competing advancement must publish, including a successful no-op")
			_, err = manager.SetGCBarrier(scope, "expired", 150, time.Second, now.Add(-time.Minute))
			re.NoError(err)
			barrierPath := keypath.GCBarrierPath(scope, "expired")
			beforeBarrier, err := storage.Load(barrierPath)
			re.NoError(err)
			re.NotEmpty(beforeBarrier)
			beforeRevision, err := storage.Load(keypath.GCStateRevisionPath())
			re.NoError(err)

			release()
			<-done
			re.True(pingcaperrors.ErrorEqual(staleErr, errs.ErrEtcdTxnConflict), "stale report must conflict, got %v", staleErr)
			persisted, err := provider.LoadTxnSafePoint(scope)
			re.NoError(err)
			re.Equal(current, persisted)
			afterBarrier, err := storage.Load(barrierPath)
			re.NoError(err)
			re.Equal(beforeBarrier, afterBarrier)
			afterRevision, err := storage.Load(keypath.GCStateRevisionPath())
			re.NoError(err)
			re.Equal(beforeRevision, afterRevision)
			_, published, err = watcher.receiveOne(false)
			re.NoError(err)
			re.False(published, "a stale report must not publish a GC state")
			if tc.unified {
				persisted, err = provider.LoadTxnSafePoint(1)
				re.NoError(err)
				re.Zero(persisted, "the source keyspace must remain untouched")
			}
		})
	}
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
			// A report failure must leave expired barriers and the GC revision unchanged.
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
			watcher, err := manager.WatchGCStates(ctx, true)
			re.NoError(err)
			t.Cleanup(watcher.Close)
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
				_, published, err := watcher.receiveOne(false)
				re.NoError(err)
				re.False(published, "a report failure must not publish a GC state")
			}
		})
	}
}

func TestMetaServiceGroupRoutingRevision(t *testing.T) {
	for _, tc := range []struct {
		name, hook, initialGroup, finalGroup string
		legacy, restoreGroup, unrelated      bool
	}{
		{name: "after-metadata", hook: "txnSafePointKeyspaceLoaded", initialGroup: "group-a", finalGroup: "group-b"},
		{name: "after-report", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-b"},
		{name: "legacy-after-report", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-b", legacy: true},
		{name: "ABA", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-a", restoreGroup: true},
		{name: "unassigned-to-assigned", hook: "txnSafePointKeyspaceLoaded", finalGroup: "group-b"},
		{name: "assigned-to-unassigned", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a"},
		{name: "unrelated-config", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-a", unrelated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			groups := make(map[string]string)
			clients := make(map[string]*clientv3.Client)
			for _, group := range []string{"group-a", "group-b"} {
				_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
				t.Cleanup(clean)
				groups[group] = strings.Join(client.Endpoints(), ",")
				clients[group] = client
			}
			opt := newGCStateManagerForTestOptions{metaServiceGroups: groups}
			opt.generateKeyspacesByCount(1)
			storage, provider, manager, clean, cancel := newGCStateManagerForTest(t, opt)
			t.Cleanup(clean)
			t.Cleanup(cancel)
			assign := func(group string) {
				_, err := manager.keyspaceManager.UpdateKeyspaceConfig("ks1", []*keyspace.Mutation{{Op: keyspace.OpPut, Key: keyspace.MetaServiceGroupIDKey, Value: group}})
				re.NoError(err)
			}
			assign(tc.initialGroup)
			now := time.Now()
			_, err := manager.AdvanceTxnSafePoint(1, 50, now)
			re.NoError(err)
			_, err = manager.SetGCBarrier(1, "expired", 60, time.Second, now.Add(-time.Minute))
			re.NoError(err)
			barrierPath := keypath.GCBarrierPath(1, "expired")
			beforeBarrier, err := storage.Load(barrierPath)
			re.NoError(err)
			re.NotEmpty(beforeBarrier)
			beforeRevision, err := storage.Load(keypath.GCStateRevisionPath())
			re.NoError(err)
			reportKey := keypath.CompatibleTiDBMinStartTSPrefix(1) + "tidb-routing"
			if tc.finalGroup == "" {
				re.NoError(storage.Save(reportKey, "100"))
			} else {
				client := clients[tc.finalGroup]
				lease, err := client.Grant(ctx, 60)
				re.NoError(err)
				_, err = client.Put(ctx, reportKey, "100", clientv3.WithLease(lease.ID))
				re.NoError(err)
			}
			watcher, err := manager.WatchGCStates(ctx, true)
			re.NoError(err)
			defer watcher.Close()
			hook := "github.com/tikv/pd/pkg/gc/" + tc.hook
			mutated := false
			re.NoError(failpoint.EnableCall(hook, func() {
				mutated = true
				if tc.unrelated {
					_, err := manager.keyspaceManager.UpdateKeyspaceConfig("ks1", []*keyspace.Mutation{{Op: keyspace.OpPut, Key: "unrelated-config", Value: "changed"}})
					re.NoError(err)
				} else {
					if tc.restoreGroup {
						assign("group-b")
					}
					assign(tc.finalGroup)
				}
			}))
			defer func() { re.NoError(failpoint.Disable(hook)) }()
			_, err = advanceRoutingTestSafePoint(t, manager, tc.legacy, now)
			re.True(mutated, "the routing mutation must run before commit")
			re.True(pingcaperrors.ErrorEqual(err, errs.ErrEtcdTxnConflict), "stale routing must conflict, got %v", err)
			re.NoError(failpoint.Disable(hook))
			persisted, err := provider.LoadTxnSafePoint(1)
			re.NoError(err)
			re.Equal(uint64(50), persisted)
			barrier, err := storage.Load(barrierPath)
			re.NoError(err)
			re.Equal(beforeBarrier, barrier)
			revision, err := storage.Load(keypath.GCStateRevisionPath())
			re.NoError(err)
			re.Equal(beforeRevision, revision)
			_, published, err := watcher.receiveOne(false)
			re.NoError(err)
			re.False(published, "a rejected advancement must not publish a GC state")
			safePoint, err := advanceRoutingTestSafePoint(t, manager, tc.legacy, now)
			re.NoError(err)
			re.Equal(uint64(100), safePoint)
			change, published, err := watcher.receiveOne(false)
			re.NoError(err)
			re.True(published)
			state, upsert := change.Upsert()
			re.True(upsert)
			re.Equal(uint64(100), state.TxnSafePoint)
		})
	}
}

func TestTxnSafePointRoutingRevisionScopes(t *testing.T) {
	t.Run("direct-null-without-keyspace-manager", func(t *testing.T) {
		re := require.New(t)
		_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
		defer clean()
		storage := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)
		manager := NewGCStateManager(storage.GetGCStateProvider(), config.PDServerConfig{}, nil, nil)
		defer manager.CloseBarrierMetrics()
		re.NoError(storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(constant.NullKeyspaceID)+"tidb-local", "100"))
		for _, id := range []uint32{constant.NullKeyspaceID, constant.ValidKeyspaceIDMask + 1} {
			result, err := manager.AdvanceTxnSafePoint(id, 200, time.Now())
			re.NoError(err)
			re.Equal(uint64(100), result.NewTxnSafePoint)
			minimum, _, err := manager.CompatibleUpdateServiceGCSafePoint(id, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, time.Now())
			re.NoError(err)
			re.Equal(uint64(100), minimum.SafePoint)
		}
	})
	t.Run("legacy-unified-guards-source-keyspace", func(t *testing.T) {
		re := require.New(t)
		opt := newGCStateManagerForTestOptions{}
		opt.generateKeyspacesByCount(1)
		opt.specifyInitialKeyspaces[0].Config[keyspace.GCManagementType] = "unified"
		storage, provider, manager, clean, cancel := newGCStateManagerForTest(t, opt)
		defer clean()
		defer cancel()
		now := time.Now()
		_, err := manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 50, now)
		re.NoError(err)
		re.NoError(storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(constant.NullKeyspaceID)+"tidb-local", "100"))
		re.NoError(storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(1)+"wrong-scope", "20"))
		beforeRevision, err := storage.Load(keypath.GCStateRevisionPath())
		re.NoError(err)
		hook := "github.com/tikv/pd/pkg/gc/txnSafePointMinStartTSLoaded"
		mutated := false
		re.NoError(failpoint.EnableCall(hook, func() {
			mutated = true
			_, err := manager.keyspaceManager.UpdateKeyspaceConfig("ks1", []*keyspace.Mutation{{Op: keyspace.OpPut, Key: "unrelated-config", Value: "changed"}})
			re.NoError(err)
		}))
		defer func() { re.NoError(failpoint.Disable(hook)) }()
		_, updated, err := manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
		re.True(mutated)
		re.False(updated)
		re.True(pingcaperrors.ErrorEqual(err, errs.ErrEtcdTxnConflict), "%v", err)
		re.NoError(failpoint.Disable(hook))
		safePoint, err := provider.LoadTxnSafePoint(constant.NullKeyspaceID)
		re.NoError(err)
		re.Equal(uint64(50), safePoint)
		revision, err := storage.Load(keypath.GCStateRevisionPath())
		re.NoError(err)
		re.Equal(beforeRevision, revision)
		minimum, updated, err := manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
		re.NoError(err)
		re.True(updated)
		re.Equal(uint64(100), minimum.SafePoint)
		safePoint, err = provider.LoadTxnSafePoint(1)
		re.NoError(err)
		re.Zero(safePoint, "the redirected request must only write unified GC state")
	})
}
