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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

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

func TestMetaServiceGroupMinStartTSWithTrailingComma(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	_, groupClient, cleanGroup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(cleanGroup)
	opt := newGCStateManagerForTestOptions{
		metaServiceGroups: map[string]string{"group1": strings.Join(groupClient.Endpoints(), ",") + ","},
	}
	opt.generateKeyspacesByCount(1)
	storage, provider, manager, clean, cancel := newGCStateManagerForTest(t, opt)
	t.Cleanup(clean)
	t.Cleanup(cancel)
	lease, err := groupClient.Grant(ctx, 60)
	re.NoError(err)
	_, err = groupClient.Put(ctx, keypath.CompatibleTiDBMinStartTSPrefix(1)+"tidb-group", "100", clientv3.WithLease(lease.ID))
	re.NoError(err)
	for _, legacy := range []bool{false, true} {
		safePoint, err := advanceRoutingTestSafePoint(t, manager, legacy, time.Now())
		re.NoError(err)
		re.Equal(uint64(100), safePoint)
		persisted, err := provider.LoadTxnSafePoint(1)
		re.NoError(err)
		re.Equal(uint64(100), persisted)
	}
	// The initial file configuration remains usable without a persisted config key.
	cfg, err := storage.Load(keypath.ConfigPath())
	re.NoError(err)
	re.Empty(cfg)
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

func TestLocalTxnSafePointSerializesReportReads(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		legacy, unified, noOp bool
		null                  bool
	}{
		{name: "modern-null", null: true},
		{name: "legacy-null", legacy: true, null: true},
		{name: "modern-unassigned"},
		{name: "legacy-unassigned", legacy: true},
		{name: "legacy-unified", legacy: true, unified: true},
		{name: "legacy-unified-no-op", legacy: true, unified: true, noOp: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			scope, source := uint32(1), uint32(1)
			if tc.null || tc.unified {
				scope = constant.NullKeyspaceID
			}
			if tc.null {
				source = scope
			}
			var rejectPreparation atomic.Bool
			opt := newGCStateManagerForTestOptions{
				etcdClientCfgModifier: func(cfg *clientv3.Config) {
					cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
						// Ordinary local GC reads remain available while the extra standalone
						// revision read used for preparing external reports is unavailable.
						if txn, ok := req.(*etcdserverpb.TxnRequest); ok && rejectPreparation.Load() && len(txn.Compare) == 0 && len(txn.Success) == 1 {
							if read := txn.Success[0].GetRequestRange(); read != nil && string(read.Key) == keypath.TxnSafePointPath(scope) {
								return errors.New("scope revision preparation unavailable")
							}
						}
						return invoke(ctx, method, req, reply, cc, opts...)
					}))
				},
			}
			opt.generateKeyspacesByCount(1)
			if tc.unified {
				opt.specifyInitialKeyspaces[0].Config[keyspace.GCManagementType] = keyspace.UnifiedGC
			}
			storage, provider, manager, clean, cancel := newGCStateManagerForTest(t, opt)
			t.Cleanup(clean)
			t.Cleanup(cancel)
			now := time.Now()
			_, err := manager.AdvanceTxnSafePoint(scope, 50, now)
			re.NoError(err)
			reportKey := keypath.CompatibleTiDBMinStartTSPrefix(scope) + "tidb-local"
			re.NoError(storage.Save(reportKey, "100"))
			if tc.unified {
				re.NoError(storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(source)+"wrong-scope", "20"))
			}
			watcher, err := manager.WatchGCStates(context.Background(), true)
			re.NoError(err)
			t.Cleanup(watcher.Close)

			reached, queued, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			var metadataReads, reportReads atomic.Int32
			const metadataHook = "github.com/tikv/pd/pkg/gc/txnSafePointKeyspaceLoaded"
			const reportHook = "github.com/tikv/pd/pkg/gc/txnSafePointMinStartTSLoaded"
			re.NoError(failpoint.EnableCall(metadataHook, func() {
				if metadataReads.Add(1) == 2 {
					close(queued)
				}
			}))
			re.NoError(failpoint.EnableCall(reportHook, func() {
				if reportReads.Add(1) == 1 {
					close(reached)
					<-resume
				}
			}))
			t.Cleanup(func() {
				re.NoError(failpoint.Disable(metadataHook))
				re.NoError(failpoint.Disable(reportHook))
			})
			advance := func(id uint32, legacy bool, target uint64) (uint64, error) {
				if legacy {
					minimum, _, err := manager.CompatibleUpdateServiceGCSafePoint(id, keypath.GCWorkerServiceSafePointID, target, math.MaxInt64, now)
					if err != nil {
						return 0, err
					}
					return minimum.SafePoint, nil
				}
				result, err := manager.AdvanceTxnSafePoint(id, target, now)
				return result.NewTxnSafePoint, err
			}
			firstDone, secondDone := make(chan struct{}), make(chan struct{})
			var first, second uint64
			var firstErr, secondErr error
			go func() {
				defer close(firstDone)
				first, firstErr = advance(source, tc.legacy, 200)
			}()
			t.Cleanup(func() {
				release()
				<-firstDone
			})
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("first local report did not reach the pause")
			}
			rejectPreparation.Store(true)
			secondTarget := uint64(200)
			wantSecond := uint64(150)
			if tc.noOp {
				secondTarget, wantSecond = 100, 100
			}
			go func() {
				defer close(secondDone)
				second, secondErr = advance(scope, !tc.legacy, secondTarget)
			}()
			t.Cleanup(func() {
				release()
				<-secondDone
			})
			select {
			case <-queued:
			case <-time.After(5 * time.Second):
				t.Fatal("overlapping local advancement did not load its routing")
			}
			// The queued request must read this report after the first advancement,
			// and must not acquire an extra prepared-report revision dependency.
			re.NoError(storage.Save(reportKey, "150"))
			release()
			<-firstDone
			<-secondDone
			re.NoError(firstErr)
			re.NoError(secondErr)
			re.Equal(uint64(100), first)
			re.Equal(wantSecond, second)
			persisted, err := provider.LoadTxnSafePoint(scope)
			re.NoError(err)
			re.Equal(wantSecond, persisted)
			for _, expected := range []uint64{100, wantSecond} {
				change, published, err := watcher.receiveOne(false)
				re.NoError(err)
				re.True(published, "both local calls must publish, including a successful no-op")
				state, upsert := change.Upsert()
				re.True(upsert)
				re.Equal(expected, state.TxnSafePoint)
			}
			if tc.unified {
				persisted, err = provider.LoadTxnSafePoint(source)
				re.NoError(err)
				re.Zero(persisted, "the redirected request must only write unified GC state")
			}
		})
	}
}

func TestTxnSafePointRejectsStalePreparedReport(t *testing.T) {
	for _, tc := range []struct {
		name         string
		legacy, noOp bool
		initial      uint64
	}{
		{name: "modern-after-legacy-advance", initial: 50},
		{name: "legacy-after-modern-advance", legacy: true, initial: 50},
		{name: "modern-after-legacy-no-op", noOp: true, initial: 50},
		{name: "legacy-after-modern-no-op", legacy: true, noOp: true, initial: 50},
		{name: "missing-safe-point"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			scope := uint32(1)
			storage, manager, groupClient, _ := newMetaServiceGroupGCFixture(t)
			provider := storage.GetGCStateProvider()
			now := time.Now()
			if tc.initial != 0 {
				_, err := manager.AdvanceTxnSafePoint(scope, tc.initial, now)
				re.NoError(err)
			}
			reportKey := keypath.CompatibleTiDBMinStartTSPrefix(scope) + "tidb-group"
			_, err := groupClient.Put(ctx, reportKey, "200")
			re.NoError(err)
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
			_, err = groupClient.Put(ctx, reportKey, "100")
			re.NoError(err)
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
	for _, failure := range []string{"missing-group", "invalid-group-id", "missing-manager", "TLS", "unavailable-backend", "malformed-report"} {
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
		legacy, restoreGroup                 bool
	}{
		{name: "after-metadata", hook: "txnSafePointKeyspaceLoaded", initialGroup: "group-a", finalGroup: "group-b"},
		{name: "after-report", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-b"},
		{name: "legacy-after-report", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-b", legacy: true},
		{name: "ABA", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a", finalGroup: "group-a", restoreGroup: true},
		{name: "unassigned-to-assigned", hook: "txnSafePointKeyspaceLoaded", finalGroup: "group-b"},
		{name: "assigned-to-unassigned", hook: "txnSafePointMinStartTSLoaded", initialGroup: "group-a"},
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
				if tc.restoreGroup {
					assign("group-b")
				}
				assign(tc.finalGroup)
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

func TestMetaServiceGroupConfigRevision(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		legacy, restore, initialAbsent bool
	}{
		{name: "replacement"},
		{name: "legacy-replacement", legacy: true},
		{name: "ABA", restore: true},
		{name: "initial-config-absent", initialAbsent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			storage, manager, original, _ := newMetaServiceGroupGCFixture(t)
			_, replacement, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
			t.Cleanup(clean)
			cfg := &config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: manager.metaServiceGroupManager.GetGroups()}}
			if !tc.initialAbsent {
				re.NoError(storage.SaveConfig(cfg))
			}
			now := time.Now()
			_, err := manager.AdvanceTxnSafePoint(1, 50, now)
			re.NoError(err)
			if tc.legacy {
				// An existing worker barrier enables downgrade-compatible writes.
				re.NoError(storage.GetGCStateProvider().RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
					return wb.SetGCBarrier(1, endpoint.NewGCBarrier(keypath.GCWorkerServiceSafePointID, 50, nil))
				}))
			}
			_, err = manager.SetGCBarrier(1, "expired", 60, time.Second, now.Add(-time.Minute))
			re.NoError(err)
			paths := []string{keypath.TxnSafePointPath(1), keypath.GCBarrierPath(1, "expired"), keypath.GCStateRevisionPath(), keypath.GCBarrierPath(1, keypath.GCWorkerServiceSafePointID)}
			before := make([]string, len(paths))
			for i, path := range paths {
				before[i], err = storage.Load(path)
				re.NoError(err)
			}
			lease, err := replacement.Grant(ctx, 60)
			re.NoError(err)
			reportKey := keypath.CompatibleTiDBMinStartTSPrefix(1) + "active-tidb"
			_, err = replacement.Put(ctx, reportKey, "100", clientv3.WithLease(lease.ID))
			re.NoError(err)
			watcher, err := manager.WatchGCStates(ctx, true)
			re.NoError(err)
			defer watcher.Close()
			update := func(client *clientv3.Client) {
				cfg.Keyspace.MetaServiceGroups = map[string]string{"group1": strings.Join(client.Endpoints(), ",")}
				re.NoError(manager.metaServiceGroupManager.UpdateGroupsSafely(ctx, cfg.Keyspace.MetaServiceGroups, nil, func() error {
					return storage.SaveConfig(cfg)
				}, nil))
			}
			const hook = "github.com/tikv/pd/pkg/gc/txnSafePointMinStartTSLoaded"
			changed := false
			re.NoError(failpoint.EnableCall(hook, func() {
				changed = true
				update(replacement)
				if tc.restore {
					update(original)
				}
			}))
			defer func() { re.NoError(failpoint.Disable(hook)) }()
			_, err = advanceRoutingTestSafePoint(t, manager, tc.legacy, now)
			re.True(changed)
			re.True(pingcaperrors.ErrorEqual(err, errs.ErrEtcdTxnConflict), "config changed after report read: %v", err)
			re.NoError(failpoint.Disable(hook))
			for i, path := range paths {
				after, err := storage.Load(path)
				re.NoError(err)
				re.Equal(before[i], after, path)
			}
			_, published, err := watcher.receiveOne(false)
			re.NoError(err)
			re.False(published)
			if tc.restore {
				update(replacement)
			}
			result, err := advanceRoutingTestSafePoint(t, manager, tc.legacy, now)
			re.NoError(err)
			re.Equal(uint64(100), result)
			if tc.legacy {
				worker, err := storage.GetGCStateProvider().LoadGCBarrier(1, keypath.GCWorkerServiceSafePointID)
				re.NoError(err)
				re.NotNil(worker)
				re.Equal(uint64(100), worker.BarrierTS)
			}
			change, published, err := watcher.receiveOne(false)
			re.NoError(err)
			re.True(published)
			state, upsert := change.Upsert()
			re.True(upsert)
			re.Equal(uint64(100), state.TxnSafePoint)
		})
	}
}

func TestMetaServiceGroupPersistedRouting(t *testing.T) {
	for _, invalid := range []string{"missing-keyspace-config", "missing-group", "empty-endpoints", "malformed-config", "stale-cache"} {
		t.Run(invalid, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			storage, manager, _, _ := newMetaServiceGroupGCFixture(t)
			_, replacement, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
			t.Cleanup(clean)
			lease, err := replacement.Grant(ctx, 60)
			re.NoError(err)
			_, err = replacement.Put(ctx, keypath.CompatibleTiDBMinStartTSPrefix(1)+"active-tidb", "100", clientv3.WithLease(lease.ID))
			re.NoError(err)
			groups := map[string]string{"group1": strings.Join(replacement.Endpoints(), ",")}
			cfg := &config.Config{Keyspace: config.KeyspaceConfig{MetaServiceGroups: groups}}
			switch invalid {
			case "missing-keyspace-config":
				re.NoError(storage.Save(keypath.ConfigPath(), `{}`))
			case "missing-group":
				cfg.Keyspace.MetaServiceGroups = nil
				re.NoError(storage.SaveConfig(cfg))
			case "empty-endpoints":
				cfg.Keyspace.MetaServiceGroups = map[string]string{"group1": ""}
				re.NoError(storage.SaveConfig(cfg))
			case "malformed-config":
				re.NoError(storage.Save(keypath.ConfigPath(), `{`))
			case "stale-cache":
				re.NoError(storage.SaveConfig(cfg))
			}
			for _, legacy := range []bool{false, true} {
				if legacy {
					_, _, err = manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, time.Now())
				} else {
					_, err = manager.AdvanceTxnSafePoint(1, 200, time.Now())
				}
				re.Error(err)
				safePoint, err := storage.GetGCStateProvider().LoadTxnSafePoint(1)
				re.NoError(err)
				re.Zero(safePoint)
			}
			// Applying the persisted route resolves a stale cache; unchanged config is usable.
			cfg.Keyspace.MetaServiceGroups = groups
			re.NoError(manager.metaServiceGroupManager.UpdateGroupsSafely(ctx, groups, nil, func() error { return storage.SaveConfig(cfg) }, nil))
			result, err := manager.AdvanceTxnSafePoint(1, 200, time.Now())
			re.NoError(err)
			re.Equal(uint64(100), result.NewTxnSafePoint)
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
		opt.specifyInitialKeyspaces[0].Config[keyspace.GCManagementType] = keyspace.UnifiedGC
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
			meta, err := manager.keyspaceManager.LoadKeyspace("ks1")
			re.NoError(err)
			meta.Config[keyspace.GCManagementType] = keyspace.KeyspaceLevelGC
			persistHistoricalGCKeyspace(t, storage, meta)
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
		// Restore unified GC so the retry verifies redirected writes.
		meta, err := manager.keyspaceManager.LoadKeyspace("ks1")
		re.NoError(err)
		meta.Config[keyspace.GCManagementType] = keyspace.UnifiedGC
		persistHistoricalGCKeyspace(t, storage, meta)
		minimum, updated, err := manager.CompatibleUpdateServiceGCSafePoint(1, keypath.GCWorkerServiceSafePointID, 200, math.MaxInt64, now)
		re.NoError(err)
		re.True(updated)
		re.Equal(uint64(100), minimum.SafePoint)
		safePoint, err = provider.LoadTxnSafePoint(1)
		re.NoError(err)
		re.Zero(safePoint, "the redirected request must only write unified GC state")
	})
}
