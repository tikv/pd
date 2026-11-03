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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

func TestMetaServiceGroupConfigRevision(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		legacy, restore, unrelated, initialAbsent bool
	}{
		{name: "replacement"},
		{name: "legacy-replacement", legacy: true},
		{name: "ABA", restore: true},
		{name: "unrelated-config", unrelated: true},
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
				if tc.unrelated {
					cfg.LabelProperty = config.LabelPropertyConfig{"unrelated": nil}
					re.NoError(storage.SaveConfig(cfg))
					return
				}
				update(replacement)
				if tc.restore {
					update(original)
				}
			}))
			defer func() { re.NoError(failpoint.Disable(hook)) }()
			_, err = advanceRoutingTestSafePoint(t, manager, tc.legacy, now)
			re.True(changed)
			re.True(errors.ErrorEqual(err, errs.ErrEtcdTxnConflict), "config changed after report read: %v", err)
			re.NoError(failpoint.Disable(hook))
			for i, path := range paths {
				after, err := storage.Load(path)
				re.NoError(err)
				re.Equal(before[i], after, path)
			}
			_, published, err := watcher.receiveOne(false)
			re.NoError(err)
			re.False(published)
			if tc.restore || tc.unrelated {
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
