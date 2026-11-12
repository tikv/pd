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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func putGCConfigDiagnosticMetas(t *testing.T, client *clientv3.Client, metas []*keyspacepb.KeyspaceMeta) int64 {
	t.Helper()
	var revision int64
	for start := 0; start < len(metas); start += 100 {
		ops := make([]clientv3.Op, 0, 100)
		for _, meta := range metas[start:min(start+100, len(metas))] {
			value, err := proto.Marshal(meta)
			require.NoError(t, err)
			ops = append(ops, clientv3.OpPut(fmt.Sprintf("%s%08d", enabledKeyspaceTestPrefix, meta.GetId()), string(value)))
		}
		resp, err := client.Txn(context.Background()).Then(ops...).Commit()
		require.NoError(t, err)
		revision = resp.Header.Revision
	}
	return revision
}

func captureGCConfigDiagnostics(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(restore)
	return logs
}

func TestGCConfigLoadDiagnosticsDetailsAndReload(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	logs := captureGCConfigDiagnostics(t)
	configs := []map[string]string{
		nil,
		{keyspace.GCManagementType: keyspace.UnifiedGC},
		{keyspace.GCManagementType: "", "safe_point_version": "v2"},
		{keyspace.GCManagementType: "unknown", "safe_point_version": "v2"},
		{"safe_point_version": "v1"},
		{"safe_point_version": "v2"},
		{keyspace.GCManagementType: keyspace.KeyspaceLevelGC, "safe_point_version": "v1"},
	}
	states := []keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED,
		keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE,
	}
	metas := make([]*keyspacepb.KeyspaceMeta, 0, len(configs))
	for n, config := range configs {
		metas = append(metas, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: uint32(n + 1)},
			Name:     fmt.Sprintf("history-%d", n+1), State: states[n%len(states)], Config: config,
		})
	}
	wantRevision := putGCConfigDiagnosticMetas(t, client, metas)
	index := newEnabledKeyspaceIndex(client, enabledKeyspaceTestPrefix)
	// Reusing the index models a watch-recovery reload; replacing it models a new leadership term.
	for attempt := range 3 {
		if attempt == 2 {
			index = newEnabledKeyspaceIndex(client, enabledKeyspaceTestPrefix)
		}
		entries, revision, err := index.load(context.Background(), nil)
		require.NoError(t, err)
		require.Equal(t, wantRevision, revision)
		require.Equal(t, map[uint32]enabledKeyspace{
			1: {id: 1, gcManagementType: keyspace.UnifiedGC},
			5: {id: 5, gcManagementType: keyspace.UnifiedGC},
		}, entries)
		if !kerneltype.IsNextGen() {
			require.Empty(t, logs.All(), "Classic must not collect configuration diagnostics")
			continue
		}
		require.Len(t, logs.All(), attempt+1, "each successful load must emit exactly one summary")
		fields := logs.All()[attempt].ContextMap()
		require.Equal(t, revision, fields["revision"])
		require.EqualValues(t, 5, fields["violation-count"])
		require.EqualValues(t, 0, fields["omitted-count"])
		details, ok := fields["details"].([]any)
		require.True(t, ok)
		require.Len(t, details, 5)
		seen := make(map[uint32]struct{})
		for _, detail := range details {
			fields := detail.(map[string]any)
			id := fields["keyspace-id"].(uint32)
			require.Contains(t, []uint32{1, 2, 3, 4, 5}, id)
			_, exists := seen[id]
			require.False(t, exists)
			seen[id] = struct{}{}
			require.Equal(t, fmt.Sprintf("history-%d", id), fields["keyspace-name"])
			require.Equal(t, keyspace.UnifiedGC, fields["effective-gc-mode"])
			require.Equal(t, id >= 2 && id <= 4, fields["gc-management-type-present"])
			require.Equal(t, []string{"", keyspace.UnifiedGC, "", "unknown", ""}[id-1], fields["gc-management-type"])
			require.Equal(t, id >= 3, fields["safe-point-version-present"])
			require.Equal(t, []string{"", "", "v2", "v2", "v1"}[id-1], fields["safe-point-version"])
			reason := "effective gc mode does not match kernel"
			if id == 3 || id == 4 {
				reason = "malformed gc_management_type; " + reason
			}
			require.Equal(t, reason, fields["reason"])
		}
	}
}

func TestGCConfigLoadDiagnosticsBoundParallelPages(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	logs := captureGCConfigDiagnostics(t)
	// A healthy first page forces all violations into concurrently loaded ranges.
	putEnabledKeyspaceTestIDs(t, client, enabledKeyspaceTestIDs(256))
	metas := make([]*keyspacepb.KeyspaceMeta, 0, 30)
	for n := range 30 {
		id := uint32(5000*(1+n/10) + n%10)
		metas = append(metas, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
			Name:     fmt.Sprintf("invalid-%d", id), State: keyspacepb.KeyspaceState_DISABLED,
			Config: map[string]string{keyspace.GCManagementType: "invalid"},
		})
	}
	putGCConfigDiagnosticMetas(t, client, metas)
	putEnabledKeyspaceTestWatermark(t, client, 20000)
	index := newEnabledKeyspaceIndex(client, enabledKeyspaceTestPrefix)
	entries, revision, err := index.load(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, entries, 256)
	if !kerneltype.IsNextGen() {
		require.Empty(t, logs.All())
		return
	}
	require.Len(t, logs.All(), 1)
	fields := logs.All()[0].ContextMap()
	require.Equal(t, revision, fields["revision"])
	require.EqualValues(t, 30, fields["violation-count"])
	require.EqualValues(t, 10, fields["omitted-count"])
	details, ok := fields["details"].([]any)
	require.True(t, ok)
	require.Len(t, details, 20)
	seen := make(map[uint32]struct{})
	for _, detail := range details {
		id := detail.(map[string]any)["keyspace-id"].(uint32)
		require.True(t, (id >= 5000 && id < 5010) || (id >= 10000 && id < 10010) || (id >= 15000 && id < 15010))
		_, exists := seen[id]
		require.False(t, exists)
		seen[id] = struct{}{}
	}
}

func TestGCConfigLoadDiagnosticsDiscardFailedAttempt(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	metas := make([]*keyspacepb.KeyspaceMeta, 0, 258)
	for id := uint32(1); id <= 258; id++ {
		metas = append(metas, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, State: keyspacepb.KeyspaceState_ENABLED,
			Config: map[string]string{keyspace.GCManagementType: "invalid"},
		})
	}
	putGCConfigDiagnosticMetas(t, client, metas)
	for _, mode := range []string{"cancel", "read-error", "decode-error"} {
		t.Run(mode, func(t *testing.T) {
			logs := captureGCConfigDiagnostics(t)
			injected := errors.New("injected metadata read error")
			inject := true
			intercepted := newEnabledKeyspaceInterceptClient(t, client, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if _, ok := req.(*pb.RangeRequest); ok && inject {
					if mode == "read-error" {
						return injected
					}
					if mode == "decode-error" {
						response := reply.(*pb.RangeResponse)
						response.Kvs = []*mvccpb.KeyValue{{Key: []byte(enabledKeyspaceTestPrefix + "00000257"), Value: []byte("malformed")}}
						return nil
					}
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			})
			index := newEnabledKeyspaceIndex(intercepted, enabledKeyspaceTestPrefix)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pages := 0
			entries, revision, err := index.load(ctx, func(page []enabledKeyspace) {
				pages++
				require.Len(t, page, 256)
				if mode == "cancel" {
					cancel()
				}
			})
			require.Equal(t, 1, pages, "failure must follow a successfully decoded page")
			require.Nil(t, entries)
			require.Zero(t, revision)
			switch mode {
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			case "read-error":
				require.ErrorIs(t, err, injected)
			case "decode-error":
				require.ErrorContains(t, err, "decode keyspace metadata")
			}
			require.Empty(t, logs.All(), "a partial failed load must not emit configuration diagnostics")
			inject = false
			entries, revision, err = index.load(context.Background(), nil)
			require.NoError(t, err)
			require.Len(t, entries, 258)
			if !kerneltype.IsNextGen() {
				require.Empty(t, logs.All())
				return
			}
			require.Len(t, logs.All(), 1)
			fields := logs.All()[0].ContextMap()
			require.Equal(t, revision, fields["revision"])
			require.EqualValues(t, 258, fields["violation-count"])
			require.EqualValues(t, 238, fields["omitted-count"])
			require.Len(t, fields["details"], 20)
		})
	}
}

func TestGCConfigLoadDiagnosticsCancelSinglePage(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	logs := captureGCConfigDiagnostics(t)
	putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_ENABLED, "invalid")
	index := newEnabledKeyspaceIndex(client, enabledKeyspaceTestPrefix)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entries, revision, err := index.load(ctx, func([]enabledKeyspace) { cancel() })
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, entries)
	require.Zero(t, revision)
	require.Empty(t, logs.All(), "cancellation must also suppress a completed single-page scan")
}

func TestGCConfigLoadDiagnosticsHealthyAndWatchSilent(t *testing.T) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	t.Cleanup(clean)
	logs := captureGCConfigDiagnostics(t)
	putEnabledKeyspaceTestMeta(t, client, 1, keyspacepb.KeyspaceState_ENABLED, keyspace.KeyspaceLevelGC)
	index := startEnabledKeyspaceTestIndex(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, index.waitReady(ctx))
	require.Empty(t, logs.All(), "a healthy snapshot needs no diagnostic")
	for _, state := range []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ARCHIVED} {
		revision := putEnabledKeyspaceTestMeta(t, client, 1, state, "invalid")
		entries, _, err := index.snapshotAtLeast(ctx, revision)
		require.NoError(t, err)
		if state == keyspacepb.KeyspaceState_ENABLED {
			require.Equal(t, []enabledKeyspace{{id: 1, gcManagementType: keyspace.UnifiedGC}}, entries)
		} else {
			require.Empty(t, entries)
		}
		require.Empty(t, logs.All(), "incremental watch updates must not report configuration diagnostics")
	}
}
