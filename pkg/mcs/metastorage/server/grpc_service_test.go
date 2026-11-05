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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestValidateMetaStorageAccess(t *testing.T) {
	labelPrefix := strings.TrimSuffix(keypath.RegionLabelPathPrefix(), "/")
	for _, tc := range []struct {
		name    string
		key     string
		end     string
		allowed bool
	}{
		{name: "service discovery", key: "/ms/42/router/registry/address", allowed: true},
		{name: "resource group", key: "resource_group/settings/default", allowed: true},
		{name: "controller", key: "resource_group/controller", allowed: true},
		{name: "keyspace resource group", key: "resource_group/keyspace/settings/1/default", allowed: true},
		{name: "backup checkpoint", key: "/tidb/br-stream/storage-checkpoint/task/1", allowed: true},
		{name: "region label", key: labelPrefix + "/rule", allowed: true},
		{name: "region label prefix", key: labelPrefix, end: fmt.Sprintf("/pd/%d/region_labem", keypath.ClusterID()), allowed: true},
		{name: "discovery prefix", key: "/ms/42/router/registry/", end: "/ms/42/router/registry0", allowed: true},
		{name: "settings prefix", key: "resource_group/settings", end: "resource_group/settingt", allowed: true},
		{name: "whole discovery namespace", key: "/ms/", end: "/ms0", allowed: true},
		{name: "whole resource namespace", key: "resource_group/", end: "resource_group0", allowed: true},
		{name: "backup snapshot prefix", key: "/tidb/br-stream", end: "/tidb/br-strean", allowed: true},
		{name: "binary suffix", key: "/ms/\xff\xff", end: "/ms0", allowed: true},
		{name: "literal dot segments", key: "/ms/../pd/42/leader", allowed: true},
		{name: "empty key"},
		{name: "whole keyspace", end: "\x00"},
		{name: "root", key: "/"},
		{name: "pd election", key: "/pd/42/leader"},
		{name: "raft", key: "/raft/42/meta"},
		{name: "encryption keys", key: "encryption_keys"},
		{name: "unscoped key", key: "test"},
		{name: "discovery lookalike", key: "/ms_evil/42"},
		{name: "resource group lookalike", key: "resource_group_evil/settings"},
		{name: "backup sibling", key: "/tidb/br-strean"},
		{name: "other cluster labels", key: fmt.Sprintf("/pd/%d/region_label/rule", keypath.ClusterID()+1)},
		{name: "region label sibling", key: fmt.Sprintf("/pd/%d/region_labem", keypath.ClusterID())},
		{name: "cross namespace", key: "/ms/42/", end: "resource_group/settings"},
		{name: "unbounded discovery", key: "/ms/42/", end: "\x00"},
		{name: "unbounded resources", key: "resource_group/", end: "\x00"},
		{name: "unbounded backup", key: "/tidb/br-stream", end: "\x00"},
		{name: "unbounded region labels", key: labelPrefix, end: "\x00"},
		{name: "past region label boundary", key: labelPrefix, end: fmt.Sprintf("/pd/%d/region_labem\x00", keypath.ClusterID())},
		{name: "past exclusive boundary", key: "/ms/", end: "/ms0\x00"},
		{name: "reversed range", key: "resource_group/z", end: "resource_group/a"},
		{name: "empty range", key: "resource_group/a", end: "resource_group/a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMetaStorageAccess([]byte(tc.key), []byte(tc.end))
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
		})
	}
}

func TestValidateMetaStorageAccessClusterID(t *testing.T) {
	previousID := keypath.ClusterID()
	t.Cleanup(func() { keypath.SetClusterID(previousID) })
	keypath.SetClusterID(42)
	require.NoError(t, validateMetaStorageAccess([]byte("/pd/42/region_label/rule"), nil))

	keypath.SetClusterID(43)
	require.Equal(t, codes.PermissionDenied, status.Code(validateMetaStorageAccess([]byte("/pd/42/region_label/rule"), nil)))
	require.NoError(t, validateMetaStorageAccess([]byte("/pd/43/region_label/rule"), nil))
}

func BenchmarkValidateMetaStorageAccess(b *testing.B) {
	previousID := keypath.ClusterID()
	b.Cleanup(func() { keypath.SetClusterID(previousID) })
	keypath.SetClusterID(42)
	for _, tc := range []struct {
		name    string
		key     string
		end     string
		allowed bool
	}{
		{name: "discovery_get", key: "/ms/42/tso/00000/primary", allowed: true},
		{name: "controller_get", key: "resource_group/controller", allowed: true},
		{name: "resource_groups_watch", key: "resource_group/settings", end: "resource_group/settingt", allowed: true},
		{name: "backup_snapshot", key: "/tidb/br-stream", end: "/tidb/br-strean", allowed: true},
		{name: "backup_checkpoint_put", key: "/tidb/br-stream/storage-checkpoint/task/1", allowed: true},
		{name: "region_label_get", key: "/pd/42/region_label/rule", allowed: true},
		{name: "region_labels_watch", key: "/pd/42/region_label", end: "/pd/42/region_labem", allowed: true},
		{name: "denied_raft_get", key: "/raft/42/meta"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			key, end := []byte(tc.key), []byte(tc.end)
			b.ReportAllocs()
			for b.Loop() {
				if err := validateMetaStorageAccess(key, end); (err == nil) != tc.allowed {
					b.Fatalf("unexpected validation result: %v", err)
				}
			}
		})
	}
}
