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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

func newNextGenGCStateManager(t *testing.T) (*endpoint.StorageEndpoint, *GCStateManager) {
	t.Helper()
	require.NoError(t, failpoint.Enable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"))
	})
	storage, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	t.Cleanup(func() { cancel(); clean() })
	return storage, manager
}

func TestNextGenGCRejectsNullScope(t *testing.T) {
	_, manager := newNextGenGCStateManager(t)
	for _, id := range []uint32{constant.NullKeyspaceID, 0x1000000, 0xfffffffe} {
		_, err := manager.GetGCState(id, true)
		require.ErrorContains(t, err, "unified gc is not supported in nextgen")
		_, _, err = manager.GetGCStateWithGlobalGCBarriers(id, true)
		require.Error(t, err)
		_, err = manager.AdvanceTxnSafePoint(id, 100, time.Now())
		require.Error(t, err)
		_, _, err = manager.AdvanceGCSafePoint(id, 100)
		require.Error(t, err)
		_, err = manager.SetGCBarrier(id, "backup", 100, time.Hour, time.Now())
		require.Error(t, err)
		_, err = manager.DeleteGCBarrier(id, "backup")
		require.Error(t, err)
		_, err = manager.CompatibleLoadGCSafePoint(id)
		require.Error(t, err)
		_, _, err = manager.CompatibleUpdateGCSafePoint(id, 100)
		require.Error(t, err)
		_, _, err = manager.CompatibleUpdateServiceGCSafePoint(id, "gc_worker", 100, math.MaxInt64, time.Now())
		require.Error(t, err)
	}
}

func TestNextGenGCStatesExcludeNullScope(t *testing.T) {
	_, manager := newNextGenGCStateManager(t)
	for _, excludeBarriers := range []bool{false, true} {
		states, err := manager.GetAllKeyspacesGCStates(context.Background(), excludeBarriers)
		require.NoError(t, err)
		require.Len(t, states, 4)
		require.NotContains(t, states, constant.NullKeyspaceID)
		for _, state := range states {
			require.True(t, state.IsKeyspaceLevel)
		}
	}
}

func TestNextGenGCRejectsLegacyMetadata(t *testing.T) {
	storage, manager := newNextGenGCStateManager(t)
	_, err := manager.AdvanceTxnSafePoint(2, 20, time.Now())
	require.NoError(t, err)
	for _, gcType := range []string{"", keyspace.UnifiedGC, "invalid"} {
		t.Run(gcType, func(t *testing.T) {
			// Model persisted metadata written by an older PD, bypassing today's configuration API.
			require.NoError(t, storage.RunInTxn(context.Background(), func(txn kv.Txn) error {
				meta, err := storage.LoadKeyspaceMeta(txn, 2)
				if err != nil {
					return err
				}
				if gcType == "" {
					delete(meta.Config, keyspace.GCManagementType)
				} else {
					meta.Config[keyspace.GCManagementType] = gcType
				}
				return storage.SaveKeyspaceMeta(txn, meta)
			}))
			_, err := manager.GetGCState(2, true)
			require.Error(t, err)
			_, err = manager.AdvanceTxnSafePoint(2, 100, time.Now())
			require.Error(t, err)
			for _, excludeBarriers := range []bool{false, true} {
				_, err = manager.GetAllKeyspacesGCStates(context.Background(), excludeBarriers)
				require.Error(t, err)
			}
			_, err = manager.SetGlobalGCBarrier(context.Background(), "backup", 100, time.Hour, time.Now())
			require.Error(t, err)
			persisted, err := manager.gcMetaStorage.LoadTxnSafePoint(2)
			require.NoError(t, err)
			require.Equal(t, uint64(20), persisted)
		})
	}
}

func TestNextGenNativeBRGlobalBarrierCompatibility(t *testing.T) {
	_, manager := newNextGenGCStateManager(t)
	_, err := manager.AdvanceTxnSafePoint(2, 10, time.Now())
	require.NoError(t, err)
	_, _, err = manager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, "native_br", 20, math.MaxInt64, time.Now())
	require.NoError(t, err)
	result, err := manager.AdvanceTxnSafePoint(2, 30, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(20), result.NewTxnSafePoint)
	_, _, err = manager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, "native_br", 20, -1, time.Now())
	require.NoError(t, err)
	result, err = manager.AdvanceTxnSafePoint(2, 30, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(30), result.NewTxnSafePoint)
}
