// Copyright 2025 TiKV Project Authors.
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

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

// Ensure WaitLoad failure won't leave background goroutines running.
func TestNewWatcherWaitLoadFailed(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail", "return(3)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/loadTemporaryFail"))
	}()

	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	watcher, err := NewWatcher(context.Background(), client, core.NewBasicCluster())
	re.Error(err)
	re.Nil(watcher)
}

// getStoreFromCache queries the store through the helpers used by the router
// service, so that both the shared SetStoreMeta update path and the query path
// are covered.
func getStoreFromCache(re *require.Assertions, rc *core.BasicCluster, storeID uint64) *metapb.Store {
	resp, err := grpcutil.GetStore(rc, &pdpb.GetStoreRequest{StoreId: storeID})
	re.NoError(err)
	re.NotNil(resp)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	re.Equal(storeID, resp.GetStore().GetId())
	return resp.GetStore()
}

// getAllStoresFromCache queries all the stores through the helpers used by the
// router service.
func getAllStoresFromCache(re *require.Assertions, rc *core.BasicCluster) []*metapb.Store {
	resp, err := grpcutil.GetAllStores(rc, &pdpb.GetAllStoresRequest{ExcludeTombstoneStores: true})
	re.NoError(err)
	re.NotNil(resp)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
	return resp.GetStores()
}

func checkStoreTxnProtocolVersionRange(re *require.Assertions, rc *core.BasicCluster, expected *metapb.TxnProtocolVersionRange) {
	check := func(store *metapb.Store) {
		actual := store.GetTxnProtocolVersionRange()
		if expected == nil {
			re.Nil(actual)
			return
		}
		re.NotNil(actual)
		re.Equal(expected.GetMin(), actual.GetMin())
		re.Equal(expected.GetMax(), actual.GetMax())
	}
	check(getStoreFromCache(re, rc, 1))
	stores := getAllStoresFromCache(re, rc)
	re.Len(stores, 1)
	check(stores[0])
}

// TestStoreTxnProtocolVersionRange verifies that the router store watcher keeps
// the txn protocol version range of an already loaded store up to date.
func TestStoreTxnProtocolVersionRange(t *testing.T) {
	re := require.New(t)
	oldClusterID := keypath.ClusterID()
	keypath.SetClusterID(1)
	t.Cleanup(func() { keypath.SetClusterID(oldClusterID) })

	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	storage := storage.NewStorageWithEtcdBackend(client)
	newStoreMeta := func(versionRange *metapb.TxnProtocolVersionRange) *metapb.Store {
		return &metapb.Store{
			Id:                      1,
			Address:                 "mock://tikv-1:1",
			Version:                 "6.5.0",
			State:                   metapb.StoreState_Up,
			NodeState:               metapb.NodeState_Serving,
			TxnProtocolVersionRange: versionRange,
		}
	}

	// Seed the store before the watcher starts so that the initial load and the
	// subsequent updates use different code paths.
	re.NoError(storage.SaveStoreMeta(newStoreMeta(nil)))

	rc := core.NewBasicCluster()
	watcher, err := NewWatcher(context.Background(), client, rc)
	re.NoError(err)
	defer watcher.Close()
	// NewWatcher waits for the initial load, so the seeded store must be visible
	// as soon as it returns.
	checkStoreTxnProtocolVersionRange(re, rc, nil)

	// Update the same store while the watcher keeps running. The binary version
	// and the other fields stay unchanged, so the range update can only come
	// from the new option.
	for _, expected := range []*metapb.TxnProtocolVersionRange{
		{Min: 0, Max: 1},
		{Min: 0, Max: 2},
		{Min: 0, Max: 1},
		{},
		nil,
	} {
		re.NoError(storage.SaveStoreMeta(newStoreMeta(expected)))
		testutil.Eventually(re, func() bool {
			store := rc.GetStore(1)
			if store == nil {
				return false
			}
			actual := store.GetMeta().GetTxnProtocolVersionRange()
			if expected == nil {
				return actual == nil
			}
			return actual != nil && actual.GetMin() == expected.GetMin() && actual.GetMax() == expected.GetMax()
		})
		checkStoreTxnProtocolVersionRange(re, rc, expected)
	}
}
