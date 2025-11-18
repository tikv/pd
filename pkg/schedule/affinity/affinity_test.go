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

package affinity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestStoreHealthCheck(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a test storage
	store := storage.NewStorageWithMemoryBackend()

	// Create mock stores
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "test2"})
	store3 := core.NewStoreInfo(&metapb.Store{Id: 3, Address: "test3"})

	// Set store1 to be healthy
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	// Set store2 to be healthy
	store2 = store2.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store2)

	// Set store3 to be unhealthy (disconnected)
	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store3)

	conf := mockconfig.NewTestOptions()

	// Create affinity manager
	manager := NewManager(ctx, store, storeInfos, conf)
	err := manager.Initialize()
	re.NoError(err)

	// Create a test affinity group with healthy stores
	group1 := &Group{
		ID:            "group1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2},
	}
	err = manager.SaveAffinityGroup(group1)
	re.NoError(err)

	// Create a test affinity group with unhealthy store
	group2 := &Group{
		ID:            "group2",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{3, 2},
	}
	err = manager.SaveAffinityGroup(group2)
	re.NoError(err)

	// Verify initial state - all groups should be in effect
	groupInfo1 := manager.groups["group1"]
	re.True(groupInfo1.Effect)
	groupInfo2 := manager.groups["group2"]
	re.True(groupInfo2.Effect)

	// Manually call checkStoreHealth to test
	manager.checkStoreHealth()

	// After health check, group1 should still be in effect (all stores healthy)
	re.True(manager.groups["group1"].Effect)

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].Effect)

	// Now make store3 healthy again
	store3Healthy := store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.checkStoreHealth()

	// Group2 should be restored to effect state
	re.True(manager.groups["group2"].Effect)
}

func TestGetUnhealthyStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()

	// Create stores with different health status
	healthyStore := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	healthyStore = healthyStore.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(healthyStore)

	unhealthyStore := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "test2"})
	unhealthyStore = unhealthyStore.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(unhealthyStore)

	disconnectedStore := core.NewStoreInfo(&metapb.Store{Id: 3, Address: "test3"})
	disconnectedStore = disconnectedStore.Clone(core.SetLastHeartbeatTS(time.Now().Add(-35 * time.Minute)))
	storeInfos.PutStore(disconnectedStore)

	conf := mockconfig.NewTestOptions()
	manager := NewManager(ctx, store, storeInfos, conf)

	// Test group with only healthy stores
	groupInfo1 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		},
	}
	unhealthy := manager.getUnhealthyStores(groupInfo1)
	re.Empty(unhealthy)

	// Test group with unhealthy leader
	groupInfo2 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 2,
			VoterStoreIDs: []uint64{2, 1},
		},
	}
	unhealthy = manager.getUnhealthyStores(groupInfo2)
	re.Contains(unhealthy, uint64(2))

	// Test group with disconnected voter
	groupInfo3 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 3},
		},
	}
	unhealthy = manager.getUnhealthyStores(groupInfo3)
	re.Contains(unhealthy, uint64(3))

	// Test group with multiple unhealthy stores
	groupInfo4 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 2,
			VoterStoreIDs: []uint64{2, 3},
		},
	}
	unhealthy = manager.getUnhealthyStores(groupInfo4)
	re.Len(unhealthy, 2)
	re.Contains(unhealthy, uint64(2))
	re.Contains(unhealthy, uint64(3))
}
