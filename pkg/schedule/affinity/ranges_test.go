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

package affinity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newAffinityRangesTestManager(t *testing.T, store storage.Storage) *Manager {
	t.Helper()
	re := require.New(t)
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 3; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id}, core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(t.Context(), store, stores, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return manager
}

func TestAffinityGroupKeyRangesSnapshot(t *testing.T) {
	re := require.New(t)
	manager := newAffinityRangesTestManager(t, storage.NewStorageWithMemoryBackend())
	ranges := GroupKeyRanges{GroupID: "g", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	_, err := manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)
	group := manager.GetAffinityGroupState("g")
	copy := manager.GetAffinityGroupKeyRanges(group)
	re.Equal(ranges, copy)
	copy.KeyRanges[0].StartKey[0] = 'x'
	copy.KeyRanges[0].EndKey[0] = 'y'
	copy.KeyRanges[0] = keyutil.KeyRange{}
	re.Equal(ranges, manager.GetAffinityGroupKeyRanges(group), "callers cannot mutate the manager's ranges")

	_, err = manager.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2, 3})
	re.NoError(err)
	re.Empty(manager.GetAffinityGroupKeyRanges(group).KeyRanges, "an old target cannot authorize a move or merge")

	group = manager.GetAffinityGroupState("g")
	added := GroupKeyRanges{GroupID: "g", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("c"), EndKey: []byte("d")}}}
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{added}, nil))
	re.Empty(manager.GetAffinityGroupKeyRanges(group).KeyRanges, "coverage must be checked again when ranges change")
	group = manager.GetAffinityGroupState("g")
	re.Len(manager.GetAffinityGroupKeyRanges(group).KeyRanges, 2)
	re.NoError(manager.DeleteAffinityGroups([]string{"g"}, true))
	re.Empty(manager.GetAffinityGroupKeyRanges(group).KeyRanges)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	_, err = manager.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2, 3})
	re.NoError(err)
	re.Empty(manager.GetAffinityGroupKeyRanges(group).KeyRanges, "a recreated group invalidates the old state")
	re.Equal(ranges, manager.GetAffinityGroupKeyRanges(manager.GetAffinityGroupState("g")))
}

func TestAffinityGroupKeyRangesAfterRestart(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	manager := newAffinityRangesTestManager(t, store)
	ranges := GroupKeyRanges{GroupID: "g", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("z")}}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	_, err := manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)
	reloaded := newAffinityRangesTestManager(t, store)
	// Availability checks run separately from reloading the stored target.
	reloaded.RestoreAffinityGroup("g")
	group := reloaded.GetAffinityGroupState("g")
	re.True(group.AffinitySchedulingAllowed)
	re.Equal(uint64(1), group.LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, group.VoterStoreIDs)
	re.Equal(ranges, reloaded.GetAffinityGroupKeyRanges(group))
}
