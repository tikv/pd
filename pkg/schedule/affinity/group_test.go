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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestTiDBGroupIDValidation(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	for _, nextGen := range []bool{false, true} {
		t.Run(fmt.Sprintf("nextgen=%t", nextGen), func(t *testing.T) {
			require.NoError(t, failpoint.Enable(modeFlag, fmt.Sprintf("return(%t)", nextGen)))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
			for _, id := range []string{"custom_group", "_tidb_ks0_t_1", "_tidb_ks42_pt_1001_p3", "_tidb_ks16777215_t_9223372036854775807"} {
				require.NoError(t, ValidateGroupID(id), id)
			}
			for _, id := range []string{"_tidb_t_1001", "_tidb_pt_1001_p3", "_tidb_ks16777216_t_1", "_tidb_ks42_t_0", "_tidb_ks42_pt_1_p0", "_tidb_ks42_t_9223372036854775808", "_tidb_ks42_pt_1_p9223372036854775808", "_tidb_ks042_t_1", "_tidb_ks42_t_01", "_tidb_ks42", "_tidb_ks42_t_1_extra"} {
				if nextGen {
					require.Error(t, ValidateGroupID(id), id)
				} else {
					require.NoError(t, ValidateGroupID(id), id)
				}
			}
			for _, keyspaceID := range []uint32{0, 42, 43, 16777215} {
				lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, keyspaceID)
				upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, keyspaceID+1)
				group := GroupKeyRanges{
					GroupID:   fmt.Sprintf("_tidb_ks%d_pt_1001_p3", keyspaceID),
					KeyRanges: []keyutil.KeyRange{{StartKey: lower[:], EndKey: upper[:]}},
				}
				require.NoError(t, validateGroupKeyRanges(group))
				for _, kr := range []keyutil.KeyRange{
					{},
					{StartKey: []byte("a"), EndKey: []byte("b")},
					{StartKey: lower[:], EndKey: nil},
					{StartKey: upper[:], EndKey: upper[:]},
					{StartKey: upper[:], EndKey: []byte("z")},
				} {
					group.KeyRanges = []keyutil.KeyRange{kr}
					if nextGen {
						require.Error(t, validateGroupKeyRanges(group))
					} else {
						require.NoError(t, validateGroupKeyRanges(group))
					}
				}
			}
		})
	}
}

func TestGroupState(t *testing.T) {
	re := require.New(t)
	group := &GroupState{
		Group: Group{
			ID:            "group1",
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 2, 3},
		},
		AffinitySchedulingAllowed: true,
	}
	// keyRange is unused in this test.
	region := generateRegionForTest(100, []uint64{1, 2, 3}, nonOverlappingRange)
	re.True(group.isRegionAffinity(region))
	region = generateRegionForTest(100, []uint64{2, 1, 3}, nonOverlappingRange)
	re.False(group.isRegionAffinity(region))
	region = generateRegionForTest(100, []uint64{1, 2, 4}, nonOverlappingRange)
	re.False(group.isRegionAffinity(region))

	group.AffinitySchedulingAllowed = false
	region = generateRegionForTest(100, []uint64{1, 2, 3}, nonOverlappingRange)
	re.False(group.isRegionAffinity(region))
}

func TestAdjustGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: "test"})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	group := &Group{
		ID:            "TEST-group_1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	re.NoError(manager.AdjustGroup(group))
	// test id error
	group.ID = ""
	re.Error(manager.AdjustGroup(group))
	group.ID = strings.Repeat("a", 65)
	re.Error(manager.AdjustGroup(group))
	group.ID = "TEST-group@2"
	re.Error(manager.AdjustGroup(group))
	// test no leader
	group.ID = "TEST-group_1"
	group.LeaderStoreID = 0
	re.Error(manager.AdjustGroup(group))
	// test no voters
	group.LeaderStoreID = 1
	group.VoterStoreIDs = nil
	re.Error(manager.AdjustGroup(group))
	// test leader not in voters
	group.LeaderStoreID = 1
	group.VoterStoreIDs = []uint64{2, 3}
	re.Error(manager.AdjustGroup(group))
	// test duplicate voter id
	group.LeaderStoreID = 1
	group.VoterStoreIDs = []uint64{1, 1, 3}
	re.Error(manager.AdjustGroup(group))
	// test error store id
	group.LeaderStoreID = 1
	group.VoterStoreIDs = []uint64{1, 4}
	re.Error(manager.AdjustGroup(group))
}
