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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func TestTiDBGroupIDValidation(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	for _, nextGen := range []bool{false, true} {
		t.Run(fmt.Sprintf("nextgen=%t", nextGen), func(t *testing.T) {
			require.NoError(t, failpoint.Enable(modeFlag, fmt.Sprintf("return(%t)", nextGen)))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
			for _, id := range []string{"custom_group", "_tidb_ks0_t_1", "_tidb_ks42_pt_1001_p3", "_tidb_ks16777215_t_9223372036854775807", "_tidb_ks0_tg_1", "_tidb_ks42_tg_100", "_tidb_ks16777215_tg_9223372036854775807", "_tidb_ks42_tg_100_i0", "_tidb_ks42_tg_100_i1"} {
				require.NoError(t, ValidateGroupID(id), id)
			}
			for _, id := range []string{"_tidb_t_1001", "_tidb_pt_1001_p3", "_tidb_tg_100", "_tidb_tg_100_i0", "_tidb_ks16777216_t_1", "_tidb_ks42_t_0", "_tidb_ks42_pt_1_p0", "_tidb_ks42_t_9223372036854775808", "_tidb_ks42_pt_1_p9223372036854775808", "_tidb_ks042_t_1", "_tidb_ks42_t_01", "_tidb_ks42", "_tidb_ks42_t_1_extra", "_tidb_ks16777216_tg_1", "_tidb_ks01_tg_1", "_tidb_ks1_tg_0", "_tidb_ks1_tg_01", "_tidb_ks1_tg_9223372036854775808", "_tidb_ks1_tg_1_extra", "_tidb_ks1_tg_1_i01", "_tidb_ks1_tg_1_i9223372036854775808"} {
				if nextGen {
					require.Error(t, ValidateGroupID(id), id)
				} else {
					require.NoError(t, ValidateGroupID(id), id)
				}
			}
			for _, keyspaceID := range []uint32{0, 42, 43, 16777215} {
				lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, keyspaceID)
				upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, keyspaceID+1)
				for _, id := range []string{fmt.Sprintf("_tidb_ks%d_pt_1001_p3", keyspaceID), fmt.Sprintf("_tidb_ks%d_tg_100", keyspaceID), "custom_group"} {
					group := GroupKeyRanges{
						GroupID:   id,
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
			}
		})
	}
}

func TestNextGenRangeBoundaries(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 1<<24)
	for _, id := range []string{"custom", "_tidb_ks16777215_t_1"} {
		for _, start := range [][]byte{codec.EncodeBytes([]byte("y")), codec.EncodeBytes([]byte("y\x00")), {'x', 0xff, 0xff, 0xff, 't'}} {
			re.ErrorIs(validateGroupKeyRanges(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: start, EndKey: upper[:]}}}), errs.ErrAffinityGroupContent)
		}
	}

	// A short encoded x-prefix below boundary(2) still belongs to keyspace 1.
	end := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 2)
	beforeTwo := append([]byte(nil), end[:]...)
	beforeTwo[len(beforeTwo)-1]--
	re.NoError(validateGroupKeyRanges(GroupKeyRanges{GroupID: "custom", KeyRanges: []keyutil.KeyRange{{StartKey: beforeTwo, EndKey: end[:]}}}))
}

func TestGroupState(t *testing.T) {
	re := require.New(t)
	group := &GroupState{
		ID:                        "group1",
		LeaderStoreID:             1,
		VoterStoreIDs:             []uint64{1, 2, 3},
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

func TestCustomGroupPolicyInheritanceKeyspace(t *testing.T) {
	re := require.New(t)
	re.Error(validateGroupKeyRanges(GroupKeyRanges{GroupID: "custom", InheritPolicyFrom: "bad id"}))
	lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 42)
	upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 43)
	group := GroupKeyRanges{GroupID: "custom", InheritPolicyFrom: "_tidb_ks42_t_1",
		KeyRanges: []keyutil.KeyRange{{StartKey: lower[:], EndKey: upper[:]}}}
	if kerneltype.IsNextGen() {
		re.ErrorIs(validateGroupKeyRanges(group), errs.ErrAffinityGroupContent)
	} else {
		re.NoError(validateGroupKeyRanges(group))
	}
	group.InheritPolicyFrom = "_tidb_ks43_t_1"
	if kerneltype.IsNextGen() {
		re.ErrorIs(validateGroupKeyRanges(group), errs.ErrAffinityGroupContent)
		group.InheritPolicyFrom = "_tidb_t_1"
		re.Error(validateGroupKeyRanges(group))
	} else {
		re.NoError(validateGroupKeyRanges(group))
	}
}
