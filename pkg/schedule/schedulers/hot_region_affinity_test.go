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

package schedulers

import (
	"fmt"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestHotRegionRevertAffinity(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		resource    resourceType
		withBuckets bool
	}{
		{name: "move-peer", resource: writePeer},
		{name: "transfer-leader", resource: readLeader},
		{name: "split-buckets", resource: writePeer, withBuckets: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, groupState := range []string{"no-affinity", "active", "no-target", "expired"} {
				t.Run(groupState, func(t *testing.T) {
					re := require.New(t)
					cancel, _, tc, oc := prepareSchedulersTest()
					t.Cleanup(cancel)
					schedulerType, rwTy := writeType, utils.Write
					updateStats := tc.UpdateStorageWrittenStats
					checkTransfer := operatorutil.CheckTransferPeer
					regions := []testRegionInfo{
						{6, []uint64{3, 2, 4}, 3 * units.MiB, 1.8 * units.MiB, 0},
						{7, []uint64{1, 4, 5}, 0.1 * units.MiB, 2 * units.MiB, 0},
					}
					if testCase.resource == readLeader {
						schedulerType, rwTy = readType, utils.Read
						updateStats = tc.UpdateStorageReadStats
						checkTransfer = operatorutil.CheckTransferLeader
						regions[0].peers = []uint64{2, 1, 5}
						regions[1].peers = []uint64{5, 4, 2}
					}
					sche, err := CreateScheduler(schedulerType, oc, storage.NewStorageWithMemoryBackend(), nil, nil)
					re.NoError(err)
					hb := sche.(*hotScheduler)
					hb.types = []resourceType{testCase.resource}
					hb.conf.setDstToleranceRatio(0)
					hb.conf.setSrcToleranceRatio(0)
					hb.conf.setRankFormulaVersion("v1")
					hb.conf.setHistorySampleDuration(0)
					hb.conf.WritePeerPriorities = []string{utils.BytePriority, utils.KeyPriority}
					hb.conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
					tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
					for id := uint64(1); id <= 5; id++ {
						tc.AddRegionStore(id, 20)
						updateStats(id, 15*units.MiB*utils.StoreHeartBeatReportInterval, 15*units.MiB*utils.StoreHeartBeatReportInterval)
					}
					updateStats(2, 20*units.MiB*utils.StoreHeartBeatReportInterval, 14*units.MiB*utils.StoreHeartBeatReportInterval)
					updateStats(5, 10*units.MiB*utils.StoreHeartBeatReportInterval, 16*units.MiB*utils.StoreHeartBeatReportInterval)
					// Region 6 moves 2 -> 5; reverting region 7 from 5 -> 2 improves the solution.
					addRegionInfo(tc, rwTy, regions)
					revertRegion := tc.GetRegion(7)
					splitKey := fmt.Appendf(nil, "%20d5", 7)
					if testCase.withBuckets {
						tc.SetRegionBucketEnabled(true)
						revertRegion = revertRegion.Clone(core.SetApproximateSize(1024))
						re.True(revertRegion.UpdateBuckets(&metapb.Buckets{
							RegionId:   7,
							PeriodInMs: 1000,
							Keys:       [][]byte{revertRegion.GetStartKey(), splitKey, revertRegion.GetEndKey()},
						}, revertRegion.GetBuckets()))
						tc.PutRegion(revertRegion)
					}
					protected := groupState == "active"
					if groupState != "no-affinity" {
						const groupID = "hot-revert"
						mgr := tc.GetAffinityManager()
						re.NoError(mgr.CreateAffinityGroups([]affinity.GroupKeyRanges{{
							GroupID: groupID,
							KeyRanges: []keyutil.KeyRange{keyutil.NewKeyRange(
								string(revertRegion.GetStartKey()), string(revertRegion.GetEndKey()))},
						}}))
						if groupState != "no-target" {
							_, err = mgr.UpdateAffinityGroupPeers(groupID, regions[1].peers[0], regions[1].peers)
							re.NoError(err)
						}
						if groupState == "expired" {
							mgr.ExpireAffinityGroup(groupID)
						}
						group, isAffinity := mgr.GetRegionAffinityGroupState(revertRegion)
						re.NotNil(group)
						re.Equal(!protected, group.RegularSchedulingAllowed)
						if protected {
							re.True(isAffinity)
						}
					}
					re.Equal(!protected, filter.NewAffinityFilter(tc).Select(revertRegion).IsOK())

					ops, _ := hb.Schedule(tc, false)
					re.Len(ops, 1)
					checkTransfer(re, ops[0], operator.OpHotRegion, 2, 5)
					clearPendingInfluence(hb)
					hb.conf.setRankFormulaVersion("v2")
					// The first v2 round enables the revert search for the next round.
					ops, _ = hb.Schedule(tc, false)
					re.Len(ops, 1)
					re.True(hb.searchRevertRegions[testCase.resource])
					clearPendingInfluence(hb)
					ops, _ = hb.Schedule(tc, false)
					for _, op := range ops {
						t.Logf("region=%d desc=%s kind=%s brief=%s", op.RegionID(), op.Desc(), op.Kind(), op.Brief())
					}
					if protected {
						re.Len(ops, 1, "the affinity revert region must not be moved or split")
						re.Equal(uint64(6), ops[0].RegionID())
						checkTransfer(re, ops[0], operator.OpHotRegion, 2, 5)
					} else if testCase.withBuckets {
						re.Len(ops, 1)
						re.Equal(uint64(7), ops[0].RegionID())
						re.Equal(splitHotWriteBuckets, ops[0].Desc())
						step, ok := ops[0].Step(0).(operator.SplitRegion)
						re.True(ok)
						re.Equal(pdpb.CheckPolicy_USEKEY, step.Policy)
						re.Equal([][]byte{splitKey}, step.SplitKeys)
					} else {
						re.Len(ops, 2)
						re.Equal(uint64(6), ops[0].RegionID())
						checkTransfer(re, ops[0], operator.OpHotRegion, 2, 5)
						re.Equal(uint64(7), ops[1].RegionID())
						checkTransfer(re, ops[1], operator.OpHotRegion, 5, 2)
					}
				})
			}
		})
	}
}
