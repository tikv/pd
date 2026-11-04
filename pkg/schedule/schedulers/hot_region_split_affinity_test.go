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
	"testing/synctest"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func setHotSplitAffinityGroup(t *testing.T, tc *mockcluster.Cluster, region *core.RegionInfo, state string) {
	t.Helper()
	if state == "none" {
		return
	}
	re := require.New(t)
	const groupID = "hot-split"
	manager := tc.GetAffinityManager()
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID: groupID,
		KeyRanges: []keyutil.KeyRange{{
			StartKey: region.GetStartKey(), EndKey: region.GetEndKey(),
		}},
	}}))
	if state != "no-target" {
		voters := make([]uint64, 0, len(region.GetVoters()))
		for _, peer := range region.GetVoters() {
			voters = append(voters, peer.GetStoreId())
		}
		_, err := manager.UpdateAffinityGroupPeers(groupID, region.GetLeader().GetStoreId(), voters)
		re.NoError(err)
	}
	if state == "expired" {
		manager.ExpireAffinityGroup(groupID)
	}
	group, _ := manager.GetRegionAffinityGroupState(region)
	re.NotNil(group)
	re.Equal(state != "available", group.RegularSchedulingAllowed)
}

func TestHotRegionAffinityBucketSplitByLoad(t *testing.T) {
	for _, state := range []string{"none", "no-target", "available", "expired"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				cancel, _, tc, oc := prepareSchedulersTest()
				t.Cleanup(cancel)
				sche, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
				re.NoError(err)
				hb := sche.(*hotScheduler)
				hb.types = []resourceType{readLeader}
				hb.conf.setHistorySampleDuration(0)
				hb.conf.setRankFormulaVersion("v1")
				hb.conf.setSrcToleranceRatio(0)
				hb.conf.setDstToleranceRatio(0)
				hb.conf.SplitThresholds = 0.1
				hb.conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
				tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
				tc.SetRegionBucketEnabled(true)
				for id := uint64(1); id <= 5; id++ {
					tc.AddRegionStore(id, 20)
					tc.UpdateStorageReadStats(id, 15*units.MiB*utils.StoreHeartBeatReportInterval, 15*units.MiB*utils.StoreHeartBeatReportInterval)
				}
				tc.UpdateStorageReadStats(2, 20*units.MiB*utils.StoreHeartBeatReportInterval, 14*units.MiB*utils.StoreHeartBeatReportInterval)
				tc.UpdateStorageReadStats(5, 10*units.MiB*utils.StoreHeartBeatReportInterval, 16*units.MiB*utils.StoreHeartBeatReportInterval)
				addRegionInfo(tc, utils.Read, []testRegionInfo{
					{1, []uint64{2, 1, 5}, 3 * units.MiB, 1.8 * units.MiB, 0},
					{2, []uint64{5, 4, 2}, 0.1 * units.MiB, 2 * units.MiB, 0},
				})
				setHotSplitAffinityGroup(t, tc, tc.GetRegion(1), state)
				re.True(tc.CheckAsync(buckets.NewCheckPeerTask(&metapb.Buckets{
					RegionId: 1, PeriodInMs: 1000,
					Keys: [][]byte{
						fmt.Appendf(nil, "%21d", 11), fmt.Appendf(nil, "%21d", 12),
						fmt.Appendf(nil, "%21d", 13), fmt.Appendf(nil, "%21d", 14), fmt.Appendf(nil, "%21d", 15),
					},
					Stats: &metapb.BucketStats{
						ReadBytes:  []uint64{10 * units.KiB, 11 * units.KiB, 11 * units.KiB, 10 * units.KiB},
						ReadKeys:   []uint64{256, 256, 156, 256},
						ReadQps:    make([]uint64, 4),
						WriteBytes: make([]uint64, 4),
						WriteKeys:  make([]uint64, 4),
						WriteQps:   make([]uint64, 4),
					},
				})))
				synctest.Sleep(10 * time.Millisecond)
				ops, _ := hb.Schedule(tc, false)
				if state == "available" {
					for _, op := range ops {
						re.NotEqual(uint64(1), op.RegionID())
					}
					return
				}
				re.Len(ops, 1)
				t.Logf("region=%d kind=%s brief=%s", ops[0].RegionID(), ops[0].Kind(), ops[0].Brief())
				if state == "expired" {
					re.IsType(operator.TransferLeader{}, ops[0].Step(0))
					operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 2, 5)
					return
				}
				step, ok := ops[0].Step(0).(operator.SplitRegion)
				re.True(ok)
				re.Equal(pdpb.CheckPolicy_USEKEY, step.Policy)
				re.Equal([][]byte{fmt.Appendf(nil, "%21d", 13)}, step.SplitKeys)
			})
		})
	}
}

func TestHotRegionAffinityBucketSplitBySize(t *testing.T) {
	for _, candidate := range []string{"main", "revert"} {
		t.Run(candidate, func(t *testing.T) {
			for _, state := range []string{"none", "no-target", "expired"} {
				t.Run(state, func(t *testing.T) {
					re := require.New(t)
					cancel, _, tc, oc := prepareSchedulersTest()
					t.Cleanup(cancel)
					sche, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
					re.NoError(err)
					hb := sche.(*hotScheduler)
					hb.types = []resourceType{writePeer}
					hb.conf.setRankFormulaVersion("v2")
					hb.conf.setSrcToleranceRatio(0)
					hb.conf.setDstToleranceRatio(0)
					hb.conf.setHistorySampleDuration(0)
					hb.conf.WritePeerPriorities = []string{utils.BytePriority, utils.KeyPriority}
					hb.searchRevertRegions[writePeer] = true
					tc.SetRegionBucketEnabled(true)
					tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
					for id := uint64(1); id <= 5; id++ {
						tc.AddRegionStore(id, 20)
						tc.UpdateStorageWrittenStats(id, 15*units.MiB*utils.StoreHeartBeatReportInterval, 15*units.MiB*utils.StoreHeartBeatReportInterval)
					}
					tc.UpdateStorageWrittenStats(2, 20*units.MiB*utils.StoreHeartBeatReportInterval, 14*units.MiB*utils.StoreHeartBeatReportInterval)
					tc.UpdateStorageWrittenStats(5, 10*units.MiB*utils.StoreHeartBeatReportInterval, 16*units.MiB*utils.StoreHeartBeatReportInterval)
					addRegionInfo(tc, utils.Write, []testRegionInfo{
						{6, []uint64{3, 2, 4}, 3 * units.MiB, 1.8 * units.MiB, 0},
						{7, []uint64{1, 4, 5}, 0.1 * units.MiB, 2 * units.MiB, 0},
					})
					regionID := uint64(6)
					if candidate == "revert" {
						regionID = 7
					}
					region := tc.GetRegion(regionID).Clone(core.SetApproximateSize(1024))
					splitKey := fmt.Appendf(nil, "%20d5", regionID)
					re.True(region.UpdateBuckets(&metapb.Buckets{
						RegionId: regionID, PeriodInMs: 1000,
						Keys: [][]byte{region.GetStartKey(), splitKey, region.GetEndKey()},
					}, region.GetBuckets()))
					tc.PutRegion(region)
					setHotSplitAffinityGroup(t, tc, region, state)
					ops, _ := hb.Schedule(tc, false)
					for _, op := range ops {
						t.Logf("region=%d kind=%s brief=%s", op.RegionID(), op.Kind(), op.Brief())
					}
					if state == "expired" {
						re.Len(ops, 2, "expired groups allow peer moves without splitting")
						operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
						operatorutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
						return
					}
					re.Len(ops, 1)
					re.Equal(regionID, ops[0].RegionID())
					step, ok := ops[0].Step(0).(operator.SplitRegion)
					re.True(ok)
					re.Equal(pdpb.CheckPolicy_USEKEY, step.Policy)
					re.Equal([][]byte{splitKey}, step.SplitKeys)
				})
			}
		})
	}
}
