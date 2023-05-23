// Copyright 2022 TiKV Project Authors.
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
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestHotWriteRegionScheduleWithRevertRegionsDimSecond(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -1.
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	statistics.Denoising = false

	sche, err := CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageWrittenStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 16*units.MiB*statistics.StoreHeartBeatReportInterval, 20*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 14*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 2, 4}, 2 * units.MiB, 3 * units.MiB, 0},
		{7, []uint64{1, 4, 5}, 2 * units.MiB, 0.1 * units.MiB, 0},
	})
	// No operators can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
	re.False(hb.searchRevertRegions[writePeer])

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	re.True(hb.searchRevertRegions[writePeer])
	// Two operators can be generated when RankFormulaVersion == "v2".
	ops, _ = hb.Schedule(tc, false)
	/* The revert region is currently disabled for the -1 case.
	re.Len(ops, 2)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	operatorutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
	*/
	re.Empty(ops)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// When there is a better solution, there will only be one operator.
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{8, []uint64{3, 2, 4}, 0.5 * units.MiB, 3 * units.MiB, 0},
	})
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
}

func TestHotWriteRegionScheduleWithRevertRegionsDimFirst(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -3.
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	statistics.Denoising = false

	sche, err := CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageWrittenStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 20*units.MiB*statistics.StoreHeartBeatReportInterval, 14*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 16*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 2, 4}, 3 * units.MiB, 1.8 * units.MiB, 0},
		{7, []uint64{1, 4, 5}, 0.1 * units.MiB, 2 * units.MiB, 0},
	})
	// One operator can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// Two operators can be generated when RankFormulaVersion == "v2".
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 2)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	operatorutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
}

func TestHotWriteRegionScheduleWithRevertRegionsDimFirstOnly(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -2.
	re := require.New(t)
	statistics.Denoising = false

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sche, err := CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageWrittenStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 20*units.MiB*statistics.StoreHeartBeatReportInterval, 14*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 16*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 18*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 2, 4}, 3 * units.MiB, 3 * units.MiB, 0},
		{7, []uint64{1, 4, 5}, 0.1 * units.MiB, 0.1 * units.MiB, 0},
	})
	// One operator can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// There is still the solution with one operator after that.
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// Two operators can be generated when there is a better solution
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{8, []uint64{1, 4, 5}, 0.1 * units.MiB, 3 * units.MiB, 0},
	})
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 2)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	operatorutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
}

func TestHotReadRegionScheduleWithRevertRegionsDimSecond(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -1.
	re := require.New(t)
	statistics.Denoising = false

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sche, err := CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageReadStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 16*units.MiB*statistics.StoreHeartBeatReportInterval, 20*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(5, 14*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{6, []uint64{2, 1, 5}, 2 * units.MiB, 3 * units.MiB, 0},
		{7, []uint64{5, 4, 2}, 2 * units.MiB, 0.1 * units.MiB, 0},
	})
	// No operators can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
	re.False(hb.searchRevertRegions[readLeader])

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	re.True(hb.searchRevertRegions[readLeader])
	// Two operators can be generated when RankFormulaVersion == "v2".
	ops, _ = hb.Schedule(tc, false)
	/* The revert region is currently disabled for the -1 case.
	re.Len(ops, 2)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 2, 5)
	operatorutil.CheckTransferLeader(re, ops[1], operator.OpHotRegion, 5, 2)
	*/
	re.Empty(ops)
	re.True(hb.searchRevertRegions[readLeader])
	clearPendingInfluence(hb)
	// When there is a better solution, there will only be one operator.
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{8, []uint64{2, 1, 5}, 0.5 * units.MiB, 3 * units.MiB, 0},
	})
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[readLeader])
	clearPendingInfluence(hb)
}

func TestSkipUniformStore(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetRankFormulaVersion("v2")
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	// Case1: two dim are both enough uniform
	tc.UpdateStorageReadStats(1, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.15*units.MB*statistics.StoreHeartBeatReportInterval, 9.15*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 10.0*units.MB*statistics.StoreHeartBeatReportInterval, 10.0*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.3 * units.MB, 0.3 * units.MB, 0},
	})
	// when there is no uniform store filter, still schedule although the cluster is enough uniform
	stddevThreshold = 0.0
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	// when there is uniform store filter, not schedule
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 0)
	clearPendingInfluence(hb.(*hotScheduler))

	// Case2: the first dim is enough uniform, we should schedule the second dim
	tc.UpdateStorageReadStats(1, 10.15*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.25*units.MB*statistics.StoreHeartBeatReportInterval, 9.85*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 9.85*units.MB*statistics.StoreHeartBeatReportInterval, 16.0*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.3 * units.MB, 0.3 * units.MB, 0},
		{2, []uint64{3, 2, 1}, 0.3 * units.MB, 2 * units.MB, 0},
	})
	// when there is no uniform store filter, still schedule although the first dim is enough uniform
	stddevThreshold = 0.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	// when there is uniform store filter, schedule the second dim, which is no uniform
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 3, 2)
	clearPendingInfluence(hb.(*hotScheduler))

	// Case3: the second dim is enough uniform, we should schedule the first dim, although its rank is higher than the second dim
	tc.UpdateStorageReadStats(1, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.85*units.MB*statistics.StoreHeartBeatReportInterval, 9.45*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 16*units.MB*statistics.StoreHeartBeatReportInterval, 9.85*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.3 * units.MB, 0.3 * units.MB, 0},
		{2, []uint64{3, 2, 1}, 2 * units.MB, 0.3 * units.MB, 0},
	})
	// when there is no uniform store filter, schedule the first dim, which is no uniform
	stddevThreshold = 0.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 3, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	// when there is uniform store filter, schedule the first dim, which is no uniform
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 3, 2)
	clearPendingInfluence(hb.(*hotScheduler))
}
