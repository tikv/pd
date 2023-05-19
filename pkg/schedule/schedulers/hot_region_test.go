// Copyright 2019 TiKV Project Authors.
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
	"encoding/hex"
	"math"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func init() {
	schedulePeerPr = 1.0
	schedule.RegisterScheduler(statistics.Write.String(), func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		cfg := initHotRegionScheduleConfig()
		return newHotWriteScheduler(opController, cfg), nil
	})
	schedule.RegisterScheduler(statistics.Read.String(), func(opController *operator.Controller, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newHotReadScheduler(opController, initHotRegionScheduleConfig()), nil
	})
}

func newHotReadScheduler(opController *operator.Controller, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []statistics.RWType{statistics.Read}
	return ret
}

func newHotWriteScheduler(opController *operator.Controller, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.name = ""
	ret.types = []statistics.RWType{statistics.Write}
	return ret
}

func clearPendingInfluence(h *hotScheduler) {
	h.regionPendings = make(map[uint64]*pendingInfluence)
}

func TestUpgrade(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()
	// new
	sche, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(HotRegionType, nil))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	re.Equal([]string{statistics.QueryPriority, statistics.BytePriority}, hb.conf.GetReadPriorities())
	re.Equal([]string{statistics.QueryPriority, statistics.BytePriority}, hb.conf.GetWriteLeaderPriorities())
	re.Equal([]string{statistics.BytePriority, statistics.KeyPriority}, hb.conf.GetWritePeerPriorities())
	re.Equal("v2", hb.conf.GetRankFormulaVersion())
	// upgrade from json(null)
	sche, err = schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb = sche.(*hotScheduler)
	re.Equal([]string{statistics.QueryPriority, statistics.BytePriority}, hb.conf.GetReadPriorities())
	re.Equal([]string{statistics.QueryPriority, statistics.BytePriority}, hb.conf.GetWriteLeaderPriorities())
	re.Equal([]string{statistics.BytePriority, statistics.KeyPriority}, hb.conf.GetWritePeerPriorities())
	re.Equal("v2", hb.conf.GetRankFormulaVersion())
	// upgrade from < 5.2
	config51 := `{"min-hot-byte-rate":100,"min-hot-key-rate":10,"min-hot-query-rate":10,"max-zombie-rounds":5,"max-peer-number":1000,"byte-rate-rank-step-ratio":0.05,"key-rate-rank-step-ratio":0.05,"query-rate-rank-step-ratio":0.05,"count-rank-step-ratio":0.01,"great-dec-ratio":0.95,"minor-dec-ratio":0.99,"src-tolerance-ratio":1.05,"dst-tolerance-ratio":1.05,"strict-picking-store":"true","enable-for-tiflash":"true"}`
	sche, err = schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte(config51)))
	re.NoError(err)
	hb = sche.(*hotScheduler)
	re.Equal([]string{statistics.BytePriority, statistics.KeyPriority}, hb.conf.GetReadPriorities())
	re.Equal([]string{statistics.KeyPriority, statistics.BytePriority}, hb.conf.GetWriteLeaderPriorities())
	re.Equal([]string{statistics.BytePriority, statistics.KeyPriority}, hb.conf.GetWritePeerPriorities())
	re.Equal("v1", hb.conf.GetRankFormulaVersion())
	// upgrade from < 6.4
	config54 := `{"min-hot-byte-rate":100,"min-hot-key-rate":10,"min-hot-query-rate":10,"max-zombie-rounds":5,"max-peer-number":1000,"byte-rate-rank-step-ratio":0.05,"key-rate-rank-step-ratio":0.05,"query-rate-rank-step-ratio":0.05,"count-rank-step-ratio":0.01,"great-dec-ratio":0.95,"minor-dec-ratio":0.99,"src-tolerance-ratio":1.05,"dst-tolerance-ratio":1.05,"read-priorities":["query","byte"],"write-leader-priorities":["query","byte"],"write-peer-priorities":["byte","key"],"strict-picking-store":"true","enable-for-tiflash":"true","forbid-rw-type":"none"}`
	sche, err = schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte(config54)))
	re.NoError(err)
	hb = sche.(*hotScheduler)
	re.Equal([]string{statistics.QueryPriority, statistics.BytePriority}, hb.conf.GetReadPriorities())
	re.Equal([]string{statistics.QueryPriority, statistics.BytePriority}, hb.conf.GetWriteLeaderPriorities())
	re.Equal([]string{statistics.BytePriority, statistics.KeyPriority}, hb.conf.GetWritePeerPriorities())
	re.Equal("v1", hb.conf.GetRankFormulaVersion())
}

func TestGCPendingOpInfos(t *testing.T) {
	re := require.New(t)
	checkGCPendingOpInfos(re, false /* disable placement rules */)
	checkGCPendingOpInfos(re, true /* enable placement rules */)
}

func checkGCPendingOpInfos(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	for id := uint64(1); id <= 10; id++ {
		tc.PutStoreWithLabels(id)
	}

	sche, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)

	notDoneOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		var op *operator.Operator
		var err error
		switch ty {
		case movePeer:
			op, err = operator.CreateMovePeerOperator("move-peer-test", tc, region, operator.OpAdmin, 2, &metapb.Peer{Id: region.GetID()*10000 + 1, StoreId: 4})
		case transferLeader:
			op, err = operator.CreateTransferLeaderOperator("transfer-leader-test", tc, region, 1, 2, []uint64{}, operator.OpAdmin)
		}
		re.NoError(err)
		re.NotNil(op)
		op.Start()
		op.SetStatusReachTime(operator.CREATED, time.Now().Add(-5*statistics.StoreHeartBeatReportInterval*time.Second))
		op.SetStatusReachTime(operator.STARTED, time.Now().Add((-5*statistics.StoreHeartBeatReportInterval+1)*time.Second))
		return newPendingInfluence(op, 2, 4, statistics.Influence{}, hb.conf.GetStoreStatZombieDuration())
	}
	justDoneOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		infl := notDoneOpInfluence(region, ty)
		infl.op.Cancel()
		return infl
	}
	shouldRemoveOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		infl := justDoneOpInfluence(region, ty)
		infl.op.SetStatusReachTime(operator.CANCELED, time.Now().Add(-3*statistics.StoreHeartBeatReportInterval*time.Second))
		return infl
	}
	opInfluenceCreators := [3]func(region *core.RegionInfo, ty opType) *pendingInfluence{shouldRemoveOpInfluence, notDoneOpInfluence, justDoneOpInfluence}

	typs := []opType{movePeer, transferLeader}

	for i, creator := range opInfluenceCreators {
		for j, typ := range typs {
			regionID := uint64(i*len(typs) + j + 1)
			region := newTestRegion(regionID)
			hb.regionPendings[regionID] = creator(region, typ)
		}
	}

	hb.summaryPendingInfluence(tc) // Calling this function will GC.

	for i := range opInfluenceCreators {
		for j, typ := range typs {
			regionID := uint64(i*len(typs) + j + 1)
			if i < 1 { // shouldRemoveOpInfluence
				re.NotContains(hb.regionPendings, regionID)
			} else { // notDoneOpInfluence, justDoneOpInfluence
				re.Contains(hb.regionPendings, regionID)
				kind := hb.regionPendings[regionID].op.Kind()
				switch typ {
				case transferLeader:
					re.True(kind&operator.OpLeader != 0)
					re.True(kind&operator.OpRegion == 0)
				case movePeer:
					re.True(kind&operator.OpLeader == 0)
					re.True(kind&operator.OpRegion != 0)
				}
			}
		}
	}
}

func newTestRegion(id uint64) *core.RegionInfo {
	peers := []*metapb.Peer{{Id: id*100 + 1, StoreId: 1}, {Id: id*100 + 2, StoreId: 2}, {Id: id*100 + 3, StoreId: 3}}
	return core.NewRegionInfo(&metapb.Region{Id: id, Peers: peers}, peers[0])
}

func TestHotWriteRegionScheduleByteRateOnly(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statistics.HistorySampleDuration = 0
	statisticsInterval = 0
	checkHotWriteRegionScheduleByteRateOnly(re, false /* disable placement rules */)
	checkHotWriteRegionScheduleByteRateOnly(re, true /* enable placement rules */)
}

func TestSplitBuckets(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	cancel, _, tc, oc := prepareSchedulersTest()
	tc.SetHotRegionCacheHitsThreshold(1)
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	solve := newBalanceSolver(hb.(*hotScheduler), tc, statistics.Read, transferLeader)
	region := core.NewTestRegionInfo(1, 1, []byte(""), []byte(""))

	// the hot range is [a,c],[e,f]
	b := &metapb.Buckets{
		RegionId:   1,
		PeriodInMs: 1000,
		Keys:       [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")},
		Stats: &metapb.BucketStats{
			ReadBytes:  []uint64{10 * units.KiB, 10 * units.KiB, 0, 10 * units.KiB, 10 * units.KiB},
			ReadKeys:   []uint64{256, 256, 0, 256, 256},
			ReadQps:    []uint64{0, 0, 0, 0, 0},
			WriteBytes: []uint64{0, 0, 0, 0, 0},
			WriteQps:   []uint64{0, 0, 0, 0, 0},
			WriteKeys:  []uint64{0, 0, 0, 0, 0},
		},
	}

	task := buckets.NewCheckPeerTask(b)
	re.True(tc.HotBucketCache.CheckAsync(task))
	time.Sleep(time.Millisecond * 10)
	ops := solve.createSplitOperator([]*core.RegionInfo{region})
	re.Equal(1, len(ops))
	op := ops[0]
	re.Equal(splitBucket, op.Desc())
	expectKeys := [][]byte{[]byte("a"), []byte("c"), []byte("d"), []byte("f")}
	expectOp, err := operator.CreateSplitRegionOperator(splitBucket, region, operator.OpSplit, pdpb.CheckPolicy_USEKEY, expectKeys)
	re.NoError(err)
	expectOp.GetCreateTime()
	re.Equal(expectOp.Brief(), op.Brief())
	re.Equal(expectOp.GetAdditionalInfo(), op.GetAdditionalInfo())
}

func checkHotWriteRegionScheduleByteRateOnly(re *require.Assertions, enablePlacementRules bool) {
	cancel, opt, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	tc.SetHotRegionCacheHitsThreshold(0)

	// Add stores 1, 2, 3, 4, 5, 6  with region counts 3, 2, 2, 2, 0, 0.
	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.SetStoreDown(7)

	// | store_id | write_bytes_rate |
	// |----------|------------------|
	// |    1     |       7.5MB      |
	// |    2     |       4.5MB      |
	// |    3     |       4.5MB      |
	// |    4     |        6MB       |
	// |    5     |        0MB       |
	// |    6     |        0MB       |
	tc.UpdateStorageWrittenBytes(1, 7.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	// | region_id | leader_store | follower_store | follower_store | written_bytes |
	// |-----------|--------------|----------------|----------------|---------------|
	// |     1     |       1      |        2       |       3        |      512KB    |
	// |     2     |       1      |        3       |       4        |      512KB    |
	// |     3     |       1      |        2       |       4        |      512KB    |
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{1, 3, 4}, 512 * units.KiB, 0, 0},
		{3, []uint64{1, 2, 4}, 512 * units.KiB, 0, 0},
	})
	ops, _ := hb.Schedule(tc, false)
	re.NotEmpty(ops)
	clearPendingInfluence(hb.(*hotScheduler))

	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is larger than other stores.
	for i := 0; i < 20; i++ {
		ops, _ = hb.Schedule(tc, false)
		op := ops[0]
		clearPendingInfluence(hb.(*hotScheduler))
		switch op.Len() {
		case 1:
			// balance by leader selected
			operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
		case 4:
			// balance by peer selected
			if op.RegionID() == 2 {
				// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
				operatorutil.CheckTransferPeerWithLeaderTransferFrom(re, op, operator.OpHotRegion, 1)
			} else {
				// peer in store 1 of the region 1,3 can only transfer to store 6
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 6)
			}
		default:
			re.FailNow("wrong op: " + op.String())
		}
	}

	// hot region scheduler is restricted by `hot-region-schedule-limit`.
	tc.SetHotRegionScheduleLimit(0)
	re.False(hb.IsScheduleAllowed(tc))
	clearPendingInfluence(hb.(*hotScheduler))
	tc.SetHotRegionScheduleLimit(int(opt.GetHotRegionScheduleLimit()))

	for i := 0; i < 20; i++ {
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		clearPendingInfluence(hb.(*hotScheduler))
		re.Equal(4, op.Len())
		if op.RegionID() == 2 {
			// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
			operatorutil.CheckTransferPeerWithLeaderTransferFrom(re, op, operator.OpHotRegion, 1)
		} else {
			// peer in store 1 of the region 1,3 can only transfer to store 6
			operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 6)
		}
	}

	// hot region scheduler is not affect by `balance-region-schedule-limit`.
	tc.SetRegionScheduleLimit(0)
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	clearPendingInfluence(hb.(*hotScheduler))
	// Always produce operator
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	clearPendingInfluence(hb.(*hotScheduler))
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	clearPendingInfluence(hb.(*hotScheduler))

	// | store_id | write_bytes_rate |
	// |----------|------------------|
	// |    1     |        6MB       |
	// |    2     |        5MB       |
	// |    3     |        6MB       |
	// |    4     |        3.1MB     |
	// |    5     |        0MB       |
	// |    6     |        3MB       |
	tc.UpdateStorageWrittenBytes(1, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3.1*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 3*units.MiB*statistics.StoreHeartBeatReportInterval)

	// | region_id | leader_store | follower_store | follower_store | written_bytes |
	// |-----------|--------------|----------------|----------------|---------------|
	// |     1     |       1      |        2       |       3        |      512KB    |
	// |     2     |       1      |        2       |       3        |      512KB    |
	// |     3     |       6      |        1       |       4        |      512KB    |
	// |     4     |       5      |        6       |       4        |      512KB    |
	// |     5     |       3      |        4       |       5        |      512KB    |
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{3, []uint64{6, 1, 4}, 512 * units.KiB, 0, 0},
		{4, []uint64{5, 6, 4}, 512 * units.KiB, 0, 0},
		{5, []uint64{3, 4, 5}, 512 * units.KiB, 0, 0},
	})

	// 6 possible operator.
	// Assuming different operators have the same possibility,
	// if code has bug, at most 6/7 possibility to success,
	// test 30 times, possibility of success < 0.1%.
	// Cannot transfer leader because store 2 and store 3 are hot.
	// Source store is 1 or 3.
	//   Region 1 and 2 are the same, cannot move peer to store 5 due to the label.
	//   Region 3 can only move peer to store 5.
	//   Region 5 can only move peer to store 6.
	for i := 0; i < 30; i++ {
		ops, _ = hb.Schedule(tc, false)
		op := ops[0]
		clearPendingInfluence(hb.(*hotScheduler))
		switch op.RegionID() {
		case 1, 2:
			if op.Len() == 3 {
				operatorutil.CheckTransferPeer(re, op, operator.OpHotRegion, 3, 6)
			} else if op.Len() == 4 {
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 6)
			} else {
				re.FailNow("wrong operator: " + op.String())
			}
		case 3:
			operatorutil.CheckTransferPeer(re, op, operator.OpHotRegion, 1, 5)
		case 5:
			operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 3, 6)
		default:
			re.FailNow("wrong operator: " + op.String())
		}
	}

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		r := tc.GetRegion(i)
		tc.RemoveRegion(r)
		tc.RemoveRegionFromSubTree(r)
	}
	hb.Schedule(tc, false)
	clearPendingInfluence(hb.(*hotScheduler))
}

func TestHotWriteRegionScheduleByteRateOnlyWithTiFlash(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	re.NoError(tc.RuleManager.SetRules([]*placement.Rule{
		{
			GroupID:        "pd",
			ID:             "default",
			Role:           placement.Voter,
			Count:          3,
			LocationLabels: []string{"zone", "host"},
		},
		{
			GroupID:        "tiflash",
			ID:             "tiflash",
			Role:           placement.Learner,
			Count:          1,
			LocationLabels: []string{"zone", "host"},
			LabelConstraints: []placement.LabelConstraint{
				{
					Key:    core.EngineKey,
					Op:     placement.In,
					Values: []string{core.EngineTiFlash},
				},
			},
		},
	}))
	sche, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)

	// Add TiKV stores 1, 2, 3, 4, 5, 6, 7 (Down) with region counts 3, 3, 2, 2, 0, 0, 0.
	// Add TiFlash stores 8, 9, 10, 11 with region counts 3, 1, 1, 0.
	storeCount := uint64(11)
	aliveTiKVStartID := uint64(1)
	aliveTiKVLastID := uint64(6)
	aliveTiFlashStartID := uint64(8)
	aliveTiFlashLastID := uint64(11)
	downStoreID := uint64(7)
	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 3, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.AddLabelsStore(8, 3, map[string]string{"zone": "z1", "host": "h8", "engine": "tiflash"})
	tc.AddLabelsStore(9, 1, map[string]string{"zone": "z2", "host": "h9", "engine": "tiflash"})
	tc.AddLabelsStore(10, 1, map[string]string{"zone": "z5", "host": "h10", "engine": "tiflash"})
	tc.AddLabelsStore(11, 0, map[string]string{"zone": "z3", "host": "h11", "engine": "tiflash"})
	tc.SetStoreDown(downStoreID)
	for i := uint64(1); i <= storeCount; i++ {
		if i != downStoreID {
			tc.UpdateStorageWrittenBytes(i, 0)
		}
	}
	// | region_id | leader_store | follower_store | follower_store | learner_store | written_bytes |
	// |-----------|--------------|----------------|----------------|---------------|---------------|
	// |     1     |       1      |        2       |       3        |       8       |      512 KB   |
	// |     2     |       1      |        3       |       4        |       8       |      512 KB   |
	// |     3     |       1      |        2       |       4        |       9       |      512 KB   |
	// |     4     |       2      |                |                |      10       |      100 B    |
	// Region 1, 2 and 3 are hot regions.
	testRegions := []testRegionInfo{
		{1, []uint64{1, 2, 3, 8}, 512 * units.KiB, 5 * units.KiB, 3000},
		{2, []uint64{1, 3, 4, 8}, 512 * units.KiB, 5 * units.KiB, 3000},
		{3, []uint64{1, 2, 4, 9}, 512 * units.KiB, 5 * units.KiB, 3000},
		{4, []uint64{2, 10}, 100, 1, 1},
	}
	addRegionInfo(tc, statistics.Write, testRegions)
	regionBytesSum := 0.0
	regionKeysSum := 0.0
	regionQuerySum := 0.0
	hotRegionBytesSum := 0.0
	hotRegionKeysSum := 0.0
	hotRegionQuerySum := 0.0
	for _, r := range testRegions {
		regionBytesSum += r.byteRate
		regionKeysSum += r.keyRate
		regionQuerySum += r.queryRate
	}
	for _, r := range testRegions[0:3] {
		hotRegionBytesSum += r.byteRate
		hotRegionKeysSum += r.keyRate
		hotRegionQuerySum += r.queryRate
	}
	// Will transfer a hot learner from store 8, because the total count of peers
	// which is hot for store 8 is larger than other TiFlash stores.
	for i := 0; i < 20; i++ {
		clearPendingInfluence(hb)
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		switch op.Len() {
		case 1:
			// balance by leader selected
			operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
		case 2:
			// balance by peer selected
			operatorutil.CheckTransferLearner(re, op, operator.OpHotRegion, 8, 10)
		default:
			re.FailNow("wrong op: " + op.String())
		}
	}
	// Disable for TiFlash
	hb.conf.SetEnableForTiFlash(false)
	for i := 0; i < 20; i++ {
		clearPendingInfluence(hb)
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
	}
	// | store_id | write_bytes_rate |
	// |----------|------------------|
	// |    1     |       7.5MB      |
	// |    2     |       4.5MB      |
	// |    3     |       4.5MB      |
	// |    4     |        6MB       |
	// |    5     |        0MB(Evict)|
	// |    6     |        0MB       |
	// |    7     |        n/a (Down)|
	// |    8     |        n/a       | <- TiFlash is always 0.
	// |    9     |        n/a       |
	// |   10     |        n/a       |
	// |   11     |        n/a       |
	storesBytes := map[uint64]uint64{
		1: 7.5 * units.MiB * statistics.StoreHeartBeatReportInterval,
		2: 4.5 * units.MiB * statistics.StoreHeartBeatReportInterval,
		3: 4.5 * units.MiB * statistics.StoreHeartBeatReportInterval,
		4: 6 * units.MiB * statistics.StoreHeartBeatReportInterval,
	}
	tc.SetStoreEvictLeader(5, true)
	tikvBytesSum, tikvKeysSum, tikvQuerySum := 0.0, 0.0, 0.0
	for i := aliveTiKVStartID; i <= aliveTiKVLastID; i++ {
		tikvBytesSum += float64(storesBytes[i]) / 10
		tikvKeysSum += float64(storesBytes[i]/100) / 10
		tikvQuerySum += float64(storesBytes[i]/100) / 10
	}

	for i := uint64(1); i <= storeCount; i++ {
		if i != downStoreID {
			tc.UpdateStorageWrittenBytes(i, storesBytes[i])
		}
	}

	{ // Check the load expect
		aliveTiKVCount := float64(aliveTiKVLastID - aliveTiKVStartID + 1)
		allowLeaderTiKVCount := aliveTiKVCount - 1 // store 5 with evict leader
		aliveTiFlashCount := float64(aliveTiFlashLastID - aliveTiFlashStartID + 1)
		tc.ObserveRegionsStats()
		ops, _ := hb.Schedule(tc, false)
		re.NotEmpty(ops)
		re.True(
			loadsEqual(
				hb.stLoadInfos[writeLeader][1].LoadPred.Expect.Loads,
				[]float64{hotRegionBytesSum / allowLeaderTiKVCount, hotRegionKeysSum / allowLeaderTiKVCount, tikvQuerySum / allowLeaderTiKVCount}))
		re.True(tikvQuerySum != hotRegionQuerySum)
		re.True(
			loadsEqual(
				hb.stLoadInfos[writePeer][1].LoadPred.Expect.Loads,
				[]float64{tikvBytesSum / aliveTiKVCount, tikvKeysSum / aliveTiKVCount, 0}))
		re.True(
			loadsEqual(
				hb.stLoadInfos[writePeer][8].LoadPred.Expect.Loads,
				[]float64{regionBytesSum / aliveTiFlashCount, regionKeysSum / aliveTiFlashCount, 0}))
		// check IsTraceRegionFlow == false
		pdServerCfg := tc.GetPDServerConfig()
		pdServerCfg.FlowRoundByDigit = 8
		tc.SetPDServerConfig(pdServerCfg)
		clearPendingInfluence(hb)
		ops, _ = hb.Schedule(tc, false)
		re.NotEmpty(ops)
		re.True(
			loadsEqual(
				hb.stLoadInfos[writePeer][8].LoadPred.Expect.Loads,
				[]float64{hotRegionBytesSum / aliveTiFlashCount, hotRegionKeysSum / aliveTiFlashCount, 0}))
		// revert
		pdServerCfg.FlowRoundByDigit = 3
		tc.SetPDServerConfig(pdServerCfg)
	}
	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is larger than other stores.
	for i := 0; i < 20; i++ {
		clearPendingInfluence(hb)
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		switch op.Len() {
		case 1:
			// balance by leader selected
			operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
		case 4:
			// balance by peer selected
			if op.RegionID() == 2 {
				// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
				operatorutil.CheckTransferPeerWithLeaderTransferFrom(re, op, operator.OpHotRegion, 1)
			} else {
				// peer in store 1 of the region 1,3 can only transfer to store 6
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 6)
			}
		default:
			re.FailNow("wrong op: " + op.String())
		}
	}
}

func TestHotWriteRegionScheduleWithQuery(t *testing.T) {
	re := require.New(t)
	// TODO: add schedulePeerPr,Denoising,statisticsInterval to prepare function
	originValue := schedulePeerPr
	defer func() {
		schedulePeerPr = originValue
	}()
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	statistics.Denoising = false
	statisticsInterval = 0

	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.QueryPriority, statistics.BytePriority}

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWriteQuery(1, 11000*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWriteQuery(2, 10000*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWriteQuery(3, 9000*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 500, 0, 500},
		{2, []uint64{1, 2, 3}, 500, 0, 500},
		{3, []uint64{2, 1, 3}, 500, 0, 500},
	})
	schedulePeerPr = 0.0
	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 1, 3)
	}
}

func TestHotWriteRegionScheduleWithKeyRate(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 9.8*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*units.MiB*statistics.StoreHeartBeatReportInterval, 9*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 8.9*units.MiB*statistics.StoreHeartBeatReportInterval, 9.2*units.MiB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{2, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{3, []uint64{2, 4, 3}, 0.05 * units.MiB, 0.1 * units.MiB, 0},
	})

	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		// byteDecRatio <= 0.95 && keyDecRatio <= 0.95
		operatorutil.CheckTransferPeer(re, op, operator.OpHotRegion, 1, 4)
		// store byte rate (min, max): (10, 10.5) | 9.5 | 9.5 | (9, 9.5) | 8.9
		// store key rate (min, max):  (10, 10.5) | 9.5 | 9.8 | (9, 9.5) | 9.2

		ops, _ = hb.Schedule(tc, false)
		op = ops[0]
		// byteDecRatio <= 0.99 && keyDecRatio <= 0.95
		operatorutil.CheckTransferPeer(re, op, operator.OpHotRegion, 3, 5)
		// store byte rate (min, max): (10, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 8.95)
		// store key rate (min, max):  (10, 10.5) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.3)

		// byteDecRatio <= 0.95
		// op = hb.Schedule(tc, false)[0]
		// FIXME: cover this case
		// operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 5)
		// store byte rate (min, max): (9.5, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 9.45)
		// store key rate (min, max):  (9.2, 10.2) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.8)
	}
}

func TestHotWriteRegionScheduleUnhealthyStore(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 0*units.MiB*statistics.StoreHeartBeatReportInterval, 0*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{2, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{3, []uint64{3, 2, 1}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
	})

	intervals := []time.Duration{
		9 * time.Second,
		10 * time.Second,
		19 * time.Second,
		20 * time.Second,
		9 * time.Minute,
		10 * time.Minute,
		29 * time.Minute,
		30 * time.Minute,
	}
	// test dst
	for _, interval := range intervals {
		tc.SetStoreLastHeartbeatInterval(4, interval)
		clearPendingInfluence(hb.(*hotScheduler))
		hb.Schedule(tc, false)
		// no panic
	}
}

func TestHotWriteRegionScheduleCheckHot(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 90, 0.5 * units.MiB, 0},              // no hot
		{1, []uint64{2, 1, 3}, 90, 0.5 * units.MiB, 0},              // no hot
		{2, []uint64{3, 2, 1}, 0.5 * units.MiB, 0.5 * units.MiB, 0}, // byteDecRatio is greater than greatDecRatio
	})

	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
}

func TestHotWriteRegionScheduleWithLeader(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	re.NoError(err)

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWrittenBytes(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*units.MiB*statistics.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*units.MiB*statistics.StoreHeartBeatReportInterval)

	// store1 has 2 peer as leader
	// store2 has 3 peer as leader
	// store3 has 2 peer as leader
	// If transfer leader from store2 to store1 or store3, it will keep on looping, which introduces a lot of unnecessary scheduling
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{2, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{3, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{4, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{5, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{6, []uint64{3, 1, 2}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{7, []uint64{3, 1, 2}, 0.5 * units.MiB, 1 * units.MiB, 0},
	})

	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		re.Empty(ops)
	}

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{8, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
	})

	// store1 has 2 peer as leader
	// store2 has 4 peer as leader
	// store3 has 2 peer as leader
	// We expect to transfer leader from store2 to store1 or store3
	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 2)
		ops, _ = hb.Schedule(tc, false)
		re.Empty(ops)
	}
}

func TestHotWriteRegionScheduleWithPendingInfluence(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0
	checkHotWriteRegionScheduleWithPendingInfluence(re, 0) // 0: byte rate
	checkHotWriteRegionScheduleWithPendingInfluence(re, 1) // 1: key rate
}

func checkHotWriteRegionScheduleWithPendingInfluence(re *require.Assertions, dim int) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	old := pendingAmpFactor
	pendingAmpFactor = 0.0
	defer func() {
		pendingAmpFactor = old
	}()

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	updateStore := tc.UpdateStorageWrittenBytes // byte rate
	if dim == 1 {                               // key rate
		updateStore = tc.UpdateStorageWrittenKeys
	}
	updateStore(1, 8*units.MiB*statistics.StoreHeartBeatReportInterval)
	updateStore(2, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	updateStore(3, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	updateStore(4, 4*units.MiB*statistics.StoreHeartBeatReportInterval)

	if dim == 0 { // byte rate
		addRegionInfo(tc, statistics.Write, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{2, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{3, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{4, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{5, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{6, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		})
	} else if dim == 1 { // key rate
		addRegionInfo(tc, statistics.Write, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{2, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{3, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{4, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{5, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{6, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
		})
	}

	for i := 0; i < 20; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		cnt := 0
	testLoop:
		for j := 0; j < 1000; j++ {
			re.LessOrEqual(cnt, 5)
			emptyCnt := 0
			ops, _ := hb.Schedule(tc, false)
			for len(ops) == 0 {
				emptyCnt++
				if emptyCnt >= 10 {
					break testLoop
				}
				ops, _ = hb.Schedule(tc, false)
			}
			op := ops[0]
			switch op.Len() {
			case 1:
				// balance by leader selected
				operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
			case 4:
				// balance by peer selected
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 4)
				cnt++
				if cnt == 3 {
					re.True(op.Cancel())
				}
			default:
				re.FailNow("wrong op: " + op.String())
			}
		}
		re.Equal(4, cnt)
	}
}

func TestHotWriteRegionScheduleWithRuleEnabled(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetEnablePlacementRules(true)
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.KeyPriority, statistics.BytePriority}

	tc.SetHotRegionCacheHitsThreshold(0)
	key, err := hex.DecodeString("")
	re.NoError(err)
	// skip stddev check
	origin := stddevThreshold
	stddevThreshold = -1.0
	defer func() {
		stddevThreshold = origin
	}()

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	err = tc.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "leader",
		Index:    1,
		Override: true,
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "ID",
				Op:     placement.In,
				Values: []string{"2", "1"},
			},
		},
		StartKey: key,
		EndKey:   key,
	})
	re.NoError(err)
	err = tc.SetRule(&placement.Rule{
		GroupID:  "pd",
		ID:       "voter",
		Index:    2,
		Override: false,
		Role:     placement.Voter,
		Count:    2,
		StartKey: key,
		EndKey:   key,
	})
	re.NoError(err)

	tc.UpdateStorageWrittenBytes(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*units.MiB*statistics.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*units.MiB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{2, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{3, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{4, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{5, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{6, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{7, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
	})

	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		// The targetID should always be 1 as leader is only allowed to be placed in store1 or store2 by placement rule
		operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 2, 1)
		ops, _ = hb.Schedule(tc, false)
		re.Empty(ops)
	}
}

func TestHotReadRegionScheduleByteRateOnly(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	scheduler, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := scheduler.(*hotScheduler)
	hb.conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	tc.SetHotRegionCacheHitsThreshold(0)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 2)
	tc.AddRegionStore(5, 0)

	// | store_id | read_bytes_rate |
	// |----------|-----------------|
	// |    1     |     7.5MB       |
	// |    2     |     4.9MB       |
	// |    3     |     3.7MB       |
	// |    4     |       6MB       |
	// |    5     |       0MB       |
	tc.UpdateStorageReadBytes(1, 7.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.9*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 3.7*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 0)

	// | region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	// |-----------|--------------|----------------|----------------|--------------------|
	// |     1     |       1      |        2       |       3        |        512KB       |
	// |     2     |       2      |        1       |       3        |        511KB       |
	// |     3     |       1      |        2       |       3        |        510KB       |
	// |     11    |       1      |        2       |       3        |          7KB       |
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{2, 1, 3}, 511 * units.KiB, 0, 0},
		{3, []uint64{1, 2, 3}, 510 * units.KiB, 0, 0},
		{11, []uint64{1, 2, 3}, 7 * units.KiB, 0, 0},
	})

	re.True(tc.IsRegionHot(tc.GetRegion(1)))
	re.False(tc.IsRegionHot(tc.GetRegion(11)))
	// check randomly pick hot region
	r := tc.HotRegionsFromStore(2, statistics.Read)
	re.Len(r, 3)
	// check hot items
	stats := tc.HotCache.RegionStats(statistics.Read, 0)
	re.Len(stats, 3)
	for _, ss := range stats {
		for _, s := range ss {
			re.Less(500.0*units.KiB, s.GetLoad(statistics.ByteDim))
		}
	}

	ops, _ := hb.Schedule(tc, false)
	op := ops[0]

	// move leader from store 1 to store 5
	// it is better than transfer leader from store 1 to store 3
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 5)
	re.Contains(hb.regionPendings, uint64(1))
	re.True(typeutil.Float64Equal(512.0*units.KiB, hb.regionPendings[1].origin.Loads[statistics.RegionReadBytes]))
	clearPendingInfluence(hb)

	// assume handle the transfer leader operator rather than move leader
	tc.AddRegionWithReadInfo(3, 3, 512*units.KiB*statistics.ReadReportInterval, 0, 0, statistics.ReadReportInterval, []uint64{1, 2})
	// After transfer a hot region leader from store 1 to store 3
	// the three region leader will be evenly distributed in three stores

	// | store_id | read_bytes_rate |
	// |----------|-----------------|
	// |    1     |       6MB       |
	// |    2     |       5.5MB     |
	// |    3     |       5.5MB     |
	// |    4     |       3.4MB     |
	// |    5     |       3MB       |
	tc.UpdateStorageReadBytes(1, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 5.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 5.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 3.4*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 3*units.MiB*statistics.StoreHeartBeatReportInterval)

	// | region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	// |-----------|--------------|----------------|----------------|--------------------|
	// |     1     |       1      |        2       |       3        |        512KB       |
	// |     2     |       2      |        1       |       3        |        511KB       |
	// |     3     |       3      |        2       |       1        |        510KB       |
	// |     4     |       1      |        2       |       3        |        509KB       |
	// |     5     |       4      |        2       |       5        |        508KB       |
	// |     11    |       1      |        2       |       3        |          7KB       |
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 509 * units.KiB, 0, 0},
		{5, []uint64{4, 2, 5}, 508 * units.KiB, 0, 0},
	})

	// We will move leader peer of region 1 from 1 to 5
	ops, _ = hb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion|operator.OpLeader, 1, 5)
	re.Contains(hb.regionPendings, uint64(1))
	re.True(typeutil.Float64Equal(512.0*units.KiB, hb.regionPendings[1].origin.Loads[statistics.RegionReadBytes]))
	clearPendingInfluence(hb)

	// Should not panic if region not found.
	for i := uint64(1); i <= 3; i++ {
		r := tc.GetRegion(i)
		tc.RemoveRegion(r)
		tc.RemoveRegionFromSubTree(r)
	}
	hb.updateReadTime = time.Now().Add(-time.Second)
	hb.Schedule(tc, false)
	re.Contains(hb.regionPendings, uint64(4))
	re.True(typeutil.Float64Equal(509.0*units.KiB, hb.regionPendings[4].origin.Loads[statistics.RegionReadBytes]))
	clearPendingInfluence(hb)
}

func TestHotReadRegionScheduleWithQuery(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageReadQuery(1, 10500*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(2, 10000*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(3, 9000*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0, 0, 500},
		{2, []uint64{2, 1, 3}, 0, 0, 500},
	})

	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 1, 3)
	}
}

func TestHotReadRegionScheduleWithKeyRate(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageReadStats(1, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval, 10.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 9.8*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 9*units.MiB*statistics.StoreHeartBeatReportInterval, 9*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(5, 8.9*units.MiB*statistics.StoreHeartBeatReportInterval, 9.2*units.MiB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 4}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{2, []uint64{1, 2, 4}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{3, []uint64{3, 4, 5}, 0.05 * units.MiB, 0.1 * units.MiB, 0},
	})

	for i := 0; i < 100; i++ {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		// byteDecRatio <= 0.95 && keyDecRatio <= 0.95
		operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 1, 4)
		// store byte rate (min, max): (10, 10.5) | 9.5 | 9.5 | (9, 9.5) | 8.9
		// store key rate (min, max):  (10, 10.5) | 9.5 | 9.8 | (9, 9.5) | 9.2

		ops, _ = hb.Schedule(tc, false)
		op = ops[0]
		// byteDecRatio <= 0.99 && keyDecRatio <= 0.95
		operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 3, 5)
		// store byte rate (min, max): (10, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 8.95)
		// store key rate (min, max):  (10, 10.5) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.3)

		// byteDecRatio <= 0.95
		// FIXME: cover this case
		// op = hb.Schedule(tc, false)[0]
		// operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 5)
		// store byte rate (min, max): (9.5, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 9.45)
		// store key rate (min, max):  (9.2, 10.2) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.8)
	}
}

func TestHotReadRegionScheduleWithPendingInfluence(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0
	checkHotReadRegionScheduleWithPendingInfluence(re, 0) // 0: byte rate
	checkHotReadRegionScheduleWithPendingInfluence(re, 1) // 1: key rate
}

func checkHotReadRegionScheduleWithPendingInfluence(re *require.Assertions, dim int) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	// For test
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.GreatDecRatio = 0.99
	hb.(*hotScheduler).conf.MinorDecRatio = 1
	hb.(*hotScheduler).conf.DstToleranceRatio = 1
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	old := pendingAmpFactor
	pendingAmpFactor = 0.0
	defer func() {
		pendingAmpFactor = old
	}()

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	updateStore := tc.UpdateStorageReadBytes // byte rate
	if dim == 1 {                            // key rate
		updateStore = tc.UpdateStorageReadKeys
	}
	updateStore(1, 7.1*units.MiB*statistics.StoreHeartBeatReportInterval)
	updateStore(2, 6.1*units.MiB*statistics.StoreHeartBeatReportInterval)
	updateStore(3, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	updateStore(4, 5*units.MiB*statistics.StoreHeartBeatReportInterval)

	if dim == 0 { // byte rate
		addRegionInfo(tc, statistics.Read, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{2, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{3, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{4, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{5, []uint64{2, 1, 3}, 512 * units.KiB, 0, 0},
			{6, []uint64{2, 1, 3}, 512 * units.KiB, 0, 0},
			{7, []uint64{3, 2, 1}, 512 * units.KiB, 0, 0},
			{8, []uint64{3, 2, 1}, 512 * units.KiB, 0, 0},
		})
	} else if dim == 1 { // key rate
		addRegionInfo(tc, statistics.Read, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{2, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{3, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{4, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{5, []uint64{2, 1, 3}, 0, 512 * units.KiB, 0},
			{6, []uint64{2, 1, 3}, 0, 512 * units.KiB, 0},
			{7, []uint64{3, 2, 1}, 0, 512 * units.KiB, 0},
			{8, []uint64{3, 2, 1}, 0, 512 * units.KiB, 0},
		})
	}

	// Before schedule, store byte/key rate: 7.1 | 6.1 | 6 | 5
	// Min and max from storeLoadPred. They will be generated in the comparison of current and future.
	for j := 0; j < 20; j++ {
		clearPendingInfluence(hb.(*hotScheduler))

		ops, _ := hb.Schedule(tc, false)
		op1 := ops[0]
		operatorutil.CheckTransferPeer(re, op1, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

		pendingAmpFactor = old
		ops, _ = hb.Schedule(tc, false)
		re.Empty(ops)
		pendingAmpFactor = 0.0

		ops, _ = hb.Schedule(tc, false)
		op2 := ops[0]
		operatorutil.CheckTransferPeer(re, op2, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)

		ops, _ = hb.Schedule(tc, false)
		re.Empty(ops)
	}

	// Before schedule, store byte/key rate: 7.1 | 6.1 | 6 | 5
	for j := 0; j < 20; j++ {
		clearPendingInfluence(hb.(*hotScheduler))

		ops, _ := hb.Schedule(tc, false)
		op1 := ops[0]
		operatorutil.CheckTransferPeer(re, op1, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

		ops, _ = hb.Schedule(tc, false)
		op2 := ops[0]
		operatorutil.CheckTransferPeer(re, op2, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)
		re.True(op2.Cancel())

		ops, _ = hb.Schedule(tc, false)
		op2 = ops[0]
		operatorutil.CheckTransferPeer(re, op2, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

		re.True(op1.Cancel())
		// store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

		ops, _ = hb.Schedule(tc, false)
		op3 := ops[0]
		operatorutil.CheckTransferPeer(re, op3, operator.OpHotRegion, 1, 4)
		// store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)

		ops, _ = hb.Schedule(tc, false)
		re.Empty(ops)
	}
}

func TestHotReadWithEvictLeaderScheduler(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetStrictPickingStore(false)
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	tc.AddRegionStore(6, 20)

	// no uniform among four stores
	tc.UpdateStorageReadStats(1, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 0.0*units.MB*statistics.StoreHeartBeatReportInterval, 0.0*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 5, 6}, 0.05 * units.MB, 0.05 * units.MB, 0},
	})
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	clearPendingInfluence(hb.(*hotScheduler))
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, ops[0], operator.OpHotRegion|operator.OpLeader, 1, 4)
	// two dim are both enough uniform among three stores
	tc.SetStoreEvictLeader(4, true)
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 0)
	clearPendingInfluence(hb.(*hotScheduler))
}

func TestHotCacheUpdateCache(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.SetHotRegionCacheHitsThreshold(0)

	// For read flow
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{2, 1, 3}, 512 * units.KiB, 0, 0},
		{3, []uint64{1, 2, 3}, 20 * units.KiB, 0, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{11, []uint64{1, 2, 3}, 7 * units.KiB, 0, 0},
	})
	stats := tc.RegionStats(statistics.Read, 0)
	re.Len(stats[1], 3)
	re.Len(stats[2], 3)
	re.Len(stats[3], 3)

	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{3, []uint64{2, 1, 3}, 20 * units.KiB, 0, 0},
		{11, []uint64{1, 2, 3}, 7 * units.KiB, 0, 0},
	})
	stats = tc.RegionStats(statistics.Read, 0)
	re.Len(stats[1], 3)
	re.Len(stats[2], 3)
	re.Len(stats[3], 3)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{5, []uint64{1, 2, 3}, 20 * units.KiB, 0, 0},
		{6, []uint64{1, 2, 3}, 0.8 * units.KiB, 0, 0},
	})
	stats = tc.RegionStats(statistics.Write, 0)
	re.Len(stats[1], 2)
	re.Len(stats[2], 2)
	re.Len(stats[3], 2)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{5, []uint64{1, 2, 5}, 20 * units.KiB, 0, 0},
	})
	stats = tc.RegionStats(statistics.Write, 0)

	re.Len(stats[1], 2)
	re.Len(stats[2], 2)
	re.Len(stats[3], 1)
	re.Len(stats[5], 1)

	// For leader read flow
	addRegionLeaderReadInfo(tc, []testRegionInfo{
		{21, []uint64{4, 5, 6}, 512 * units.KiB, 0, 0},
		{22, []uint64{5, 4, 6}, 512 * units.KiB, 0, 0},
		{23, []uint64{4, 5, 6}, 20 * units.KiB, 0, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{31, []uint64{4, 5, 6}, 7 * units.KiB, 0, 0},
	})
	stats = tc.RegionStats(statistics.Read, 0)
	re.Len(stats[4], 2)
	re.Len(stats[5], 1)
	re.Empty(stats[6])
}

func TestHotCacheKeyThresholds(t *testing.T) {
	re := require.New(t)
	statistics.ThresholdsUpdateInterval = 0
	defer func() {
		statistics.ThresholdsUpdateInterval = 8 * time.Second
	}()
	{ // only a few regions
		cancel, _, tc, _ := prepareSchedulersTest()
		defer cancel()
		tc.SetHotRegionCacheHitsThreshold(0)
		addRegionInfo(tc, statistics.Read, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 1, 0},
			{2, []uint64{1, 2, 3}, 0, 1 * units.KiB, 0},
		})
		stats := tc.RegionStats(statistics.Read, 0)
		re.Len(stats[1], 1)
		addRegionInfo(tc, statistics.Write, []testRegionInfo{
			{3, []uint64{4, 5, 6}, 0, 1, 0},
			{4, []uint64{4, 5, 6}, 0, 1 * units.KiB, 0},
		})
		stats = tc.RegionStats(statistics.Write, 0)
		re.Len(stats[4], 1)
		re.Len(stats[5], 1)
		re.Len(stats[6], 1)
	}
	{ // many regions
		cancel, _, tc, _ := prepareSchedulersTest()
		defer cancel()
		regions := []testRegionInfo{}
		for i := 1; i <= 1000; i += 2 {
			regions = append(regions,
				testRegionInfo{
					id:      uint64(i),
					peers:   []uint64{1, 2, 3},
					keyRate: 100 * units.KiB,
				},
				testRegionInfo{
					id:      uint64(i + 1),
					peers:   []uint64{1, 2, 3},
					keyRate: 10 * units.KiB,
				},
			)
		}

		{ // read
			addRegionInfo(tc, statistics.Read, regions)
			stats := tc.RegionStats(statistics.Read, 0)
			re.Greater(len(stats[1]), 500)

			// for AntiCount
			addRegionInfo(tc, statistics.Read, regions)
			addRegionInfo(tc, statistics.Read, regions)
			addRegionInfo(tc, statistics.Read, regions)
			addRegionInfo(tc, statistics.Read, regions)
			stats = tc.RegionStats(statistics.Read, 0)
			re.Len(stats[1], 500)
		}
		{ // write
			addRegionInfo(tc, statistics.Write, regions)
			stats := tc.RegionStats(statistics.Write, 0)
			re.Greater(len(stats[1]), 500)
			re.Greater(len(stats[2]), 500)
			re.Greater(len(stats[3]), 500)

			// for AntiCount
			addRegionInfo(tc, statistics.Write, regions)
			addRegionInfo(tc, statistics.Write, regions)
			addRegionInfo(tc, statistics.Write, regions)
			addRegionInfo(tc, statistics.Write, regions)
			stats = tc.RegionStats(statistics.Write, 0)
			re.Len(stats[1], 500)
			re.Len(stats[2], 500)
			re.Len(stats[3], 500)
		}
	}
}

func TestHotCacheByteAndKey(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.SetHotRegionCacheHitsThreshold(0)
	statistics.ThresholdsUpdateInterval = 0
	defer func() {
		statistics.ThresholdsUpdateInterval = 8 * time.Second
	}()
	regions := []testRegionInfo{}
	for i := 1; i <= 500; i++ {
		regions = append(regions, testRegionInfo{
			id:       uint64(i),
			peers:    []uint64{1, 2, 3},
			byteRate: 100 * units.KiB,
			keyRate:  100 * units.KiB,
		})
	}
	{ // read
		addRegionInfo(tc, statistics.Read, regions)
		stats := tc.RegionStats(statistics.Read, 0)
		re.Len(stats[1], 500)

		addRegionInfo(tc, statistics.Read, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * units.KiB, 10 * units.KiB, 0},
			{10002, []uint64{1, 2, 3}, 500 * units.KiB, 10 * units.KiB, 0},
			{10003, []uint64{1, 2, 3}, 10 * units.KiB, 500 * units.KiB, 0},
			{10004, []uint64{1, 2, 3}, 500 * units.KiB, 500 * units.KiB, 0},
		})
		stats = tc.RegionStats(statistics.Read, 0)
		re.Len(stats[1], 503)
	}
	{ // write
		addRegionInfo(tc, statistics.Write, regions)
		stats := tc.RegionStats(statistics.Write, 0)
		re.Len(stats[1], 500)
		re.Len(stats[2], 500)
		re.Len(stats[3], 500)
		addRegionInfo(tc, statistics.Write, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * units.KiB, 10 * units.KiB, 0},
			{10002, []uint64{1, 2, 3}, 500 * units.KiB, 10 * units.KiB, 0},
			{10003, []uint64{1, 2, 3}, 10 * units.KiB, 500 * units.KiB, 0},
			{10004, []uint64{1, 2, 3}, 500 * units.KiB, 500 * units.KiB, 0},
		})
		stats = tc.RegionStats(statistics.Write, 0)
		re.Len(stats[1], 503)
		re.Len(stats[2], 503)
		re.Len(stats[3], 503)
	}
}

type testRegionInfo struct {
	id uint64
	// the storeID list for the peers, the leader is stored in the first store
	peers     []uint64
	byteRate  float64
	keyRate   float64
	queryRate float64
}

func addRegionInfo(tc *mockcluster.Cluster, rwTy statistics.RWType, regions []testRegionInfo) {
	addFunc := tc.AddRegionWithReadInfo
	if rwTy == statistics.Write {
		addFunc = tc.AddLeaderRegionWithWriteInfo
	}
	reportIntervalSecs := statistics.WriteReportInterval
	if rwTy == statistics.Read {
		reportIntervalSecs = statistics.ReadReportInterval
	}
	for _, r := range regions {
		addFunc(
			r.id, r.peers[0],
			uint64(r.byteRate*float64(reportIntervalSecs)),
			uint64(r.keyRate*float64(reportIntervalSecs)),
			uint64(r.queryRate*float64(reportIntervalSecs)),
			uint64(reportIntervalSecs),
			r.peers[1:],
		)
	}
}

func addRegionLeaderReadInfo(tc *mockcluster.Cluster, regions []testRegionInfo) {
	addFunc := tc.AddRegionLeaderWithReadInfo
	reportIntervalSecs := statistics.ReadReportInterval
	for _, r := range regions {
		addFunc(
			r.id, r.peers[0],
			uint64(r.byteRate*float64(reportIntervalSecs)),
			uint64(r.keyRate*float64(reportIntervalSecs)),
			uint64(r.queryRate*float64(reportIntervalSecs)),
			uint64(reportIntervalSecs),
			r.peers[1:],
		)
	}
}

type testHotCacheCheckRegionFlowCase struct {
	kind                      statistics.RWType
	onlyLeader                bool
	DegreeAfterTransferLeader int
}

func TestHotCacheCheckRegionFlow(t *testing.T) {
	re := require.New(t)
	testCases := []testHotCacheCheckRegionFlowCase{
		{
			kind:                      statistics.Write,
			onlyLeader:                false,
			DegreeAfterTransferLeader: 3,
		},
		{
			kind:                      statistics.Read,
			onlyLeader:                false,
			DegreeAfterTransferLeader: 4,
		},
		{
			kind:                      statistics.Read,
			onlyLeader:                true,
			DegreeAfterTransferLeader: 1,
		},
	}

	for _, testCase := range testCases {
		checkHotCacheCheckRegionFlow(re, testCase, false /* disable placement rules */)
		checkHotCacheCheckRegionFlow(re, testCase, true /* enable placement rules */)
	}
}

func checkHotCacheCheckRegionFlow(re *require.Assertions, testCase testHotCacheCheckRegionFlowCase, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	sche, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	heartbeat := tc.AddLeaderRegionWithWriteInfo
	if testCase.kind == statistics.Read {
		if testCase.onlyLeader {
			heartbeat = tc.AddRegionLeaderWithReadInfo
		} else {
			heartbeat = tc.AddRegionWithReadInfo
		}
	}
	tc.AddRegionStore(2, 20)
	tc.UpdateStorageReadStats(2, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval, 9.5*units.MiB*statistics.StoreHeartBeatReportInterval)
	reportInterval := uint64(statistics.WriteReportInterval)
	if testCase.kind == statistics.Read {
		reportInterval = uint64(statistics.ReadReportInterval)
	}
	// hot degree increase
	heartbeat(1, 1, 512*units.KiB*reportInterval, 0, 0, reportInterval, []uint64{2, 3}, 1)
	heartbeat(1, 1, 512*units.KiB*reportInterval, 0, 0, reportInterval, []uint64{2, 3}, 1)
	items := heartbeat(1, 1, 512*units.KiB*reportInterval, 0, 0, reportInterval, []uint64{2, 3}, 1)
	re.NotEmpty(items)
	for _, item := range items {
		re.Equal(3, item.HotDegree)
	}
	// transfer leader
	items = heartbeat(1, 2, 512*units.KiB*reportInterval, 0, 0, reportInterval, []uint64{1, 3}, 1)
	for _, item := range items {
		if item.StoreID == 2 {
			re.Equal(testCase.DegreeAfterTransferLeader, item.HotDegree)
		}
	}

	if testCase.DegreeAfterTransferLeader >= 3 {
		// try schedule
		hb.prepareForBalance(testCase.kind, tc)
		leaderSolver := newBalanceSolver(hb, tc, testCase.kind, transferLeader)
		leaderSolver.cur = &solution{srcStore: hb.stLoadInfos[toResourceType(testCase.kind, transferLeader)][2]}
		re.Empty(leaderSolver.filterHotPeers(leaderSolver.cur.srcStore)) // skip schedule
		threshold := tc.GetHotRegionCacheHitsThreshold()
		leaderSolver.minHotDegree = 0
		re.Len(leaderSolver.filterHotPeers(leaderSolver.cur.srcStore), 1)
		leaderSolver.minHotDegree = threshold
	}

	// move peer: add peer and remove peer
	items = heartbeat(1, 2, 512*units.KiB*reportInterval, 0, 0, reportInterval, []uint64{1, 3, 4}, 1)
	re.NotEmpty(items)
	for _, item := range items {
		re.Equal(testCase.DegreeAfterTransferLeader+1, item.HotDegree)
	}
	items = heartbeat(1, 2, 512*units.KiB*reportInterval, 0, 0, reportInterval, []uint64{1, 4}, 1)
	re.NotEmpty(items)
	for _, item := range items {
		if item.StoreID == 3 {
			re.Equal(statistics.Remove, item.GetActionType())
			continue
		}
		re.Equal(testCase.DegreeAfterTransferLeader+2, item.HotDegree)
	}
}

func TestHotCacheCheckRegionFlowWithDifferentThreshold(t *testing.T) {
	re := require.New(t)
	checkHotCacheCheckRegionFlowWithDifferentThreshold(re, false /* disable placement rules */)
	checkHotCacheCheckRegionFlowWithDifferentThreshold(re, true /* enable placement rules */)
}

func checkHotCacheCheckRegionFlowWithDifferentThreshold(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	statistics.ThresholdsUpdateInterval = 0
	defer func() {
		statistics.ThresholdsUpdateInterval = statistics.StoreHeartBeatReportInterval
	}()
	// some peers are hot, and some are cold #3198

	rate := uint64(512 * units.KiB)
	for i := 0; i < statistics.TopNN; i++ {
		for j := 0; j < statistics.DefaultAotSize; j++ {
			tc.AddLeaderRegionWithWriteInfo(uint64(i+100), 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{2, 3}, 1)
		}
	}
	items := tc.AddLeaderRegionWithWriteInfo(201, 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{2, 3}, 1)
	re.Equal(float64(rate)*statistics.HotThresholdRatio, tc.HotCache.GetThresholds(statistics.Write, items[0].StoreID)[0])
	// Threshold of store 1,2,3 is 409.6 units.KiB and others are 1 units.KiB
	// Make the hot threshold of some store is high and the others are low
	rate = 10 * units.KiB
	tc.AddLeaderRegionWithWriteInfo(201, 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{2, 3, 4}, 1)
	items = tc.AddLeaderRegionWithWriteInfo(201, 1, rate*statistics.WriteReportInterval, 0, 0, statistics.WriteReportInterval, []uint64{3, 4}, 1)
	for _, item := range items {
		if item.StoreID < 4 {
			re.Equal(statistics.Remove, item.GetActionType())
		} else {
			re.Equal(statistics.Update, item.GetActionType())
		}
	}
}

func TestHotCacheSortHotPeer(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sche, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	leaderSolver := newBalanceSolver(hb, tc, statistics.Read, transferLeader)
	hotPeers := []*statistics.HotPeerStat{{
		RegionID: 1,
		Loads: []float64{
			statistics.QueryDim: 10,
			statistics.ByteDim:  1,
		},
	}, {
		RegionID: 2,
		Loads: []float64{
			statistics.QueryDim: 1,
			statistics.ByteDim:  10,
		},
	}, {
		RegionID: 3,
		Loads: []float64{
			statistics.QueryDim: 5,
			statistics.ByteDim:  6,
		},
	}}

	leaderSolver.maxPeerNum = 1
	u := leaderSolver.sortHotPeers(hotPeers)
	checkSortResult(re, []uint64{1}, u)

	leaderSolver.maxPeerNum = 2
	u = leaderSolver.sortHotPeers(hotPeers)
	checkSortResult(re, []uint64{1, 2}, u)
}

func checkSortResult(re *require.Assertions, regions []uint64, hotPeers map[*statistics.HotPeerStat]struct{}) {
	re.Equal(len(hotPeers), len(regions))
	for _, region := range regions {
		in := false
		for hotPeer := range hotPeers {
			if hotPeer.RegionID == region {
				in = true
				break
			}
		}
		re.True(in)
	}
}

func TestInfluenceByRWType(t *testing.T) {
	re := require.New(t)
	statistics.HistorySampleDuration = 0
	originValue := schedulePeerPr
	defer func() {
		schedulePeerPr = originValue
	}()
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.UpdateStorageWrittenStats(1, 99*units.MiB*statistics.StoreHeartBeatReportInterval, 99*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 50*units.MiB*statistics.StoreHeartBeatReportInterval, 98*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 2*units.MiB*statistics.StoreHeartBeatReportInterval, 2*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
	})
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
	})
	// must move peer
	schedulePeerPr = 1.0
	// must move peer from 1 to 4
	ops, _ := hb.Schedule(tc, false)
	op := ops[0]
	re.NotNil(op)

	hb.(*hotScheduler).summaryPendingInfluence(tc)
	stInfos := hb.(*hotScheduler).stInfos
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteKeys], -0.5*units.MiB))
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteBytes], -0.5*units.MiB))
	re.True(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionWriteKeys], 0.5*units.MiB))
	re.True(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionWriteBytes], 0.5*units.MiB))
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadKeys], -0.5*units.MiB))
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadBytes], -0.5*units.MiB))
	re.True(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionReadKeys], 0.5*units.MiB))
	re.True(nearlyAbout(stInfos[4].PendingSum.Loads[statistics.RegionReadBytes], 0.5*units.MiB))

	// consider pending amp, there are nine regions or more.
	for i := 2; i < 13; i++ {
		addRegionInfo(tc, statistics.Write, []testRegionInfo{
			{uint64(i), []uint64{1, 2, 3}, 0.7 * units.MiB, 0.7 * units.MiB, 0},
		})
	}

	// must transfer leader
	schedulePeerPr = 0
	// must transfer leader from 1 to 3
	ops, _ = hb.Schedule(tc, false)
	op = ops[0]
	re.NotNil(op)

	hb.(*hotScheduler).summaryPendingInfluence(tc)
	stInfos = hb.(*hotScheduler).stInfos
	// assert read/write influence is the sum of write peer and write leader
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteKeys], -1.2*units.MiB))
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionWriteBytes], -1.2*units.MiB))
	re.True(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionWriteKeys], 0.7*units.MiB))
	re.True(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionWriteBytes], 0.7*units.MiB))
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadKeys], -1.2*units.MiB))
	re.True(nearlyAbout(stInfos[1].PendingSum.Loads[statistics.RegionReadBytes], -1.2*units.MiB))
	re.True(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionReadKeys], 0.7*units.MiB))
	re.True(nearlyAbout(stInfos[3].PendingSum.Loads[statistics.RegionReadBytes], 0.7*units.MiB))
}

func nearlyAbout(f1, f2 float64) bool {
	if f1-f2 < 0.1*units.KiB || f2-f1 < 0.1*units.KiB {
		return true
	}
	return false
}

func loadsEqual(loads1, loads2 []float64) bool {
	if len(loads1) != statistics.DimLen || len(loads2) != statistics.DimLen {
		return false
	}
	for i, load := range loads1 {
		if math.Abs(load-loads2[i]) > 0.01 {
			return false
		}
	}
	return true
}

func TestHotReadPeerSchedule(t *testing.T) {
	re := require.New(t)
	checkHotReadPeerSchedule(re, false /* disable placement rules */)
	checkHotReadPeerSchedule(re, true /* enable placement rules */)
}

func checkHotReadPeerSchedule(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	for id := uint64(1); id <= 6; id++ {
		tc.PutStoreWithLabels(id)
	}

	sche, err := schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageReadStats(1, 20*units.MiB, 20*units.MiB)
	tc.UpdateStorageReadStats(2, 19*units.MiB, 19*units.MiB)
	tc.UpdateStorageReadStats(3, 19*units.MiB, 19*units.MiB)
	tc.UpdateStorageReadStats(4, 0*units.MiB, 0*units.MiB)
	tc.AddRegionWithPeerReadInfo(1, 3, 1, uint64(0.9*units.KiB*float64(10)), uint64(0.9*units.KiB*float64(10)), 10, []uint64{1, 2}, 3)
	ops, _ := hb.Schedule(tc, false)
	op := ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpHotRegion, 1, 4)
}

func TestHotScheduleWithPriority(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1.05)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1.05)
	// skip stddev check
	origin := stddevThreshold
	stddevThreshold = -1.0
	defer func() {
		stddevThreshold = origin
	}()

	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 9*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6*units.MiB*statistics.StoreHeartBeatReportInterval, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*units.MiB*statistics.StoreHeartBeatReportInterval, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	// must transfer peer
	schedulePeerPr = 1.0
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 2 * units.MiB, 1 * units.MiB, 0},
		{6, []uint64{4, 2, 3}, 1 * units.MiB, 2 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 1, 5)
	clearPendingInfluence(hb.(*hotScheduler))
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 4, 5)
	clearPendingInfluence(hb.(*hotScheduler))

	// assert read priority schedule
	hb, err = schedule.CreateScheduler(statistics.Read.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	tc.UpdateStorageReadStats(5, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 7*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 7*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 2 * units.MiB, 2 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 3)

	hb, err = schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	re.NoError(err)

	// assert loose store picking
	tc.UpdateStorageWrittenStats(1, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6.1*units.MiB*statistics.StoreHeartBeatReportInterval, 6.1*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*units.MiB*statistics.StoreHeartBeatReportInterval, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 6*units.MiB*statistics.StoreHeartBeatReportInterval, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	hb.(*hotScheduler).conf.StrictPickingStore = true
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	hb.(*hotScheduler).conf.StrictPickingStore = false
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 1, 5)
	clearPendingInfluence(hb.(*hotScheduler))

	tc.UpdateStorageWrittenStats(1, 6*units.MiB*statistics.StoreHeartBeatReportInterval, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6.1*units.MiB*statistics.StoreHeartBeatReportInterval, 6.1*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*units.MiB*statistics.StoreHeartBeatReportInterval, 6*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.StrictPickingStore = true
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	hb.(*hotScheduler).conf.StrictPickingStore = false
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 4, 5)
	clearPendingInfluence(hb.(*hotScheduler))
}

func TestHotScheduleWithStddev(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1.0)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1.0)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.(*hotScheduler).conf.StrictPickingStore = false

	// skip uniform cluster
	tc.UpdateStorageWrittenStats(1, 5*units.MiB*statistics.StoreHeartBeatReportInterval, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 5.3*units.MiB*statistics.StoreHeartBeatReportInterval, 5.3*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 5*units.MiB*statistics.StoreHeartBeatReportInterval, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 5*units.MiB*statistics.StoreHeartBeatReportInterval, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 4.8*units.MiB*statistics.StoreHeartBeatReportInterval, 4.8*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 4, 2}, 0.1 * units.MiB, 0.1 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	stddevThreshold = 0.1
	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
	stddevThreshold = -1.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	clearPendingInfluence(hb.(*hotScheduler))

	// skip -1 case (uniform cluster)
	tc.UpdateStorageWrittenStats(1, 5*units.MiB*statistics.StoreHeartBeatReportInterval, 100*units.MiB*statistics.StoreHeartBeatReportInterval) // two dims are not uniform.
	tc.UpdateStorageWrittenStats(2, 5.3*units.MiB*statistics.StoreHeartBeatReportInterval, 4.8*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 5*units.MiB*statistics.StoreHeartBeatReportInterval, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 5*units.MiB*statistics.StoreHeartBeatReportInterval, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 4.8*units.MiB*statistics.StoreHeartBeatReportInterval, 5*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 4, 2}, 0.1 * units.MiB, 0.1 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	stddevThreshold = -1.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	clearPendingInfluence(hb.(*hotScheduler))
}

func TestHotWriteLeaderScheduleWithPriority(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.UpdateStorageWrittenStats(1, 31*units.MiB*statistics.StoreHeartBeatReportInterval, 31*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 1*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 1*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)

	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 10 * units.MiB, 10 * units.MiB, 0},
		{2, []uint64{1, 2, 3}, 10 * units.MiB, 10 * units.MiB, 0},
		{3, []uint64{1, 2, 3}, 10 * units.MiB, 10 * units.MiB, 0},
		{4, []uint64{2, 1, 3}, 10 * units.MiB, 0 * units.MiB, 0},
		{5, []uint64{3, 2, 1}, 0 * units.MiB, 10 * units.MiB, 0},
	})
	old1, old2 := schedulePeerPr, pendingAmpFactor
	schedulePeerPr, pendingAmpFactor = 0.0, 0.0
	defer func() {
		schedulePeerPr, pendingAmpFactor = old1, old2
	}()
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 3)
}

func TestCompatibility(t *testing.T) {
	re := require.New(t)
	statistics.Denoising = false
	statisticsInterval = 0

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	// default
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// config error value
	hb.(*hotScheduler).conf.ReadPriorities = []string{"error"}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{"error", statistics.BytePriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.QueryPriority, statistics.BytePriority, statistics.KeyPriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// low version
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version5_0))
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// config byte and key
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.KeyPriority, statistics.BytePriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
	})
	// config query in low version
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.QueryPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{statistics.QueryPriority, statistics.BytePriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.QueryPriority, statistics.BytePriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// config error value
	hb.(*hotScheduler).conf.ReadPriorities = []string{"error", "error"}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{statistics.QueryPriority, statistics.BytePriority, statistics.KeyPriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	// test version change
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.HotScheduleWithQuery))
	re.False(hb.(*hotScheduler).conf.lastQuerySupported) // it will updated after scheduling
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
	re.True(hb.(*hotScheduler).conf.lastQuerySupported)
}

func TestCompatibilityConfig(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// From new or 3.x cluster, it will use new config
	hb, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder("hot-region", nil))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})

	// Config file is not currently supported
	hb, err = schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(),
		schedule.ConfigSliceDecoder("hot-region", []string{"read-priorities=byte,query"}))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.QueryDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})

	// from 4.0 or 5.0 or 5.1 cluster
	var data []byte
	storage := storage.NewStorageWithMemoryBackend()
	data, err = schedule.EncodeConfig(map[string]interface{}{
		"min-hot-byte-rate":         100,
		"min-hot-key-rate":          10,
		"max-zombie-rounds":         3,
		"max-peer-number":           1000,
		"byte-rate-rank-step-ratio": 0.05,
		"key-rate-rank-step-ratio":  0.05,
		"count-rank-step-ratio":     0.01,
		"great-dec-ratio":           0.95,
		"minor-dec-ratio":           0.99,
		"src-tolerance-ratio":       1.05,
		"dst-tolerance-ratio":       1.05,
	})
	re.NoError(err)
	err = storage.SaveScheduleConfig(HotRegionName, data)
	re.NoError(err)
	hb, err = schedule.CreateScheduler(HotRegionType, oc, storage, schedule.ConfigJSONDecoder(data))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.ByteDim, statistics.KeyDim},
		{statistics.KeyDim, statistics.ByteDim},
		{statistics.ByteDim, statistics.KeyDim},
	})

	// From configured cluster
	cfg := initHotRegionScheduleConfig()
	cfg.ReadPriorities = []string{"key", "query"}
	cfg.WriteLeaderPriorities = []string{"query", "key"}
	data, err = schedule.EncodeConfig(cfg)
	re.NoError(err)
	err = storage.SaveScheduleConfig(HotRegionName, data)
	re.NoError(err)
	hb, err = schedule.CreateScheduler(HotRegionType, oc, storage, schedule.ConfigJSONDecoder(data))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{statistics.KeyDim, statistics.QueryDim},
		{statistics.QueryDim, statistics.KeyDim},
		{statistics.ByteDim, statistics.KeyDim},
	})
}

func checkPriority(re *require.Assertions, hb *hotScheduler, tc *mockcluster.Cluster, dims [3][2]int) {
	readSolver := newBalanceSolver(hb, tc, statistics.Read, transferLeader)
	writeLeaderSolver := newBalanceSolver(hb, tc, statistics.Write, transferLeader)
	writePeerSolver := newBalanceSolver(hb, tc, statistics.Write, movePeer)
	re.Equal(dims[0][0], readSolver.firstPriority)
	re.Equal(dims[0][1], readSolver.secondPriority)
	re.Equal(dims[1][0], writeLeaderSolver.firstPriority)
	re.Equal(dims[1][1], writeLeaderSolver.secondPriority)
	re.Equal(dims[2][0], writePeerSolver.firstPriority)
	re.Equal(dims[2][1], writePeerSolver.secondPriority)
}

func TestConfigValidation(t *testing.T) {
	re := require.New(t)

	hc := initHotRegionScheduleConfig()
	err := hc.valid()
	re.NoError(err)

	// priorities is illegal
	hc.ReadPriorities = []string{"byte", "error"}
	err = hc.valid()
	re.Error(err)

	// priorities should have at least 2 dimensions
	hc = initHotRegionScheduleConfig()
	hc.WriteLeaderPriorities = []string{"byte"}
	err = hc.valid()
	re.Error(err)

	// query is not allowed to be set in priorities for write-peer-priorities
	hc = initHotRegionScheduleConfig()
	hc.WritePeerPriorities = []string{"query", "byte"}
	err = hc.valid()
	re.Error(err)
	// priorities shouldn't be repeated
	hc.WritePeerPriorities = []string{"byte", "byte"}
	err = hc.valid()
	re.Error(err)
	// no error
	hc.WritePeerPriorities = []string{"byte", "key"}
	err = hc.valid()
	re.NoError(err)

	// rank-formula-version
	// default
	hc = initHotRegionScheduleConfig()
	re.Equal("v2", hc.GetRankFormulaVersion())
	// v1
	hc.RankFormulaVersion = "v1"
	err = hc.valid()
	re.NoError(err)
	re.Equal("v1", hc.GetRankFormulaVersion())
	// v2
	hc.RankFormulaVersion = "v2"
	err = hc.valid()
	re.NoError(err)
	re.Equal("v2", hc.GetRankFormulaVersion())
	// illegal
	hc.RankFormulaVersion = "v0"
	err = hc.valid()
	re.Error(err)

	// forbid-rw-type
	// default
	hc = initHotRegionScheduleConfig()
	re.False(hc.IsForbidRWType(statistics.Read))
	re.False(hc.IsForbidRWType(statistics.Write))
	// read
	hc.ForbidRWType = "read"
	err = hc.valid()
	re.NoError(err)
	re.True(hc.IsForbidRWType(statistics.Read))
	re.False(hc.IsForbidRWType(statistics.Write))
	// write
	hc.ForbidRWType = "write"
	err = hc.valid()
	re.NoError(err)
	re.False(hc.IsForbidRWType(statistics.Read))
	re.True(hc.IsForbidRWType(statistics.Write))
	// illegal
	hc.ForbidRWType = "test"
	err = hc.valid()
	re.Error(err)
}

type maxZombieDurTestCase struct {
	typ           resourceType
	isTiFlash     bool
	firstPriority int
	maxZombieDur  int
}

func TestMaxZombieDuration(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder("hot-region", nil))
	re.NoError(err)
	maxZombieDur := hb.(*hotScheduler).conf.getValidConf().MaxZombieRounds
	testCases := []maxZombieDurTestCase{
		{
			typ:          readPeer,
			maxZombieDur: maxZombieDur * statistics.StoreHeartBeatReportInterval,
		},
		{
			typ:          readLeader,
			maxZombieDur: maxZombieDur * statistics.StoreHeartBeatReportInterval,
		},
		{
			typ:          writePeer,
			maxZombieDur: maxZombieDur * statistics.StoreHeartBeatReportInterval,
		},
		{
			typ:          writePeer,
			isTiFlash:    true,
			maxZombieDur: maxZombieDur * statistics.RegionHeartBeatReportInterval,
		},
		{
			typ:           writeLeader,
			firstPriority: statistics.KeyDim,
			maxZombieDur:  maxZombieDur * statistics.RegionHeartBeatReportInterval,
		},
		{
			typ:           writeLeader,
			firstPriority: statistics.QueryDim,
			maxZombieDur:  maxZombieDur * statistics.StoreHeartBeatReportInterval,
		},
	}
	for _, testCase := range testCases {
		src := &statistics.StoreLoadDetail{
			StoreSummaryInfo: &statistics.StoreSummaryInfo{},
		}
		if testCase.isTiFlash {
			src.SetEngineAsTiFlash()
		}
		bs := &balanceSolver{
			sche:          hb.(*hotScheduler),
			resourceTy:    testCase.typ,
			firstPriority: testCase.firstPriority,
			best:          &solution{srcStore: src},
		}
		re.Equal(time.Duration(testCase.maxZombieDur)*time.Second, bs.calcMaxZombieDur())
	}
}

func TestExpect(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder("hot-region", nil))
	re.NoError(err)
	testCases := []struct {
		initFunc       func(*balanceSolver)
		strict         bool
		isSrc          bool
		allow          bool
		toleranceRatio float64
		rs             resourceType
		load           *statistics.StoreLoad
		expect         *statistics.StoreLoad
	}{
		// test src, it will be allowed when loads are higher than expect
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true, // all of
			load: &statistics.StoreLoad{ // all dims are higher than expect, allow schedule
				Loads:        []float64{2.0, 2.0, 2.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {2.0, 2.0}, {2.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true, // all of
			load: &statistics.StoreLoad{ // all dims are higher than expect, but lower than expect*toleranceRatio, not allow schedule
				Loads:        []float64{2.0, 2.0, 2.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {2.0, 2.0}, {2.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc:          true,
			toleranceRatio: 2.2,
			allow:          false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true, // all of
			load: &statistics.StoreLoad{ // only queryDim is lower, but the dim is no selected, allow schedule
				Loads:        []float64{2.0, 2.0, 1.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {2.0, 2.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true, // all of
			load: &statistics.StoreLoad{ // only keyDim is lower, and the dim is selected, not allow schedule
				Loads:        []float64{2.0, 1.0, 2.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   false, // first only
			load: &statistics.StoreLoad{ // keyDim is higher, and the dim is selected, allow schedule
				Loads:        []float64{1.0, 2.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   false, // first only
			load: &statistics.StoreLoad{ // although byteDim is higher, the dim is not first, not allow schedule
				Loads:        []float64{2.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{2.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   false, // first only
			load: &statistics.StoreLoad{ // although queryDim is higher, the dim is no selected, not allow schedule
				Loads:        []float64{1.0, 1.0, 2.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {2.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   false, // first only
			load: &statistics.StoreLoad{ // all dims are lower than expect, not allow schedule
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{2.0, 2.0, 2.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {2.0, 2.0}, {2.0, 2.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true,
			rs:       writeLeader,
			load: &statistics.StoreLoad{ // only keyDim is higher, but write leader only consider the first priority
				Loads:        []float64{1.0, 2.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {2.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true,
			rs:       writeLeader,
			load: &statistics.StoreLoad{ // although byteDim is higher, the dim is not first, not allow schedule
				Loads:        []float64{2.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {1.0, 1.0}, {2.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV1,
			strict:   true,
			rs:       writeLeader,
			load: &statistics.StoreLoad{ // history loads is not higher than the expected.
				Loads:        []float64{2.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {1.0, 2.0}, {1.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 2.0}, {1.0, 2.0}, {1.0, 2.0}},
			},
			isSrc: true,
			allow: false,
		},
		// v2
		{
			initFunc: (*balanceSolver).pickCheckPolicyV2,
			strict:   false, // any of
			load: &statistics.StoreLoad{ // keyDim is higher, and the dim is selected, allow schedule
				Loads:        []float64{1.0, 2.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV2,
			strict:   false, // any of
			load: &statistics.StoreLoad{ // byteDim is higher, and the dim is selected, allow schedule
				Loads:        []float64{2.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV2,
			strict:   false, // any of
			load: &statistics.StoreLoad{ // although queryDim is higher, the dim is no selected, not allow schedule
				Loads:        []float64{1.0, 1.0, 2.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {2.0, 2.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV2,
			strict:   false, // any of
			load: &statistics.StoreLoad{ // all dims are lower than expect, not allow schedule
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{2.0, 2.0, 2.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {2.0, 2.0}, {2.0, 2.0}},
			},
			isSrc: true,
			allow: false,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV2,
			strict:   true,
			rs:       writeLeader,
			load: &statistics.StoreLoad{ // only keyDim is higher, but write leader only consider the first priority
				Loads:        []float64{1.0, 2.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: true,
		},
		{
			initFunc: (*balanceSolver).pickCheckPolicyV2,
			strict:   true,
			rs:       writeLeader,
			load: &statistics.StoreLoad{ // although byteDim is higher, the dim is not first, not allow schedule
				Loads:        []float64{2.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{2.0, 2.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			expect: &statistics.StoreLoad{
				Loads:        []float64{1.0, 1.0, 1.0},
				HistoryLoads: [][]float64{{1.0, 1.0}, {1.0, 1.0}, {1.0, 1.0}},
			},
			isSrc: true,
			allow: false,
		},
	}

	srcToDst := func(src *statistics.StoreLoad) *statistics.StoreLoad {
		dst := make([]float64, len(src.Loads))
		for i, v := range src.Loads {
			dst[i] = 3.0 - v
		}
		historyLoads := make([][]float64, len(src.HistoryLoads))
		for i, dim := range src.HistoryLoads {
			historyLoads[i] = make([]float64, len(dim))
			for j, load := range dim {
				historyLoads[i][j] = 3.0 - load
			}
		}
		return &statistics.StoreLoad{
			Loads:        dst,
			HistoryLoads: historyLoads,
		}
	}

	for _, testCase := range testCases {
		toleranceRatio := testCase.toleranceRatio
		if toleranceRatio == 0.0 {
			toleranceRatio = 1.0 // default for test case
		}
		bs := &balanceSolver{
			sche:           hb.(*hotScheduler),
			firstPriority:  statistics.KeyDim,
			secondPriority: statistics.ByteDim,
			resourceTy:     testCase.rs,
		}
		bs.sche.conf.StrictPickingStore = testCase.strict
		testCase.initFunc(bs)
		re.Equal(testCase.allow, bs.checkSrcByPriorityAndTolerance(testCase.load, testCase.expect, toleranceRatio))
		re.Equal(testCase.allow, bs.checkDstByPriorityAndTolerance(srcToDst(testCase.load), srcToDst(testCase.expect), toleranceRatio))
		re.Equal(testCase.allow, bs.checkSrcHistoryLoadsByPriorityAndTolerance(testCase.load, testCase.expect, toleranceRatio))
		re.Equal(testCase.allow, bs.checkDstHistoryLoadsByPriorityAndTolerance(srcToDst(testCase.load), srcToDst(testCase.expect), toleranceRatio))
	}
}

// ref https://github.com/tikv/pd/issues/5701
func TestEncodeConfig(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()
	sche, err := schedule.CreateScheduler(HotRegionType, oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	data, err := sche.EncodeConfig()
	re.NoError(err)
	re.NotEqual("null", string(data))
}
