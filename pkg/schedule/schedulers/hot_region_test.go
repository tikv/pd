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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

var (
	writeType = types.CheckerSchedulerType(utils.Write.String())
	readType  = types.CheckerSchedulerType(utils.Read.String())
)

func init() {
	// disable denoising in test.
	statistics.Denoising = false
	statisticsInterval = 0
	RegisterScheduler(writeType, func(opController *operator.Controller, _ endpoint.ConfigStorage, _ ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		cfg := initHotRegionScheduleConfig()
		return newHotWriteScheduler(opController, cfg), nil
	})
	RegisterScheduler(readType, func(opController *operator.Controller, _ endpoint.ConfigStorage, _ ConfigDecoder, _ ...func(string) error) (Scheduler, error) {
		return newHotReadScheduler(opController, initHotRegionScheduleConfig()), nil
	})
}

func newHotReadScheduler(opController *operator.Controller, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.types = []resourceType{readLeader, readPeer}
	return ret
}

func newHotWriteScheduler(opController *operator.Controller, conf *hotRegionSchedulerConfig) *hotScheduler {
	ret := newHotScheduler(opController, conf)
	ret.types = []resourceType{writeLeader, writePeer}
	return ret
}

func clearPendingInfluence(h *hotScheduler) {
	h.regionPendings = make(map[uint64]*pendingInfluence)
}

func newTestRegion(id uint64) *core.RegionInfo {
	peers := []*metapb.Peer{{Id: id*100 + 1, StoreId: 1}, {Id: id*100 + 2, StoreId: 2}, {Id: id*100 + 3, StoreId: 3}}
	return core.NewRegionInfo(&metapb.Region{Id: id, Peers: peers}, peers[0])
}

func TestUpgrade(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()
	// new
	sche, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	re.Equal([]string{utils.QueryPriority, utils.BytePriority}, hb.conf.getReadPriorities())
	re.Equal([]string{utils.QueryPriority, utils.BytePriority}, hb.conf.getWriteLeaderPriorities())
	re.Equal([]string{utils.BytePriority, utils.KeyPriority}, hb.conf.getWritePeerPriorities())
	re.Equal("v2", hb.conf.getRankFormulaVersion())
	// upgrade from json(null)
	sche, err = CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb = sche.(*hotScheduler)
	re.Equal([]string{utils.QueryPriority, utils.BytePriority}, hb.conf.getReadPriorities())
	re.Equal([]string{utils.QueryPriority, utils.BytePriority}, hb.conf.getWriteLeaderPriorities())
	re.Equal([]string{utils.BytePriority, utils.KeyPriority}, hb.conf.getWritePeerPriorities())
	re.Equal("v2", hb.conf.getRankFormulaVersion())
	// upgrade from < 5.2
	config51 := `{"min-hot-byte-rate":100,"min-hot-key-rate":10,"min-hot-query-rate":10,"max-zombie-rounds":5,"max-peer-number":1000,"byte-rate-rank-step-ratio":0.05,"key-rate-rank-step-ratio":0.05,"query-rate-rank-step-ratio":0.05,"count-rank-step-ratio":0.01,"great-dec-ratio":0.95,"minor-dec-ratio":0.99,"src-tolerance-ratio":1.05,"dst-tolerance-ratio":1.05,"strict-picking-store":"true","enable-for-tiflash":"true"}`
	sche, err = CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte(config51)))
	re.NoError(err)
	hb = sche.(*hotScheduler)
	re.Equal([]string{utils.BytePriority, utils.KeyPriority}, hb.conf.getReadPriorities())
	re.Equal([]string{utils.KeyPriority, utils.BytePriority}, hb.conf.getWriteLeaderPriorities())
	re.Equal([]string{utils.BytePriority, utils.KeyPriority}, hb.conf.getWritePeerPriorities())
	re.Equal("v1", hb.conf.getRankFormulaVersion())
	// upgrade from < 6.4
	config54 := `{"min-hot-byte-rate":100,"min-hot-key-rate":10,"min-hot-query-rate":10,"max-zombie-rounds":5,"max-peer-number":1000,"byte-rate-rank-step-ratio":0.05,"key-rate-rank-step-ratio":0.05,"query-rate-rank-step-ratio":0.05,"count-rank-step-ratio":0.01,"great-dec-ratio":0.95,"minor-dec-ratio":0.99,"src-tolerance-ratio":1.05,"dst-tolerance-ratio":1.05,"read-priorities":["query","byte"],"write-leader-priorities":["query","byte"],"write-peer-priorities":["byte","key"],"strict-picking-store":"true","enable-for-tiflash":"true","forbid-rw-type":"none"}`
	sche, err = CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte(config54)))
	re.NoError(err)
	hb = sche.(*hotScheduler)
	re.Equal([]string{utils.QueryPriority, utils.BytePriority}, hb.conf.getReadPriorities())
	re.Equal([]string{utils.QueryPriority, utils.BytePriority}, hb.conf.getWriteLeaderPriorities())
	re.Equal([]string{utils.BytePriority, utils.KeyPriority}, hb.conf.getWritePeerPriorities())
	re.Equal("v1", hb.conf.getRankFormulaVersion())
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

	sche, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)

	notDoneOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		var op *operator.Operator
		var err error
		switch ty {
		case movePeer:
			op, err = operator.CreateMovePeerOperator("move-peer-test", tc, region, operator.OpAdmin, 2, &metapb.Peer{Id: region.GetID()*10000 + 1, StoreId: 4})
		case transferLeader:
			op, err = operator.CreateTransferLeaderOperator("transfer-leader-test", tc, region, 2, []uint64{}, operator.OpAdmin)
		}
		re.NoError(err)
		re.NotNil(op)
		op.Start()
		op.SetStatusReachTime(operator.CREATED, time.Now().Add(-5*utils.StoreHeartBeatReportInterval*time.Second))
		op.SetStatusReachTime(operator.STARTED, time.Now().Add((-5*utils.StoreHeartBeatReportInterval+1)*time.Second))
		return newPendingInfluence(op, []uint64{2}, 4, statistics.Influence{}, hb.conf.getStoreStatZombieDuration())
	}
	justDoneOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		infl := notDoneOpInfluence(region, ty)
		infl.op.Cancel(operator.AdminStop)
		return infl
	}
	shouldRemoveOpInfluence := func(region *core.RegionInfo, ty opType) *pendingInfluence {
		infl := justDoneOpInfluence(region, ty)
		infl.op.SetStatusReachTime(operator.CANCELED, time.Now().Add(-3*utils.StoreHeartBeatReportInterval*time.Second))
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

	storeInfos := statistics.SummaryStoreInfos(tc.GetStores())
	hb.summaryPendingInfluence(storeInfos) // Calling this function will GC.

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
					re.NotZero(kind & operator.OpLeader)
					re.Zero(kind & operator.OpRegion)
				case movePeer:
					re.Zero(kind & operator.OpLeader)
					re.NotZero(kind & operator.OpRegion)
				}
			}
		}
	}
}

func TestSplitIfRegionTooHot(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	b := &metapb.Buckets{
		RegionId:   1,
		PeriodInMs: 1000,
		Keys: [][]byte{
			[]byte(fmt.Sprintf("%21d", 11)),
			[]byte(fmt.Sprintf("%21d", 12)),
			[]byte(fmt.Sprintf("%21d", 13)),
			[]byte(fmt.Sprintf("%21d", 14)),
			[]byte(fmt.Sprintf("%21d", 15)),
		},
		Stats: &metapb.BucketStats{
			ReadBytes:  []uint64{10 * units.KiB, 11 * units.KiB, 11 * units.KiB, 10 * units.KiB},
			ReadKeys:   []uint64{256, 256, 156, 256},
			ReadQps:    []uint64{0, 0, 0, 0},
			WriteBytes: []uint64{100 * units.KiB, 10 * units.KiB, 10 * units.KiB, 10 * units.KiB},
			WriteQps:   []uint64{256, 256, 156, 256},
			WriteKeys:  []uint64{0, 0, 0, 0},
		},
	}

	task := buckets.NewCheckPeerTask(b)
	re.True(tc.HotBucketCache.CheckAsync(task))
	time.Sleep(time.Millisecond * 10)

	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)

	tc.UpdateStorageReadBytes(1, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 4 * units.MiB, 0, 0},
	})
	tc.SetRegionBucketEnabled(true)
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	expectOp, _ := operator.CreateSplitRegionOperator(splitHotReadBuckets, tc.GetRegion(1), operator.OpSplit,
		pdpb.CheckPolicy_USEKEY, [][]byte{[]byte(fmt.Sprintf("%21d", 13))})
	re.Equal(expectOp.Brief(), ops[0].Brief())
	re.Equal(expectOp.Kind(), ops[0].Kind())

	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)

	tc.UpdateStorageWrittenBytes(1, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 4 * units.MiB, 0, 0},
	})
	hb, _ = CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	expectOp, _ = operator.CreateSplitRegionOperator(splitHotReadBuckets, tc.GetRegion(1), operator.OpSplit,
		pdpb.CheckPolicy_USEKEY, [][]byte{[]byte(fmt.Sprintf("%21d", 12))})
	re.Equal(expectOp.Brief(), ops[0].Brief())
	re.Equal(expectOp.Kind(), ops[0].Kind())
	re.Equal(operator.OpSplit, ops[0].Kind())

	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
}

func TestSplitBucketsBySize(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	tc.SetRegionBucketEnabled(true)
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	solve := newBalanceSolver(hb.(*hotScheduler), tc, utils.Read, transferLeader)
	solve.cur = &solution{}
	region := core.NewTestRegionInfo(1, 1, []byte("a"), []byte("f"))

	testdata := []struct {
		hotBuckets [][]byte
		splitKeys  [][]byte
	}{
		{
			[][]byte{[]byte("a"), []byte("b"), []byte("f")},
			[][]byte{[]byte("b")},
		},
		{
			[][]byte{[]byte(""), []byte("a"), []byte("")},
			nil,
		},
		{
			[][]byte{},
			nil,
		},
	}

	for _, data := range testdata {
		b := &metapb.Buckets{
			RegionId:   1,
			PeriodInMs: 1000,
			Keys:       data.hotBuckets,
		}
		region.UpdateBuckets(b, region.GetBuckets())
		ops := solve.createSplitOperator([]*core.RegionInfo{region}, bySize)
		if data.splitKeys == nil {
			re.Empty(ops)
			continue
		}
		re.Len(ops, 1)
		op := ops[0]
		re.Equal(splitHotReadBuckets, op.Desc())

		expectOp, err := operator.CreateSplitRegionOperator(splitHotReadBuckets, region, operator.OpSplit, pdpb.CheckPolicy_USEKEY, data.splitKeys)
		re.NoError(err)
		re.Equal(expectOp.Brief(), op.Brief())
	}
}

func TestSplitBucketsByLoad(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	tc.SetRegionBucketEnabled(true)
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	solve := newBalanceSolver(hb.(*hotScheduler), tc, utils.Read, transferLeader)
	solve.cur = &solution{}
	region := core.NewTestRegionInfo(1, 1, []byte("a"), []byte("f"))
	testdata := []struct {
		hotBuckets [][]byte
		splitKeys  [][]byte
	}{
		{
			[][]byte{[]byte(""), []byte("b"), []byte("")},
			[][]byte{[]byte("b")},
		},
		{
			[][]byte{[]byte(""), []byte("a"), []byte("")},
			nil,
		},
		{
			[][]byte{[]byte("b"), []byte("c"), []byte("")},
			[][]byte{[]byte("c")},
		},
	}
	for _, data := range testdata {
		b := &metapb.Buckets{
			RegionId:   1,
			PeriodInMs: 1000,
			Keys:       data.hotBuckets,
			Stats: &metapb.BucketStats{
				ReadBytes:  []uint64{10 * units.KiB, 10 * units.MiB},
				ReadKeys:   []uint64{256, 256},
				ReadQps:    []uint64{0, 0},
				WriteBytes: []uint64{0, 0},
				WriteQps:   []uint64{0, 0},
				WriteKeys:  []uint64{0, 0},
			},
		}
		task := buckets.NewCheckPeerTask(b)
		re.True(tc.HotBucketCache.CheckAsync(task))
		time.Sleep(time.Millisecond * 10)
		ops := solve.createSplitOperator([]*core.RegionInfo{region}, byLoad)
		if data.splitKeys == nil {
			re.Empty(ops)
			continue
		}
		re.Len(ops, 1)
		op := ops[0]
		re.Equal(splitHotReadBuckets, op.Desc())

		expectOp, err := operator.CreateSplitRegionOperator(splitHotReadBuckets, region, operator.OpSplit, pdpb.CheckPolicy_USEKEY, data.splitKeys)
		re.NoError(err)
		re.Equal(expectOp.Brief(), op.Brief())
	}
}

func TestHotWriteRegionScheduleByteRateOnly(t *testing.T) {
	re := require.New(t)
	checkHotWriteRegionScheduleByteRateOnly(re, false /* disable placement rules */)
	checkHotWriteRegionScheduleByteRateOnly(re, true /* enable placement rules */)
	checkHotWriteRegionPlacement(re, true)
}

func checkHotWriteRegionPlacement(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetEnableUseJointConsensus(true)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.ConfChangeV2))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.AddLabelsStore(1, 2, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z2", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z2", "host": "h4"})
	tc.AddLabelsStore(5, 2, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsStore(6, 2, map[string]string{"zone": "z2", "host": "h6"})
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd", ID: "leader", Role: placement.Leader, Count: 1, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "in", Values: []string{"z1"}}},
	})
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd", ID: "voter", Role: placement.Follower, Count: 2, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "in", Values: []string{"z2"}}},
	})
	tc.RuleManager.DeleteRule("pd", "default")

	tc.UpdateStorageWrittenBytes(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 0)
	tc.UpdateStorageWrittenBytes(3, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 3*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(6, 6*units.MiB*utils.StoreHeartBeatReportInterval)

	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 3, 5}, 512 * units.KiB, 0, 0},
		{2, []uint64{1, 4, 6}, 512 * units.KiB, 0, 0},
		{3, []uint64{1, 3, 6}, 512 * units.KiB, 0, 0},
	})
	ops, _ := hb.Schedule(tc, false)
	re.NotEmpty(ops)
	re.NotContains(ops[0].Step(1).String(), "transfer leader")
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))

	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd", ID: "voter", Role: placement.Voter, Count: 2, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "in", Values: []string{"z2"}}},
	})
	tc.RuleManager.DeleteRule("pd", "follower")
	ops, _ = hb.Schedule(tc, false)
	re.NotEmpty(ops)
	re.NotContains(ops[0].Step(1).String(), "transfer leader")
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, ops[0], operator.OpHotRegion, 1, 2)
}

func checkHotWriteRegionScheduleByteRateOnly(re *require.Assertions, enablePlacementRules bool) {
	cancel, opt, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.ConfChangeV2))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	tc.SetHotRegionCacheHitsThreshold(0)

	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.BytePriority, utils.KeyPriority}

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
	tc.UpdateStorageWrittenBytes(1, 7.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	// | region_id | leader_store | follower_store | follower_store | written_bytes |
	// |-----------|--------------|----------------|----------------|---------------|
	// |     1     |       1      |        2       |       3        |      512KB    |
	// |     2     |       1      |        3       |       4        |      512KB    |
	// |     3     |       1      |        2       |       4        |      512KB    |
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{1, 3, 4}, 512 * units.KiB, 0, 0},
		{3, []uint64{1, 2, 4}, 512 * units.KiB, 0, 0},
	})
	ops, _ := hb.Schedule(tc, false)
	re.NotEmpty(ops)
	clearPendingInfluence(hb.(*hotScheduler))

	// Will transfer a hot region from store 1, because the total count of peers
	// which is hot for store 1 is larger than other stores.
	hasLeaderOperator, hasPeerOperator := false, false
	for i := 0; i < 20 || !(hasLeaderOperator && hasPeerOperator); i++ {
		ops, _ = hb.Schedule(tc, false)
		op := ops[0]
		clearPendingInfluence(hb.(*hotScheduler))
		switch op.Len() {
		case 1:
			// balance by leader selected
			re.Equal("transfer-hot-write-leader", op.Desc())
			operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
			hasLeaderOperator = true
		case 5:
			// balance by peer selected
			re.Equal("move-hot-write-leader", op.Desc())
			if op.RegionID() == 2 {
				// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 0)
			} else {
				// peer in store 1 of the region 1,3 can only transfer to store 6
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 6)
			}
			hasPeerOperator = true
		default:
			re.FailNow("wrong op: " + op.String())
		}
	}

	// hot region scheduler is restricted by `hot-region-schedule-limit`.
	tc.SetHotRegionScheduleLimit(0)
	re.False(hb.IsScheduleAllowed(tc))
	clearPendingInfluence(hb.(*hotScheduler))
	tc.SetHotRegionScheduleLimit(int(opt.GetHotRegionScheduleLimit()))

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
	tc.UpdateStorageWrittenBytes(1, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3.1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 3*units.MiB*utils.StoreHeartBeatReportInterval)

	// | region_id | leader_store | follower_store | follower_store | written_bytes |
	// |-----------|--------------|----------------|----------------|---------------|
	// |     1     |       1      |        2       |       3        |      512KB    |
	// |     2     |       1      |        2       |       3        |      512KB    |
	// |     3     |       6      |        1       |       4        |      512KB    |
	// |     4     |       5      |        6       |       4        |      512KB    |
	// |     5     |       3      |        4       |       5        |      512KB    |
	addRegionInfo(tc, utils.Write, []testRegionInfo{
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
	// Source store is 1 or 3.
	//   Region 1 and 2 are the same, cannot move peer to store 5 due to the label.
	//   Region 3 can only move peer to store 5.
	//   Region 5 can only move peer to store 6.
	for range 30 {
		ops, _ = hb.Schedule(tc, false)
		op := ops[0]
		clearPendingInfluence(hb.(*hotScheduler))
		switch op.RegionID() {
		case 1, 2:
			switch op.Len() {
			case 1:
				operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 1, 2)
			case 3:
				operatorutil.CheckTransferPeer(re, op, operator.OpHotRegion, 3, 6)
			case 4:
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 6)
			default:
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
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.ConfChangeV2))
	re.NoError(tc.RuleManager.SetRules([]*placement.Rule{
		{
			GroupID:        placement.DefaultGroupID,
			ID:             placement.DefaultRuleID,
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
	sche, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.setHistorySampleDuration(0)

	// Add TiKV stores 1, 2, 3, 4, 5, 6, 7 (Down) with region counts 3, 3, 2, 2, 0, 0, 0.
	// Add TiFlash stores 8, 9, 10 with region counts 2, 1, 1.
	storeCount := uint64(10)
	aliveTiKVStartID := uint64(1)
	aliveTiKVLastID := uint64(6)
	aliveTiFlashStartID := uint64(8)
	aliveTiFlashLastID := uint64(10)
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
	addRegionInfo(tc, utils.Write, testRegions)
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
	pdServerCfg := tc.GetPDServerConfig()
	pdServerCfg.FlowRoundByDigit = 6
	hasLeaderOperator, hasPeerOperator := false, false
	for i := 0; i < 20 || !(hasLeaderOperator && hasPeerOperator); i++ {
		clearPendingInfluence(hb)
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		switch op.Len() {
		case 1:
			// balance by leader selected
			re.Equal("transfer-hot-write-leader", op.Desc())
			operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
			hasLeaderOperator = true
		case 2:
			// balance by peer selected
			re.Equal("move-hot-write-peer", op.Desc())
			operatorutil.CheckTransferLearner(re, op, operator.OpHotRegion, 8, 10)
			hasPeerOperator = true
		default:
			re.FailNow("wrong op: " + op.String())
		}
	}
	pdServerCfg.FlowRoundByDigit = 3
	// Disable for TiFlash
	hb.conf.setEnableForTiFlash(false)
	for range 20 {
		clearPendingInfluence(hb)
		ops, _ := hb.Schedule(tc, false)
		if len(ops) == 0 {
			continue
		}
		operatorutil.CheckTransferLeaderFrom(re, ops[0], operator.OpHotRegion, 1)
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
	storesBytes := map[uint64]uint64{
		1: 7.5 * units.MiB * utils.StoreHeartBeatReportInterval,
		2: 4.5 * units.MiB * utils.StoreHeartBeatReportInterval,
		3: 4.5 * units.MiB * utils.StoreHeartBeatReportInterval,
		4: 6 * units.MiB * utils.StoreHeartBeatReportInterval,
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
		testutil.Eventually(re, func() bool {
			ops, _ := hb.Schedule(tc, false)
			return len(ops) != 0
		})
		re.True(
			loadsEqual(
				hb.stLoadInfos[writeLeader][1].LoadPred.Expect.Loads,
				[]float64{hotRegionBytesSum / allowLeaderTiKVCount, hotRegionKeysSum / allowLeaderTiKVCount, tikvQuerySum / allowLeaderTiKVCount}))
		re.NotEqual(tikvQuerySum, hotRegionQuerySum)
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
		testutil.Eventually(re, func() bool {
			ops, _ := hb.Schedule(tc, false)
			return len(ops) != 0
		})
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
	for range 20 {
		clearPendingInfluence(hb)
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		switch op.Len() {
		case 1:
			// balance by leader selected
			re.Equal("transfer-hot-write-leader", op.Desc())
			operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
		case 5:
			// balance by peer selected
			re.Equal("move-hot-write-leader", op.Desc())
			if op.RegionID() == 2 {
				// peer in store 1 of the region 2 can transfer to store 5 or store 6 because of the label
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 0)
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
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.QueryPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWriteQuery(1, 11000*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWriteQuery(2, 10000*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWriteQuery(3, 9000*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 500, 0, 500},
		{2, []uint64{1, 2, 3}, 500, 0, 500},
		{3, []uint64{2, 1, 3}, 500, 0, 500},
	})
	for range 100 {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		if len(ops) == 0 {
			continue
		}
		operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 3)
	}
}

func TestHotWriteRegionScheduleWithKeyRate(t *testing.T) {
	// This test is used to test move peer.
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*units.MiB*utils.StoreHeartBeatReportInterval, 10.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 9.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 9.8*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*units.MiB*utils.StoreHeartBeatReportInterval, 9*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 8.9*units.MiB*utils.StoreHeartBeatReportInterval, 9.2*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{2, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{3, []uint64{2, 4, 3}, 0.05 * units.MiB, 0.1 * units.MiB, 0},
	})

	for range 100 {
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

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*units.MiB*utils.StoreHeartBeatReportInterval, 10.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 9.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 0*units.MiB*utils.StoreHeartBeatReportInterval, 0*units.MiB*utils.StoreHeartBeatReportInterval)
	addRegionInfo(tc, utils.Write, []testRegionInfo{
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

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*units.MiB*utils.StoreHeartBeatReportInterval, 10.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10.5*units.MiB*utils.StoreHeartBeatReportInterval, 10.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 10.5*units.MiB*utils.StoreHeartBeatReportInterval, 10.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 90, 0.5 * units.MiB, 0},              // no hot
		{1, []uint64{2, 1, 3}, 90, 0.5 * units.MiB, 0},              // no hot
		{2, []uint64{3, 2, 1}, 0.5 * units.MiB, 0.5 * units.MiB, 0}, // byteDecRatio is greater than greatDecRatio
	})

	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
}

func TestHotWriteRegionScheduleWithLeader(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	hb.(*hotScheduler).types = []resourceType{writeLeader}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	re.NoError(err)

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageWrittenBytes(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	// store1 has 2 peer as leader
	// store2 has 3 peer as leader
	// store3 has 2 peer as leader
	// If transfer leader from store2 to store1 or store3, it will keep on looping, which introduces a lot of unnecessary scheduling
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{2, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{3, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{4, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{5, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{6, []uint64{3, 1, 2}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{7, []uint64{3, 1, 2}, 0.5 * units.MiB, 1 * units.MiB, 0},
	})

	for range 100 {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		re.Empty(ops)
	}

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{8, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
	})

	// store1 has 2 peer as leader
	// store2 has 4 peer as leader
	// store3 has 2 peer as leader
	// We expect to transfer leader from store2 to store1 or store3
	for range 100 {
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
	checkHotWriteRegionScheduleWithPendingInfluence(re, 0) // 0: byte rate
	checkHotWriteRegionScheduleWithPendingInfluence(re, 1) // 1: key rate
}

func checkHotWriteRegionScheduleWithPendingInfluence(re *require.Assertions, dim int) {
	pendingAmpFactor = 0.0
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	updateStore := tc.UpdateStorageWrittenBytes // byte rate
	if dim == 1 {                               // key rate
		updateStore = tc.UpdateStorageWrittenKeys
	}
	updateStore(1, 8*units.MiB*utils.StoreHeartBeatReportInterval)
	updateStore(2, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	updateStore(3, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	updateStore(4, 4*units.MiB*utils.StoreHeartBeatReportInterval)

	if dim == 0 { // byte rate
		hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
		addRegionInfo(tc, utils.Write, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{2, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{3, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{4, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{5, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
			{6, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		})
	} else if dim == 1 { // key rate
		hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.BytePriority, utils.KeyPriority}
		addRegionInfo(tc, utils.Write, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{2, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{3, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{4, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{5, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
			{6, []uint64{1, 2, 3}, 0, 512 * units.KiB, 0},
		})
	}

	for range 20 {
		clearPendingInfluence(hb.(*hotScheduler))
		cnt := 0
	testLoop:
		for range 1000 {
			re.LessOrEqual(cnt, 5)
			emptyCnt := 0
			ops, _ := hb.Schedule(tc, false)
			for len(ops) == 0 {
				emptyCnt++
				if emptyCnt >= 100 {
					break testLoop
				}
				ops, _ = hb.Schedule(tc, false)
			}
			op := ops[0]
			switch op.Len() {
			case 1:
				// balance by leader selected
				re.Equal("transfer-hot-write-leader", op.Desc())
				operatorutil.CheckTransferLeaderFrom(re, op, operator.OpHotRegion, 1)
			case 5:
				// balance by peer selected
				re.Equal("move-hot-write-leader", op.Desc())
				operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 4)
				cnt++
				if cnt == 3 {
					re.True(op.Cancel(operator.AdminStop))
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
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetEnablePlacementRules(true)
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	key, err := hex.DecodeString("")
	re.NoError(err)
	// skip stddev check
	stddevThreshold = -1.0

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	err = tc.SetRule(&placement.Rule{
		GroupID:  placement.DefaultGroupID,
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
		GroupID:  placement.DefaultGroupID,
		ID:       "voter",
		Index:    2,
		Override: false,
		Role:     placement.Voter,
		Count:    2,
		StartKey: key,
		EndKey:   key,
	})
	re.NoError(err)

	tc.UpdateStorageWrittenBytes(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{2, []uint64{1, 2, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{3, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{4, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{5, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{6, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
		{7, []uint64{2, 1, 3}, 0.5 * units.MiB, 1 * units.MiB, 0},
	})

	for range 100 {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		if len(ops) == 0 {
			continue
		}
		// The targetID should always be 1 as leader is only allowed to be placed in store1 or store2 by placement rule
		operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 2, 1)
		ops, _ = hb.Schedule(tc, false)
		re.Empty(ops)
	}
}

func TestHotReadRegionScheduleByteRateOnly(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	scheduler, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := scheduler.(*hotScheduler)
	hb.conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
	hb.conf.setHistorySampleDuration(0)

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
	tc.UpdateStorageReadBytes(1, 7.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.9*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 3.7*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 0)

	// | region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	// |-----------|--------------|----------------|----------------|--------------------|
	// |     1     |       1      |        2       |       3        |        512KB       |
	// |     2     |       2      |        1       |       3        |        511KB       |
	// |     3     |       1      |        2       |       3        |        510KB       |
	// |     11    |       1      |        2       |       3        |          7KB       |
	// Region 1, 2 and 3 are hot regions.
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{2, 1, 3}, 511 * units.KiB, 0, 0},
		{3, []uint64{1, 2, 3}, 510 * units.KiB, 0, 0},
		{11, []uint64{1, 2, 3}, 7 * units.KiB, 0, 0},
	})

	testutil.Eventually(re, func() bool {
		return tc.IsRegionHot(tc.GetRegion(1)) && !tc.IsRegionHot(tc.GetRegion(11))
	})
	// check randomly pick hot region
	r := tc.HotRegionsFromStore(2, utils.Read)
	re.Len(r, 3)
	// check hot items
	stats := tc.HotCache.GetHotPeerStats(utils.Read, 0)
	re.Len(stats, 3)
	for _, ss := range stats {
		for _, s := range ss {
			re.Less(500.0*units.KiB, s.GetLoad(utils.ByteDim))
		}
	}

	ops, _ := hb.Schedule(tc, false)
	op := ops[0]

	// move leader from store 1 to store 5
	// it is better than transfer leader from store 1 to store 3
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion, 1, 5)
	re.Contains(hb.regionPendings, uint64(1))
	re.True(typeutil.Float64Equal(512.0*units.KiB, hb.regionPendings[1].origin.Loads[utils.RegionReadBytes]))
	clearPendingInfluence(hb)

	// assume handle the transfer leader operator rather than move leader
	tc.AddRegionWithReadInfo(3, 3, 512*units.KiB*utils.StoreHeartBeatReportInterval, 0, 0, utils.StoreHeartBeatReportInterval, []uint64{1, 2})
	// After transfer a hot region leader from store 1 to store 3
	// the three region leader will be evenly distributed in three stores

	// | store_id | read_bytes_rate |
	// |----------|-----------------|
	// |    1     |       6MB       |
	// |    2     |       5.5MB     |
	// |    3     |       5.5MB     |
	// |    4     |       3.4MB     |
	// |    5     |       3MB       |
	tc.UpdateStorageReadBytes(1, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 5.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 5.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(4, 3.4*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(5, 3*units.MiB*utils.StoreHeartBeatReportInterval)

	// | region_id | leader_store | follower_store | follower_store |   read_bytes_rate  |
	// |-----------|--------------|----------------|----------------|--------------------|
	// |     1     |       1      |        2       |       3        |        512KB       |
	// |     2     |       2      |        1       |       3        |        511KB       |
	// |     3     |       3      |        2       |       1        |        510KB       |
	// |     4     |       1      |        2       |       3        |        509KB       |
	// |     5     |       4      |        2       |       5        |        508KB       |
	// |     11    |       1      |        2       |       3        |          7KB       |
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 509 * units.KiB, 0, 0},
		{5, []uint64{4, 2, 5}, 508 * units.KiB, 0, 0},
	})

	// We will move leader peer of region 1 from 1 to 5
	ops, _ = hb.Schedule(tc, false)
	op = ops[0]
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpHotRegion|operator.OpLeader, 1, 5)
	re.Contains(hb.regionPendings, uint64(1))
	re.True(typeutil.Float64Equal(512.0*units.KiB, hb.regionPendings[1].origin.Loads[utils.RegionReadBytes]))
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
	re.True(typeutil.Float64Equal(509.0*units.KiB, hb.regionPendings[4].origin.Loads[utils.RegionReadBytes]))
	clearPendingInfluence(hb)
}

func TestHotReadRegionScheduleWithQuery(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)

	tc.UpdateStorageReadQuery(1, 10500*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(2, 10000*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(3, 9000*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0, 0, 500},
		{2, []uint64{2, 1, 3}, 0, 0, 500},
	})

	for range 100 {
		clearPendingInfluence(hb.(*hotScheduler))
		ops, _ := hb.Schedule(tc, false)
		op := ops[0]
		operatorutil.CheckTransferLeader(re, op, operator.OpHotRegion, 1, 3)
	}
}

func TestHotReadRegionScheduleWithKeyRate(t *testing.T) {
	re := require.New(t)

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageReadStats(1, 10.5*units.MiB*utils.StoreHeartBeatReportInterval, 10.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 9.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 9.8*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 9*units.MiB*utils.StoreHeartBeatReportInterval, 9*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(5, 8.9*units.MiB*utils.StoreHeartBeatReportInterval, 9.2*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 2, 4}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{2, []uint64{1, 2, 4}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
		{3, []uint64{3, 4, 5}, 0.05 * units.MiB, 0.1 * units.MiB, 0},
	})

	for range 100 {
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
	checkHotReadRegionScheduleWithPendingInfluence(re, 0) // 0: byte rate
	checkHotReadRegionScheduleWithPendingInfluence(re, 1) // 1: key rate
}

func checkHotReadRegionScheduleWithPendingInfluence(re *require.Assertions, dim int) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	// For test
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.GreatDecRatio = 0.99
	hb.(*hotScheduler).conf.MinorDecRatio = 1
	hb.(*hotScheduler).conf.DstToleranceRatio = 1
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	pendingAmpFactor = 0.0

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)

	updateStore := tc.UpdateStorageReadBytes // byte rate
	if dim == 1 {                            // key rate
		updateStore = tc.UpdateStorageReadKeys
	}
	updateStore(1, 7.1*units.MiB*utils.StoreHeartBeatReportInterval)
	updateStore(2, 6.1*units.MiB*utils.StoreHeartBeatReportInterval)
	updateStore(3, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	updateStore(4, 5*units.MiB*utils.StoreHeartBeatReportInterval)

	if dim == 0 { // byte rate
		addRegionInfo(tc, utils.Read, []testRegionInfo{
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
		addRegionInfo(tc, utils.Read, []testRegionInfo{
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
	for range 20 {
		clearPendingInfluence(hb.(*hotScheduler))

		ops, _ := hb.Schedule(tc, false)
		op1 := ops[0]
		operatorutil.CheckTransferPeer(re, op1, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

		pendingAmpFactor = defaultPendingAmpFactor
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
	for range 20 {
		clearPendingInfluence(hb.(*hotScheduler))

		ops, _ := hb.Schedule(tc, false)
		op1 := ops[0]
		operatorutil.CheckTransferPeer(re, op1, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

		ops, _ = hb.Schedule(tc, false)
		op2 := ops[0]
		operatorutil.CheckTransferPeer(re, op2, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)
		re.True(op2.Cancel(operator.AdminStop))

		ops, _ = hb.Schedule(tc, false)
		op2 = ops[0]
		operatorutil.CheckTransferPeer(re, op2, operator.OpHotRegion, 1, 4)
		// After move-peer, store byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

		re.True(op1.Cancel(operator.AdminStop))
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

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.setStrictPickingStore(false)
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	tc.AddRegionStore(6, 20)

	// no uniform among four stores
	tc.UpdateStorageReadStats(1, 10.05*units.MB*utils.StoreHeartBeatReportInterval, 10.05*units.MB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 10.05*units.MB*utils.StoreHeartBeatReportInterval, 10.05*units.MB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 10.05*units.MB*utils.StoreHeartBeatReportInterval, 10.05*units.MB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 0.0*units.MB*utils.StoreHeartBeatReportInterval, 0.0*units.MB*utils.StoreHeartBeatReportInterval)
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 5, 6}, 0.05 * units.MB, 0.05 * units.MB, 0},
	})
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	clearPendingInfluence(hb.(*hotScheduler))
	operatorutil.CheckTransferPeerWithLeaderTransfer(re, ops[0], operator.OpHotRegion|operator.OpLeader, 1, 4)
	// two dim are both enough uniform among three stores
	tc.SetStoreEvictLeader(4, true)
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	clearPendingInfluence(hb.(*hotScheduler))
}

func TestHotCacheUpdateCache(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, _ := prepareSchedulersTest()
	defer cancel()
	for i := range 3 {
		tc.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i + 1)}))
	}

	// For read flow
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{2, []uint64{2, 1, 3}, 512 * units.KiB, 0, 0},
		{3, []uint64{1, 2, 3}, 20 * units.KiB, 0, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{11, []uint64{1, 2, 3}, 7 * units.KiB, 0, 0},
	})
	stats := tc.HotCache.GetHotPeerStats(utils.Read, 0)
	re.Len(stats[1], 3)
	re.Len(stats[2], 3)
	re.Len(stats[3], 3)

	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{3, []uint64{2, 1, 3}, 20 * units.KiB, 0, 0},
		{11, []uint64{1, 2, 3}, 7 * units.KiB, 0, 0},
	})
	stats = tc.HotCache.GetHotPeerStats(utils.Read, 0)
	re.Len(stats[1], 3)
	re.Len(stats[2], 3)
	re.Len(stats[3], 3)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{4, []uint64{1, 2, 3}, 512 * units.KiB, 0, 0},
		{5, []uint64{1, 2, 3}, 20 * units.KiB, 0, 0},
		{6, []uint64{1, 2, 3}, 0.8 * units.KiB, 0, 0},
	})
	stats = tc.HotCache.GetHotPeerStats(utils.Write, 0)
	re.Len(stats[1], 2)
	re.Len(stats[2], 2)
	re.Len(stats[3], 2)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{5, []uint64{1, 2, 5}, 20 * units.KiB, 0, 0},
	})
	stats = tc.HotCache.GetHotPeerStats(utils.Write, 0)

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
	stats = tc.HotCache.GetHotPeerStats(utils.Read, 0)
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
		for i := range 6 {
			tc.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i + 1)}))
		}
		addRegionInfo(tc, utils.Read, []testRegionInfo{
			{1, []uint64{1, 2, 3}, 0, 1, 0},
			{2, []uint64{1, 2, 3}, 0, 1 * units.KiB, 0},
		})
		stats := tc.HotCache.GetHotPeerStats(utils.Read, 0)
		re.Len(stats[1], 1)
		addRegionInfo(tc, utils.Write, []testRegionInfo{
			{3, []uint64{4, 5, 6}, 0, 1, 0},
			{4, []uint64{4, 5, 6}, 0, 1 * units.KiB, 0},
		})
		stats = tc.HotCache.GetHotPeerStats(utils.Write, 0)
		re.Len(stats[4], 1)
		re.Len(stats[5], 1)
		re.Len(stats[6], 1)
	}
	{ // many regions
		cancel, _, tc, _ := prepareSchedulersTest()
		defer cancel()
		for i := range 3 {
			tc.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i + 1)}))
		}
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
			addRegionInfo(tc, utils.Read, regions)
			stats := tc.HotCache.GetHotPeerStats(utils.Read, 0)
			re.Greater(len(stats[1]), 500)

			// for AntiCount
			addRegionInfo(tc, utils.Read, regions)
			addRegionInfo(tc, utils.Read, regions)
			addRegionInfo(tc, utils.Read, regions)
			addRegionInfo(tc, utils.Read, regions)
			stats = tc.HotCache.GetHotPeerStats(utils.Read, 0)
			re.Len(stats[1], 500)
		}
		{ // write
			addRegionInfo(tc, utils.Write, regions)
			stats := tc.HotCache.GetHotPeerStats(utils.Write, 0)
			re.Greater(len(stats[1]), 500)
			re.Greater(len(stats[2]), 500)
			re.Greater(len(stats[3]), 500)

			// for AntiCount
			addRegionInfo(tc, utils.Write, regions)
			addRegionInfo(tc, utils.Write, regions)
			addRegionInfo(tc, utils.Write, regions)
			addRegionInfo(tc, utils.Write, regions)
			stats = tc.HotCache.GetHotPeerStats(utils.Write, 0)
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
	for i := range 3 {
		tc.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i + 1)}))
	}
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
		addRegionInfo(tc, utils.Read, regions)
		stats := tc.HotCache.GetHotPeerStats(utils.Read, 0)
		re.Len(stats[1], 500)

		addRegionInfo(tc, utils.Read, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * units.KiB, 10 * units.KiB, 0},
			{10002, []uint64{1, 2, 3}, 500 * units.KiB, 10 * units.KiB, 0},
			{10003, []uint64{1, 2, 3}, 10 * units.KiB, 500 * units.KiB, 0},
			{10004, []uint64{1, 2, 3}, 500 * units.KiB, 500 * units.KiB, 0},
		})
		stats = tc.HotCache.GetHotPeerStats(utils.Read, 0)
		re.Len(stats[1], 503)
	}
	{ // write
		addRegionInfo(tc, utils.Write, regions)
		stats := tc.HotCache.GetHotPeerStats(utils.Write, 0)
		re.Len(stats[1], 500)
		re.Len(stats[2], 500)
		re.Len(stats[3], 500)
		addRegionInfo(tc, utils.Write, []testRegionInfo{
			{10001, []uint64{1, 2, 3}, 10 * units.KiB, 10 * units.KiB, 0},
			{10002, []uint64{1, 2, 3}, 500 * units.KiB, 10 * units.KiB, 0},
			{10003, []uint64{1, 2, 3}, 10 * units.KiB, 500 * units.KiB, 0},
			{10004, []uint64{1, 2, 3}, 500 * units.KiB, 500 * units.KiB, 0},
		})
		stats = tc.HotCache.GetHotPeerStats(utils.Write, 0)
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

func addRegionInfo(tc *mockcluster.Cluster, rwTy utils.RWType, regions []testRegionInfo) {
	addFunc := tc.AddRegionWithReadInfo
	if rwTy == utils.Write {
		addFunc = tc.AddLeaderRegionWithWriteInfo
	}
	reportIntervalSecs := utils.RegionHeartBeatReportInterval
	if rwTy == utils.Read {
		reportIntervalSecs = utils.StoreHeartBeatReportInterval
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
	reportIntervalSecs := utils.StoreHeartBeatReportInterval
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
	kind                      utils.RWType
	onlyLeader                bool
	DegreeAfterTransferLeader int
}

func TestHotCacheCheckRegionFlow(t *testing.T) {
	re := require.New(t)
	testCases := []testHotCacheCheckRegionFlowCase{
		{
			kind:                      utils.Write,
			onlyLeader:                false,
			DegreeAfterTransferLeader: 3,
		},
		{
			kind:                      utils.Read,
			onlyLeader:                false,
			DegreeAfterTransferLeader: 4,
		},
		{
			kind:                      utils.Read,
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
	for i := range 3 {
		tc.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i + 1)}))
	}
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	sche, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	heartbeat := tc.AddLeaderRegionWithWriteInfo
	if testCase.kind == utils.Read {
		if testCase.onlyLeader {
			heartbeat = tc.AddRegionLeaderWithReadInfo
		} else {
			heartbeat = tc.AddRegionWithReadInfo
		}
	}
	tc.AddRegionStore(2, 20)
	tc.UpdateStorageReadStats(2, 9.5*units.MiB*utils.StoreHeartBeatReportInterval, 9.5*units.MiB*utils.StoreHeartBeatReportInterval)
	reportInterval := uint64(utils.RegionHeartBeatReportInterval)
	if testCase.kind == utils.Read {
		reportInterval = uint64(utils.StoreHeartBeatReportInterval)
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
		typ := toResourceType(testCase.kind, transferLeader)
		hb.prepareForBalance(typ, tc)
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
			re.Equal(utils.Remove, item.GetActionType())
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
	for i := range 3 {
		tc.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i + 1)}))
	}
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	statistics.ThresholdsUpdateInterval = 0
	defer func() {
		statistics.ThresholdsUpdateInterval = utils.StoreHeartBeatReportInterval
	}()
	// some peers are hot, and some are cold #3198

	rate := uint64(512 * units.KiB)
	for i := range statistics.TopNN {
		for range utils.DefaultAotSize {
			tc.AddLeaderRegionWithWriteInfo(uint64(i+100), 1, rate*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3}, 1)
		}
	}
	items := tc.AddLeaderRegionWithWriteInfo(201, 1, rate*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3}, 1)
	re.Equal(float64(rate)*statistics.HotThresholdRatio, tc.HotCache.GetThresholds(utils.Write, items[0].StoreID)[0])
	// Threshold of store 1,2,3 is 409.6 units.KiB and others are 1 units.KiB
	// Make the hot threshold of some store is high and the others are low
	rate = 10 * units.KiB
	tc.AddLeaderRegionWithWriteInfo(201, 1, rate*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3, 4}, 1)
	items = tc.AddLeaderRegionWithWriteInfo(201, 1, rate*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{3, 4}, 1)
	for _, item := range items {
		if item.StoreID < 4 {
			re.Equal(utils.Remove, item.GetActionType())
		} else {
			re.Equal(utils.Update, item.GetActionType())
		}
	}
}

func TestHotCacheSortHotPeer(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	sche, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	leaderSolver := newBalanceSolver(hb, tc, utils.Read, transferLeader)
	hotPeers := []*statistics.HotPeerStat{{
		RegionID: 1,
		Loads: []float64{
			utils.QueryDim: 10,
			utils.ByteDim:  1,
		},
	}, {
		RegionID: 2,
		Loads: []float64{
			utils.QueryDim: 1,
			utils.ByteDim:  10,
		},
	}, {
		RegionID: 3,
		Loads: []float64{
			utils.QueryDim: 5,
			utils.ByteDim:  6,
		},
	}}

	st := &statistics.StoreLoadDetail{
		HotPeers: hotPeers,
	}
	leaderSolver.maxPeerNum = 1
	u := leaderSolver.filterHotPeers(st)
	checkSortResult(re, []uint64{1}, u)

	leaderSolver.maxPeerNum = 2
	u = leaderSolver.filterHotPeers(st)
	checkSortResult(re, []uint64{1, 2}, u)
}

func checkSortResult(re *require.Assertions, regions []uint64, hotPeers []*statistics.HotPeerStat) {
	re.Equal(len(hotPeers), len(regions))
	for _, region := range regions {
		in := false
		for _, hotPeer := range hotPeers {
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
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.UpdateStorageWrittenStats(1, 99*units.MiB*utils.StoreHeartBeatReportInterval, 99*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 50*units.MiB*utils.StoreHeartBeatReportInterval, 98*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 2*units.MiB*utils.StoreHeartBeatReportInterval, 2*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 1*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
	})
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{2, 1, 3}, 0.5 * units.MiB, 0.5 * units.MiB, 0},
	})
	// must move peer from 1 to 4
	ops, _ := hb.Schedule(tc, false)
	op := ops[0]
	re.NotNil(op)

	storeInfos := statistics.SummaryStoreInfos(tc.GetStores())
	hb.(*hotScheduler).summaryPendingInfluence(storeInfos)
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionWriteKeys], -0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionWriteBytes], -0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[4].PendingSum.Loads[utils.RegionWriteKeys], 0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[4].PendingSum.Loads[utils.RegionWriteBytes], 0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionReadKeys], -0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionReadBytes], -0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[4].PendingSum.Loads[utils.RegionReadKeys], 0.5*units.MiB))
	re.True(nearlyAbout(storeInfos[4].PendingSum.Loads[utils.RegionReadBytes], 0.5*units.MiB))

	// consider pending amp, there are nine regions or more.
	for i := 2; i < 13; i++ {
		addRegionInfo(tc, utils.Write, []testRegionInfo{
			{uint64(i), []uint64{1, 2, 3}, 0.7 * units.MiB, 0.7 * units.MiB, 0},
		})
	}

	// must transfer leader
	hb.(*hotScheduler).types = []resourceType{writeLeader}
	// must transfer leader from 1 to 3
	ops, _ = hb.Schedule(tc, false)
	op = ops[0]
	re.NotNil(op)

	storeInfos = statistics.SummaryStoreInfos(tc.GetStores())
	hb.(*hotScheduler).summaryPendingInfluence(storeInfos)
	// assert read/write influence is the sum of write peer and write leader
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionWriteKeys], -1.2*units.MiB))
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionWriteBytes], -1.2*units.MiB))
	re.True(nearlyAbout(storeInfos[3].PendingSum.Loads[utils.RegionWriteKeys], 0.7*units.MiB))
	re.True(nearlyAbout(storeInfos[3].PendingSum.Loads[utils.RegionWriteBytes], 0.7*units.MiB))
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionReadKeys], -1.2*units.MiB))
	re.True(nearlyAbout(storeInfos[1].PendingSum.Loads[utils.RegionReadBytes], -1.2*units.MiB))
	re.True(nearlyAbout(storeInfos[3].PendingSum.Loads[utils.RegionReadKeys], 0.7*units.MiB))
	re.True(nearlyAbout(storeInfos[3].PendingSum.Loads[utils.RegionReadBytes], 0.7*units.MiB))
}

func nearlyAbout(f1, f2 float64) bool {
	if f1-f2 < 0.1*units.KiB || f2-f1 < 0.1*units.KiB {
		return true
	}
	return false
}

func loadsEqual(loads1, loads2 []float64) bool {
	if len(loads1) != utils.DimLen || len(loads2) != utils.DimLen {
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

	sche, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}

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

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	hb.(*hotScheduler).conf.setDstToleranceRatio(1.05)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1.05)
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	// skip stddev check
	stddevThreshold = -1.0

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	tc.UpdateStorageWrittenStats(1, 10*units.MiB*utils.StoreHeartBeatReportInterval, 9*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6*units.MiB*utils.StoreHeartBeatReportInterval, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*units.MiB*utils.StoreHeartBeatReportInterval, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 2 * units.MiB, 1 * units.MiB, 0},
		{6, []uint64{4, 2, 3}, 1 * units.MiB, 2 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.BytePriority, utils.KeyPriority}
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 1, 5)
	clearPendingInfluence(hb.(*hotScheduler))
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.KeyPriority, utils.BytePriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 4, 5)
	clearPendingInfluence(hb.(*hotScheduler))

	// assert read priority schedule
	hb, err = CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	tc.UpdateStorageReadStats(5, 10*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 10*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(1, 10*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 1*units.MiB*utils.StoreHeartBeatReportInterval, 7*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 7*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	addRegionInfo(tc, utils.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 2 * units.MiB, 2 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.BytePriority, utils.KeyPriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.KeyPriority, utils.BytePriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 3)

	hb, err = CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)
	re.NoError(err)

	// assert loose store picking
	tc.UpdateStorageWrittenStats(1, 10*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6.1*units.MiB*utils.StoreHeartBeatReportInterval, 6.1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*units.MiB*utils.StoreHeartBeatReportInterval, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 6*units.MiB*utils.StoreHeartBeatReportInterval, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.BytePriority, utils.KeyPriority}
	hb.(*hotScheduler).conf.StrictPickingStore = true
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	hb.(*hotScheduler).conf.StrictPickingStore = false
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 1, 5)
	clearPendingInfluence(hb.(*hotScheduler))

	tc.UpdateStorageWrittenStats(1, 6*units.MiB*utils.StoreHeartBeatReportInterval, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 6.1*units.MiB*utils.StoreHeartBeatReportInterval, 6.1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 6*units.MiB*utils.StoreHeartBeatReportInterval, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 1*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 1*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.KeyPriority, utils.BytePriority}
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
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).types = []resourceType{writePeer}
	hb.(*hotScheduler).conf.setDstToleranceRatio(1.0)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1.0)
	hb.(*hotScheduler).conf.RankFormulaVersion = "v1"
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.(*hotScheduler).conf.StrictPickingStore = false
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	// skip uniform cluster
	tc.UpdateStorageWrittenStats(1, 5*units.MiB*utils.StoreHeartBeatReportInterval, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 5.3*units.MiB*utils.StoreHeartBeatReportInterval, 5.3*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 5*units.MiB*utils.StoreHeartBeatReportInterval, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 5*units.MiB*utils.StoreHeartBeatReportInterval, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 4.8*units.MiB*utils.StoreHeartBeatReportInterval, 4.8*units.MiB*utils.StoreHeartBeatReportInterval)
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{6, []uint64{3, 4, 2}, 0.1 * units.MiB, 0.1 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.BytePriority, utils.KeyPriority}
	stddevThreshold = 0.1
	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
	stddevThreshold = -1.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	clearPendingInfluence(hb.(*hotScheduler))

	// skip -1 case (uniform cluster)
	tc.UpdateStorageWrittenStats(1, 5*units.MiB*utils.StoreHeartBeatReportInterval, 100*units.MiB*utils.StoreHeartBeatReportInterval) // two dims are not uniform.
	tc.UpdateStorageWrittenStats(2, 5.3*units.MiB*utils.StoreHeartBeatReportInterval, 4.8*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 5*units.MiB*utils.StoreHeartBeatReportInterval, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 5*units.MiB*utils.StoreHeartBeatReportInterval, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 4.8*units.MiB*utils.StoreHeartBeatReportInterval, 5*units.MiB*utils.StoreHeartBeatReportInterval)
	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{6, []uint64{3, 4, 2}, 0.1 * units.MiB, 0.1 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.BytePriority, utils.KeyPriority}
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
	pendingAmpFactor = 0.0
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).types = []resourceType{writeLeader}
	hb.(*hotScheduler).conf.setDstToleranceRatio(1)
	hb.(*hotScheduler).conf.setSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.setHistorySampleDuration(0)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.UpdateStorageWrittenStats(1, 31*units.MiB*utils.StoreHeartBeatReportInterval, 31*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*units.MiB*utils.StoreHeartBeatReportInterval, 1*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 1*units.MiB*utils.StoreHeartBeatReportInterval, 10*units.MiB*utils.StoreHeartBeatReportInterval)

	addRegionInfo(tc, utils.Write, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 10 * units.MiB, 10 * units.MiB, 0},
		{2, []uint64{1, 2, 3}, 10 * units.MiB, 10 * units.MiB, 0},
		{3, []uint64{1, 2, 3}, 10 * units.MiB, 10 * units.MiB, 0},
		{4, []uint64{2, 1, 3}, 10 * units.MiB, 0 * units.MiB, 0},
		{5, []uint64{3, 2, 1}, 0 * units.MiB, 10 * units.MiB, 0},
	})
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.KeyPriority, utils.BytePriority}
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.BytePriority, utils.KeyPriority}
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 3)
}

func TestCompatibility(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	hb, err := CreateScheduler(writeType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	// default
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.QueryDim, utils.ByteDim},
		{utils.QueryDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})
	// config error value
	hb.(*hotScheduler).conf.ReadPriorities = []string{"error"}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{"error", utils.BytePriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.QueryPriority, utils.BytePriority, utils.KeyPriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.QueryDim, utils.ByteDim},
		{utils.QueryDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})
	// low version
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version5_0))
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.ByteDim, utils.KeyDim},
		{utils.KeyDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})
	// config byte and key
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.KeyPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.BytePriority, utils.KeyPriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.KeyPriority, utils.BytePriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.KeyDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
		{utils.KeyDim, utils.ByteDim},
	})
	// config query in low version
	hb.(*hotScheduler).conf.ReadPriorities = []string{utils.QueryPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{utils.QueryPriority, utils.BytePriority}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.QueryPriority, utils.BytePriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.ByteDim, utils.KeyDim},
		{utils.KeyDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})
	// config error value
	hb.(*hotScheduler).conf.ReadPriorities = []string{"error", "error"}
	hb.(*hotScheduler).conf.WriteLeaderPriorities = []string{}
	hb.(*hotScheduler).conf.WritePeerPriorities = []string{utils.QueryPriority, utils.BytePriority, utils.KeyPriority}
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.ByteDim, utils.KeyDim},
		{utils.KeyDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})
	// test version change
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.HotScheduleWithQuery))
	re.False(hb.(*hotScheduler).conf.lastQuerySupported) // it will updated after scheduling
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.QueryDim, utils.ByteDim},
		{utils.QueryDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})
	re.True(hb.(*hotScheduler).conf.lastQuerySupported)
}

func TestCompatibilityConfig(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	// From new or 3.x cluster, it will use new config
	hb, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.QueryDim, utils.ByteDim},
		{utils.QueryDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})

	// Config file is not currently supported
	hb, err = CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.BalanceHotRegionScheduler, []string{"read-priorities=byte,query"}))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.QueryDim, utils.ByteDim},
		{utils.QueryDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})

	// from 4.0 or 5.0 or 5.1 cluster
	var data []byte
	storage := storage.NewStorageWithMemoryBackend()
	data, err = EncodeConfig(map[string]any{
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
	err = storage.SaveSchedulerConfig(types.BalanceHotRegionScheduler.String(), data)
	re.NoError(err)
	hb, err = CreateScheduler(types.BalanceHotRegionScheduler, oc, storage, ConfigJSONDecoder(data))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.ByteDim, utils.KeyDim},
		{utils.KeyDim, utils.ByteDim},
		{utils.ByteDim, utils.KeyDim},
	})

	// From configured cluster
	cfg := initHotRegionScheduleConfig()
	cfg.ReadPriorities = []string{"key", "query"}
	cfg.WriteLeaderPriorities = []string{"query", "key"}
	data, err = EncodeConfig(cfg)
	re.NoError(err)
	err = storage.SaveSchedulerConfig(types.BalanceHotRegionScheduler.String(), data)
	re.NoError(err)
	hb, err = CreateScheduler(types.BalanceHotRegionScheduler, oc, storage, ConfigJSONDecoder(data))
	re.NoError(err)
	checkPriority(re, hb.(*hotScheduler), tc, [3][2]int{
		{utils.KeyDim, utils.QueryDim},
		{utils.QueryDim, utils.KeyDim},
		{utils.ByteDim, utils.KeyDim},
	})
}

func checkPriority(re *require.Assertions, hb *hotScheduler, tc *mockcluster.Cluster, dims [3][2]int) {
	readSolver := newBalanceSolver(hb, tc, utils.Read, transferLeader)
	writeLeaderSolver := newBalanceSolver(hb, tc, utils.Write, transferLeader)
	writePeerSolver := newBalanceSolver(hb, tc, utils.Write, movePeer)
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
	err := hc.validateLocked()
	re.NoError(err)

	// priorities is illegal
	hc.ReadPriorities = []string{"byte", "error"}
	err = hc.validateLocked()
	re.Error(err)

	// priorities should have at least 2 dimensions
	hc = initHotRegionScheduleConfig()
	hc.WriteLeaderPriorities = []string{"byte"}
	err = hc.validateLocked()
	re.Error(err)

	// query is not allowed to be set in priorities for write-peer-priorities
	hc = initHotRegionScheduleConfig()
	hc.WritePeerPriorities = []string{"query", "byte"}
	err = hc.validateLocked()
	re.Error(err)
	// priorities shouldn't be repeated
	hc.WritePeerPriorities = []string{"byte", "byte"}
	err = hc.validateLocked()
	re.Error(err)
	// no error
	hc.WritePeerPriorities = []string{"byte", "key"}
	err = hc.validateLocked()
	re.NoError(err)

	// rank-formula-version
	// default
	hc = initHotRegionScheduleConfig()
	re.Equal("v2", hc.getRankFormulaVersion())
	// v1
	hc.RankFormulaVersion = "v1"
	err = hc.validateLocked()
	re.NoError(err)
	re.Equal("v1", hc.getRankFormulaVersion())
	// v2
	hc.RankFormulaVersion = "v2"
	err = hc.validateLocked()
	re.NoError(err)
	re.Equal("v2", hc.getRankFormulaVersion())
	// illegal
	hc.RankFormulaVersion = "v0"
	err = hc.validateLocked()
	re.Error(err)

	// forbid-rw-type
	// default
	hc = initHotRegionScheduleConfig()
	re.False(hc.isForbidRWType(utils.Read))
	re.False(hc.isForbidRWType(utils.Write))
	// read
	hc.ForbidRWType = "read"
	err = hc.validateLocked()
	re.NoError(err)
	re.True(hc.isForbidRWType(utils.Read))
	re.False(hc.isForbidRWType(utils.Write))
	// write
	hc.ForbidRWType = "write"
	err = hc.validateLocked()
	re.NoError(err)
	re.False(hc.isForbidRWType(utils.Read))
	re.True(hc.isForbidRWType(utils.Write))
	// illegal
	hc.ForbidRWType = "test"
	err = hc.validateLocked()
	re.Error(err)

	hc.SplitThresholds = 0
	err = hc.validateLocked()
	re.Error(err)

	hc.SplitThresholds = 1.1
	err = hc.validateLocked()
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
	hb, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
	re.NoError(err)
	maxZombieDur := hb.(*hotScheduler).conf.getValidConf().MaxZombieRounds
	testCases := []maxZombieDurTestCase{
		{
			typ:          readPeer,
			maxZombieDur: maxZombieDur * utils.StoreHeartBeatReportInterval,
		},
		{
			typ:          readLeader,
			maxZombieDur: maxZombieDur * utils.StoreHeartBeatReportInterval,
		},
		{
			typ:          writePeer,
			maxZombieDur: maxZombieDur * utils.StoreHeartBeatReportInterval,
		},
		{
			typ:          writePeer,
			isTiFlash:    true,
			maxZombieDur: maxZombieDur * utils.RegionHeartBeatReportInterval,
		},
		{
			typ:           writeLeader,
			firstPriority: utils.KeyDim,
			maxZombieDur:  maxZombieDur * utils.RegionHeartBeatReportInterval,
		},
		{
			typ:           writeLeader,
			firstPriority: utils.QueryDim,
			maxZombieDur:  maxZombieDur * utils.StoreHeartBeatReportInterval,
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
	hb, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
	re.NoError(err)
	testCases := []struct {
		rankVersion    string
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
			rankVersion: "v1",
			strict:      true, // all of
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
			rankVersion: "v1",
			strict:      true, // all of
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
			rankVersion: "v1",
			strict:      true, // all of
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
			rankVersion: "v1",
			strict:      true, // all of
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
			rankVersion: "v1",
			strict:      false, // first only
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
			rankVersion: "v1",
			strict:      false, // first only
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
			rankVersion: "v1",
			strict:      false, // first only
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
			rankVersion: "v1",
			strict:      false, // first only
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
			rankVersion: "v1",
			strict:      true,
			rs:          writeLeader,
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
			rankVersion: "v1",
			strict:      true,
			rs:          writeLeader,
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
			rankVersion: "v1",
			strict:      true,
			rs:          writeLeader,
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
			rankVersion: "v2",
			strict:      false, // any of
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
			rankVersion: "v2",
			strict:      false, // any of
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
			rankVersion: "v2",
			strict:      false, // any of
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
			rankVersion: "v2",
			strict:      false, // any of
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
			rankVersion: "v2",
			strict:      true,
			rs:          writeLeader,
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
			rankVersion: "v2",
			strict:      true,
			rs:          writeLeader,
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
			firstPriority:  utils.KeyDim,
			secondPriority: utils.ByteDim,
			resourceTy:     testCase.rs,
		}
		if testCase.rankVersion == "v1" {
			bs.rank = initRankV1(bs)
		} else {
			bs.rank = initRankV2(bs)
		}

		bs.sche.conf.StrictPickingStore = testCase.strict
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
	sche, err := CreateScheduler(types.BalanceHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigJSONDecoder([]byte("null")))
	re.NoError(err)
	data, err := sche.EncodeConfig()
	re.NoError(err)
	re.NotEqual("null", string(data))
}

func TestBucketFirstStat(t *testing.T) {
	re := require.New(t)
	testdata := []struct {
		firstPriority  int
		secondPriority int
		rwTy           utils.RWType
		expect         utils.RegionStatKind
	}{
		{
			firstPriority:  utils.KeyDim,
			secondPriority: utils.ByteDim,
			rwTy:           utils.Write,
			expect:         utils.RegionWriteKeys,
		},
		{
			firstPriority:  utils.QueryDim,
			secondPriority: utils.ByteDim,
			rwTy:           utils.Write,
			expect:         utils.RegionWriteBytes,
		},
		{
			firstPriority:  utils.KeyDim,
			secondPriority: utils.ByteDim,
			rwTy:           utils.Read,
			expect:         utils.RegionReadKeys,
		},
		{
			firstPriority:  utils.QueryDim,
			secondPriority: utils.ByteDim,
			rwTy:           utils.Read,
			expect:         utils.RegionReadBytes,
		},
	}
	for _, data := range testdata {
		bs := &balanceSolver{
			firstPriority:  data.firstPriority,
			secondPriority: data.secondPriority,
			rwTy:           data.rwTy,
		}
		re.Equal(data.expect, bs.bucketFirstStat())
	}
}
