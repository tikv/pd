// Copyright 2024 TiKV Project Authors.
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
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
)

type balanceKeyrangeSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *operator.Controller
	conf   config.SchedulerConfigProvider
}

func TestBalanceKeyrangeSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceKeyrangeSchedulerTestSuite))
}

func (suite *balanceKeyrangeSchedulerTestSuite) SetupTest() {
	suite.cancel, suite.conf, suite.tc, suite.oc = prepareSchedulersTest()
}

func (suite *balanceKeyrangeSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeNormal() {
	re := suite.Require()

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	sb, err := CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", "100000", "", ""}))
	re.NoError(err)

	tc.AddLabelsStore(10, 16, map[string]string{"engine": "tiflash"})
	tc.AddLabelsStore(11, 16, map[string]string{})
	tc.AddLabelsStore(12, 16, map[string]string{})

	tc.AddLeaderRegion(1, 10, 11)
	tc.AddLeaderRegion(2, 10, 11)
	tc.AddLeaderRegion(3, 10, 12)

	ops, _ := sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	op := ops[0]
	operatorutil.CheckTransferPeer(re, op, operator.OpKind(0), 10, 12)

	sb, err = CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", "100000", url.QueryEscape("11"), ""}))
	ops, _ = sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	re.Empty(ops)

	sb, err = CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", "", "100000", url.QueryEscape("21")}))
	ops, _ = sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	re.Empty(ops)

	sb, err = CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "[{\"key\":\"engine\",\"value\":\"tiflash\"}]", "100000", "", ""}))
	ops, _ = sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	re.Empty(ops)
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeLabel() {
	re := suite.Require()

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	tc.AddLabelsStore(10, 16, map[string]string{"engine": "tiflash"})
	tc.AddLabelsStore(11, 16, map[string]string{})
	tc.AddLabelsStore(12, 16, map[string]string{"engine": "tiflash", "label1": "value1"})

	tc.AddLeaderRegion(1, 10, 11)
	tc.AddLeaderRegion(2, 10, 11)
	tc.AddLeaderRegion(3, 10, 11)
	tc.AddLeaderRegion(4, 10, 11)
	tc.AddLeaderRegion(5, 10, 12)

	sb, err := CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"5", "[{\"key\":\"engine\",\"value\":\"tiflash\"}]", "100000", "", ""}))
	re.NoError(err)

	ops, _ := sb.Schedule(tc, false)
	re.Equal(2, len(ops))

	sb, err = CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"5", "[{\"key\":\"engine\",\"value\":\"tiflash\"},{\"label1\": \"value1\"}]", "100000", "", ""}))
	re.NoError(err)

	ops, _ = sb.Schedule(tc, false)
	re.Empty(ops)
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeFinish() {
	re := suite.Require()

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	sb, err := CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", "100000", "", ""}))
	re.NoError(err)

	tc.AddLabelsStore(10, 16, map[string]string{"engine": "tiflash"})
	tc.AddLabelsStore(11, 16, map[string]string{})
	tc.AddLabelsStore(12, 16, map[string]string{})

	tc.AddLeaderRegion(1, 10, 11)
	tc.AddLeaderRegion(2, 10, 11)
	tc.AddLeaderRegion(3, 10, 11)
	tc.AddLeaderRegion(4, 10, 11)
	tc.AddLeaderRegion(5, 10, 11)
	tc.AddLeaderRegion(6, 10, 12)

	// store 10: 1 2 3 4 5 6
	// store 11: 1 2 3 4 5
	// store 12: 6
	// ==>
	// store 10: 3 4 5 6
	// store 11: 1 2 3 4
	// store 12: 1 2 5 6

	sb.Schedule(tc, false)
	re.False(sb.IsFinished())
	sb.Schedule(tc, false)
	re.False(sb.IsFinished())
	sb.Schedule(tc, false)
	re.True(sb.IsFinished())
	sb.Schedule(tc, false)
	sb.Schedule(tc, false)
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeConfChanged() {
	// TODO
}

func (suite *balanceKeyrangeSchedulerTestSuite) TestBalanceKeyrangeConfTimeout() {
	re := suite.Require()

	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	sb, err := CreateScheduler(types.BalanceKeyrangeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceKeyrangeScheduler, []string{"1", "", "1000", "", ""}))
	re.NoError(err)

	tc.AddLabelsStore(10, 16, map[string]string{"engine": "tiflash"})
	tc.AddLabelsStore(11, 16, map[string]string{})
	tc.AddLabelsStore(12, 16, map[string]string{})

	tc.AddLeaderRegion(1, 10, 11)
	tc.AddLeaderRegion(2, 10, 11)
	tc.AddLeaderRegion(3, 10, 11)
	tc.AddLeaderRegion(4, 10, 11)
	tc.AddLeaderRegion(5, 10, 11)
	tc.AddLeaderRegion(6, 10, 12)

	sb.Schedule(tc, false)
	re.False(sb.IsFinished())
	time.Sleep(time.Second * 2)
	sb.Schedule(tc, false)
	re.True(sb.IsFinished())
}

func assertvalidateMigrationPlan(re *require.Assertions, ops []*MigrationOp, storeIDs []uint64, regions []*core.RegionInfo, storeCounts []int) {
	storesIn := make(map[uint64]int)
	storesOut := make(map[uint64]int)
	regionMap := make(map[uint64]*core.RegionInfo)
	for _, storeId := range storeIDs {
		storesIn[storeId] = 0
		storesOut[storeId] = 0
	}
	for _, r := range regions {
		regionMap[r.GetID()] = r
	}
	for _, op := range ops {
		log.Info("!!! r", zap.Any("to", op.ToStore), zap.Any("fr", op.FromStore))
		storesIn[op.ToStore] += len(op.Regions)
		storesOut[op.FromStore] += len(op.Regions)
		// For each region in migration plan, it no longer exists in FromStore, and exists in ToStore
		for rid, _ := range op.Regions {
			inTo := false
			inFr := false
			r, ok := regionMap[rid]
			re.True(ok)
			for _, p := range r.GetPeers() {
				if p.StoreId == op.ToStore {
					inTo = true
				}
				if p.StoreId == op.FromStore {
					inFr = true
				}
			}
			re.True(inFr)
			re.False(inTo)
		}
	}
	storeList := []int{}
	for _, storeId := range storeIDs {
		in := storesIn[storeId]
		out := storesOut[storeId]
		re.True(in == 0 || out == 0)
		log.Info("!!! f", zap.Any("storeId", storeId), zap.Any("in", in), zap.Any("out", out))
		storeList = append(storeList, in-out)
	}
	re.Equal(storeCounts, storeList)
}

type regionStoresPair struct {
	RegionId uint64
	StorePos []uint64
}

func buildRedistributeRegionsTestCases(storeIDs []uint64, regionDist []regionStoresPair) ([]*metapb.Store, []*core.RegionInfo) {
	stores := []*metapb.Store{}
	regions := []*core.RegionInfo{}
	for _, i := range storeIDs {
		stores = append(stores, &metapb.Store{
			Id:            i,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		})
	}

	var peerIdAllocator uint64
	peerIdAllocator = 10000
	for _, p := range regionDist {
		regionId := p.RegionId
		holdingStores := p.StorePos
		var peers []*metapb.Peer
		for _, storePos := range holdingStores {
			s := stores[storePos]
			peerIdAllocator += 1
			peers = append(peers, &metapb.Peer{
				StoreId: s.GetId(),
				Id:      peerIdAllocator,
			})
		}
		region := core.NewTestRegionInfo(regionId, stores[holdingStores[0]].GetId(), []byte(fmt.Sprintf("r%v", regionId)), []byte(fmt.Sprintf("r%v", regionId+1)), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetPeers(peers))
		regions = append(regions, region)
	}

	return stores, regions
}

func TestBalanceKeyrangeAlgorithm(t *testing.T) {
	re := require.New(t)
	// 10: 1 2 3
	// 11:
	// 12:
	storeIds := []uint64{10, 11, 12}
	stores, regions := buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0}},
		{2, []uint64{0}},
		{3, []uint64{0}},
	})
	s := ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ := BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{-2, 1, 1})

	// 10: 1 2 3 4 5 6
	// 11: 1 2 3 4 5
	// 12: 6
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0, 1}},
		{2, []uint64{0, 1}},
		{3, []uint64{0, 1}},
		{4, []uint64{0, 1}},
		{5, []uint64{0, 1}},
		{6, []uint64{0, 2}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{-2, -1, 3})

	// 10: 1 2
	// 11: 2 3
	// 12: 1 3
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0, 1}},
		{2, []uint64{1, 2}},
		{3, []uint64{2, 0}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{0, 0, 0})

	// 10: 1 2
	// 11: 3
	storeIds = []uint64{10, 11}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0}},
		{2, []uint64{0}},
		{3, []uint64{1}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{0, 0})

	// 10: 1
	// 11:
	// 12:
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{0, 0, 0})

	// 10:
	// 11:
	// 12:
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{0, 0, 0})

	// 10: 1 2 3 4 5
	// 11: 1 2 3 4 5
	// 12:
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0, 1}},
		{2, []uint64{0, 1}},
		{3, []uint64{0, 1}},
		{4, []uint64{0, 1}},
		{5, []uint64{0, 1}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{-1, -2, 3})

	// 10: 1 2 3 4 5
	// 11: 1 2 3 4 5
	storeIds = []uint64{10, 11}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0, 1}},
		{2, []uint64{0, 1}},
		{3, []uint64{0, 1}},
		{4, []uint64{0, 1}},
		{5, []uint64{0, 1}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{0, 0})

	// 10:
	// 11: 1 2 3 4 5 6 7 8 9
	// 12:
	// 13: 1 2 3 4 5 6 7 8 9
	storeIds = []uint64{10, 11, 12, 13}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{3, 1}},
		{2, []uint64{3, 1}},
		{3, []uint64{3, 1}},
		{4, []uint64{3, 1}},
		{5, []uint64{3, 1}},
		{6, []uint64{3, 1}},
		{7, []uint64{3, 1}},
		{8, []uint64{3, 1}},
		{9, []uint64{3, 1}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{4, -4, 4, -4})

	// Won't happen, since regions with in a table have the same replica count, however, test it in case.
	// 10: 1 2 3 4 5
	// 11: 1 2 3
	// 12:
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0, 1}},
		{2, []uint64{0, 1}},
		{3, []uint64{0, 1}},
		{4, []uint64{0}},
		{5, []uint64{0}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{-2, 0, 2})

	// Won't happen
	// 10: 1 2 3
	// 11: 3 4 5 6
	// 12:
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0}},
		{2, []uint64{0}},
		{3, []uint64{0, 1}},
		{4, []uint64{1}},
		{5, []uint64{1}},
		{6, []uint64{1}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{-1, -1, 2})

	// Won't happen because a region can't have two peers in a store.
	// 10: 1 1
	// 11: 2
	// 12:
	storeIds = []uint64{10, 11, 12}
	stores, regions = buildRedistributeRegionsTestCases(storeIds, []regionStoresPair{
		{1, []uint64{0, 0}},
		{2, []uint64{1}},
	})
	s = ComputeCandidateStores([]*metapb.StoreLabel{}, stores, regions)
	_, _, ops, _ = BuildMigrationPlan(s)
	assertvalidateMigrationPlan(re, ops, storeIds, regions, []int{0, 0, 0})
}
