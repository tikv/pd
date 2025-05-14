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

package schedulers

import (
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
)

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
		re.True(tc.CheckAsync(task))
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
	re.Len(hotPeers, len(regions))
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

func TestSortHotPeers(t *testing.T) {
	re := require.New(t)

	type testPeer struct {
		id int
	}

	peer1 := &testPeer{id: 1}
	peer2 := &testPeer{id: 2}
	peer3 := &testPeer{id: 3}
	peer4 := &testPeer{id: 4}

	tests := []struct {
		name       string
		firstSort  []*testPeer
		secondSort []*testPeer
		maxPeerNum int
		expected   []*testPeer
	}{
		{
			name:       "No duplicates, maxPeerNum greater than total peers",
			firstSort:  []*testPeer{peer1, peer2},
			secondSort: []*testPeer{peer3, peer4},
			maxPeerNum: 5,
			expected:   []*testPeer{peer1, peer3, peer2, peer4},
		},
		{
			name:       "No duplicates, maxPeerNum less than total peers",
			firstSort:  []*testPeer{peer1, peer2},
			secondSort: []*testPeer{peer3, peer4},
			maxPeerNum: 3,
			expected:   []*testPeer{peer1, peer3, peer2},
		},
		{
			name:       "Duplicates in both lists",
			firstSort:  []*testPeer{peer1, peer2},
			secondSort: []*testPeer{peer2, peer3},
			maxPeerNum: 3,
			expected:   []*testPeer{peer1, peer2, peer3},
		},
		{
			name:       "Empty firstSort",
			firstSort:  []*testPeer{},
			secondSort: []*testPeer{peer3, peer4},
			maxPeerNum: 2,
			expected:   []*testPeer{peer3, peer4},
		},
		{
			name:       "Empty secondSort",
			firstSort:  []*testPeer{peer1, peer2},
			secondSort: []*testPeer{},
			maxPeerNum: 2,
			expected:   []*testPeer{peer1, peer2},
		},
		{
			name:       "Both lists empty",
			firstSort:  []*testPeer{},
			secondSort: []*testPeer{},
			maxPeerNum: 2,
			expected:   []*testPeer{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			result := sortHotPeers(tt.firstSort, tt.secondSort, tt.maxPeerNum)
			re.Len(result, len(tt.expected))

			for _, expectedPeer := range tt.expected {
				_, exists := result[expectedPeer]
				re.True(exists, "Expected peer not found in result")
			}
		})
	}
}
