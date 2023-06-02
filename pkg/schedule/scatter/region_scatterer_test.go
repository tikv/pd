// Copyright 2021 TiKV Project Authors.
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

package scatter

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/versioninfo"
)

type sequencer struct {
	minID uint64
	maxID uint64
	curID uint64
}

func newSequencer(maxID uint64) *sequencer {
	return newSequencerWithMinID(1, maxID)
}

func newSequencerWithMinID(minID, maxID uint64) *sequencer {
	return &sequencer{
		minID: minID,
		maxID: maxID,
		curID: maxID,
	}
}

func (s *sequencer) next() uint64 {
	s.curID++
	if s.curID > s.maxID {
		s.curID = s.minID
	}
	return s.curID
}

func TestScatterRegions(t *testing.T) {
	re := require.New(t)
	scatter(re, 5, 50, true)
	scatter(re, 5, 500, true)
	scatter(re, 6, 50, true)
	scatter(re, 5, 50, false)
	scatterSpecial(re, 3, 6, 50)
	scatterSpecial(re, 5, 5, 50)
}

func checkOperator(re *require.Assertions, op *operator.Operator) {
	for i := 0; i < op.Len(); i++ {
		if rp, ok := op.Step(i).(operator.RemovePeer); ok {
			for j := i + 1; j < op.Len(); j++ {
				if tr, ok := op.Step(j).(operator.TransferLeader); ok {
					re.NotEqual(tr.FromStore, rp.FromStore)
					re.NotEqual(tr.ToStore, rp.FromStore)
				}
			}
		}
	}
}

func scatter(re *require.Assertions, numStores, numRegions uint64, useRules bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	// Add ordinary stores.
	for i := uint64(1); i <= numStores; i++ {
		tc.AddRegionStore(i, 0)
	}
	tc.SetEnablePlacementRules(useRules)

	for i := uint64(1); i <= numRegions; i++ {
		// region distributed in same stores.
		tc.AddLeaderRegion(i, 1, 2, 3)
	}
	scatterer := NewRegionScatterer(ctx, tc, oc)

	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		if op, _ := scatterer.Scatter(region, ""); op != nil {
			checkOperator(re, op)
			operator.ApplyOperator(tc, op)
		}
	}

	countPeers := make(map[uint64]uint64)
	countLeader := make(map[uint64]uint64)
	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		leaderStoreID := region.GetLeader().GetStoreId()
		for _, peer := range region.GetPeers() {
			countPeers[peer.GetStoreId()]++
			if peer.GetStoreId() == leaderStoreID {
				countLeader[peer.GetStoreId()]++
			}
		}
	}

	// Each store should have the same number of peers.
	for _, count := range countPeers {
		re.LessOrEqual(float64(count), 1.1*float64(numRegions*3)/float64(numStores))
		re.GreaterOrEqual(float64(count), 0.9*float64(numRegions*3)/float64(numStores))
	}

	// Each store should have the same number of leaders.
	re.Len(countPeers, int(numStores))
	re.Len(countLeader, int(numStores))
	for _, count := range countLeader {
		re.LessOrEqual(float64(count), 1.1*float64(numRegions)/float64(numStores))
		re.GreaterOrEqual(float64(count), 0.9*float64(numRegions)/float64(numStores))
	}
}

func scatterSpecial(re *require.Assertions, numOrdinaryStores, numSpecialStores, numRegions uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	// Add ordinary stores.
	for i := uint64(1); i <= numOrdinaryStores; i++ {
		tc.AddRegionStore(i, 0)
	}
	// Add special stores.
	for i := uint64(1); i <= numSpecialStores; i++ {
		tc.AddLabelsStore(numOrdinaryStores+i, 0, map[string]string{"engine": "tiflash"})
	}
	tc.SetEnablePlacementRules(true)
	re.NoError(tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd", ID: "learner", Role: placement.Learner, Count: 3,
		LabelConstraints: []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}}))

	// Region 1 has the same distribution with the Region 2, which is used to test selectPeerToReplace.
	tc.AddRegionWithLearner(1, 1, []uint64{2, 3}, []uint64{numOrdinaryStores + 1, numOrdinaryStores + 2, numOrdinaryStores + 3})
	for i := uint64(2); i <= numRegions; i++ {
		tc.AddRegionWithLearner(
			i,
			1,
			[]uint64{2, 3},
			[]uint64{numOrdinaryStores + 1, numOrdinaryStores + 2, numOrdinaryStores + 3},
		)
	}
	scatterer := NewRegionScatterer(ctx, tc, oc)

	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		if op, _ := scatterer.Scatter(region, ""); op != nil {
			checkOperator(re, op)
			operator.ApplyOperator(tc, op)
		}
	}

	countOrdinaryPeers := make(map[uint64]uint64)
	countSpecialPeers := make(map[uint64]uint64)
	countOrdinaryLeaders := make(map[uint64]uint64)
	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		leaderStoreID := region.GetLeader().GetStoreId()
		for _, peer := range region.GetPeers() {
			storeID := peer.GetStoreId()
			store := tc.Stores.GetStore(storeID)
			if store.GetLabelValue("engine") == "tiflash" {
				countSpecialPeers[storeID]++
			} else {
				countOrdinaryPeers[storeID]++
			}
			if peer.GetStoreId() == leaderStoreID {
				countOrdinaryLeaders[storeID]++
			}
		}
	}

	// Each store should have the same number of peers.
	for _, count := range countOrdinaryPeers {
		re.LessOrEqual(float64(count), 1.1*float64(numRegions*3)/float64(numOrdinaryStores))
		re.GreaterOrEqual(float64(count), 0.9*float64(numRegions*3)/float64(numOrdinaryStores))
	}
	for _, count := range countSpecialPeers {
		re.LessOrEqual(float64(count), 1.1*float64(numRegions*3)/float64(numSpecialStores))
		re.GreaterOrEqual(float64(count), 0.9*float64(numRegions*3)/float64(numSpecialStores))
	}
	for _, count := range countOrdinaryLeaders {
		re.LessOrEqual(float64(count), 1.1*float64(numRegions)/float64(numOrdinaryStores))
		re.GreaterOrEqual(float64(count), 0.9*float64(numRegions)/float64(numOrdinaryStores))
	}
}

func TestStoreLimit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)

	// Add stores 1~6.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}

	// Add regions 1~4.
	seq := newSequencer(3)
	// Region 1 has the same distribution with the Region 2, which is used to test selectPeerToReplace.
	tc.AddLeaderRegion(1, 1, 2, 3)
	for i := uint64(2); i <= 5; i++ {
		tc.AddLeaderRegion(i, seq.next(), seq.next(), seq.next())
	}

	scatterer := NewRegionScatterer(ctx, tc, oc)

	for i := uint64(1); i <= 5; i++ {
		region := tc.GetRegion(i)
		if op, _ := scatterer.Scatter(region, ""); op != nil {
			re.Equal(1, oc.AddWaitingOperator(op))
		}
	}
}

func TestScatterCheck(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 5 stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}
	testCases := []struct {
		name        string
		checkRegion *core.RegionInfo
		needFix     bool
	}{
		{
			name:        "region with 4 replicas",
			checkRegion: tc.AddLeaderRegion(1, 1, 2, 3, 4),
			needFix:     true,
		},
		{
			name:        "region with 3 replicas",
			checkRegion: tc.AddLeaderRegion(1, 1, 2, 3),
			needFix:     false,
		},
		{
			name:        "region with 2 replicas",
			checkRegion: tc.AddLeaderRegion(1, 1, 2),
			needFix:     true,
		},
	}
	for _, testCase := range testCases {
		t.Log(testCase.name)
		scatterer := NewRegionScatterer(ctx, tc, oc)
		_, err := scatterer.Scatter(testCase.checkRegion, "")
		if testCase.needFix {
			re.Error(err)
			re.True(tc.CheckRegionUnderSuspect(1))
		} else {
			re.NoError(err)
			re.False(tc.CheckRegionUnderSuspect(1))
		}
		tc.ResetSuspectRegions()
	}
}

// TestSomeStoresFilteredScatterGroupInConcurrency is used to test #5317 panic and won't test scatter result
func TestSomeStoresFilteredScatterGroupInConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 5 connected stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	// Add 10 disconnected stores.
	for i := uint64(6); i <= 15; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, 10*time.Minute)
	}
	// Add 85 down stores.
	for i := uint64(16); i <= 100; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, 40*time.Minute)
	}
	re.Equal(tc.GetStore(uint64(6)).IsDisconnected(), true)
	scatterer := NewRegionScatterer(ctx, tc, oc)
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go scatterOnce(tc, scatterer, fmt.Sprintf("group-%v", j), &wg)
	}
	wg.Wait()
}

func scatterOnce(tc *mockcluster.Cluster, scatter *RegionScatterer, group string, wg *sync.WaitGroup) {
	regionID := 1
	for i := 0; i < 100; i++ {
		scatter.scatterRegion(tc.AddLeaderRegion(uint64(regionID), 1, 2, 3), group)
		regionID++
	}
	wg.Done()
}

func TestScatterGroupInConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 5 stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	testCases := []struct {
		name       string
		groupCount int
	}{
		{
			name:       "1 group",
			groupCount: 1,
		},
		{
			name:       "2 group",
			groupCount: 2,
		},
		{
			name:       "3 group",
			groupCount: 3,
		},
	}

	// We send scatter interweave request for each group to simulate scattering multiple region groups in concurrency.
	for _, testCase := range testCases {
		t.Log(testCase.name)
		scatterer := NewRegionScatterer(ctx, tc, oc)
		regionID := 1
		for i := 0; i < 100; i++ {
			for j := 0; j < testCase.groupCount; j++ {
				scatterer.scatterRegion(tc.AddLeaderRegion(uint64(regionID), 1, 2, 3),
					fmt.Sprintf("group-%v", j))
				regionID++
			}
		}

		checker := func(ss *selectedStores, expected uint64, delta float64) {
			for i := 0; i < testCase.groupCount; i++ {
				// comparing the leader distribution
				group := fmt.Sprintf("group-%v", i)
				max := uint64(0)
				min := uint64(math.MaxUint64)
				groupDistribution, _ := ss.groupDistribution.Get(group)
				for _, count := range groupDistribution.(map[uint64]uint64) {
					if count > max {
						max = count
					}
					if count < min {
						min = count
					}
				}
				re.LessOrEqual(math.Abs(float64(max)-float64(expected)), delta)
				re.LessOrEqual(math.Abs(float64(min)-float64(expected)), delta)
			}
		}
		// For leader, we expect each store have about 20 leader for each group
		checker(scatterer.ordinaryEngine.selectedLeader, 20, 5)
		// For peer, we expect each store have about 60 peers for each group
		checker(scatterer.ordinaryEngine.selectedPeer, 60, 15)
	}
}

func TestScatterForManyRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 60 stores.
	for i := uint64(1); i <= 60; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	scatterer := NewRegionScatterer(ctx, tc, oc)
	regions := make(map[uint64]*core.RegionInfo)
	for i := 1; i <= 1200; i++ {
		regions[uint64(i)] = tc.AddLightWeightLeaderRegion(uint64(i), 1, 2, 3)
	}
	failures := map[uint64]error{}
	group := "group"
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain", `return(true)`))
	scatterer.scatterRegions(regions, failures, group, 3)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain"))
	re.Len(failures, 0)
}

func TestScattersGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 5 stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}
	testCases := []struct {
		name    string
		failure bool
	}{
		{
			name:    "have failure",
			failure: true,
		},
		{
			name:    "no failure",
			failure: false,
		},
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain", `return(true)`))
	for id, testCase := range testCases {
		group := fmt.Sprintf("gourp-%d", id)
		t.Log(testCase.name)
		scatterer := NewRegionScatterer(ctx, tc, oc)
		regions := map[uint64]*core.RegionInfo{}
		for i := 1; i <= 100; i++ {
			regions[uint64(i)] = tc.AddLightWeightLeaderRegion(uint64(i), 1, 2, 3)
		}
		failures := map[uint64]error{}
		if testCase.failure {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/scatter/scatterFail", `return(true)`))
		}

		scatterer.scatterRegions(regions, failures, group, 3)
		max := uint64(0)
		min := uint64(math.MaxUint64)
		groupDistribution, exist := scatterer.ordinaryEngine.selectedLeader.GetGroupDistribution(group)
		re.True(exist)
		for _, count := range groupDistribution {
			if count > max {
				max = count
			}
			if count < min {
				min = count
			}
		}
		// 100 regions divided 5 stores, each store expected to have about 20 regions.
		re.LessOrEqual(min, uint64(20))
		re.GreaterOrEqual(max, uint64(20))
		re.LessOrEqual(max-min, uint64(3))
		if testCase.failure {
			re.Len(failures, 1)
			_, ok := failures[1]
			re.True(ok)
			re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/scatter/scatterFail"))
		} else {
			re.Empty(failures)
		}
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain"))
}

func TestSelectedStoreGC(t *testing.T) {
	re := require.New(t)
	gcInterval = time.Second
	gcTTL = time.Second * 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stores := newSelectedStores(ctx)
	stores.Put(1, "testgroup")
	_, ok := stores.GetGroupDistribution("testgroup")
	re.True(ok)
	_, ok = stores.GetGroupDistribution("testgroup")
	re.True(ok)
	time.Sleep(gcTTL)
	_, ok = stores.GetGroupDistribution("testgroup")
	re.False(ok)
	_, ok = stores.GetGroupDistribution("testgroup")
	re.False(ok)
}

// TestRegionFromDifferentGroups test the multi regions. each region have its own group.
// After scatter, the distribution for the whole cluster should be well.
func TestRegionFromDifferentGroups(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 6 stores.
	storeCount := 6
	for i := uint64(1); i <= uint64(storeCount); i++ {
		tc.AddRegionStore(i, 0)
	}
	scatterer := NewRegionScatterer(ctx, tc, oc)
	regionCount := 50
	for i := 1; i <= regionCount; i++ {
		p := rand.Perm(storeCount)
		scatterer.scatterRegion(tc.AddLeaderRegion(uint64(i), uint64(p[0])+1, uint64(p[1])+1, uint64(p[2])+1), fmt.Sprintf("t%d", i))
	}
	check := func(ss *selectedStores) {
		max := uint64(0)
		min := uint64(math.MaxUint64)
		for i := uint64(1); i <= uint64(storeCount); i++ {
			count := ss.TotalCountByStore(i)
			if count > max {
				max = count
			}
			if count < min {
				min = count
			}
		}
		re.LessOrEqual(max-min, uint64(2))
	}
	check(scatterer.ordinaryEngine.selectedPeer)
}

func TestRegionHasLearner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 8 stores.
	voterCount := uint64(6)
	storeCount := uint64(8)
	for i := uint64(1); i <= voterCount; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"zone": "z1"})
	}
	for i := voterCount + 1; i <= 8; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"zone": "z2"})
	}
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd",
		ID:      "default",
		Role:    placement.Voter,
		Count:   3,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z1"},
			},
		},
	})
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "pd",
		ID:      "learner",
		Role:    placement.Learner,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "zone",
				Op:     placement.In,
				Values: []string{"z2"},
			},
		},
	})
	scatterer := NewRegionScatterer(ctx, tc, oc)
	regionCount := 50
	for i := 1; i <= regionCount; i++ {
		_, err := scatterer.Scatter(tc.AddRegionWithLearner(uint64(i), uint64(1), []uint64{uint64(2), uint64(3)}, []uint64{7}), "group")
		re.NoError(err)
	}
	check := func(ss *selectedStores) {
		max := uint64(0)
		min := uint64(math.MaxUint64)
		for i := uint64(1); i <= max; i++ {
			count := ss.TotalCountByStore(i)
			if count > max {
				max = count
			}
			if count < min {
				min = count
			}
		}
		re.LessOrEqual(max-min, uint64(2))
	}
	check(scatterer.ordinaryEngine.selectedPeer)
	checkLeader := func(ss *selectedStores) {
		max := uint64(0)
		min := uint64(math.MaxUint64)
		for i := uint64(1); i <= voterCount; i++ {
			count := ss.TotalCountByStore(i)
			if count > max {
				max = count
			}
			if count < min {
				min = count
			}
		}
		re.LessOrEqual(max-2, uint64(regionCount)/voterCount)
		re.LessOrEqual(min-1, uint64(regionCount)/voterCount)
		for i := voterCount + 1; i <= storeCount; i++ {
			count := ss.TotalCountByStore(i)
			re.LessOrEqual(count, uint64(0))
		}
	}
	checkLeader(scatterer.ordinaryEngine.selectedLeader)
}

// TestSelectedStoresTooFewPeers tests if the peer count has changed due to the picking strategy.
// Ref https://github.com/tikv/pd/issues/4565
func TestSelectedStoresTooFewPeers(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 4 stores.
	for i := uint64(1); i <= 4; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc)

	// Put a lot of regions in Store 1/2/3.
	for i := uint64(1); i < 100; i++ {
		region := tc.AddLeaderRegion(i+10, i%3+1, (i+1)%3+1, (i+2)%3+1)
		peers := make(map[uint64]*metapb.Peer, 3)
		for _, peer := range region.GetPeers() {
			peers[peer.GetStoreId()] = peer
		}
		scatterer.Put(peers, i%3+1, group)
	}

	// Try to scatter a region with peer store id 2/3/4
	for i := uint64(1); i < 20; i++ {
		region := tc.AddLeaderRegion(i+200, i%3+2, (i+1)%3+2, (i+2)%3+2)
		op := scatterer.scatterRegion(region, group)
		re.False(isPeerCountChanged(op))
	}
}

// TestSelectedStoresTooManyPeers tests if the peer count has changed due to the picking strategy.
// Ref https://github.com/tikv/pd/issues/5909
func TestSelectedStoresTooManyPeers(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 4 stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc)
	// priority 4 > 1 > 5 > 2 == 3
	for i := 0; i < 1200; i++ {
		scatterer.ordinaryEngine.selectedPeer.Put(2, group)
		scatterer.ordinaryEngine.selectedPeer.Put(3, group)
	}
	for i := 0; i < 800; i++ {
		scatterer.ordinaryEngine.selectedPeer.Put(5, group)
	}
	for i := 0; i < 400; i++ {
		scatterer.ordinaryEngine.selectedPeer.Put(1, group)
	}
	// test region with peer 1 2 3
	for i := uint64(1); i < 20; i++ {
		region := tc.AddLeaderRegion(i+200, i%3+1, (i+1)%3+1, (i+2)%3+1)
		op := scatterer.scatterRegion(region, group)
		re.False(isPeerCountChanged(op))
	}
}

// TestBalanceRegion tests whether region peers and leaders are balanced after scatter.
// ref https://github.com/tikv/pd/issues/6017
func TestBalanceRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	opt.SetLocationLabels([]string{"host"})
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetOpts(), stream)
	// Add 6 stores in 3 hosts.
	for i := uint64(2); i <= 7; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"host": strconv.FormatUint(i/2, 10)})
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc)
	for i := uint64(1001); i <= 1300; i++ {
		region := tc.AddLeaderRegion(i, 2, 4, 6)
		op := scatterer.scatterRegion(region, group)
		re.False(isPeerCountChanged(op))
	}
	for i := uint64(2); i <= 7; i++ {
		re.Equal(uint64(150), scatterer.ordinaryEngine.selectedPeer.Get(i, group))
		re.Equal(uint64(50), scatterer.ordinaryEngine.selectedLeader.Get(i, group))
	}
	// Test for unhealthy region
	// ref https://github.com/tikv/pd/issues/6099
	region := tc.AddLeaderRegion(1500, 2, 3, 4, 6)
	op := scatterer.scatterRegion(region, group)
	re.False(isPeerCountChanged(op))
}

func isPeerCountChanged(op *operator.Operator) bool {
	if op == nil {
		return false
	}
	add, remove := 0, 0
	for i := 0; i < op.Len(); i++ {
		step := op.Step(i)
		switch step.(type) {
		case operator.AddPeer, operator.AddLearner:
			add++
		case operator.RemovePeer:
			remove++
		}
	}
	return add != remove
}
