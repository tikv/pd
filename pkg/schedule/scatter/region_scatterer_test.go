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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

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
	scatter(re, 7, 71, true)
	scatter(re, 5, 50, false)
	scatterSpecial(re, 3, 6, 50)
	scatterSpecial(re, 5, 5, 50)
}

func checkOperator(re *require.Assertions, op *operator.Operator) {
	re.Equal(operator.OpAdmin, op.SchedulerKind())
	re.Equal(constant.High, op.GetPriorityLevel())
	for i := range op.Len() {
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

func newTestScatterState(scatterer *RegionScatterer) *scatterState {
	return scatterer.newScatterState()
}

func (s *localSelectedStores) getGroupDistributionClone(group string) (map[uint64]uint64, bool) {
	distribution, ok := s.groupDistribution[group]
	if !ok {
		return nil, false
	}
	return cloneDistribution(distribution), true
}

func scatter(re *require.Assertions, numStores, numRegions uint64, useRules bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
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
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	noNeedMoveNum := 0
	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		if op, err := scatterer.Scatter(region, "", false); err == nil {
			if op == nil {
				noNeedMoveNum++
				continue
			}
			checkOperator(re, op)
			operator.ApplyOperator(tc, op)
		} else {
			re.Nil(op)
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
	maxStorePeerTotalCount := uint64(0)
	minStorePeerTotalCount := uint64(math.MaxUint64)

	// Each store should have the same number of peers.
	for _, count := range countPeers {
		if count > maxStorePeerTotalCount {
			maxStorePeerTotalCount = count
		}
		if count < minStorePeerTotalCount {
			minStorePeerTotalCount = count
		}
	}
	re.LessOrEqual(maxStorePeerTotalCount-minStorePeerTotalCount, uint64(1))

	// Each store should have the same number of leaders.
	re.Len(countPeers, int(numStores))
	re.Len(countLeader, int(numStores))

	maxStoreLeaderTotalCount := uint64(0)
	minStoreLeaderTotalCount := uint64(math.MaxUint64)
	for _, count := range countLeader {
		if count > maxStoreLeaderTotalCount {
			maxStoreLeaderTotalCount = count
		}
		if count < minStoreLeaderTotalCount {
			minStoreLeaderTotalCount = count
		}
	}
	// Since the scatter leader depends on the scatter result of the peer, the maximum difference is 2.
	re.LessOrEqual(maxStoreLeaderTotalCount-minStoreLeaderTotalCount, uint64(2))
	re.GreaterOrEqual(noNeedMoveNum, 0)
}

func scatterSpecial(re *require.Assertions, numOrdinaryStores, numSpecialStores, numRegions uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
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
	re.NoError(tc.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 3,
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
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	for i := uint64(1); i <= numRegions; i++ {
		region := tc.GetRegion(i)
		if op, err := scatterer.Scatter(region, "", false); op != nil {
			re.NoError(err)
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
			store := tc.GetStore(storeID)
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
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

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

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	for i := uint64(1); i <= 5; i++ {
		region := tc.GetRegion(i)
		if op, err := scatterer.Scatter(region, "", false); op != nil {
			re.NoError(err)
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
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
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
		scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
		_, err := scatterer.Scatter(testCase.checkRegion, "", false)
		if testCase.needFix {
			re.Error(err)
			re.True(tc.CheckPendingProcessedRegions(1))
		} else {
			re.NoError(err)
			re.False(tc.CheckPendingProcessedRegions(1))
		}
		tc.ResetPendingProcessedRegions()
	}
}

// TestSomeStoresFilteredScatterGroupInConcurrency is used to test #5317 panic and won't test scatter result
func TestSomeStoresFilteredScatterGroupInConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
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
	re.True(tc.GetStore(uint64(6)).IsDisconnected())
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	var wg sync.WaitGroup
	for j := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scatterOnce(re, tc, scatterer, fmt.Sprintf("group-%v", j))
		}()
	}
	wg.Wait()
}

func scatterOnce(re *require.Assertions, tc *mockcluster.Cluster, scatter *RegionScatterer, group string) {
	regionID := 1
	for range 100 {
		_, err := scatter.Scatter(tc.AddLeaderRegion(uint64(regionID), 1, 2, 3), group, false)
		re.NoError(err)
		regionID++
	}
}

func TestScatterGroupInConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
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
		scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
		regionID := 1
		for range 100 {
			for j := range testCase.groupCount {
				_, err := scatterer.Scatter(tc.AddLeaderRegion(uint64(regionID), 1, 2, 3),
					fmt.Sprintf("group-%v", j), false)
				re.NoError(err)
				regionID++
			}
		}

		checker := func(ss *selectedStores, expected uint64, delta float64) {
			for i := range testCase.groupCount {
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
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// Add 60 stores.
	for i := uint64(1); i <= 60; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	regions := make(map[uint64]*core.RegionInfo)
	for i := 1; i <= 1200; i++ {
		regions[uint64(i)] = tc.AddLightWeightLeaderRegion(uint64(i), 1, 2, 3)
	}
	failures := map[uint64]error{}
	group := "group"
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain"))
	}()
	_, err := scatterer.scatterRegions(regions, failures, group, 3, false)
	re.NoError(err)
	re.Empty(failures)
}

func TestScattersGroup(t *testing.T) {
	re := require.New(t)
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
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/scatter/scatterHbStreamsDrain"))
	}()
	for id, testCase := range testCases {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opt := mockconfig.NewTestOptions()
			tc := mockcluster.NewCluster(ctx, opt)
			stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
			defer stream.Close()
			oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
			for i := uint64(1); i <= 5; i++ {
				tc.AddRegionStore(i, 0)
			}
			group := fmt.Sprintf("group-%d", id)
			t.Log(testCase.name)
			scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
			regions := map[uint64]*core.RegionInfo{}
			for i := 1; i <= 100; i++ {
				regions[uint64(i)] = tc.AddLightWeightLeaderRegion(uint64(i), 1, 2, 3)
			}
			failures := map[uint64]error{}
			if testCase.failure {
				re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/scatter/scatterFail", `return(true)`))
				defer func() {
					re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/scatter/scatterFail"))
				}()
			}

			_, err := scatterer.scatterRegions(regions, failures, group, 3, false)
			re.NoError(err)
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
			} else {
				re.Empty(failures)
			}
		}()
	}
}

func TestSelectedStoreGC(t *testing.T) {
	re := require.New(t)
	originalGCInterval := gcInterval
	originalGCTTL := gcTTL
	gcInterval = time.Second
	gcTTL = time.Second * 3
	defer func() {
		gcInterval = originalGCInterval
		gcTTL = originalGCTTL
	}()

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

func TestRegionHasLearner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := "group"
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// Add 8 stores.
	voterCount := uint64(6)
	storeCount := uint64(8)
	for i := uint64(1); i <= voterCount; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"zone": "z1"})
	}
	for i := voterCount + 1; i <= 8; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"zone": "z2"})
	}
	err := tc.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
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
	re.NoError(err)
	err = tc.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
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
	re.NoError(err)
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	regionCount := 50
	for i := 1; i <= regionCount; i++ {
		_, err := scatterer.Scatter(tc.AddRegionWithLearner(uint64(i), uint64(1), []uint64{uint64(2), uint64(3)}, []uint64{7}), group, false)
		re.NoError(err)
	}
	check := func(ss *selectedStores) {
		max := uint64(0)
		min := uint64(math.MaxUint64)
		for i := uint64(1); i <= max; i++ {
			count := ss.Get(i, group)
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
			count := ss.Get(i, group)
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
			count := ss.Get(i, group)
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
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// Add 4 stores.
	for i := uint64(1); i <= 4; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

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
		op, err := scatterer.Scatter(region, group, false)
		re.NoError(err)
		re.False(isPeerCountChanged(op))
		if op != nil {
			val, exist := op.GetAdditionalInfo("group")
			re.True(exist)
			re.Equal(group, val)
		}
	}
}

func TestSeedGroupDistributionByRange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	for i := uint64(1); i <= 4; i++ {
		tc.AddRegionStore(i, 0)
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	tc.AddLeaderRegionWithRange(1, "a", "j", 1, 2, 3)
	tc.AddLeaderRegionWithRange(2, "j", "t", 1, 2, 3)
	tc.AddLeaderRegionWithRange(3, "t", "z", 1, 2, 3)

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	group := "seeded"
	state := scatterer.newInternalScatterState(group, []byte("a"), []byte("z"))

	peerDistribution, ok := state.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	re.Equal(uint64(3), peerDistribution[1])
	re.Equal(uint64(3), peerDistribution[2])
	re.Equal(uint64(3), peerDistribution[3])
	re.Equal(uint64(0), peerDistribution[4])

	leaderDistribution, ok := state.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	re.Equal(uint64(3), leaderDistribution[1])
	re.Equal(uint64(0), leaderDistribution[2])
	re.Equal(uint64(0), leaderDistribution[3])
	re.Equal(uint64(0), leaderDistribution[4])

	op, err := scatterer.ScatterInternal(tc.GetRegion(3), group, []byte("a"), []byte("z"))
	re.NoError(err)
	re.NotNil(op)
	re.Equal(operator.OpSplitScatter, op.SchedulerKind())
	re.Zero(op.Kind() & operator.OpAdmin)
	re.Equal(constant.High, op.GetPriorityLevel())
	re.NotZero(op.Kind() & operator.OpSplitScatter)
	re.NotZero(op.Kind() & operator.OpRegion)
	val, exist := op.GetAdditionalInfo("group")
	re.True(exist)
	re.Equal(group, val)
}

func TestSeedGroupDistributionByRangeAppliesNetChange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	for i := uint64(1); i <= 4; i++ {
		tc.AddRegionStore(i, 0)
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	tc.AddLeaderRegionWithRange(1, "a", "j", 1, 2, 3)
	tc.AddLeaderRegionWithRange(2, "j", "t", 2, 1, 3)
	tc.AddLeaderRegionWithRange(3, "t", "z", 3, 1, 2)

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	group := "seeded-net-change"
	state := scatterer.newInternalScatterState(group, []byte("a"), []byte("z"))

	region := tc.GetRegion(1)
	peerBefore, ok := state.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	leaderBefore, ok := state.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	peerSnapshot := cloneDistribution(peerBefore)
	leaderSnapshot := cloneDistribution(leaderBefore)

	op, err := scatterer.ScatterInternal(region, group, []byte("a"), []byte("z"))
	re.NoError(err)
	re.NotNil(op)
	re.Equal(operator.OpSplitScatter, op.SchedulerKind())
	re.Zero(op.Kind() & operator.OpAdmin)
	re.Equal(constant.High, op.GetPriorityLevel())
	re.NotZero(op.Kind() & operator.OpSplitScatter)
	re.NotZero(op.Kind() & operator.OpRegion)
	finalPeers, finalLeaderStoreID := finalPlacementAfterOperator(region, op)

	expectedPeerDistribution := cloneDistribution(peerSnapshot)
	for _, peer := range region.GetPeers() {
		expectedPeerDistribution[peer.GetStoreId()]--
	}
	for storeID := range finalPeers {
		expectedPeerDistribution[storeID]++
	}

	expectedLeaderDistribution := cloneDistribution(leaderSnapshot)
	expectedLeaderDistribution[region.GetLeader().GetStoreId()]--
	expectedLeaderDistribution[finalLeaderStoreID]++
	removeZeroCountEntries(expectedPeerDistribution)
	removeZeroCountEntries(expectedLeaderDistribution)

	re.True(oc.AddOperator(op))
	nextState := scatterer.newInternalScatterState(group, []byte("a"), []byte("z"))
	peerDistribution, ok := nextState.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	re.Equal(expectedPeerDistribution, peerDistribution)

	leaderDistribution, ok := nextState.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	re.Equal(expectedLeaderDistribution, leaderDistribution)
}

func TestSeededDistributionSkipsUpdateWhenAddOperatorRejected(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	for i := uint64(1); i <= 4; i++ {
		tc.AddRegionStore(i, 0)
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	tc.AddLeaderRegionWithRange(1, "a", "j", 1, 2, 3)
	tc.AddLeaderRegionWithRange(2, "j", "t", 1, 2, 3)
	tc.AddLeaderRegionWithRange(3, "t", "z", 1, 2, 3)

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	group := "seeded-rollback"
	state := scatterer.newInternalScatterState(group, []byte("a"), []byte("z"))

	peerBefore, ok := state.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	leaderBefore, ok := state.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	peerSnapshot := cloneDistribution(peerBefore)
	leaderSnapshot := cloneDistribution(leaderBefore)

	scheduleCfg := tc.GetScheduleConfig().Clone()
	scheduleCfg.SchedulerMaxWaitingOperator = 0
	tc.SetScheduleConfig(scheduleCfg)

	region := tc.GetRegion(1)
	op, err := scatterer.ScatterInternal(region, group, []byte("a"), []byte("z"))
	re.NoError(err)
	re.NotNil(op)
	re.False(oc.AddOperator(op))

	nextState := scatterer.newInternalScatterState(group, []byte("a"), []byte("z"))
	peerAfter, ok := nextState.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	for i := uint64(1); i <= 4; i++ {
		re.Equal(peerSnapshot[i], peerAfter[i])
	}

	leaderAfter, ok := nextState.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	for i := uint64(1); i <= 4; i++ {
		re.Equal(leaderSnapshot[i], leaderAfter[i])
	}
}

func TestCreateScatterRegionOperatorFailureAccountsCurrentPlacement(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	for i := uint64(1); i <= 4; i++ {
		tc.AddRegionStore(i, 0)
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}

	tc.AddLeaderRegionWithRange(1, "a", "j", 1, 2, 3)
	region := tc.GetRegion(1)
	region = region.Clone(core.WithRole(region.GetPeers()[1].GetId(), metapb.PeerRole_IncomingVoter))
	tc.PutRegion(region)
	region = tc.GetRegion(1)

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	group := "create-fail"
	state := newTestScatterState(scatterer)
	for _, storeID := range []uint64{1, 2, 3} {
		state.ordinaryEngine.selectedPeer.Update(group, nil, []uint64{storeID})
	}
	state.ordinaryEngine.selectedLeader.Update(group, nil, []uint64{1})

	peerBefore, ok := state.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	leaderBefore, ok := state.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	peerSnapshot := cloneDistribution(peerBefore)
	leaderSnapshot := cloneDistribution(leaderBefore)

	op, err := scatterer.scatterWithOptions(region, group, true, false, state)
	re.Nil(op)
	re.Error(err)
	re.Contains(err.Error(), "failed to create scatter region operator")

	expectedPeerDistribution := cloneDistribution(peerSnapshot)
	for _, peer := range region.GetPeers() {
		expectedPeerDistribution[peer.GetStoreId()]++
	}
	expectedLeaderDistribution := cloneDistribution(leaderSnapshot)
	expectedLeaderDistribution[region.GetLeader().GetStoreId()]++

	peerAfter, ok := state.ordinaryEngine.selectedPeer.getGroupDistributionClone(group)
	re.True(ok)
	re.Equal(expectedPeerDistribution, peerAfter)

	leaderAfter, ok := state.ordinaryEngine.selectedLeader.getGroupDistributionClone(group)
	re.True(ok)
	re.Equal(expectedLeaderDistribution, leaderAfter)
}

func removeZeroCountEntries(distribution map[uint64]uint64) {
	for storeID, count := range distribution {
		if count == 0 {
			delete(distribution, storeID)
		}
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
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// Add 4 stores.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	// priority 4 > 1 > 5 > 2 == 3
	for range 1200 {
		scatterer.ordinaryEngine.selectedPeer.Put(2, group)
		scatterer.ordinaryEngine.selectedPeer.Put(3, group)
	}
	for range 800 {
		scatterer.ordinaryEngine.selectedPeer.Put(5, group)
	}
	for range 400 {
		scatterer.ordinaryEngine.selectedPeer.Put(1, group)
	}
	// test region with peer 1 2 3
	for i := uint64(1); i < 20; i++ {
		region := tc.AddLeaderRegion(i+200, i%3+1, (i+1)%3+1, (i+2)%3+1)
		op, err := scatterer.Scatter(region, group, false)
		re.NoError(err)
		re.False(isPeerCountChanged(op))
	}
}

// TestBalanceLeader only tests whether region leaders are balanced after scatter.
func TestBalanceLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// Add 3 stores
	for i := uint64(2); i <= 4; i++ {
		tc.AddLabelsStore(i, 0, nil)
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	for i := uint64(1001); i <= 1300; i++ {
		region := tc.AddLeaderRegion(i, 2, 3, 4)
		op, err := scatterer.Scatter(region, group, false)
		re.NoError(err)
		re.False(isPeerCountChanged(op))
	}
	// all leader will be balanced in three stores.
	for i := uint64(2); i <= 4; i++ {
		re.Equal(uint64(100), scatterer.ordinaryEngine.selectedLeader.Get(i, group))
	}
}

// TestBalanceRegion tests whether region peers are balanced after scatter.
// ref https://github.com/tikv/pd/issues/6017
func TestBalanceRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	opt.SetLocationLabels([]string{"host"})
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	// Add 6 stores in 3 hosts.
	for i := uint64(2); i <= 7; i++ {
		tc.AddLabelsStore(i, 0, map[string]string{"host": strconv.FormatUint(i/2, 10)})
		// prevent store from being disconnected
		tc.SetStoreLastHeartbeatInterval(i, -10*time.Minute)
	}
	group := "group"
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	for i := uint64(1001); i <= 1300; i++ {
		region := tc.AddLeaderRegion(i, 2, 4, 6)
		op, err := scatterer.Scatter(region, group, false)
		re.NoError(err)
		re.False(isPeerCountChanged(op))
	}
	for i := uint64(2); i <= 7; i++ {
		re.Equal(uint64(150), scatterer.ordinaryEngine.selectedPeer.Get(i, group))
	}
	// Test for unhealthy region
	// ref https://github.com/tikv/pd/issues/6099
	region := tc.AddLeaderRegion(1500, 2, 3, 4, 6)
	op, err := scatterer.Scatter(region, group, false)
	re.ErrorContains(err, "is not fully replicated")
	re.Nil(op)
}

func isPeerCountChanged(op *operator.Operator) bool {
	if op == nil {
		return false
	}
	add, remove := 0, 0
	for i := range op.Len() {
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

func TestRemoveStoreLimit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

	// Add stores 1~6.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
		tc.SetStoreLimit(i, storelimit.AddPeer, 1)
		tc.SetStoreLimit(i, storelimit.RemovePeer, 1)
	}

	// Add regions 1~4.
	seq := newSequencer(3)
	// Region 1 has the same distribution with the Region 2, which is used to test selectPeerToReplace.
	tc.AddLeaderRegion(1, 1, 2, 3)
	for i := uint64(2); i <= 5; i++ {
		tc.AddLeaderRegion(i, seq.next(), seq.next(), seq.next())
	}

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	for i := uint64(1); i <= 5; i++ {
		region := tc.GetRegion(i)
		if op, err := scatterer.Scatter(region, "", true); op != nil {
			re.NoError(err)
			re.True(oc.AddOperator(op))
		}
	}
}

func TestScatterReplace(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

	// Add stores 1~5.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}
	// Add regions 1~5.
	tc.AddLeaderRegion(1, 1, 2, 3)
	for i := uint64(2); i <= 5; i++ {
		tc.AddLeaderRegion(i, 1, 2, 3)
	}

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	for i := uint64(1); i <= 5; i++ {
		region := tc.GetRegion(i)
		if op, err := scatterer.Scatter(region, "", true); op != nil {
			re.NoError(err)
			re.True(oc.AddOperator(op))
		}
	}

	// same scatter operator should be skipped
	region := tc.GetRegion(2)
	op, err := scatterer.Scatter(region, "", true)
	re.NoError(err)
	re.Nil(op)

	// A same-group operator from an older epoch must not consume a newer
	// split-scatter attempt for the same region.
	tc.PutRegion(tc.MockRegionInfo(2, 1, []uint64{2, 3}, nil, &metapb.RegionEpoch{ConfVer: 1, Version: 2}))
	op, err = scatterer.Scatter(tc.GetRegion(2), "", true)
	re.Error(err)
	re.Nil(op)

	// different scatter operator should be rejected
	region = tc.GetRegion(3)
	op, err = scatterer.Scatter(region, "test", true)
	re.Error(err)
	re.Nil(op)

	// exist lower operator
	op = oc.GetOperator(1)
	if op != nil {
		re.True(oc.RemoveOperator(op))
	}
	region = tc.GetRegion(1)
	op = operator.NewTestOperator(region.GetID(), region.GetRegionEpoch(), operator.OpRegion)
	op.SetPriorityLevel(constant.Low)
	re.True(oc.AddOperator(op))

	testutil.Eventually(re, func() bool {
		op, err = scatterer.Scatter(tc.GetRegion(1), "", true)
		re.NoError(err)
		return op != nil
	})
}

func TestScatterWithAffinity(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

	// Add stores 1~5.
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}

	// Add region 1 with leader on store 1
	tc.AddLeaderRegion(1, 1, 2, 3)
	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	// Create affinity manager and group
	affinityManager := tc.GetAffinityManager()
	re.NotNil(affinityManager)

	// Create an affinity group for region 1
	// When LeaderStoreID and VoterStoreIDs are set and the group is available (not expired),
	// RegularSchedulingAllowed will be false
	group := &affinity.Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}

	keyRanges := []affinity.GroupKeyRanges{{
		GroupID: group.ID,
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte(""),
			EndKey:   []byte(""),
		}},
	}}
	err := affinityManager.CreateAffinityGroups(keyRanges)
	re.NoError(err)
	_, err = affinityManager.UpdateAffinityGroupPeers(group.ID, group.LeaderStoreID, group.VoterStoreIDs)
	re.NoError(err)

	// Test scatter with affinity (RegularSchedulingAllowed=false)
	// Should return (nil, nil) to prevent client from retrying
	region := tc.GetRegion(1)
	op, err := scatterer.Scatter(region, "", true)
	re.NoError(err)
	re.Nil(op)
}

func TestScatterInternalSkipsHotOnlyForAdmin(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, 0)
	}

	tc.AddRegionWithReadInfo(
		1,
		1,
		512*1024*utils.StoreHeartBeatReportInterval,
		0,
		0,
		utils.StoreHeartBeatReportInterval,
		[]uint64{2, 3},
	)
	region := tc.GetRegion(1)
	re.True(tc.IsRegionHot(region))

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)

	op, err := scatterer.Scatter(region, "", true)
	re.ErrorContains(err, "is hot")
	re.Nil(op)

	op, err = scatterer.ScatterInternal(region, "", region.GetStartKey(), region.GetEndKey())
	re.NoError(err)
	if op != nil {
		re.Equal(InternalScatterOperatorDesc, op.Desc())
	}
}

func TestInternalScatterPeerSelection(t *testing.T) {
	testCases := []struct {
		name              string
		storeRegionCounts map[uint64]int
		peerDistribution  map[uint64]uint64
		excludedStores    []uint64
		checkAdmin        bool
		wantAdmin         uint64
		want              uint64
		wantAny           []uint64
	}{
		{
			name:             "keeps origin when peer counts are even",
			peerDistribution: map[uint64]uint64{},
			excludedStores:   []uint64{2, 3},
			checkAdmin:       true,
			wantAdmin:        1,
			want:             1,
		},
		{
			name:              "prefers less loaded lowest count store",
			storeRegionCounts: map[uint64]int{4: 100, 5: 10},
			peerDistribution:  map[uint64]uint64{1: 3, 2: 3, 3: 3, 4: 1, 5: 1},
			excludedStores:    []uint64{2, 3},
			want:              5,
		},
		{
			name:             "keeps origin when source target gap is one",
			peerDistribution: map[uint64]uint64{1: 2, 2: 2, 3: 2, 4: 1, 5: 1},
			want:             1,
		},
		{
			name:             "moves when source target gap exceeds one",
			peerDistribution: map[uint64]uint64{1: 6, 2: 4, 3: 4, 4: 1, 5: 1},
			wantAny:          []uint64{4, 5},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			scatterer, state, _, peer := newInternalScatterSelectionTestFixture(t, testCase.storeRegionCounts)
			group := "test-peer-selection"
			re.True(state.ordinaryEngine.selectedPeer.InitGroupDistribution(group, testCase.peerDistribution))
			filters := newTestExcludedStoreFilter(testCase.excludedStores...)

			if testCase.checkAdmin {
				adminPeer := scatterer.selectNewPeer(scatterer.ordinaryEngine.asSelectionContext(), group, peer, filters, false)
				re.Equal(testCase.wantAdmin, adminPeer.GetStoreId())
			}

			internalPeer := scatterer.selectNewPeer(state.ordinaryEngine.asSelectionContext(), group, peer, filters, true)
			if len(testCase.wantAny) > 0 {
				re.Contains(testCase.wantAny, internalPeer.GetStoreId())
			} else {
				re.Equal(testCase.want, internalPeer.GetStoreId())
			}
		})
	}
}

func TestInternalScatterLeaderSelection(t *testing.T) {
	testCases := []struct {
		name               string
		leaderDistribution map[uint64]uint64
		peerDistribution   map[uint64]uint64
		checkAdmin         bool
		wantAdmin          uint64
		want               uint64
	}{
		{
			name:               "prefers unused store",
			leaderDistribution: map[uint64]uint64{1: 2, 4: 0, 5: 0},
			checkAdmin:         true,
			wantAdmin:          1,
			want:               4,
		},
		{
			name:               "keeps origin when source target gap is one",
			leaderDistribution: map[uint64]uint64{1: 1, 4: 0, 5: 0},
			want:               1,
		},
		{
			name:               "breaks ties by peer deficit",
			leaderDistribution: map[uint64]uint64{1: 3, 4: 1, 5: 1},
			peerDistribution:   map[uint64]uint64{1: 10, 4: 9, 5: 2},
			checkAdmin:         true,
			wantAdmin:          1,
			want:               5,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			scatterer, state, region, _ := newInternalScatterSelectionTestFixture(t, nil)
			group := "test-leader-selection"
			candidates := []uint64{1, 4, 5}
			re.True(state.ordinaryEngine.selectedLeader.InitGroupDistribution(group, testCase.leaderDistribution))
			if testCase.peerDistribution != nil {
				re.True(state.ordinaryEngine.selectedPeer.InitGroupDistribution(group, testCase.peerDistribution))
			}

			if testCase.checkAdmin {
				adminLeader, adminCount := scatterer.selectAvailableLeaderStore(group, region, candidates, scatterer.ordinaryEngine.asSelectionContext(), false)
				re.Equal(testCase.wantAdmin, adminLeader)
				re.Equal(scatterer.ordinaryEngine.selectedLeader.Get(adminLeader, group), adminCount)
			}

			internalLeader, internalCount := scatterer.selectAvailableLeaderStore(group, region, candidates, state.ordinaryEngine.asSelectionContext(), true)
			re.Equal(testCase.want, internalLeader)
			re.Equal(state.ordinaryEngine.selectedLeader.Get(internalLeader, group), internalCount)
		})
	}
}

func TestInternalScatterLeaderFiltersRejectedTarget(t *testing.T) {
	re := require.New(t)
	scatterer, state, region, _ := newInternalScatterSelectionTestFixture(t, nil)
	tc := scatterer.cluster.(*mockcluster.Cluster)
	tc.SetLabelProperty(config.RejectLeader, "reject", "leader")
	tc.PutStoreWithLabels(4, "reject", "leader")

	group := "test-leader-target-filter"
	re.True(state.ordinaryEngine.selectedLeader.InitGroupDistribution(group, map[uint64]uint64{1: 3, 4: 0, 5: 1}))
	targetPeers := map[uint64]*metapb.Peer{
		1: region.GetStorePeer(1),
		4: {StoreId: 4, Role: metapb.PeerRole_Voter},
		5: {StoreId: 5, Role: metapb.PeerRole_Voter},
	}
	candidates := scatterer.filterAllowedLeaderCandidateStores(
		region,
		targetPeers,
		[]uint64{1, 4, 5},
	)
	re.NotContains(candidates, uint64(4))

	leader, _ := scatterer.selectAvailableLeaderStore(group, region, candidates, state.ordinaryEngine.asSelectionContext(), true)
	re.Equal(uint64(5), leader)
}

func TestInternalScatterLeaderKeepsOriginWhenSourcePausedOut(t *testing.T) {
	re := require.New(t)
	scatterer, state, region, _ := newInternalScatterSelectionTestFixture(t, nil)
	tc := scatterer.cluster.(*mockcluster.Cluster)
	re.NoError(tc.PauseLeaderTransfer(1, constant.Out))

	group := "test-leader-source-filter"
	re.True(state.ordinaryEngine.selectedLeader.InitGroupDistribution(group, map[uint64]uint64{1: 3, 4: 0, 5: 1}))
	targetPeers := map[uint64]*metapb.Peer{
		1: region.GetStorePeer(1),
		4: {StoreId: 4, Role: metapb.PeerRole_Voter},
		5: {StoreId: 5, Role: metapb.PeerRole_Voter},
	}
	candidates := scatterer.filterAllowedLeaderCandidateStores(region, targetPeers, []uint64{1, 4, 5})
	re.Equal([]uint64{1}, candidates)

	leader, _ := scatterer.selectAvailableLeaderStore(group, region, candidates, state.ordinaryEngine.asSelectionContext(), true)
	re.Equal(uint64(1), leader)
}

func TestInternalScatterKeepsLeaderPeerWhenSourcePausedOut(t *testing.T) {
	re := require.New(t)
	scatterer, _, region, _ := newInternalScatterSelectionTestFixture(t, map[uint64]int{
		1: 10,
		2: 10,
		3: 10,
		4: 1,
		5: 1,
	})
	tc := scatterer.cluster.(*mockcluster.Cluster)
	re.NoError(tc.PauseLeaderTransfer(1, constant.Out))

	op, err := scatterer.ScatterInternal(region, "paused-out", []byte(""), []byte(""))
	re.NoError(err)
	if op == nil {
		return
	}
	targetPeers, targetLeader := finalPlacementAfterOperator(region, op)
	re.Contains(targetPeers, uint64(1))
	re.Equal(uint64(1), targetLeader)
}

func newInternalScatterSelectionTestFixture(t *testing.T, storeRegionCounts map[uint64]int) (*RegionScatterer, *scatterState, *core.RegionInfo, *metapb.Peer) {
	t.Helper()
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	t.Cleanup(stream.Close)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	for i := uint64(1); i <= 5; i++ {
		tc.AddRegionStore(i, storeRegionCounts[i])
	}
	region := tc.AddLeaderRegion(1, 1, 2, 3)
	peer := region.GetStorePeer(1)
	re.NotNil(peer)

	scatterer := NewRegionScatterer(ctx, tc, oc, tc.AddPendingProcessedRegions)
	return scatterer, newTestScatterState(scatterer), region, peer
}

func newTestExcludedStoreFilter(stores ...uint64) []filter.Filter {
	if len(stores) == 0 {
		return nil
	}
	excluded := make(map[uint64]struct{}, len(stores))
	for _, storeID := range stores {
		excluded[storeID] = struct{}{}
	}
	return []filter.Filter{filter.NewExcludedFilter("test", nil, excluded)}
}
