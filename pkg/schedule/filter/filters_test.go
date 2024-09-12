// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package filter

import (
	"context"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
)

func TestDistinctScoreFilter(t *testing.T) {
	re := require.New(t)
	labels := []string{"zone", "rack", "host"}
	allStores := []*core.StoreInfo{
		core.NewStoreInfoWithLabel(1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}),
		core.NewStoreInfoWithLabel(2, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}),
		core.NewStoreInfoWithLabel(3, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}),
		core.NewStoreInfoWithLabel(4, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}),
		core.NewStoreInfoWithLabel(5, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"}),
		core.NewStoreInfoWithLabel(6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"}),
	}

	testCases := []struct {
		stores       []uint64
		source       uint64
		target       uint64
		safeGuardRes plan.StatusCode
		improverRes  plan.StatusCode
	}{
		{[]uint64{1, 2, 3}, 1, 4, plan.StatusOK, plan.StatusOK},
		{[]uint64{1, 3, 4}, 1, 2, plan.StatusOK, plan.StatusStoreNotMatchIsolation},
		{[]uint64{1, 4, 6}, 4, 2, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation},
	}
	for _, testCase := range testCases {
		var stores []*core.StoreInfo
		for _, id := range testCase.stores {
			stores = append(stores, allStores[id-1])
		}
		ls := NewLocationSafeguard("", labels, stores, allStores[testCase.source-1])
		li := NewLocationImprover("", labels, stores, allStores[testCase.source-1])
		re.Equal(testCase.safeGuardRes, ls.Target(mockconfig.NewTestOptions(), allStores[testCase.target-1]).StatusCode)
		re.Equal(testCase.improverRes, li.Target(mockconfig.NewTestOptions(), allStores[testCase.target-1]).StatusCode)
	}
}

func TestLabelConstraintsFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	testCluster := mockcluster.NewCluster(ctx, opt)
	store := core.NewStoreInfoWithLabel(1, map[string]string{"id": "1"})

	testCases := []struct {
		key    string
		op     string
		values []string
		res    plan.StatusCode
	}{
		{"id", "in", []string{"1"}, plan.StatusOK},
		{"id", "in", []string{"2"}, plan.StatusStoreNotMatchRule},
		{"id", "in", []string{"1", "2"}, plan.StatusOK},
		{"id", "notIn", []string{"2", "3"}, plan.StatusOK},
		{"id", "notIn", []string{"1", "2"}, plan.StatusStoreNotMatchRule},
		{"id", "exists", []string{}, plan.StatusOK},
		{"_id", "exists", []string{}, plan.StatusStoreNotMatchRule},
		{"id", "notExists", []string{}, plan.StatusStoreNotMatchRule},
		{"_id", "notExists", []string{}, plan.StatusOK},
	}
	for _, testCase := range testCases {
		filter := NewLabelConstraintFilter("", []placement.LabelConstraint{{Key: testCase.key, Op: placement.LabelConstraintOp(testCase.op), Values: testCase.values}})
		re.Equal(testCase.res, filter.Source(testCluster.GetSharedConfig(), store).StatusCode)
	}
}

func TestRuleFitFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	testCluster := mockcluster.NewCluster(ctx, opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.SetEnablePlacementRules(true)
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 5},
		{StoreId: 7, Id: 7, IsWitness: true},
	}}, &metapb.Peer{StoreId: 1, Id: 1})

	testCases := []struct {
		storeID     uint64
		regionCount int
		labels      map[string]string
		sourceRes   plan.StatusCode
		targetRes   plan.StatusCode
	}{
		{1, 1, map[string]string{"zone": "z1"}, plan.StatusOK, plan.StatusOK},
		{2, 1, map[string]string{"zone": "z1"}, plan.StatusOK, plan.StatusOK},
		// store 3 and store 1 is the peers of this region, so it will allow transferring leader to store 3.
		{3, 1, map[string]string{"zone": "z2"}, plan.StatusOK, plan.StatusOK},
		// the labels of store 4 and store 3 are same, so the isolation score will decrease.
		{4, 1, map[string]string{"zone": "z2"}, plan.StatusOK, plan.StatusStoreNotMatchRule},
		// store 5 and store 1 is the peers of this region, so it will allow transferring leader to store 3.
		{5, 1, map[string]string{"zone": "z3"}, plan.StatusOK, plan.StatusOK},
		{6, 1, map[string]string{"zone": "z4"}, plan.StatusOK, plan.StatusOK},
		// store 7 and store 1 is the peers of this region, but it's a witness, so it won't allow transferring leader to store 7.
		{7, 1, map[string]string{"zone": "z2"}, plan.StatusOK, plan.StatusStoreNotMatchRule},
	}
	// Init cluster
	for _, testCase := range testCases {
		testCluster.AddLabelsStore(testCase.storeID, testCase.regionCount, testCase.labels)
	}
	for _, testCase := range testCases {
		filter := newRuleFitFilter("", testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, nil, 1)
		re.Equal(testCase.sourceRes, filter.Source(testCluster.GetSharedConfig(), testCluster.GetStore(testCase.storeID)).StatusCode)
		re.Equal(testCase.targetRes, filter.Target(testCluster.GetSharedConfig(), testCluster.GetStore(testCase.storeID)).StatusCode)
		leaderFilter := newRuleLeaderFitFilter("", testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, 1, true)
		re.Equal(testCase.targetRes, leaderFilter.Target(testCluster.GetSharedConfig(), testCluster.GetStore(testCase.storeID)).StatusCode)
	}

	// store-6 is not exist in the peers, so it will not allow transferring leader to store 6.
	leaderFilter := newRuleLeaderFitFilter("", testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, 1, false)
	re.False(leaderFilter.Target(testCluster.GetSharedConfig(), testCluster.GetStore(6)).IsOK())
}

func TestRuleFitFilterWithPlacementRule(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	testCluster := mockcluster.NewCluster(ctx, opt)
	testCluster.SetEnablePlacementRules(true)
	ruleManager := testCluster.RuleManager
	ruleManager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
	err := ruleManager.SetRules([]*placement.Rule{
		{
			GroupID: "test",
			ID:      "r1",
			Index:   100,
			Role:    placement.Leader,
			Count:   1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "dc", Op: "in", Values: []string{"dc1"}},
			},
			LocationLabels: []string{"dc", "zone", "host"},
		},
		{
			GroupID: "test",
			ID:      "r2",
			Index:   100,
			Role:    placement.Voter,
			Count:   2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "dc", Op: "in", Values: []string{"dc1"}},
			},
			LocationLabels: []string{"dc", "zone", "host"},
		},
		{
			GroupID: "test",
			ID:      "r3",
			Index:   100,
			Role:    placement.Voter,
			Count:   2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "dc", Op: "in", Values: []string{"dc2"}},
			},
			LocationLabels: []string{"dc", "zone", "host"},
		},
	})
	re.NoError(err)
	stores := []struct {
		storeID     uint64
		regionCount int
		labels      map[string]string
	}{
		{1, 1, map[string]string{"dc": "dc1", "zone": "z1", "host": "h1"}},
		{2, 1, map[string]string{"dc": "dc1", "zone": "z2", "host": "h2"}},
		{3, 1, map[string]string{"dc": "dc1", "zone": "z3", "host": "h3"}},
		{4, 1, map[string]string{"dc": "dc1", "zone": "z4", "host": "h4"}},
		{5, 1, map[string]string{"dc": "dc2", "zone": "z5", "host": "h5"}},
		{6, 1, map[string]string{"dc": "dc2", "zone": "z6", "host": "h6"}},
	}
	// Init cluster
	for _, store := range stores {
		testCluster.AddLabelsStore(store.storeID, store.regionCount, store.labels)
	}
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 2, Id: 2},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 4},
		{StoreId: 6, Id: 5},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	leaderFilter := newRuleLeaderFitFilter("", testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, 1, true)
	re.Equal(plan.StatusText(plan.StatusStoreNotMatchRule), leaderFilter.Target(testCluster.GetSharedConfig(), testCluster.GetStore(6)).String())
}

func TestSendStateFilter(t *testing.T) {
	re := require.New(t)
	store := core.NewStoreInfoWithLabel(1, map[string]string{}).Clone(core.SetStoreLimit(storelimit.NewSlidingWindows()))
	region := core.NewTestRegionInfo(1, 1, []byte(""), []byte(""))
	snapshotFilter := NewSnapshotSendFilter([]*core.StoreInfo{store}, constant.Medium)
	re.NotNil(SelectOneRegion([]*core.RegionInfo{region}, nil, snapshotFilter))
	re.True(store.GetStoreLimit().Take(1000, storelimit.SendSnapshot, constant.Medium))
	re.True(store.GetStoreLimit().Take(1000, storelimit.SendSnapshot, constant.Medium))
	snapshotFilter = NewSnapshotSendFilter([]*core.StoreInfo{store}, constant.Medium)
	re.Nil(SelectOneRegion([]*core.RegionInfo{region}, nil, snapshotFilter))
}

func TestStoreStateFilter(t *testing.T) {
	re := require.New(t)
	filters := []Filter{
		&StoreStateFilter{TransferLeader: true},
		&StoreStateFilter{MoveRegion: true},
		&StoreStateFilter{TransferLeader: true, MoveRegion: true},
		&StoreStateFilter{MoveRegion: true, AllowTemporaryStates: true},
	}
	opt := mockconfig.NewTestOptions()
	store := core.NewStoreInfoWithLabel(1, map[string]string{})

	type testCase struct {
		filterIdx int
		sourceRes plan.StatusCode
		targetRes plan.StatusCode
	}

	check := func(store *core.StoreInfo, testCases []testCase) {
		for _, testCase := range testCases {
			re.Equal(testCase.sourceRes, filters[testCase.filterIdx].Source(opt, store).StatusCode)
			re.Equal(testCase.targetRes, filters[testCase.filterIdx].Target(opt, store).StatusCode)
		}
	}

	store = store.Clone(core.SetLastHeartbeatTS(time.Now()))
	testCases := []testCase{
		{2, plan.StatusOK, plan.StatusOK},
	}
	check(store, testCases)

	// Disconnected
	store = store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-5 * time.Minute)))
	testCases = []testCase{
		{0, plan.StatusStoreDisconnected, plan.StatusStoreDisconnected},
		{1, plan.StatusOK, plan.StatusStoreDisconnected},
		{2, plan.StatusStoreDisconnected, plan.StatusStoreDisconnected},
		{3, plan.StatusOK, plan.StatusOK},
	}
	check(store, testCases)

	// Busy
	store = store.Clone(core.SetLastHeartbeatTS(time.Now())).
		Clone(core.SetStoreStats(&pdpb.StoreStats{IsBusy: true}))
	testCases = []testCase{
		{0, plan.StatusOK, plan.StatusStoreBusy},
		{1, plan.StatusStoreBusy, plan.StatusStoreBusy},
		{2, plan.StatusStoreBusy, plan.StatusStoreBusy},
		{3, plan.StatusOK, plan.StatusOK},
	}
	check(store, testCases)
}

func TestStoreStateFilterReason(t *testing.T) {
	re := require.New(t)
	filters := []Filter{
		&StoreStateFilter{TransferLeader: true},
		&StoreStateFilter{MoveRegion: true},
		&StoreStateFilter{TransferLeader: true, MoveRegion: true},
		&StoreStateFilter{MoveRegion: true, AllowTemporaryStates: true},
	}
	opt := mockconfig.NewTestOptions()
	store := core.NewStoreInfoWithLabel(1, map[string]string{})

	type testCase struct {
		filterIdx    int
		sourceReason string
		targetReason string
	}

	check := func(store *core.StoreInfo, testCases []testCase) {
		for _, testCase := range testCases {
			filters[testCase.filterIdx].Source(opt, store)
			re.Equal(testCase.sourceReason, filters[testCase.filterIdx].(*StoreStateFilter).Reason.String())
			filters[testCase.filterIdx].Source(opt, store)
			re.Equal(testCase.targetReason, filters[testCase.filterIdx].(*StoreStateFilter).Reason.String())
		}
	}

	// No reason caught
	store = store.Clone(core.SetLastHeartbeatTS(time.Now()))
	testCases := []testCase{
		{2, "store-state-ok-filter", "store-state-ok-filter"},
	}
	check(store, testCases)

	// Disconnected
	store = store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-5 * time.Minute)))
	testCases = []testCase{
		{0, "store-state-disconnect-filter", "store-state-disconnect-filter"},
		{1, "store-state-ok-filter", "store-state-ok-filter"},
		{2, "store-state-disconnect-filter", "store-state-disconnect-filter"},
		{3, "store-state-ok-filter", "store-state-ok-filter"},
	}
	check(store, testCases)

	// Busy
	store = store.Clone(core.SetLastHeartbeatTS(time.Now())).
		Clone(core.SetStoreStats(&pdpb.StoreStats{IsBusy: true}))
	testCases = []testCase{
		{0, "store-state-ok-filter", "store-state-ok-filter"},
		{1, "store-state-busy-filter", "store-state-busy-filter"},
		{2, "store-state-busy-filter", "store-state-busy-filter"},
		{3, "store-state-ok-filter", "store-state-ok-filter"},
	}
	check(store, testCases)
}

func TestIsolationFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	testCluster := mockcluster.NewCluster(ctx, opt)
	testCluster.SetLocationLabels([]string{"zone", "rack", "host"})
	allStores := []struct {
		storeID     uint64
		regionCount int
		labels      map[string]string
	}{
		{1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}},
		{2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"}},
		{3, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"}},
		{4, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"}},
		{5, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"}},
		{6, 1, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"}},
		{7, 1, map[string]string{"zone": "z3", "rack": "r3", "host": "h1"}},
	}
	for _, store := range allStores {
		testCluster.AddLabelsStore(store.storeID, store.regionCount, store.labels)
	}

	testCases := []struct {
		region         *core.RegionInfo
		isolationLevel string
		sourceRes      []plan.StatusCode
		targetRes      []plan.StatusCode
	}{
		{
			core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 6},
			}}, &metapb.Peer{StoreId: 1, Id: 1}),
			"zone",
			[]plan.StatusCode{plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK},
			[]plan.StatusCode{plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusOK},
		},
		{
			core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 4},
				{Id: 3, StoreId: 7},
			}}, &metapb.Peer{StoreId: 1, Id: 1}),
			"rack",
			[]plan.StatusCode{plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK},
			[]plan.StatusCode{plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusOK, plan.StatusOK, plan.StatusStoreNotMatchIsolation},
		},
		{
			core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 4},
				{Id: 3, StoreId: 6},
			}}, &metapb.Peer{StoreId: 1, Id: 1}),
			"host",
			[]plan.StatusCode{plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK, plan.StatusOK},
			[]plan.StatusCode{plan.StatusStoreNotMatchIsolation, plan.StatusStoreNotMatchIsolation, plan.StatusOK, plan.StatusStoreNotMatchIsolation, plan.StatusOK, plan.StatusStoreNotMatchIsolation, plan.StatusOK},
		},
	}

	for _, testCase := range testCases {
		filter := NewIsolationFilter("", testCase.isolationLevel, testCluster.GetLocationLabels(), testCluster.GetRegionStores(testCase.region))
		for idx, store := range allStores {
			re.Equal(testCase.sourceRes[idx], filter.Source(testCluster.GetSharedConfig(), testCluster.GetStore(store.storeID)).StatusCode)
			re.Equal(testCase.targetRes[idx], filter.Target(testCluster.GetSharedConfig(), testCluster.GetStore(store.storeID)).StatusCode)
		}
	}
}

func TestPlacementGuard(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	testCluster := mockcluster.NewCluster(ctx, opt)
	testCluster.SetLocationLabels([]string{"zone"})
	testCluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	testCluster.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2"})
	testCluster.AddLabelsStore(5, 1, map[string]string{"zone": "z3"})
	region := core.NewRegionInfo(&metapb.Region{Peers: []*metapb.Peer{
		{StoreId: 1, Id: 1},
		{StoreId: 3, Id: 3},
		{StoreId: 5, Id: 5},
	}}, &metapb.Peer{StoreId: 1, Id: 1})
	store := testCluster.GetStore(1)

	re.IsType(NewLocationSafeguard("", []string{"zone"}, testCluster.GetRegionStores(region), store),
		NewPlacementSafeguard("", testCluster.GetSharedConfig(), testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, store, nil))
	testCluster.SetEnablePlacementRules(true)
	re.IsType(newRuleFitFilter("", testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, nil, 1),
		NewPlacementSafeguard("", testCluster.GetSharedConfig(), testCluster.GetBasicCluster(), testCluster.GetRuleManager(), region, store, nil))
}

func TestSpecialUseFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	testCluster := mockcluster.NewCluster(ctx, opt)

	testCases := []struct {
		label     map[string]string
		allowUse  []string
		sourceRes plan.StatusCode
		targetRes plan.StatusCode
	}{
		{nil, []string{""}, plan.StatusOK, plan.StatusOK},
		{map[string]string{SpecialUseKey: SpecialUseHotRegion}, []string{""}, plan.StatusStoreNotMatchRule, plan.StatusStoreNotMatchRule},
		{map[string]string{SpecialUseKey: SpecialUseReserved}, []string{""}, plan.StatusStoreNotMatchRule, plan.StatusStoreNotMatchRule},
		{map[string]string{SpecialUseKey: SpecialUseReserved}, []string{SpecialUseReserved}, plan.StatusOK, plan.StatusOK},
		{map[string]string{core.EngineKey: core.EngineTiFlash}, []string{""}, plan.StatusOK, plan.StatusOK},
		{map[string]string{core.EngineKey: core.EngineTiKV}, []string{""}, plan.StatusOK, plan.StatusOK},
	}
	for _, testCase := range testCases {
		store := core.NewStoreInfoWithLabel(1, testCase.label)
		store = store.Clone(core.SetStoreStats(&pdpb.StoreStats{StoreId: 1, Capacity: 100 * units.GiB, Available: 100 * units.GiB}))
		filter := NewSpecialUseFilter("", testCase.allowUse...)
		re.Equal(testCase.sourceRes, filter.Source(testCluster.GetSharedConfig(), store).StatusCode)
		re.Equal(testCase.targetRes, filter.Target(testCluster.GetSharedConfig(), store).StatusCode)
	}
}

func BenchmarkCloneRegionTest(b *testing.B) {
	epoch := &metapb.RegionEpoch{
		ConfVer: 1,
		Version: 1,
	}
	region := core.NewRegionInfo(
		&metapb.Region{
			Id:       4,
			StartKey: []byte("x"),
			EndKey:   []byte(""),
			Peers: []*metapb.Peer{
				{Id: 108, StoreId: 4},
			},
			RegionEpoch: epoch,
		},
		&metapb.Peer{Id: 108, StoreId: 4},
		core.SetApproximateSize(50),
		core.SetApproximateKeys(20),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = createRegionForRuleFit(region.GetStartKey(), region.GetEndKey(), region.GetPeers(), region.GetLeader())
	}
}
