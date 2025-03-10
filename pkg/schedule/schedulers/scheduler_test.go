// Copyright 2016 TiKV Project Authors.
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
	"slices"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func prepareSchedulersTest(needToRunStream ...bool) (func(), config.SchedulerConfigProvider, *mockcluster.Cluster, *operator.Controller) {
	Register()
	ctx, cancel := context.WithCancel(context.Background())
	clean := func() {
		cancel()
		// reset some config to avoid affecting other tests
		pendingAmpFactor = defaultPendingAmpFactor
		stddevThreshold = defaultStddevThreshold
		topnPosition = defaultTopnPosition
	}
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	var stream *hbstream.HeartbeatStreams
	if len(needToRunStream) == 0 {
		stream = nil
	} else {
		stream = hbstream.NewTestHeartbeatStreams(ctx, tc, needToRunStream[0])
	}
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSchedulerConfig(), stream)
	tc.SetHotRegionCacheHitsThreshold(1)
	return clean, opt, tc, oc
}

func TestShuffleLeader(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	sl, err := CreateScheduler(types.ShuffleLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ShuffleLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	ops, _ := sl.Schedule(tc, false)
	re.Empty(ops)

	// Add stores 1,2,3,4
	tc.AddLeaderStore(1, 6)
	tc.AddLeaderStore(2, 7)
	tc.AddLeaderStore(3, 8)
	tc.AddLeaderStore(4, 9)
	// Add regions 1,2,3,4 with leaders in stores 1,2,3,4
	tc.AddLeaderRegion(1, 1, 2, 3, 4)
	tc.AddLeaderRegion(2, 2, 3, 4, 1)
	tc.AddLeaderRegion(3, 3, 4, 1, 2)
	tc.AddLeaderRegion(4, 4, 1, 2, 3)

	for range 4 {
		ops, _ = sl.Schedule(tc, false)
		re.NotEmpty(ops)
		re.Equal(operator.OpLeader|operator.OpAdmin, ops[0].Kind())
	}
}

func TestRejectLeader(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetLabelProperty(config.RejectLeader, "noleader", "true")
	// Add 3 stores 1,2,3.
	tc.AddLabelsStore(1, 1, map[string]string{"noleader": "true"})
	tc.UpdateLeaderCount(1, 1)
	tc.AddLeaderStore(2, 10)
	tc.AddLeaderStore(3, 0)
	// Add 2 regions with leader on 1 and 2.
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 1, 3)

	// The label scheduler transfers leader out of store1.
	sl, err := CreateScheduler(types.LabelScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.LabelScheduler, []string{"", ""}))
	re.NoError(err)
	ops, _ := sl.Schedule(tc, false)
	operatorutil.CheckTransferLeaderFrom(re, ops[0], operator.OpLeader, 1)

	// If store3 is disconnected, transfer leader to store 2.
	tc.SetStoreDisconnect(3)
	ops, _ = sl.Schedule(tc, false)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpLeader, 1, 2)

	// As store3 is disconnected, store1 rejects leader. Balancer will not create
	// any operators.
	bs, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)
	ops, _ = bs.Schedule(tc, false)
	re.Empty(ops)

	// Can't evict leader from store2, neither.
	el, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"2"}), func(string) error { return nil })
	re.NoError(err)
	ops, _ = el.Schedule(tc, false)
	re.Empty(ops)

	// If the peer on store3 is pending, not transfer to store3 neither.
	tc.SetStoreUp(3)
	region := tc.GetRegion(1)
	for _, p := range region.GetPeers() {
		if p.GetStoreId() == 3 {
			region = region.Clone(core.WithPendingPeers(append(region.GetPendingPeers(), p)))
			break
		}
	}
	origin, overlaps, rangeChanged := tc.SetRegion(region)
	tc.UpdateSubTree(region, origin, overlaps, rangeChanged)
	ops, _ = sl.Schedule(tc, false)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpLeader, 1, 2)
}

func TestRemoveRejectLeader(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.AddRegionStore(1, 0)
	tc.AddRegionStore(2, 1)
	el, err := CreateScheduler(types.EvictLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.EvictLeaderScheduler, []string{"1"}), func(string) error { return nil })
	re.NoError(err)
	tc.DeleteStore(tc.GetStore(1))
	_, err = el.(*evictLeaderScheduler).conf.removeStoreLocked(1)
	re.NoError(err)
}

func TestShuffleHotRegionScheduleBalance(t *testing.T) {
	re := require.New(t)
	checkBalance(re, false /* disable placement rules */)
	checkBalance(re, true /* enable placement rules */)
}

func checkBalance(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	labels := []string{"zone", "host"}
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3, labels...)
	hb, err := CreateScheduler(types.ShuffleHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ShuffleHotRegionScheduler, []string{"", ""}))
	re.NoError(err)
	// Add stores 1, 2, 3, 4, 5, 6  with hot peer counts 3, 2, 2, 2, 0, 0.
	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z5", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z4", "host": "h6"})

	// Report store written bytes.
	tc.UpdateStorageWrittenBytes(1, 7.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	// Region 1, 2 and 3 are hot regions.
	// | region_id | leader_store | follower_store | follower_store | written_bytes |
	// |-----------|--------------|----------------|----------------|---------------|
	// |     1     |       1      |        2       |       3        |      512KB    |
	// |     2     |       1      |        3       |       4        |      512KB    |
	// |     3     |       1      |        2       |       4        |      512KB    |
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{3, 4})
	tc.AddLeaderRegionWithWriteInfo(3, 1, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 4})

	// try to get an operator
	var ops []*operator.Operator
	for range 100 {
		ops, _ = hb.Schedule(tc, false)
		if ops != nil {
			break
		}
	}
	re.NotEmpty(ops)
	re.Equal(ops[0].Step(ops[0].Len()-1).(operator.TransferLeader).ToStore, ops[0].Step(1).(operator.PromoteLearner).ToStore)
	re.NotEqual(6, ops[0].Step(1).(operator.PromoteLearner).ToStore)
}

func TestHotRegionScheduleAbnormalReplica(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetHotRegionScheduleLimit(0)
	hb, err := CreateScheduler(readType, oc, storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)

	tc.AddRegionStore(1, 3)
	tc.AddRegionStore(2, 2)
	tc.AddRegionStore(3, 2)

	// Report store read bytes.
	tc.UpdateStorageReadBytes(1, 7.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.5*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 4.5*units.MiB*utils.StoreHeartBeatReportInterval)

	tc.AddRegionWithReadInfo(1, 1, 512*units.KiB*utils.StoreHeartBeatReportInterval, 0, 0, utils.StoreHeartBeatReportInterval, []uint64{2})
	tc.AddRegionWithReadInfo(2, 2, 512*units.KiB*utils.StoreHeartBeatReportInterval, 0, 0, utils.StoreHeartBeatReportInterval, []uint64{1, 3})
	tc.AddRegionWithReadInfo(3, 1, 512*units.KiB*utils.StoreHeartBeatReportInterval, 0, 0, utils.StoreHeartBeatReportInterval, []uint64{2, 3})
	testutil.Eventually(re, func() bool {
		return tc.IsRegionHot(tc.GetRegion(1))
	})
	re.False(hb.IsScheduleAllowed(tc))
}

func TestShuffleRegion(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	sl, err := CreateScheduler(types.ShuffleRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ShuffleRegionScheduler, []string{"", ""}))
	re.NoError(err)
	re.True(sl.IsScheduleAllowed(tc))
	ops, _ := sl.Schedule(tc, false)
	re.Empty(ops)

	// Add stores 1, 2, 3, 4
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 7)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 9)
	// Add regions 1, 2, 3, 4 with leaders in stores 1,2,3,4
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 2, 3, 4)
	tc.AddLeaderRegion(3, 3, 4, 1)
	tc.AddLeaderRegion(4, 4, 1, 2)

	for range 4 {
		ops, _ = sl.Schedule(tc, false)
		re.NotEmpty(ops)
		re.Equal(operator.OpRegion, ops[0].Kind())
	}
}

func TestShuffleRegionRole(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	// update rule to 1leader+1follower+1learner
	tc.SetEnablePlacementRules(true)
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   2,
	})
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      "learner",
		Role:    placement.Learner,
		Count:   1,
	})

	// Add stores 1, 2, 3, 4
	tc.AddRegionStore(1, 6)
	tc.AddRegionStore(2, 7)
	tc.AddRegionStore(3, 8)
	tc.AddRegionStore(4, 9)

	// Put a region with 1leader + 1follower + 1learner
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
	}
	region := core.NewRegionInfo(&metapb.Region{
		Id:          1,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       peers,
	}, peers[0])
	tc.PutRegion(region)

	sl, err := CreateScheduler(types.ShuffleRegionScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.ShuffleRegionScheduler, []string{"", ""}))
	re.NoError(err)

	conf := sl.(*shuffleRegionScheduler).conf
	conf.Roles = []string{"follower"}
	ops, _ := sl.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpKind(0), 2, 4) // transfer follower
	conf.Roles = []string{"learner"}
	ops, _ = sl.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferLearner(re, ops[0], operator.OpRegion, 3, 4)
}

func TestSpecialUseHotRegion(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	storage := storage.NewStorageWithMemoryBackend()
	cd := ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""})
	bs, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage, cd)
	re.NoError(err)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 4)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 0)
	tc.AddRegionStore(5, 10)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 1, 2, 3)
	tc.AddLeaderRegion(4, 1, 2, 3)
	tc.AddLeaderRegion(5, 1, 2, 3)

	// balance region without label
	ops, _ := bs.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpKind(0), 1, 4)

	// cannot balance to store 4 and 5 with label
	tc.AddLabelsStore(4, 0, map[string]string{"specialUse": "hotRegion"})
	tc.AddLabelsStore(5, 0, map[string]string{"specialUse": "reserved"})
	ops, _ = bs.Schedule(tc, false)
	re.Empty(ops)

	// can only move peer to 4
	tc.UpdateStorageWrittenBytes(1, 60*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*units.MiB*utils.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 0)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.AddLeaderRegionWithWriteInfo(1, 1, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(2, 1, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(3, 1, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderRegionWithWriteInfo(4, 2, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{1, 3})
	tc.AddLeaderRegionWithWriteInfo(5, 3, 512*units.KiB*utils.RegionHeartBeatReportInterval, 0, 0, utils.RegionHeartBeatReportInterval, []uint64{1, 2})
	hs, err := CreateScheduler(writeType, oc, storage, cd)
	re.NoError(err)
	for range 100 {
		ops, _ = hs.Schedule(tc, false)
		if len(ops) == 0 {
			continue
		}
		operatorutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 1, 4)
	}
}

func TestSpecialUseReserved(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	storage := storage.NewStorageWithMemoryBackend()
	cd := ConfigSliceDecoder(types.BalanceRegionScheduler, []string{"", ""})
	bs, err := CreateScheduler(types.BalanceRegionScheduler, oc, storage, cd)
	re.NoError(err)

	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.AddRegionStore(1, 10)
	tc.AddRegionStore(2, 4)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 0)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.AddLeaderRegion(2, 1, 2, 3)
	tc.AddLeaderRegion(3, 1, 2, 3)
	tc.AddLeaderRegion(4, 1, 2, 3)
	tc.AddLeaderRegion(5, 1, 2, 3)

	// balance region without label
	ops, _ := bs.Schedule(tc, false)
	re.Len(ops, 1)
	operatorutil.CheckTransferPeer(re, ops[0], operator.OpKind(0), 1, 4)

	// cannot balance to store 4 with label
	tc.AddLabelsStore(4, 0, map[string]string{"specialUse": "reserved"})
	ops, _ = bs.Schedule(tc, false)
	re.Empty(ops)
}

func TestBalanceLeaderWithConflictRule(t *testing.T) {
	// Stores:     1    2    3
	// Leaders:    1    0    0
	// Region1:    L    F    F
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetEnablePlacementRules(true)
	lb, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{"", ""}))
	re.NoError(err)

	tc.AddLeaderStore(1, 1)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	tc.AddLeaderRegion(1, 1, 2, 3)
	tc.SetStoreLabel(1, map[string]string{
		"host": "a",
	})
	tc.SetStoreLabel(2, map[string]string{
		"host": "b",
	})
	tc.SetStoreLabel(3, map[string]string{
		"host": "c",
	})

	// Stores:     1    2    3
	// Leaders:    16   0    0
	// Region1:    L    F    F
	tc.UpdateLeaderCount(1, 16)
	testCases := []struct {
		name     string
		rule     *placement.Rule
		schedule bool
	}{
		{
			name: "default Rule",
			rule: &placement.Rule{
				GroupID:        placement.DefaultGroupID,
				ID:             placement.DefaultRuleID,
				Index:          1,
				StartKey:       []byte(""),
				EndKey:         []byte(""),
				Role:           placement.Voter,
				Count:          3,
				LocationLabels: []string{"host"},
			},
			schedule: true,
		},
		{
			name: "single store allowed to be placed leader",
			rule: &placement.Rule{
				GroupID:  placement.DefaultGroupID,
				ID:       placement.DefaultRuleID,
				Index:    1,
				StartKey: []byte(""),
				EndKey:   []byte(""),
				Role:     placement.Leader,
				Count:    1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    "host",
						Op:     placement.In,
						Values: []string{"a"},
					},
				},
				LocationLabels: []string{"host"},
			},
			schedule: false,
		},
		{
			name: "2 store allowed to be placed leader",
			rule: &placement.Rule{
				GroupID:  placement.DefaultGroupID,
				ID:       placement.DefaultRuleID,
				Index:    1,
				StartKey: []byte(""),
				EndKey:   []byte(""),
				Role:     placement.Leader,
				Count:    1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    "host",
						Op:     placement.In,
						Values: []string{"a", "b"},
					},
				},
				LocationLabels: []string{"host"},
			},
			schedule: true,
		},
	}

	for _, testCase := range testCases {
		re.NoError(tc.SetRule(testCase.rule))
		ops, _ := lb.Schedule(tc, false)
		if testCase.schedule {
			re.Len(ops, 1)
		} else {
			re.Empty(ops)
		}
	}
}

func testDecoder(v any) error {
	conf, ok := v.(*scatterRangeSchedulerConfig)
	if ok {
		conf.RangeName = "test"
	}
	return nil
}

func TestIsDefault(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	for schedulerType := range types.SchedulerTypeCompatibleMap {
		bs, err := CreateScheduler(schedulerType, oc,
			storage.NewStorageWithMemoryBackend(),
			testDecoder,
			func(string) error { return nil })
		re.NoError(err)
		if slices.Contains(types.DefaultSchedulers, schedulerType) {
			re.True(bs.IsDefault())
		} else {
			re.False(bs.IsDefault())
		}
	}
}

func TestDisabled(t *testing.T) {
	re := require.New(t)
	cancel, _, _, oc := prepareSchedulersTest()
	defer cancel()

	s := storage.NewStorageWithMemoryBackend()
	for _, schedulerType := range types.DefaultSchedulers {
		bs, err := CreateScheduler(schedulerType, oc, s, testDecoder,
			func(string) error { return nil })
		re.NoError(err)
		re.False(bs.IsDisable())
		re.NoError(bs.SetDisable(true))
		re.True(bs.IsDisable())

		// test ms scheduling server, another server
		scheduling, err := CreateScheduler(schedulerType, oc, s, testDecoder,
			func(string) error { return nil })
		re.NoError(err)
		re.True(scheduling.IsDisable())
	}
}
