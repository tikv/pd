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

package checker

import (
	"context"
	"encoding/hex"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/keyspace"
	keyspaceconstant "github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestMain(m *testing.M) {
	// Pooled timers cannot cross synctest bubble boundaries.
	deadlock.Opts.TimerPool = deadlock.TimerPoolDisabled
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type mergeCheckerTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *mockcluster.Cluster
	mc      *MergeChecker
	regions []*core.RegionInfo
}

func TestMergeCheckerTestSuite(t *testing.T) {
	// These fixtures use arbitrary Classic keys that cross APIv2 mode boundaries.
	// NextGen keyspace constraints are covered by TestAllowMergeKeyspace tests.
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	require.NoError(t, failpoint.Enable(modeFlag, "return(false)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
	suite.Run(t, new(mergeCheckerTestSuite))
}

func (suite *mergeCheckerTestSuite) SetupTest() {
	cfg := mockconfig.NewTestOptions()
	gcInterval = 100 * time.Millisecond
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
	suite.cluster.SetMaxMergeRegionSize(2)
	suite.cluster.SetMaxMergeRegionKeys(2)
	suite.cluster.SetLabelProperty(config.RejectLeader, "reject", "leader")
	suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for storeID, labels := range stores {
		suite.cluster.PutStoreWithLabels(storeID, labels...)
	}
	suite.regions = []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 0, 0, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 1, 1, []uint64{109, 4}, []uint64{109, 4}),
	}

	for _, region := range suite.regions {
		suite.cluster.PutRegion(region)
	}
	suite.mc = NewMergeChecker(suite.ctx, suite.cluster, suite.cluster.GetCheckerConfig())
}

func (suite *mergeCheckerTestSuite) TearDownTest() {
	gcInterval = time.Minute
	suite.cancel()
}

func (suite *mergeCheckerTestSuite) TestBasic() {
	synctest.Test(suite.T(), func(t *testing.T) {
		suite := new(mergeCheckerTestSuite)
		suite.SetT(t)
		suite.SetupTest()
		defer suite.TearDownTest()

		re := suite.Require()
		suite.cluster.SetSplitMergeInterval(0)

		// should with same peer count
		ops := suite.mc.Check(suite.regions[0])
		re.Nil(ops)
		// The size should be small enough.
		ops = suite.mc.Check(suite.regions[1])
		re.Nil(ops)
		// target region size is too large
		suite.cluster.PutRegion(suite.regions[1].Clone(core.SetApproximateSize(600)))
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)

		// it can merge if the max region size of the store is greater than the target region size.
		suite.cluster.SetRegionMaxSize("144MiB")
		suite.cluster.SetRegionSizeMB(1024)

		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		suite.cluster.SetRegionSizeMB(144)
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)
		// change the size back
		suite.cluster.PutRegion(suite.regions[1].Clone(core.SetApproximateSize(200)))
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		// Check merge with previous region.
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())

		// Test the peer store check.
		store := suite.cluster.GetStore(1)
		re.NotNil(store)
		// Test the peer store is deleted.
		suite.cluster.DeleteStore(store)
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)
		// Test the store is normal.
		suite.cluster.PutStore(store)
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())
		// Test the store is offline.
		suite.cluster.SetStoreOffline(store.GetID())
		ops = suite.mc.Check(suite.regions[2])
		// Only target region have a peer on the offline store,
		// so it's not ok to merge.
		re.Nil(ops)
		// Test the store is up.
		suite.cluster.SetStoreUp(store.GetID())
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())
		store = suite.cluster.GetStore(5)
		re.NotNil(store)
		// Test the peer store is deleted.
		suite.cluster.DeleteStore(store)
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)
		// Test the store is normal.
		suite.cluster.PutStore(store)
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())
		// Test the store is offline.
		suite.cluster.SetStoreOffline(store.GetID())
		ops = suite.mc.Check(suite.regions[2])
		// Both regions have peers on the offline store,
		// so it's ok to merge.
		re.NotNil(ops)
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())
		// Test the store is up.
		suite.cluster.SetStoreUp(store.GetID())
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())

		// Enable one way merge
		suite.cluster.SetEnableOneWayMerge(true)
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)
		suite.cluster.SetEnableOneWayMerge(false)

		// Make up peers for next region.
		suite.regions[3] = suite.regions[3].Clone(core.WithAddPeer(&metapb.Peer{Id: 110, StoreId: 1}), core.WithAddPeer(&metapb.Peer{Id: 111, StoreId: 2}))
		suite.cluster.PutRegion(suite.regions[3])
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		// Now it merges to next region.
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[3].GetID(), ops[1].RegionID())

		// merge cannot across rule key.
		suite.cluster.SetEnablePlacementRules(true)
		err := suite.cluster.SetRule(&placement.Rule{
			GroupID:     placement.DefaultGroupID,
			ID:          "test",
			Index:       1,
			Override:    true,
			StartKeyHex: hex.EncodeToString([]byte("x")),
			EndKeyHex:   hex.EncodeToString([]byte("z")),
			Role:        placement.Voter,
			Count:       3,
		})
		re.NoError(err)
		// region 2 can only merge with previous region now.
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		re.Equal(suite.regions[2].GetID(), ops[0].RegionID())
		re.Equal(suite.regions[1].GetID(), ops[1].RegionID())
		err = suite.cluster.RuleManager.DeleteRule(placement.DefaultGroupID, "test")
		re.NoError(err)

		//  check 'merge_option' label
		err = suite.cluster.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
			ID:       "test",
			Labels:   []labeler.RegionLabel{{Key: mergeOptionLabel, Value: mergeOptionValueDeny}},
			RuleType: labeler.KeyRange,
			Data:     keyutil.BuildKeyRangeMaps("", "74"),
		})
		re.NoError(err)
		ops = suite.mc.Check(suite.regions[0])
		re.Empty(ops)
		ops = suite.mc.Check(suite.regions[1])
		re.Empty(ops)

		// Skip recently split regions.
		suite.cluster.SetSplitMergeInterval(time.Hour)
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)

		suite.mc.startTime = time.Now().Add(-2 * time.Hour)
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		ops = suite.mc.Check(suite.regions[3])
		re.NotNil(ops)

		suite.mc.RecordRegionSplit([]uint64{suite.regions[2].GetID()})
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)
		ops = suite.mc.Check(suite.regions[3])
		re.Nil(ops)

		// issue #4616
		suite.cluster.SetSplitMergeInterval(500 * time.Millisecond)
		ops = suite.mc.Check(suite.regions[2])
		re.Nil(ops)
		ops = suite.mc.Check(suite.regions[3])
		re.Nil(ops)

		// Cache entries expire strictly after their deadline.
		synctest.Sleep(500*time.Millisecond + time.Nanosecond)
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		ops = suite.mc.Check(suite.regions[3])
		re.NotNil(ops)

		// issue #8405
		suite.mc.startTime = time.Now()
		suite.cluster.SetSplitMergeInterval(time.Second)
		suite.cluster.SetSplitMergeInterval(time.Hour)
		suite.mc.RecordRegionSplit([]uint64{suite.regions[2].GetID()})
		suite.cluster.SetSplitMergeInterval(time.Second)
		suite.mc.Check(suite.regions[2])              // trigger the config update
		synctest.Sleep(time.Second + time.Nanosecond) // wait for the cache to gc
		ops = suite.mc.Check(suite.regions[2])
		re.NotNil(ops)
		ops = suite.mc.Check(suite.regions[3])
		re.NotNil(ops)
	})
}

func (suite *mergeCheckerTestSuite) TestMatchPeers() {
	re := suite.Require()
	suite.cluster.SetSplitMergeInterval(0)
	// partial store overlap not including leader
	ops := suite.mc.Check(suite.regions[2])
	re.NotNil(ops)
	operatorutil.CheckSteps(re, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.TransferLeader{FromStore: 6, ToStore: 5},
		operator.RemovePeer{FromStore: 6},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(re, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// partial store overlap including leader
	newRegion := suite.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 106, StoreId: 1},
			{Id: 107, StoreId: 5},
			{Id: 108, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 106, StoreId: 1}),
	)
	suite.regions[2] = newRegion
	suite.cluster.PutRegion(suite.regions[2])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(re, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.RemovePeer{FromStore: 6},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(re, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores overlap
	suite.regions[2] = suite.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 106, StoreId: 1},
		{Id: 107, StoreId: 5},
		{Id: 108, StoreId: 4},
	}))
	suite.cluster.PutRegion(suite.regions[2])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(re, ops[0], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(re, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores not overlap
	suite.regions[2] = suite.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 109, StoreId: 2},
		{Id: 110, StoreId: 3},
		{Id: 111, StoreId: 6},
	}), core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}))
	suite.cluster.PutRegion(suite.regions[2])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(re, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.RemovePeer{FromStore: 6},
		operator.AddLearner{ToStore: 5},
		operator.PromoteLearner{ToStore: 5},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(re, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// no overlap with reject leader label
	suite.regions[1] = suite.regions[1].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 112, StoreId: 7},
			{Id: 113, StoreId: 8},
			{Id: 114, StoreId: 1},
		}),
		core.WithLeader(&metapb.Peer{Id: 114, StoreId: 1}),
	)
	suite.cluster.PutRegion(suite.regions[1])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(re, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},

		operator.AddLearner{ToStore: 7},
		operator.PromoteLearner{ToStore: 7},
		operator.RemovePeer{FromStore: 6},

		operator.AddLearner{ToStore: 8},
		operator.PromoteLearner{ToStore: 8},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},

		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(re, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// overlap with reject leader label
	suite.regions[1] = suite.regions[1].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 115, StoreId: 7},
			{Id: 116, StoreId: 8},
			{Id: 117, StoreId: 1},
		}),
		core.WithLeader(&metapb.Peer{Id: 117, StoreId: 1}),
	)
	suite.regions[2] = suite.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 118, StoreId: 7},
			{Id: 119, StoreId: 3},
			{Id: 120, StoreId: 2},
		}),
		core.WithLeader(&metapb.Peer{Id: 120, StoreId: 2}),
	)
	suite.cluster.PutRegion(suite.regions[1])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(re, ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},
		operator.AddLearner{ToStore: 8},
		operator.PromoteLearner{ToStore: 8},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(re, ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})
}

func (suite *mergeCheckerTestSuite) TestStoreLimitWithMerge() {
	testCases := []struct {
		name string
		size int64
		keys int64
		// acceptedOps is the number of accepted operators before store limit rejects the next one.
		// 0 means the operators are never throttled.
		acceptedOps int
	}{
		// An empty region (size <= 1MB and keys == 0) has no store limit cost.
		{name: "empty region", size: 1, keys: 0},
		// A region with size <= 1MB but keys > 0 still has data and is throttled.
		{name: "small region with keys", size: 1, keys: 1, acceptedOps: 5},
		// The size of Region is more than 1MB but no more than 20MB.
		{name: "small region", size: 2, keys: 2, acceptedOps: 5},
	}
	for _, testCase := range testCases {
		suite.Run(testCase.name, func() {
			suite.checkStoreLimitWithMerge(testCase.size, testCase.keys, testCase.acceptedOps)
		})
	}
}

func (suite *mergeCheckerTestSuite) checkStoreLimitWithMerge(size, keys int64, acceptedOps int) {
	re := suite.Require()
	cfg := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, cfg)
	tc.SetMaxMergeRegionSize(2)
	tc.SetMaxMergeRegionKeys(2)
	tc.SetSplitMergeInterval(0)
	regions := []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 10, 10, []uint64{109, 4}, []uint64{109, 4}),
	}

	for i := uint64(1); i <= 6; i++ {
		tc.AddLeaderStore(i, 10)
	}

	for _, region := range regions {
		tc.PutRegion(region)
	}

	mc := NewMergeChecker(suite.ctx, tc, tc.GetCheckerConfig())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc, false /* no need to run */)
	oc := operator.NewController(suite.ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)

	regions[2] = regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 109, StoreId: 2},
			{Id: 110, StoreId: 3},
			{Id: 111, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}),
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)

	// set to a small rate to reduce unstable possibility.
	tc.SetAllStoresLimit(storelimit.AddPeer, 0.0000001)
	tc.SetAllStoresLimit(storelimit.RemovePeer, 0.0000001)
	tc.PutRegion(regions[2])

	rounds := acceptedOps
	if rounds == 0 {
		rounds = 50
	}
	for range rounds {
		ops := mc.Check(regions[2])
		re.NotNil(ops)
		re.True(oc.AddOperator(ops...))
		for _, op := range ops {
			oc.RemoveOperator(op, operator.ExceedStoreLimit)
		}
	}
	if acceptedOps > 0 {
		ops := mc.Check(regions[2])
		re.NotNil(ops)
		re.False(oc.AddOperator(ops...))
	}
}

func (suite *mergeCheckerTestSuite) TestCache() {
	synctest.Test(suite.T(), func(t *testing.T) {
		suite := new(mergeCheckerTestSuite)
		suite.SetT(t)
		suite.SetupTest()
		defer suite.TearDownTest()

		re := suite.Require()
		cfg := mockconfig.NewTestOptions()
		suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
		suite.cluster.SetMaxMergeRegionSize(2)
		suite.cluster.SetMaxMergeRegionKeys(2)
		suite.cluster.SetSplitMergeInterval(time.Hour)
		suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
		stores := map[uint64][]string{
			1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		}
		for storeID, labels := range stores {
			suite.cluster.PutStoreWithLabels(storeID, labels...)
		}
		suite.regions = []*core.RegionInfo{
			newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
			newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		}

		for _, region := range suite.regions {
			suite.cluster.PutRegion(region)
		}

		suite.mc = NewMergeChecker(suite.ctx, suite.cluster, suite.cluster.GetCheckerConfig())

		ops := suite.mc.Check(suite.regions[1])
		re.Nil(ops)
		suite.cluster.SetSplitMergeInterval(0)
		synctest.Sleep(time.Second + time.Nanosecond)
		ops = suite.mc.Check(suite.regions[1])
		re.NotNil(ops)
	})
}

func newRegionInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.RegionInfo {
	prs := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, &metapb.Peer{Id: peer[0], StoreId: peer[1]})
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			Peers:    prs,
		},
		&metapb.Peer{Id: leader[0], StoreId: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}

func TestMergeCheckerSkipsActiveAffinityGroup(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		targetStores          []uint64
		maxAffinityMergeSize  uint64
		affinityScheduleLimit uint64
		expectAffinityMerge   bool
	}{
		{"aligned", []uint64{1, 2, 3}, 20, 4, true},
		{"different-stores", []uint64{4, 5, 6}, 20, 4, false},
		{"affinity-merge-disabled", []uint64{1, 2, 3}, 0, 4, false},
		{"affinity-scheduling-disabled", []uint64{4, 5, 6}, 20, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			opt := newAffinityTestOptions()
			opt.SetMaxMergeRegionSize(20)
			opt.SetMaxMergeRegionKeys(200)
			opt.SetSplitMergeInterval(0)
			opt.SetMaxAffinityMergeRegionSize(tc.maxAffinityMergeSize)
			opt.SetAffinityScheduleLimit(tc.affinityScheduleLimit)
			cluster := mockcluster.NewCluster(ctx, opt)
			for storeID := uint64(1); storeID <= 6; storeID++ {
				cluster.AddRegionStore(storeID, 10)
			}
			cluster.AddLeaderRegion(1, 1, 2, 3)
			cluster.AddLeaderRegion(2, tc.targetStores[0], tc.targetStores[1:]...)
			source := cluster.GetRegion(1).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b")),
				core.SetApproximateSize(5), core.SetApproximateKeys(5))
			target := cluster.GetRegion(2).Clone(core.WithStartKey([]byte("b")), core.WithEndKey([]byte("c")),
				core.SetApproximateSize(5), core.SetApproximateKeys(5))
			cluster.PutRegion(source)
			cluster.PutRegion(target)

			manager := cluster.GetAffinityManager()
			re.NoError(createAffinityGroupForTest(manager, &affinity.Group{
				ID: "test", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3},
			}, []byte("a"), []byte("c")))
			re.False(manager.GetAffinityGroupState("test").RegularSchedulingAllowed)
			affinityChecker := newTestAffinityChecker(ctx, cluster, opt)
			ops := affinityChecker.Check(source)
			if tc.expectAffinityMerge {
				re.Len(ops, 2)
				for _, op := range ops {
					re.Equal(operator.OpAffinity, op.SchedulerKind())
				}
			} else {
				re.Nil(ops)
			}

			mergeChecker := NewMergeChecker(ctx, cluster, opt)
			affinityFilter := filter.NewAffinityFilter(cluster)
			for _, region := range []*core.RegionInfo{source, target} {
				// Active groups stay protected even when affinity-schedule-limit is zero.
				re.False(affinityFilter.Select(region).IsOK())
				re.Nil(mergeChecker.Check(region))
			}
			re.False(AllowMerge(cluster, source, target))
			re.False(AllowMerge(cluster, target, source))

			// A healthy region with matching voters would restore the degraded group.
			_, err := manager.UpdateAffinityGroupPeers("test", 1, []uint64{1, 2, 4})
			re.NoError(err)
			manager.DegradeAffinityGroup("test")
			state := manager.GetAffinityGroupState("test")
			re.False(state.AffinitySchedulingAllowed)
			re.False(state.RegularSchedulingAllowed)
			re.Nil(affinityChecker.Check(source))
			re.False(affinityFilter.Select(source).IsOK())
			re.Nil(mergeChecker.Check(source))
			re.False(AllowMerge(cluster, source, target))

			manager.ExpireAffinityGroup("test")
			re.True(manager.GetAffinityGroupState("test").RegularSchedulingAllowed)
			re.True(AllowMerge(cluster, source, target))
			re.True(AllowMerge(cluster, target, source))
			for _, region := range []*core.RegionInfo{source, target} {
				re.True(affinityFilter.Select(region).IsOK())
				re.Len(mergeChecker.Check(region), 2)
			}
		})
	}
}

func TestAllowMergeCrossTable(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	ctx := t.Context()
	cfg := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, cfg)
	// Disable placement rules so AllowMerge is decided only by key type / table ID.
	cluster.SetEnablePlacementRules(false)

	const (
		keyspace1 = uint32(42)
		keyspace2 = uint32(43)
		tableA    = int64(100)
		tableB    = int64(101)
		indexID   = int64(1)
	)

	// Classic keys.
	classicTableA := codec.EncodeBytes(codec.GenerateTableKey(tableA))
	classicTableB := codec.EncodeBytes(codec.GenerateTableKey(tableB))
	classicTableC := codec.EncodeBytes(codec.GenerateTableKey(tableB + 1))
	classicIndexA := codec.EncodeBytes(codec.GenerateIndexKey(tableA, indexID))
	classicRecordA := codec.EncodeBytes(codec.GenerateRowKey(tableA, 1))

	// Same-keyspace keys.
	ks1TableA := encodeKeyspaceRawKey(keyspace1, codec.GenerateTableKey(tableA))
	ks1TableB := encodeKeyspaceRawKey(keyspace1, codec.GenerateTableKey(tableB))
	ks1TableC := encodeKeyspaceRawKey(keyspace1, codec.GenerateTableKey(tableB+1))
	ks1IndexA := encodeKeyspaceRawKey(keyspace1, codec.GenerateIndexKey(tableA, indexID))
	ks1RecordA := encodeKeyspaceRawKey(keyspace1, codec.GenerateRowKey(tableA, 1))

	// Different-keyspace keys. Adjacent ranges are constructed artificially so
	// AllowMerge can reach the table-ID check; production keyspaces are usually
	// separated by fence regions and would not be merge candidates.
	ks2TableA := encodeKeyspaceRawKey(keyspace2, codec.GenerateTableKey(tableA))
	ks2TableB := encodeKeyspaceRawKey(keyspace2, codec.GenerateTableKey(tableB))
	ks2TableC := encodeKeyspaceRawKey(keyspace2, codec.GenerateTableKey(tableB+1))

	// Matrix: layout × same/diff table identity × enable-cross-table-merge.
	// Logical table identity is (keyspaceID, tableID); classic has no keyspace.
	type adjacentPair struct {
		startA, endA []byte
		startB, endB []byte
	}
	classicDiffTable := adjacentPair{classicTableA, classicTableB, classicTableB, classicTableC}
	classicSameTable := adjacentPair{classicIndexA, classicRecordA, classicRecordA, classicTableB}
	sameKSDiffTable := adjacentPair{ks1TableA, ks1TableB, ks1TableB, ks1TableC}
	sameKSSameTable := adjacentPair{ks1IndexA, ks1RecordA, ks1RecordA, ks1TableB}
	// Different keyspaces with the same numeric table id are still different tables.
	diffKSSameTable := adjacentPair{ks1TableA, ks2TableA, ks2TableA, ks2TableB}
	diffKSDiffTable := adjacentPair{ks1TableA, ks2TableB, ks2TableB, ks2TableC}

	cases := []struct {
		name            string
		pair            adjacentPair
		nextGen         bool
		crossTableMerge bool
		expectAllow     bool
	}{
		// Classic: different table IDs.
		{"classic/diff-table/cross-enabled", classicDiffTable, false, true, true},
		{"classic/diff-table/cross-disabled", classicDiffTable, false, false, false},
		// Classic: same table ID (index + record).
		{"classic/same-table/cross-enabled", classicSameTable, false, true, true},
		{"classic/same-table/cross-disabled", classicSameTable, false, false, true},

		// Same keyspace: different table IDs.
		{"same-keyspace/diff-table/cross-enabled", sameKSDiffTable, false, true, true},
		{"same-keyspace/diff-table/cross-disabled", sameKSDiffTable, false, false, false},
		// Same keyspace: same table ID (index + record).
		{"same-keyspace/same-table/cross-enabled", sameKSSameTable, false, true, true},
		{"same-keyspace/same-table/cross-disabled", sameKSSameTable, false, false, true},
		{"nextgen/same-keyspace/diff-table/cross-disabled", sameKSDiffTable, true, false, false},
		{"nextgen/same-keyspace/same-table/cross-disabled", sameKSSameTable, true, false, true},

		// Different keyspaces: same numeric table ID is not the same logical table.
		// With cross-table enabled, merge is still allowed by policy (split keys
		// from keyspace labels normally block this path in production).
		{"diff-keyspace/same-table/cross-enabled", diffKSSameTable, false, true, true},
		{"diff-keyspace/same-table/cross-disabled", diffKSSameTable, false, false, false},
		// Different keyspaces: different table IDs.
		{"diff-keyspace/diff-table/cross-enabled", diffKSDiffTable, false, true, true},
		{"diff-keyspace/diff-table/cross-disabled", diffKSDiffTable, false, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			re.NoError(failpoint.Enable(modeFlag, "return("+strconv.FormatBool(tc.nextGen)+")"))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
			cluster.SetEnableCrossTableMerge(tc.crossTableMerge)
			regionA := newRegionInfoWithBytes(1, tc.pair.startA, tc.pair.endA)
			regionB := newRegionInfoWithBytes(2, tc.pair.startB, tc.pair.endB)
			re.Equal(tc.expectAllow, AllowMerge(cluster, regionA, regionB))
		})
	}
}

func TestAllowMergeKeyspace(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	require.NoError(t, failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
	for _, mode := range codec.KeyspaceModes() {
		t.Run(string(mode), func(t *testing.T) {
			cluster := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
			cluster.SetEnableCrossTableMerge(true)
			// Both placement and label rules have been removed by reclamation.
			boundary := func(id uint32) []byte {
				key := codec.EncodeKeyspaceBoundary(mode, id)
				return key[:]
			}
			key := func(id uint32, suffix string) []byte {
				return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(mode, id), suffix...))
			}
			cases := []struct {
				name               string
				start, middle, end []byte
				allow              bool
			}{
				{"within-keyspace", boundary(42), key(42, "m"), boundary(43), true},
				{"complete-neighbors", boundary(42), boundary(43), boundary(44), true},
				{"complete-spans", boundary(40), boundary(43), boundary(46), true},
				{"partial-left", key(42, "a"), boundary(43), boundary(44), false},
				{"partial-right", boundary(42), boundary(43), key(43, "b"), false},
				{"complete-union-partial-sides", boundary(40), key(43, "m"), boundary(46), true},
				{"partial-sides-with-complete-keyspaces", key(40, "a"), key(43, "m"), key(46, "z"), true},
				{"left-only-partial-keyspace", key(42, "a"), key(43, "m"), boundary(45), false},
				{"right-only-partial-keyspace", boundary(40), key(43, "m"), key(44, "z"), false},
				{"complete-union-without-complete-right", boundary(42), key(43, "m"), boundary(44), false},
				{"maximum-id", boundary(keyspaceconstant.MaxValidKeyspaceID - 1), boundary(keyspaceconstant.MaxValidKeyspaceID), boundary(keyspaceconstant.MaxValidKeyspaceID + 1), true},
				{"first-id", boundary(0), boundary(1), boundary(2), true},
				{"unbounded-left", nil, boundary(42), boundary(43), false},
				{"unbounded-right", boundary(42), boundary(43), nil, false},
				{"short-middle", boundary(42), boundary(43)[:4], boundary(44), false},
				{"short-internal-end", boundary(42), key(42, "m"), boundary(43)[:4], true},
				{"short-internal-start", boundary(43)[:4], append(boundary(43)[:4:4], 0), boundary(43), true},
				{"outside-keyspaces", []byte("a"), []byte("b"), []byte("c"), true},
				{"mode-crossing", codec.EncodeBytes(codec.MakeKeyspacePrefix(codec.RawKeyspaceModePrefix, 42)), boundary(43), codec.EncodeBytes(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 44)), false},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					re := require.New(t)
					left := newRegionInfoWithBytes(1, tc.start, tc.middle)
					right := newRegionInfoWithBytes(2, tc.middle, tc.end)
					re.Equal(tc.allow, AllowMerge(cluster, left, right))
					re.Equal(tc.allow, AllowMerge(cluster, right, left))
				})
			}
		})
	}
}

func TestAllowMergeKeyspaceConstraints(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	require.NoError(t, failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
	re := require.New(t)
	cluster := mockcluster.NewCluster(t.Context(), mockconfig.NewTestOptions())
	cluster.SetEnableCrossTableMerge(true)
	boundary := func(id uint32) []byte {
		key := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, id)
		return key[:]
	}
	start, middle, end := boundary(42), boundary(43), boundary(44)
	left := newRegionInfoWithBytes(1, start, middle)
	right := newRegionInfoWithBytes(2, middle, end)
	for _, id := range []uint32{42, 43} {
		re.NoError(cluster.GetRegionLabeler().SetLabelRule(keyspace.MakeTxnLabelRule(id)))
	}
	merged := left.Clone(core.WithEndKey(end))
	sc := NewSplitChecker(cluster, cluster.RuleManager, cluster.GetRegionLabeler())
	re.False(AllowMerge(cluster, left, right))
	re.NotNil(sc.Check(merged))
	re.NoError(cluster.GetRegionLabeler().DeleteLabelRule("keyspaces/42"))
	re.False(AllowMerge(cluster, left, right))
	re.NotNil(sc.Check(merged))
	re.NoError(cluster.GetRegionLabeler().DeleteLabelRule("keyspaces/43"))
	re.True(AllowMerge(cluster, left, right))
	re.Nil(sc.Check(merged))

	// A placement-rule boundary remains a hard constraint.
	re.NoError(cluster.SetRule(&placement.Rule{
		GroupID:     placement.DefaultGroupID,
		ID:          "keyspace",
		Index:       1,
		Override:    true,
		StartKeyHex: hex.EncodeToString(middle),
		EndKeyHex:   hex.EncodeToString(end),
		Role:        placement.Voter,
		Count:       3,
	}))
	re.False(AllowMerge(cluster, left, right))
	re.NotNil(sc.Check(merged))
	re.NoError(cluster.RuleManager.DeleteRule(placement.DefaultGroupID, "keyspace"))
	re.True(AllowMerge(cluster, left, right))
	re.Nil(sc.Check(merged))

	// merge_option=deny applies even if it has no boundary inside the range.
	re.NoError(cluster.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID:       "deny",
		Labels:   []labeler.RegionLabel{{Key: mergeOptionLabel, Value: mergeOptionValueDeny}},
		RuleType: labeler.KeyRange,
		Data:     keyutil.BuildKeyRangeMaps(hex.EncodeToString(start), hex.EncodeToString(end)),
	}))
	re.False(AllowMerge(cluster, left, right))
	re.NoError(cluster.GetRegionLabeler().DeleteLabelRule("deny"))
}

func encodeKeyspaceRawKey(keyspaceID uint32, rawKey []byte) []byte {
	prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
	return codec.EncodeBytes(append(prefix, rawKey...))
}

func newRegionInfoWithBytes(id uint64, startKey, endKey []byte) *core.RegionInfo {
	peer := &metapb.Peer{Id: id * 10, StoreId: 1}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: startKey,
			EndKey:   endKey,
			Peers:    []*metapb.Peer{peer},
		},
		peer,
	)
}
