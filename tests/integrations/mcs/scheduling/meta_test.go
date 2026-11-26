// Copyright 2023 TiKV Project Authors.
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

package scheduling

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/keyspace"
	keyspaceconstant "github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/meta"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/tests"
)

type metaTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
}

func TestMeta(t *testing.T) {
	suite.Run(t, &metaTestSuite{})
}

func (suite *metaTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
}

func (suite *metaTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *metaTestSuite) TestStoreWatch() {
	re := suite.Require()

	cluster := core.NewBasicCluster()
	// Create a meta watcher.
	_, err := meta.NewWatcher(
		suite.ctx,
		suite.pdLeaderServer.GetEtcdClient(),
		cluster,
	)
	re.NoError(err)
	for i := uint64(1); i <= 4; i++ {
		err = suite.getRaftCluster().PutMetaStore(
			&metapb.Store{Id: i, Address: fmt.Sprintf("mock://tikv-%d:%d", i, i), State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving, LastHeartbeat: time.Now().UnixNano()},
		)
		re.NoError(err)
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/doNotBuryStore", `return(true)`))
	re.NoError(suite.getRaftCluster().RemoveStore(2, false))
	testutil.Eventually(re, func() bool {
		s := cluster.GetStore(2)
		if s == nil {
			return false
		}
		return s.GetState() == metapb.StoreState_Offline
	})
	re.Len(cluster.GetStores(), 4)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/doNotBuryStore"))
	testutil.Eventually(re, func() bool {
		return cluster.GetStore(2).GetState() == metapb.StoreState_Tombstone
	})
	re.NoError(suite.getRaftCluster().RemoveTombStoneRecords())
	testutil.Eventually(re, func() bool {
		return cluster.GetStore(2) == nil
	})

	// test synchronized store labels
	err = suite.getRaftCluster().PutMetaStore(
		&metapb.Store{Id: 5, Address: "mock://tikv-5:5", State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving, LastHeartbeat: time.Now().UnixNano(), Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}}},
	)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		if len(cluster.GetStore(5).GetLabels()) == 0 {
			return false
		}
		return cluster.GetStore(5).GetLabels()[0].GetValue() == "z1"
	})
}

func (suite *metaTestSuite) TestMetadataBoundaryScheduling() {
	re := suite.Require()
	const skip = "github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges"
	re.NoError(failpoint.Enable(skip, "return(true)"))
	defer func() { re.NoError(failpoint.Disable(skip)) }()

	pd := suite.pdLeaderServer
	opts := pd.GetPersistOptions()
	originalPD, originalReplication, originalSchedule := opts.GetPDServerConfig(), opts.GetReplicationConfig(), opts.GetScheduleConfig()
	defer func() {
		opts.SetPDServerConfig(originalPD)
		opts.SetReplicationConfig(originalReplication)
		opts.SetScheduleConfig(originalSchedule)
		persistConfig(re, pd)
	}()
	setMode := func(mode constant.KeyType) {
		cfg := originalPD.Clone()
		cfg.KeyType = mode.String()
		opts.SetPDServerConfig(cfg)
		persistConfig(re, pd)
	}
	opts.SetPlacementRuleEnabled(false)
	opts.SetMaxReplicas(1)
	schedule := originalSchedule.Clone()
	schedule.SplitMergeInterval.Duration = 0
	schedule.EnableOneWayMerge = false
	schedule.EnableCrossTableMerge = false
	opts.SetScheduleConfig(schedule)
	setMode(constant.Raw)

	const rawID, txnID, legacyID = 7001, 7003, 7005
	store, client := pd.GetServer().GetStorage(), pd.GetEtcdClient()
	meta := func(id uint32, config map[string]string) *keyspacepb.KeyspaceMeta {
		return &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: fmt.Sprintf("metadata-%d", id), Config: config}
	}
	rawMeta := meta(rawID, map[string]string{keyspace.RegionBoundType: "raw"})
	txnMeta := meta(txnID, map[string]string{keyspace.RegionBoundType: "txn"})
	// Older Classic PD encoded DEFAULT without a keyspace ID field (proto3 zero).
	legacyDefault := &keyspacepb.KeyspaceMeta{Name: keyspaceconstant.DefaultKeyspaceName}
	// Out-of-range IDs must be ignored even when their metadata cannot be decoded.
	outOfRange := keypath.KeyspaceMetaPath(keyspaceconstant.MaxValidKeyspaceID + 1)
	re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		for _, m := range []*keyspacepb.KeyspaceMeta{legacyDefault, rawMeta, txnMeta, meta(legacyID, nil)} {
			if err := store.SaveKeyspaceMeta(txn, m); err != nil {
				return err
			}
		}
		return txn.Save(outOfRange, "\xff")
	}))
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	cluster := tc.WaitForPrimaryServing(re).GetCluster()
	controller := cluster.GetCoordinator().GetCheckerController()
	defaultID := &keyspace.IDRange{Start: keyspaceconstant.DefaultKeyspaceID, End: keyspaceconstant.DefaultKeyspaceID}
	rawDefaults, _, ready := cluster.GetKeyspaceBoundaryIDs(defaultID, nil, 1)
	re.True(ready)
	re.Equal([]uint32{keyspaceconstant.DefaultKeyspaceID}, rawDefaults)
	ids := &keyspace.IDRange{Start: rawID, End: legacyID}
	checkModes := func(raw, txn []uint32) {
		testutil.Eventually(re, func() bool {
			gotRaw, gotTxn, ready := cluster.GetKeyspaceBoundaryIDs(ids, ids, 10)
			return ready && slices.Equal(raw, gotRaw) && slices.Equal(txn, gotTxn)
		})
	}
	checkModes([]uint32{legacyID, rawID}, []uint32{txnID})
	bounds := keyspace.MakeRegionBound(legacyID)
	testutil.Eventually(re, func() bool {
		kr, ok := controller.PopOneSuspectKeyRange()
		return ok && bytes.Equal(kr[0], bounds.RawLeftBound) && bytes.Equal(kr[1], bounds.RawRightBound)
	})

	start, end := keyspace.MakeRegionBound(legacyID-1).RawLeftBound, keyspace.MakeRegionBound(legacyID+2).RawLeftBound
	re.Empty(cluster.GetRegionLabeler().GetSplitKeys(start, end))
	region := func(id uint64, lo, hi []byte) *core.RegionInfo {
		peer := &metapb.Peer{Id: id, StoreId: 1}
		return core.NewRegionInfo(&metapb.Region{Id: id, StartKey: lo, EndKey: hi, Peers: []*metapb.Peer{peer}}, peer, core.SetApproximateSize(1))
	}
	ops := controller.CheckRegion(region(1, start, end))
	re.Len(ops, 1)
	re.Equal("keyspace-split-region", ops[0].Desc())
	step, ok := ops[0].Step(0).(operator.SplitRegion)
	re.True(ok)
	re.Equal([][]byte{bounds.RawLeftBound, bounds.RawRightBound}, step.SplitKeys)

	left, right := region(1, start, bounds.RawLeftBound), region(2, bounds.RawLeftBound, end)
	cluster.GetBasicCluster().PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	cluster.GetBasicCluster().PutRegion(left)
	cluster.GetBasicCluster().PutRegion(right)
	random, err := schedulers.CreateScheduler(types.RandomMergeScheduler, cluster.GetCoordinator().GetOperatorController(), storage.NewStorageWithMemoryBackend(), schedulers.ConfigSliceDecoder(types.RandomMergeScheduler, []string{"", ""}))
	re.NoError(err)
	checkMerges := func(want int) {
		re.Len(controller.GetMergeChecker().Check(left), want)
		ops, _ := random.Schedule(cluster, false)
		re.Len(ops, want)
	}
	checkMerges(0)
	inside := codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.RawKeyspaceModePrefix, legacyID), 'a'))
	re.True(checker.AllowMerge(cluster, region(3, bounds.RawLeftBound, inside), region(4, inside, bounds.RawRightBound)))

	setMode(constant.Txn)
	checkModes([]uint32{rawID}, []uint32{legacyID, txnID})
	checkMerges(2)
	table := func(id int64) []byte { return codec.EncodeBytes(codec.GenerateTableKey(id)) }
	re.False(checker.AllowMerge(cluster, region(3, table(1), table(2)), region(4, table(2), table(3))))

	rawMeta.Config[keyspace.RegionBoundType] = "txn"
	re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		if err := txn.Save(outOfRange, "\xfe"); err != nil {
			return err
		}
		return store.SaveKeyspaceMeta(txn, rawMeta)
	}))
	checkModes(nil, []uint32{legacyID, txnID, rawID})

	txnMeta.State = keyspacepb.KeyspaceState_TOMBSTONE
	re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		for _, path := range []string{keypath.KeyspaceMetaPath(rawID), outOfRange} {
			if err := txn.Remove(path); err != nil {
				return err
			}
		}
		return store.SaveKeyspaceMeta(txn, txnMeta)
	}))
	checkModes(nil, []uint32{legacyID})

	// A malformed live event keeps scheduling from consuming a partial
	// snapshot. Repairing it allows the rate-limited reload to recover.
	corrupt := keypath.KeyspaceMetaPath(legacyID + 1)
	_, err = client.Txn(suite.ctx).Then(
		clientv3.OpPut(corrupt, "\xff"),
		clientv3.OpDelete(keypath.KeyspaceMetaPath(legacyID)),
	).Commit()
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		_, _, ready := cluster.GetKeyspaceBoundaryIDs(ids, ids, 10)
		return !ready
	})
	checkMerges(0)
	_, err = client.Delete(suite.ctx, corrupt)
	re.NoError(err)
	checkModes(nil, nil)
	checkMerges(2)

	// Reuse the regions, now with each covering exactly one adjacent keyspace.
	right = region(2, bounds.RawLeftBound, bounds.RawRightBound)
	cluster.GetBasicCluster().PutRegion(right)
	re.Empty(cluster.GetRegionLabeler().GetSplitKeys(left.GetStartKey(), right.GetEndKey()))
	rawConfig := map[string]string{keyspace.RegionBoundType: "raw"}
	a, b := meta(legacyID-1, rawConfig), meta(legacyID, rawConfig)
	checkAdjacentMerge := func(live []uint32, wantOps int) {
		re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
			if err := store.SaveKeyspaceMeta(txn, a); err != nil {
				return err
			}
			return store.SaveKeyspaceMeta(txn, b)
		}))
		checkModes(live, nil)
		checkMerges(wantOps)
	}
	checkAdjacentMerge([]uint32{b.GetId(), a.GetId()}, 0)
	a.State = keyspacepb.KeyspaceState_TOMBSTONE
	checkAdjacentMerge([]uint32{b.GetId()}, 0) // B's left boundary still protects it from A's residual data.
	b.State = keyspacepb.KeyspaceState_TOMBSTONE
	checkAdjacentMerge(nil, 2)
}

func (suite *metaTestSuite) getRaftCluster() *cluster.RaftCluster {
	re := suite.Require()
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leaderServer := suite.cluster.GetServer(leaderName)
	cluster := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(cluster)
	return cluster
}
