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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	coreconstant "github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/meta"
	"github.com/tikv/pd/pkg/schedule/checker"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
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
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
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

func (suite *metaTestSuite) getRaftCluster() *cluster.RaftCluster {
	re := suite.Require()
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leaderServer := suite.cluster.GetServer(leaderName)
	cluster := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(cluster)
	return cluster
}

func (suite *metaTestSuite) TestKeyspaceMetaWatch() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges"))
	}()

	now := time.Now().Unix()
	request := &keyspace.CreateKeyspaceRequest{
		Name: fmt.Sprintf("test_keyspace_%d", 0),
		Config: map[string]string{
			"config_entry_1": "100",
		},
		CreateTime: now,
	}
	resp, err := suite.cluster.GetLeaderServer().GetKeyspaceManager().CreateKeyspace(request)
	re.NoError(err)
	re.NotNil(resp)
	re.Positive(resp.GetId())
	bound := keyspace.MakeRegionBound(resp.GetId())
	tc, err := tests.NewTestSchedulingCluster(suite.ctx, 1, suite.cluster)
	re.NoError(err)
	defer tc.Destroy()
	se := tc.WaitForPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		kr, exist := se.GetCoordinator().GetCheckerController().PopOneSuspectKeyRange()
		if exist {
			return (bytes.Equal(kr[0], bound.TxnLeftBound) && bytes.Equal(kr[1], bound.TxnRightBound)) || (bytes.Equal(kr[0], bound.RawLeftBound) && bytes.Equal(kr[1], bound.RawRightBound))
		}
		return false
	})

	// Write metadata only, with no compatibility labels to mask the boundary
	// lookup. The watches must handle both explicit modes and legacy metadata.
	const rawID, txnID, legacyID = 7001, 7003, 7005
	store := suite.pdLeaderServer.GetServer().GetStorage()
	re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		for id, mode := range map[uint32]string{rawID: "raw", txnID: "txn", legacyID: ""} {
			m := &keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: fmt.Sprintf("metadata-%d", id),
				State: keyspacepb.KeyspaceState_ENABLED,
			}
			if mode != "" {
				m.Config = map[string]string{keyspace.RegionBoundType: mode}
			}
			if err := store.SaveKeyspaceMeta(txn, m); err != nil {
				return err
			}
		}
		return nil
	}))
	cluster := se.GetCluster()
	opts := suite.pdLeaderServer.GetPersistOptions()
	original := opts.GetPDServerConfig()
	suite.T().Cleanup(func() {
		opts.SetPDServerConfig(original)
		persistConfig(re, suite.pdLeaderServer)
	})
	setKeyType := func(mode coreconstant.KeyType) {
		cfg := original.Clone()
		cfg.KeyType = mode.String()
		opts.SetPDServerConfig(cfg)
		persistConfig(re, suite.pdLeaderServer)
	}
	start, end := keyspace.MakeRegionBound(rawID-1), keyspace.MakeRegionBound(legacyID+2)
	raw, txn, legacy := keyspace.MakeRegionBound(rawID), keyspace.MakeRegionBound(txnID), keyspace.MakeRegionBound(legacyID)
	checkKeys := func(wantRaw, wantTxn [][]byte) {
		testutil.Eventually(re, func() bool {
			rawKeys, rawErr := keyspace.GetKeyspaceSplitKeys(start.RawLeftBound, end.RawLeftBound, cluster)
			txnKeys, txnErr := keyspace.GetKeyspaceSplitKeys(start.TxnLeftBound, end.TxnLeftBound, cluster)
			return rawErr == nil && txnErr == nil && slices.EqualFunc(rawKeys, wantRaw, bytes.Equal) && slices.EqualFunc(txnKeys, wantTxn, bytes.Equal)
		})
	}
	setKeyType(coreconstant.Raw)
	checkKeys([][]byte{raw.RawLeftBound, raw.RawRightBound, legacy.RawLeftBound, legacy.RawRightBound},
		[][]byte{txn.TxnLeftBound, txn.TxnRightBound})
	left := core.NewRegionInfo(&metapb.Region{Id: 1, StartKey: keyspace.MakeRegionBound(legacyID - 1).RawLeftBound, EndKey: legacy.RawLeftBound}, nil)
	right := core.NewRegionInfo(&metapb.Region{Id: 2, StartKey: legacy.RawLeftBound, EndKey: end.RawLeftBound}, nil)
	re.False(checker.AllowMerge(cluster, left, right), "raw metadata must protect a merge in MCS")
	re.Equal(coreconstant.Table, cluster.GetSharedConfig().GetKeyType(), "MCS merge policy must not change")

	// No keyspace metadata changes while the legacy boundary moves to txn.
	setKeyType(coreconstant.Txn)
	checkKeys([][]byte{raw.RawLeftBound, raw.RawRightBound},
		[][]byte{txn.TxnLeftBound, txn.TxnRightBound, legacy.TxnLeftBound, legacy.TxnRightBound})
	re.True(checker.AllowMerge(cluster, left, right), "txn fallback must not protect a phantom raw boundary")
	m, err := suite.pdLeaderServer.GetKeyspaceManager().LoadKeyspaceByID(legacyID)
	re.NoError(err)
	re.NotContains(m.GetConfig(), keyspace.RegionBoundType)
}
