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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/meta"
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
		suite.getRaftCluster().PutMetaStore(
			&metapb.Store{Id: i, Address: fmt.Sprintf("mock-%d", i), State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving, LastHeartbeat: time.Now().UnixNano()},
		)
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
	suite.getRaftCluster().PutMetaStore(
		&metapb.Store{Id: 5, Address: "mock-5", State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving, LastHeartbeat: time.Now().UnixNano(), Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}}},
	)
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
