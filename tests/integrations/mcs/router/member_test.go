// Copyright 2026 TiKV Project Authors.
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

package router

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestSyncAfterFirstPDMemberStops(t *testing.T) {
	re := require.New(t)
	pd, err := tests.NewTestClusterWithKeyspaceGroup(t.Context(), 3)
	re.NoError(err)
	t.Cleanup(pd.Destroy)
	re.NoError(pd.RunInitialServers())
	leader := pd.MoveLeaderToFirstMember(re)
	leaderName := leader.GetConfig().Name
	re.NoError(leader.BootstrapCluster())
	for i := uint64(1); i <= 3; i++ {
		tests.MustPutStore(re, pd, &metapb.Store{Id: i, State: metapb.StoreState_Up})
	}
	regions := tests.InitRegions(3)
	for _, region := range regions {
		re.NoError(pd.HandleRegionHeartbeat(region))
	}

	// The member loop only runs once a minute by default, which is too slow to
	// tell whether the router can look up the new leader after it is triggered.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop", `return(true)`))
	t.Cleanup(func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/router/server/speedUpMemberLoop"))
	})
	routerServer, cleanup, err := tests.StartSingleRouterServerWithoutCheck(t.Context(), re, leader.GetAddr(), tempurl.Alloc())
	re.NoError(err)
	t.Cleanup(cleanup)
	testutil.Eventually(re, func() bool {
		return routerServer.IsReady()
	})
	// The router etcd client starts with the leader as its only endpoint.
	// Stopping the leader before it learns the other members would make the
	// member list itself fail, which is a different failure from the one under test.
	testutil.Eventually(re, func() bool {
		return len(routerServer.GetClient().Endpoints()) > 1
	})
	regionID := regions[0].GetID()
	basicCluster := routerServer.GetCluster().GetBasicCluster()
	testutil.Eventually(re, func() bool {
		return basicCluster.GetRegion(regionID) != nil
	})

	re.NoError(leader.Stop())
	re.NotEmpty(pd.WaitLeaderChange(leaderName))
	var newLeader *tests.TestServer
	testutil.Eventually(re, func() bool {
		newLeader = pd.GetLeaderServer()
		return newLeader != nil && newLeader.GetRaftCluster() != nil && newLeader.GetRaftCluster().IsRunning()
	})

	// The first member probed is the stopped leader. Its probe must time out so
	// the same scan can reach the new leader and the router can sync from it.
	newRegion := regions[0].Clone(core.WithIncVersion())
	re.NoError(pd.HandleRegionHeartbeat(newRegion))
	testutil.Eventually(re, func() bool {
		region := basicCluster.GetRegion(regionID)
		return region != nil && region.GetRegionEpoch().GetVersion() == newRegion.GetRegionEpoch().GetVersion()
	}, testutil.WithWaitFor(20*time.Second))
}
