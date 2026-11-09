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

package scheduling

import (
	"encoding/json"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestAffinityFirstWinPersistence(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker")) })
	env := tests.NewSchedulingTestEnvironment(t, func(c *config.Config, _ string) { c.Schedule.AffinityScheduleLimit = 4 })
	t.Cleanup(env.Cleanup)
	env.RunTestInMicroserviceEnv(func(pd *tests.TestCluster) {
		leader := pd.GetLeaderServer()
		cluster := pd.GetSchedulingPrimaryServer().GetCluster()
		for id := uint64(1); id <= 3; id++ {
			tests.MustPutStore(re, pd, &metapb.Store{Id: id, NodeState: metapb.NodeState_Serving})
			tests.MustHandleStoreHeartbeat(re, pd, &pdpb.StoreHeartbeatRequest{
				Header: testutil.NewRequestHeader(leader.GetClusterID()),
				Stats:  &pdpb.StoreStats{StoreId: id, Capacity: 10 * units.GiB, Available: 9 * units.GiB, UsedSize: units.GiB},
			})
		}
		pdManager, err := leader.GetServer().GetAffinityManager()
		re.NoError(err)
		// Avoid bootstrap keyspace boundaries, which require a TiKV split.
		re.NoError(pdManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "g",
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte("b"), EndKey: []byte("c")}},
		}}))
		manager := cluster.GetAffinityManager()
		testutil.Eventually(re, func() bool {
			g := manager.GetAffinityGroupState("g")
			return g != nil && g.RangeCount == 1 && cluster.GetStore(3) != nil
		})
		peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}
		tests.MustPutRegionInfo(re, pd, core.NewRegionInfo(&metapb.Region{
			Id: 100, StartKey: []byte("b"), EndKey: []byte("c"), Peers: peers,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		}, peers[0], core.SetApproximateSize(1), core.SetApproximateKeys(1)))
		testutil.Eventually(re, func() bool {
			value, err := leader.GetServer().GetStorage().LoadAffinityGroup("g")
			re.NoError(err)
			var stored affinity.Group
			re.NoError(json.Unmarshal([]byte(value), &stored))
			pdGroup, schedulingGroup := pdManager.GetAffinityGroupState("g"), manager.GetAffinityGroupState("g")
			return stored.LeaderStoreID == 1 && len(stored.VoterStoreIDs) == 3 &&
				pdGroup.AffinitySchedulingAllowed && schedulingGroup.AffinitySchedulingAllowed &&
				stored.String() == pdGroup.String() && stored.String() == schedulingGroup.String()
		})
	})
}
