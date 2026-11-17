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
	"time"

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
	for _, name := range []string{
		"github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker",
		"github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval",
	} {
		re.NoError(failpoint.Enable(name, "return(true)"))
		t.Cleanup(func() { re.NoError(failpoint.Disable(name)) })
	}
	env := tests.NewSchedulingTestEnvironment(t, func(c *config.Config, _ string) { c.Schedule.AffinityScheduleLimit = 4 })
	env.PDCount = 3
	t.Cleanup(env.Cleanup)
	env.RunTestInMicroserviceEnv(func(pd *tests.TestCluster) {
		leader := pd.GetLeaderServer()
		primary := pd.GetSchedulingPrimaryServer()
		cluster := primary.GetCluster()
		for id := uint64(1); id <= 3; id++ {
			tests.MustPutStore(re, pd, &metapb.Store{Id: id, NodeState: metapb.NodeState_Serving})
		}
		manager := cluster.GetAffinityManager()
		peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}
		for i, id := range []string{"g", "h"} {
			if i == 1 {
				// Learn the other etcd endpoints before stopping the tracked PD leader.
				testutil.Eventually(re, func() bool { return len(primary.GetClient().Endpoints()) > 1 })
				oldLeaderName := leader.GetServer().Name()
				re.NoError(leader.Stop())
				newLeaderName := pd.WaitLeaderChange(oldLeaderName)
				re.NotEmpty(newLeaderName)
				re.NotEqual(oldLeaderName, newLeaderName)
				leader = pd.GetServer(newLeaderName)
			}
			for storeID := uint64(1); storeID <= 3; storeID++ {
				tests.MustHandleStoreHeartbeat(re, pd, &pdpb.StoreHeartbeatRequest{
					Header: testutil.NewRequestHeader(leader.GetClusterID()),
					Stats:  &pdpb.StoreStats{StoreId: storeID, Capacity: 10 * units.GiB, Available: 9 * units.GiB, UsedSize: units.GiB},
				})
			}
			pdManager, err := leader.GetServer().GetAffinityManager()
			re.NoError(err)
			startKey, endKey := []byte("b"), []byte("c")
			if i == 1 {
				startKey, endKey = []byte("d"), []byte("e")
			}
			// Avoid bootstrap keyspace boundaries, which require a TiKV split.
			re.NoError(pdManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: id,
				KeyRanges: []keyutil.KeyRange{{StartKey: startKey, EndKey: endKey}},
			}}))
			testutil.Eventually(re, func() bool {
				g := manager.GetAffinityGroupState(id)
				return g != nil && g.RangeCount == 1 && cluster.GetStore(3) != nil
			})
			tests.MustPutRegionInfo(re, pd, core.NewRegionInfo(&metapb.Region{
				Id: uint64(100 + i), StartKey: startKey, EndKey: endKey, Peers: peers,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			}, peers[i], core.SetApproximateSize(1), core.SetApproximateKeys(1)))
			testutil.Eventually(re, func() bool {
				value, err := leader.GetServer().GetStorage().LoadAffinityGroup(id)
				re.NoError(err)
				var stored affinity.Group
				re.NoError(json.Unmarshal([]byte(value), &stored))
				pdGroup, schedulingGroup := pdManager.GetAffinityGroupState(id), manager.GetAffinityGroupState(id)
				return stored.LeaderStoreID == peers[i].GetStoreId() && len(stored.VoterStoreIDs) == 3 &&
					pdGroup.AffinitySchedulingAllowed && schedulingGroup.AffinitySchedulingAllowed &&
					stored.String() == pdGroup.String() && stored.String() == schedulingGroup.String()
			}, testutil.WithWaitFor(30*time.Second))
		}
	})
}
