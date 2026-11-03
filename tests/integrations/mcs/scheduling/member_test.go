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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

// firstMemberPDLeader makes the first member in PD's membership scan the leader,
// so stopping it exercises the failed probe before any live member can be found.
func firstMemberPDLeader(t *testing.T, pd *tests.TestCluster) *tests.TestServer {
	t.Helper()
	re := require.New(t)
	leaderName := pd.WaitLeader()
	re.NotEmpty(leaderName)
	leader := pd.GetServer(leaderName)
	members, err := etcdutil.ListEtcdMembers(t.Context(), leader.GetEtcdClient())
	re.NoError(err)
	re.Len(members.Members, 3)
	for i := 1; i < len(members.Members); i++ {
		re.Less(members.Members[i-1].ID, members.Members[i].ID)
	}
	firstID := members.Members[0].ID
	if leader.GetServerID() != firstID {
		testutil.Eventually(re, func() bool {
			return leader.MoveEtcdLeader(leader.GetServerID(), firstID) == nil
		})
		leaderName = pd.WaitLeaderChange(leaderName)
		re.NotEmpty(leaderName)
		leader = pd.GetServer(leaderName)
	}
	re.Equal(firstID, leader.GetServerID())
	t.Logf("PD leader %s is the first member in the scan: %x", leaderName, firstID)
	return leader
}

func TestAllocIDAfterFirstPDMemberStops(t *testing.T) {
	re := require.New(t)
	pd, err := tests.NewTestClusterWithKeyspaceGroup(t.Context(), 3)
	re.NoError(err)
	t.Cleanup(pd.Destroy)
	re.NoError(pd.RunInitialServers())
	leader := firstMemberPDLeader(t, pd)
	leaderName := pd.WaitLeader()
	re.NotEmpty(leaderName)
	re.NoError(leader.BootstrapCluster())
	tc, err := tests.NewTestSchedulingCluster(t.Context(), 1, pd)
	re.NoError(err)
	t.Cleanup(tc.Destroy)
	primary := tc.WaitForPrimaryServing(re)
	pd.SetSchedulingCluster(tc)
	cluster := primary.GetCluster()
	before := retryAllocID(re, cluster)
	re.NotZero(before)

	re.NoError(leader.Stop())
	re.NotEmpty(pd.WaitLeaderChange(leaderName))
	// AllocID failures trigger membership checks. The first probe must time out
	// so the same scan can reach the new leader, without the one-minute ticker.
	testutil.Eventually(re, func() bool {
		id, _, err := cluster.AllocID(1)
		return err == nil && id > before
	}, testutil.WithWaitFor(15*time.Second))
}
