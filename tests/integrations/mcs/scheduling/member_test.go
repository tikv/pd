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

	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestAllocIDAfterFirstPDMemberStops(t *testing.T) {
	re := require.New(t)
	pd, err := tests.NewTestClusterWithKeyspaceGroup(t.Context(), 3)
	re.NoError(err)
	t.Cleanup(pd.Destroy)
	re.NoError(pd.RunInitialServers())
	leader := pd.MoveLeaderToFirstMember(re)
	leaderName := leader.GetConfig().Name
	re.NoError(leader.BootstrapCluster())
	tc, err := tests.NewTestSchedulingCluster(t.Context(), 1, pd)
	re.NoError(err)
	t.Cleanup(tc.Destroy)
	primary := tc.WaitForPrimaryServing(re)
	pd.SetSchedulingCluster(tc)
	cluster := primary.GetCluster()
	before := retryAllocID(re, cluster)
	re.NotZero(before)
	// The scheduling etcd client starts with the leader as its only endpoint.
	// Stopping the leader before it learns the other members would make the
	// member list itself fail, which is a different failure from the one under test.
	testutil.Eventually(re, func() bool {
		return len(primary.GetClient().Endpoints()) > 1
	})

	re.NoError(leader.Stop())
	re.NotEmpty(pd.WaitLeaderChange(leaderName))
	// AllocID failures trigger membership checks. The first probe must time out
	// so the same scan can reach the new leader, without the one-minute ticker.
	testutil.Eventually(re, func() bool {
		id, _, err := cluster.AllocID(1)
		return err == nil && id > before
	}, testutil.WithWaitFor(15*time.Second))
}
