// Copyright 2025 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"fmt"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func pauseAllCheckers(re *require.Assertions, cluster *tests.TestCluster) {
	checkerNames := []string{"learner", "replica", "rule", "split", "merge", "joint-state"}
	addr := cluster.GetLeaderServer().GetAddr()
	for _, checkerName := range checkerNames {
		resp := make(map[string]any)
		url := fmt.Sprintf("%s/pd/api/v1/checker/%s", addr, checkerName)
		err := testutil.CheckPostJSON(tests.TestDialClient, url, []byte(`{"delay":1000}`), testutil.StatusOK(re))
		re.NoError(err)
		err = testutil.ReadGetJSON(re, tests.TestDialClient, url, &resp)
		re.NoError(err)
		re.True(resp["paused"].(bool))
	}
}

func cleanStoresAndRegions(re *require.Assertions) func(*tests.TestCluster) {
	return func(cluster *tests.TestCluster) {
		// clean region cache
		for _, server := range cluster.GetServers() {
			pdAddr := cluster.GetConfig().GetClientURL()
			for _, region := range server.GetRegions() {
				url := fmt.Sprintf("%s/pd/api/v1/admin/cache/region/%d", pdAddr, region.GetID())
				err := testutil.CheckDelete(tests.TestDialClient, url, testutil.StatusOK(re))
				re.NoError(err)
			}
			re.Empty(server.GetRegions())
		}

		// clean stores
		leader := cluster.GetLeaderServer()
		for _, store := range leader.GetStores() {
			if store.NodeState == metapb.NodeState_Removed {
				continue
			}
			err := cluster.GetLeaderServer().GetRaftCluster().RemoveStore(store.GetId(), true)
			if err != nil {
				re.ErrorIs(err, errs.ErrStoreRemoved)
			}
			re.NoError(cluster.GetLeaderServer().GetRaftCluster().BuryStore(store.GetId(), true))
		}
		re.NoError(cluster.GetLeaderServer().GetRaftCluster().RemoveTombStoneRecords())
		re.Empty(leader.GetStores())
		testutil.Eventually(re, func() bool {
			if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
				for _, s := range sche.GetBasicCluster().GetStores() {
					if s.GetState() != metapb.StoreState_Tombstone {
						return false
					}
				}
			}
			return true
		})
	}
}

func cleanRules(re *require.Assertions) func(*tests.TestCluster) {
	return func(cluster *tests.TestCluster) {
		// clean rules
		defaultRule := placement.GroupBundle{
			ID: "pd",
			Rules: []*placement.Rule{
				{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
			},
		}
		data, err := json.Marshal([]placement.GroupBundle{defaultRule})
		re.NoError(err)
		urlPrefix := cluster.GetLeaderServer().GetAddr()
		err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix+"/pd/api/v1/config/placement-rule", data, testutil.StatusOK(re))
		re.NoError(err)
	}
}
