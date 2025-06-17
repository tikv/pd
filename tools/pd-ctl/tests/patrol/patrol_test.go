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

package patrol_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type patrolTestSuite struct {
	suite.Suite
	env *pdTests.SchedulingTestEnvironment
}

func TestPatrolTestSuite(t *testing.T) {
	suite.Run(t, new(patrolTestSuite))
}

func (suite *patrolTestSuite) SetupSuite() {
	suite.env = pdTests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, _ string) {
			conf.Replication.MaxReplicas = 2
		},
	)
}

func (suite *patrolTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *patrolTestSuite) TestPatrol() {
	// This tool is designed to run in a non-microservice environment.
	suite.env.RunTestInNonMicroserviceEnv(suite.checkPatrol)
}

func (suite *patrolTestSuite) checkPatrol(cluster *pdTests.TestCluster) {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	pdAddr := cluster.GetLeaderServer().GetAddr()

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            4,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().Add(-time.Minute * 20).UnixNano(),
		},
	}

	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}

	specialKey := "7480000000000AE1FFAB5F72F800000000FF052EEA0100000000FB"
	pdTests.MustPutRegion(re, cluster, 1, 1, []byte(""), []byte(specialKey), core.SetPeers([]*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
	}))
	pdTests.MustPutRegion(re, cluster, 3, 2, []byte(specialKey), []byte(""), core.SetPeers([]*metapb.Peer{
		{Id: 3, StoreId: 1},
		{Id: 4, StoreId: 2},
	}))

	// Test Patrol
	args := []string{"-u", pdAddr, "patrol"}
	var res map[string]any
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &res))
	re.Equal(2, int(res["scan_count"].(float64)))
	re.Len(res["results"], 1)
	results := res["results"].([]any)
	re.Len(results, 1)
	result := results[0].(map[string]any)
	re.Equal("merge_skipped", result["status"].(string))
	re.Equal(713131, int(result["table_id"].(float64)))
	hexKey, err := hex.DecodeString(result["key"].(string))
	re.NoError(err)
	re.Equal(specialKey, string(hexKey))

	// Test Merge
	args = []string{"-u", pdAddr, "patrol", "--enable-auto-merge"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, &res)
	re.NoError(err)
	re.Equal(2, int(res["scan_count"].(float64)))
	re.Equal(1, int(res["count"].(float64)))
	results = res["results"].([]any)
	re.Len(results, 1)
	result = results[0].(map[string]any)
	re.Equal(1, int(result["region_id"].(float64)))
	re.Equal("merge_request_sent", result["status"].(string))
	re.Equal("merge request sent for region 1 and region 3", result["description"].(string))

	args = []string{"-u", pdAddr, "operator", "show"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	var operators []string
	err = json.Unmarshal(output, &operators)
	re.NoError(err)
	re.Len(operators, 2)
	re.Contains(operators[0], "admin-merge-region {merge: region 1 to 3}")
	re.Contains(operators[1], "admin-merge-region {merge: region 1 to 3}")

	// Test limit
	args = []string{"-u", pdAddr, "patrol", "limit", "1"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
}
