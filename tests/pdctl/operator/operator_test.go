// Copyright 2019 TiKV Project Authors.
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

package operator_test

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

type operatorTestSuite struct {
	suite.Suite
}

func TestOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(operatorTestSuite))
}

func (suite *operatorTestSuite) TestOperator() {
	var start time.Time
	start = start.Add(time.Hour)
	opts := []tests.ConfigOption{
		// TODO: enable placementrules
		func(conf *config.Config, serverName string) {
			conf.Replication.MaxReplicas = 2
			conf.Replication.EnablePlacementRules = false
		},
		func(conf *config.Config, serverName string) {
			conf.Schedule.MaxStoreDownTime.Duration = time.Since(start)
		},
	}
	env := tests.NewSchedulingTestEnvironment(suite.T(), opts...)
	env.RunTestInTwoModes(suite.checkOperator)
}

func (suite *operatorTestSuite) checkOperator(cluster *tests.TestCluster) {
	re := suite.Require()

	cmd := pdctlCmd.GetRootCmd()

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
		tests.MustPutStore(re, cluster, store)
	}

	tests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetPeers([]*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
	}))
	tests.MustPutRegion(re, cluster, 3, 2, []byte("b"), []byte("d"), core.SetPeers([]*metapb.Peer{
		{Id: 3, StoreId: 1},
		{Id: 4, StoreId: 2},
	}))

	pdAddr := cluster.GetLeaderServer().GetAddr()
	args := []string{"-u", pdAddr, "operator", "show"}
	var slice []string
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &slice))
	re.Len(slice, 0)
	args = []string{"-u", pdAddr, "operator", "check", "2"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "operator not found")

	var testCases = []struct {
		cmd    []string
		show   []string
		expect string
		reset  []string
	}{
		{
			// operator add add-peer <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "add-peer", "1", "3"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "promote learner peer 1 on store 3",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add remove-peer <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "remove-peer", "1", "2"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "remove peer on store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-leader <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-leader", "1", "2"},
			show:   []string{"-u", pdAddr, "operator", "show", "leader"},
			expect: "transfer leader from store 1 to store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-region <region_id> <to_store_id>...
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-region", "1", "2", "3"},
			show:   []string{"-u", pdAddr, "operator", "show", "region"},
			expect: "remove peer on store 1",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-peer <region_id> <from_store_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-peer", "1", "2", "3"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "remove peer on store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate|usekey] [--keys=xxx(xxx is hex encoded string)]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=scan"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split region with policy SCAN",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate|usekey] [--keys=xxx(xxx is hex encoded string)]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=approximate"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split region with policy APPROXIMATE",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate|usekey] [--keys=xxx(xxx is hex encoded string)]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=scan"},
			show:   []string{"-u", pdAddr, "operator", "check", "3"},
			expect: "split region with policy SCAN",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate|usekey] [--keys=xxx(xxx is hex encoded string)]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=approximate"},
			show:   []string{"-u", pdAddr, "operator", "check", "3"},
			expect: "status: RUNNING",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate|usekey] [--keys=xxx(xxx is hex encoded string)]
			cmd: []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=usekey",
				"--keys=" + hex.EncodeToString([]byte("c"))},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split: region 3 use policy USEKEY and keys [" + hex.EncodeToString([]byte("c")) + "]",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
	}

	for _, testCase := range testCases {
		output, err = pdctl.ExecuteCommand(cmd, testCase.cmd...)
		re.NoError(err)
		re.NotContains(string(output), "Failed")
		output, err = pdctl.ExecuteCommand(cmd, testCase.show...)
		re.NoError(err)
		re.Contains(string(output), testCase.expect)
		start := time.Now()
		_, err = pdctl.ExecuteCommand(cmd, testCase.reset...)
		re.NoError(err)
		historyCmd := []string{"-u", pdAddr, "operator", "history", strconv.FormatInt(start.Unix(), 10)}
		records, err := pdctl.ExecuteCommand(cmd, historyCmd...)
		re.NoError(err)
		re.Contains(string(records), "admin")
	}

	// operator add merge-region <source_region_id> <target_region_id>
	args = []string{"-u", pdAddr, "operator", "add", "merge-region", "1", "3"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "operator", "show"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "merge region 1 into region 3")
	args = []string{"-u", pdAddr, "operator", "remove", "1"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "operator", "remove", "3"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)

	_, err = pdctl.ExecuteCommand(cmd, "config", "set", "enable-placement-rules", "true")
	re.NoError(err)
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		// wait for the scheduler server to update the config
		testutil.Eventually(re, func() bool {
			return sche.GetCluster().GetCheckerConfig().IsPlacementRulesEnabled()
		})
	}

	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-region", "1", "2", "3")
	re.NoError(err)
	re.Contains(string(output), "not supported")
	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-region", "1", "2", "follower", "3")
	re.NoError(err)
	re.Contains(string(output), "not match")
	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-peer", "1", "2", "4")
	re.NoError(err)
	re.Contains(string(output), "is unhealthy")
	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-region", "1", "2", "leader", "4", "follower")
	re.NoError(err)
	re.Contains(string(output), "is unhealthy")
	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-region", "1", "2", "follower", "leader", "3", "follower")
	re.NoError(err)
	re.Contains(string(output), "invalid")
	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-region", "1", "leader", "2", "follower", "3")
	re.NoError(err)
	re.Contains(string(output), "invalid")
	output, err = pdctl.ExecuteCommand(cmd, "operator", "add", "transfer-region", "1", "2", "leader", "3", "follower")
	re.NoError(err)
	re.Contains(string(output), "Success!")
	output, err = pdctl.ExecuteCommand(cmd, "-u", pdAddr, "operator", "remove", "1")
	re.NoError(err)
	re.Contains(string(output), "Success!")

	_, err = pdctl.ExecuteCommand(cmd, "config", "set", "enable-placement-rules", "false")
	re.NoError(err)
	// operator add scatter-region <region_id>
	args = []string{"-u", pdAddr, "operator", "add", "scatter-region", "3"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "operator", "add", "scatter-region", "1"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "operator", "show", "region"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "scatter-region")

	// test echo, as the scatter region result is random, both region 1 and region 3 can be the region to be scattered
	output1, _ := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "operator", "remove", "1")
	output2, _ := pdctl.ExecuteCommand(cmd, "-u", pdAddr, "operator", "remove", "3")
	re.Condition(func() bool {
		return strings.Contains(string(output1), "Success!") || strings.Contains(string(output2), "Success!")
	})
}
