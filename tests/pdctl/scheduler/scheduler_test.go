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

package scheduler_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestScheduler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
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
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	mustExec := func(args []string, v interface{}) string {
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		if v == nil {
			return string(output)
		}
		re.NoError(json.Unmarshal(output, v))
		return ""
	}

	mightExec := func(args []string, v interface{}) {
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		if v == nil {
			return
		}
		json.Unmarshal(output, v)
	}

	mustUsage := func(args []string) {
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.Contains(string(output), "Usage")
	}

	checkSchedulerCommand := func(args []string, expected map[string]bool) {
		if args != nil {
			mustExec(args, nil)
		}
		var schedulers []string
		mustExec([]string{"-u", pdAddr, "scheduler", "show"}, &schedulers)
		for _, scheduler := range schedulers {
			re.True(expected[scheduler])
		}
	}

	checkSchedulerConfigCommand := func(args []string, expectedConfig map[string]interface{}, schedulerName string) {
		if args != nil {
			mustExec(args, nil)
		}
		configInfo := make(map[string]interface{})
		mustExec([]string{"-u", pdAddr, "scheduler", "config", schedulerName}, &configInfo)
		re.Equal(expectedConfig, configInfo)
	}

	checkSchedulerDescribeCommand := func(schedulerName, expectedStatus, expectedSummary string) {
		result := make(map[string]interface{})
		testutil.Eventually(re, func() bool {
			mightExec([]string{"-u", pdAddr, "scheduler", "describe", schedulerName}, &result)
			return len(result) != 0
		}, testutil.WithTickInterval(50*time.Millisecond))

		re.Equal(expectedStatus, result["status"])
		re.Equal(expectedSummary, result["summary"])
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	for _, store := range stores {
		pdctl.MustPutStore(re, leaderServer.GetServer(), store)
	}

	pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"))
	time.Sleep(3 * time.Second)

	// scheduler show command
	expected := map[string]bool{
		"balance-region-scheduler":          true,
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"split-bucket-scheduler":            true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
	}
	checkSchedulerCommand(nil, expected)

	echo := mustExec([]string{"-u", pdAddr, "config", "set", "enable-diagnostic", "true"}, nil)
	re.Contains(echo, "Success!")
	checkSchedulerDescribeCommand("balance-region-scheduler", "pending", "1 store(s) RegionNotMatchRule; ")

	// scheduler delete command
	args := []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	expected = map[string]bool{
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"split-bucket-scheduler":            true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
	}
	checkSchedulerCommand(args, expected)

	checkSchedulerDescribeCommand("balance-region-scheduler", "disabled", "")

	schedulers := []string{"evict-leader-scheduler", "grant-leader-scheduler"}

	for idx := range schedulers {
		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler config show command
		expectedConfig := make(map[string]interface{})
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler config update command
		args = []string{"-u", pdAddr, "scheduler", "config", schedulers[idx], "add-store", "3"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)

		// check update success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "3": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler delete command
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx]}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler add command twice
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "4"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)

		// check add success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "4": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler remove command [old]
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-4"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)

		// check remove success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler remove command, when remove the last store, it should remove whole scheduler
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-2"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"split-bucket-scheduler":            true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
		}
		checkSchedulerCommand(args, expected)
	}

	// test shuffle region config
	checkSchedulerCommand([]string{"-u", pdAddr, "scheduler", "add", "shuffle-region-scheduler"}, map[string]bool{
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"split-bucket-scheduler":            true,
		"shuffle-region-scheduler":          true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
	})
	var roles []string
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "show-roles"}, &roles)
	re.Equal([]string{"leader", "follower", "learner"}, roles)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "set-roles", "learner"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "show-roles"}, &roles)
	re.Equal([]string{"learner"}, roles)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler"}, &roles)
	re.Equal([]string{"learner"}, roles)

	// test grant hot region scheduler config
	checkSchedulerCommand([]string{"-u", pdAddr, "scheduler", "add", "grant-hot-region-scheduler", "1", "1,2,3"}, map[string]bool{
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"split-bucket-scheduler":            true,
		"shuffle-region-scheduler":          true,
		"grant-hot-region-scheduler":        true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
	})
	var conf3 map[string]interface{}
	expected3 := map[string]interface{}{
		"store-id":        []interface{}{float64(1), float64(2), float64(3)},
		"store-leader-id": float64(1),
	}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "grant-hot-region-scheduler"}, &conf3)
	re.Equal(expected3, conf3)

	mustExec([]string{"-u", pdAddr, "scheduler", "config", "grant-hot-region-scheduler", "set", "2", "1,2,3"}, nil)
	expected3["store-leader-id"] = float64(2)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "grant-hot-region-scheduler"}, &conf3)
	re.Equal(expected3, conf3)

	// test balance region config
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "balance-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	re.NotContains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "evict-leader-scheduler", "1"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "evict-leader-scheduler-1"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "evict-leader-scheduler-1"}, nil)
	re.Contains(echo, "404")

	// test hot region config
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "config", "evict-leader-scheduler"}, nil)
	re.Contains(echo, "[404] scheduler not found")
	expected1 := map[string]interface{}{
		"min-hot-byte-rate":          float64(100),
		"min-hot-key-rate":           float64(10),
		"min-hot-query-rate":         float64(10),
		"max-zombie-rounds":          float64(3),
		"max-peer-number":            float64(1000),
		"byte-rate-rank-step-ratio":  0.05,
		"key-rate-rank-step-ratio":   0.05,
		"query-rate-rank-step-ratio": 0.05,
		"count-rank-step-ratio":      0.01,
		"great-dec-ratio":            0.95,
		"minor-dec-ratio":            0.99,
		"src-tolerance-ratio":        1.05,
		"dst-tolerance-ratio":        1.05,
		"read-priorities":            []interface{}{"byte", "key"},
		"write-leader-priorities":    []interface{}{"key", "byte"},
		"write-peer-priorities":      []interface{}{"byte", "key"},
		"strict-picking-store":       "true",
		"enable-for-tiflash":         "true",
		"rank-formula-version":       "v2",
	}
	var conf map[string]interface{}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "list"}, &conf)
	re.Equal(expected1, conf)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "show"}, &conf)
	re.Equal(expected1, conf)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "src-tolerance-ratio", "1.02"}, nil)
	expected1["src-tolerance-ratio"] = 1.02
	var conf1 map[string]interface{}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)

	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "byte,key"}, nil)
	expected1["read-priorities"] = []interface{}{"byte", "key"}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)

	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key,byte"}, nil)
	expected1["read-priorities"] = []interface{}{"key", "byte"}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "foo,bar"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", ""}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key,key"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "byte,byte"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key,key,byte"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)

	// write-priorities is divided into write-leader-priorities and write-peer-priorities
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "write-priorities", "key,byte"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)

	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "rank-formula-version", "v0"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	expected1["rank-formula-version"] = "v2"
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "rank-formula-version", "v2"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)
	expected1["rank-formula-version"] = "v1"
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "rank-formula-version", "v1"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)

	expected1["forbid-rw-type"] = "read"
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "forbid-rw-type", "read"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(expected1, conf1)

	// test compatibility
	re.Equal("2.0.0", leaderServer.GetClusterVersion().String())
	for _, store := range stores {
		version := versioninfo.HotScheduleWithQuery
		store.Version = versioninfo.MinSupportedVersion(version).String()
		pdctl.MustPutStore(re, leaderServer.GetServer(), store)
	}
	re.Equal("5.2.0", leaderServer.GetClusterVersion().String())
	// After upgrading, we should not use query.
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(conf1["read-priorities"], []interface{}{"key", "byte"})
	// cannot set qps as write-peer-priorities
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "write-peer-priorities", "query,byte"}, nil)
	re.Contains(echo, "query is not allowed to be set in priorities for write-peer-priorities")
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	re.Equal(conf1["write-peer-priorities"], []interface{}{"byte", "key"})

	// test remove and add
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-hot-region-scheduler"}, nil)
	re.Contains(echo, "Success")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "balance-hot-region-scheduler"}, nil)
	re.Contains(echo, "Success")

	// test balance leader config
	conf = make(map[string]interface{})
	conf1 = make(map[string]interface{})
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler", "show"}, &conf)
	re.Equal(4., conf["batch"])
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler", "set", "batch", "3"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler"}, &conf1)
	re.Equal(3., conf1["batch"])
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "balance-leader-scheduler"}, nil)
	re.NotContains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "404")
	re.Contains(echo, "PD:scheduler:ErrSchedulerNotFound]scheduler not found")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "404")
	re.Contains(echo, "scheduler not found")
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "Success!")

	// test show scheduler with paused and disabled status.
	checkSchedulerWithStatusCommand := func(args []string, status string, expected []string) {
		if args != nil {
			mustExec(args, nil)
		}
		var schedulers []string
		mustExec([]string{"-u", pdAddr, "scheduler", "show", "--status", status}, &schedulers)
		re.Equal(expected, schedulers)
	}

	mustUsage([]string{"-u", pdAddr, "scheduler", "pause", "balance-leader-scheduler"})
	mustExec([]string{"-u", pdAddr, "scheduler", "pause", "balance-leader-scheduler", "60"}, nil)
	checkSchedulerWithStatusCommand(nil, "paused", []string{
		"balance-leader-scheduler",
	})
	result := make(map[string]interface{})
	testutil.Eventually(re, func() bool {
		mightExec([]string{"-u", pdAddr, "scheduler", "describe", "balance-leader-scheduler"}, &result)
		return len(result) != 0 && result["status"] == "paused" && result["summary"] == ""
	}, testutil.WithWaitFor(30*time.Second))

	mustUsage([]string{"-u", pdAddr, "scheduler", "resume", "balance-leader-scheduler", "60"})
	mustExec([]string{"-u", pdAddr, "scheduler", "resume", "balance-leader-scheduler"}, nil)
	checkSchedulerWithStatusCommand(nil, "paused", nil)
	checkSchedulerDescribeCommand("balance-leader-scheduler", "normal", "")

	// set label scheduler to disabled manually.
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "label-scheduler"}, nil)
	re.Contains(echo, "Success!")
	cfg := leaderServer.GetServer().GetScheduleConfig()
	origin := cfg.Schedulers
	cfg.Schedulers = config.SchedulerConfigs{{Type: "label", Disable: true}}
	err = leaderServer.GetServer().SetScheduleConfig(*cfg)
	re.NoError(err)
	checkSchedulerWithStatusCommand(nil, "disabled", []string{"label-scheduler"})
	// reset Schedulers in ScheduleConfig
	cfg.Schedulers = origin
	err = leaderServer.GetServer().SetScheduleConfig(*cfg)
	re.NoError(err)
	checkSchedulerWithStatusCommand(nil, "disabled", nil)
}
