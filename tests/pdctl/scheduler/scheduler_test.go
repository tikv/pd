// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&schedulerTestSuite{})

type schedulerTestSuite struct{}

func (s *schedulerTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *schedulerTestSuite) TestScheduler(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers(ctx)
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
		},
		{
			Id:    2,
			State: metapb.StoreState_Up,
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
		},
		{
			Id:    4,
			State: metapb.StoreState_Up,
		},
	}

	checkSchedulerCommand := func(args []string, expected map[string]bool) {
		if args != nil {
			_, _, err = pdctl.ExecuteCommandC(cmd, args...)
			c.Assert(err, IsNil)
		}
		var schedulers []string
		args = []string{"-u", pdAddr, "scheduler", "show"}
		_, output, err := pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(err, IsNil)
		c.Assert(json.Unmarshal(output, &schedulers), IsNil)
		for _, scheduler := range schedulers {
			c.Assert(expected[scheduler], Equals, true)
		}
	}

	checkSchedulerConfigCommand := func(args []string, expectedConfig map[string]interface{}, schedulerName string) {
		if args != nil {
			_, _, err = pdctl.ExecuteCommandC(cmd, args...)
			c.Assert(err, IsNil)
		}
		args = []string{"-u", pdAddr, "scheduler", "config", "show", schedulerName}
		_, output, err := pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(err, IsNil)
		configInfo := make(map[string]interface{})
		c.Assert(json.Unmarshal(output, &configInfo), IsNil)
		c.Assert(expectedConfig, DeepEquals, configInfo)
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}

	pdctl.MustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"))
	defer cluster.Destroy()

	time.Sleep(3 * time.Second)

	// scheduler show command
	expected := map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	checkSchedulerCommand(nil, expected)

	// scheduler delete command
	args := []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	expected = map[string]bool{
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	checkSchedulerCommand(args, expected)

	schedulers := make([]string, 2)
	schedulers[0] = "evict-leader-scheduler"
	schedulers[1] = "grant-leader-scheduler"

	for idx := range schedulers {
		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler config show command
		expectedConfig := make(map[string]interface{})
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler config update command
		args = []string{"-u", pdAddr, "scheduler", "config", "update", schedulers[idx], "3"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// check update success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "3": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler delete command
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx]}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
		}
		checkSchedulerCommand(args, expected)

		// check the compactily
		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler add command twice
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "4"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// check add success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "4": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler remove command [old]
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-4"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// check remove success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler remove command, when remove the last store, it should remove whole scheduler
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-2"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
		}
		checkSchedulerCommand(args, expected)

	}

	// test echo
	echo := pdctl.GetEcho([]string{"-u", pdAddr, "scheduler", "add", "balance-region-scheduler"})
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = pdctl.GetEcho([]string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"})
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = pdctl.GetEcho([]string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"})
	c.Assert(strings.Contains(echo, "Success!"), IsFalse)
}
