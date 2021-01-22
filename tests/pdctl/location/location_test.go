// Copyright 2021 TiKV Project Authors.
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

package location_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&locationTestSuite{})

type locationTestSuite struct{}

func (s *locationTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *locationTestSuite) TestUpdateMemberDCLocationInfo(c *C) {
	tso.PriorityCheck = 5 * time.Second
	defer func() {
		tso.PriorityCheck = 1 * time.Minute
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.LocalTSO = config.LocalTSOConfig{
			EnableLocalTSO: true,
			DCLocation:     dcLocationConfig[serverName],
		}
	})
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	defer cluster.Destroy()

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctl.InitCommand()

	checkDCLocation := func(expected, unexpected []string) {
		testutil.WaitUntil(c, func(c *C) bool {
			args := []string{"-u", pdAddr, "member"}
			_, output, err := pdctl.ExecuteCommandC(cmd, args...)
			c.Assert(err, IsNil)
			resp := &pdpb.GetMembersResponse{}
			c.Assert(json.Unmarshal(output, resp), IsNil)
			c.Log(fmt.Sprintf("allocator leaders: %s", resp.GetTsoAllocatorLeaders()))

			for _, l := range unexpected {
				_, ok := resp.GetTsoAllocatorLeaders()[l]
				if ok {
					return false
				}
			}
			for _, l := range expected {
				_, ok := resp.GetTsoAllocatorLeaders()[l]
				if !ok {
					return false
				}
			}
			return true
		}, testutil.WithSleepInterval(time.Second))
	}

	// check initial dc location
	checkDCLocation([]string{"dc-1", "dc-2", "dc-3"}, []string{})

	// modify dc location of pd1
	args := []string{"-u", pdAddr, "location", "pd1", "dc-4"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Success"), IsTrue)
	// check dc location after modification
	checkDCLocation([]string{"dc-2", "dc-3", "dc-4"}, []string{"dc-1"})

	// tso allocator leader of "dc-4" should be pd1 finally.
	testutil.WaitUntil(c, func(c *C) bool {
		args := []string{"-u", pdAddr, "member"}
		_, output, err := pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(err, IsNil)
		resp := &pdpb.GetMembersResponse{}
		c.Assert(json.Unmarshal(output, resp), IsNil)

		m := resp.GetTsoAllocatorLeaders()["dc-4"]
		return m.GetName() == "pd1"

	}, testutil.WithSleepInterval(time.Second))
}
