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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unsafe_operation_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&unsafeOperationTestSuite{})

type unsafeOperationTestSuite struct{}

func (s *unsafeOperationTestSuite) TestRemoveFailedStores(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	err = cluster.GetServer(cluster.GetLeader()).BootstrapCluster()
	c.Assert(err, IsNil)
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()
	defer cluster.Destroy()

	args := []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "show"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "history"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
}
