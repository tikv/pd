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
// See the License for the specific language governing permissions and
// limitations under the License.

package join_fail_test

import (
	"context"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&joinTestSuite{})

type joinTestSuite struct{}

func (s *joinTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *joinTestSuite) TestFailedPDJoinInStep1(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	// Join the second PD.
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/join/add-member-failed", `return`), IsNil)
	_, err = cluster.Join(ctx)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "join failed"), IsTrue)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/join/add-member-failed"), IsNil)
}
