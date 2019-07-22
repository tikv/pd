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

package join_fail_test

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tests"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&serverTestSuite{})

type serverTestSuite struct{}

func (s *serverTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *serverTestSuite) TestFailedPDJoinInStep1(c *C) {
	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	// Join the second PD.
	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/add-member-failed", `return`), IsNil)
	_, err = cluster.Join()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "join failed"), IsTrue)
	c.Assert(failpoint.Disable("github.com/pingcap/pd/server/add-member-failed"), IsNil)
}
