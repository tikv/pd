// Copyright 2018 PingCAP, Inc.
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

package integration

import (
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
)

func (s *integrationTestSuite) TestReconnect(c *C) {
	c.Parallel()

	cluster, err := newTestCluster(3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	leader := cluster.WaitLeader()
	for name, s := range cluster.servers {
		if name != leader {
			res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
			c.Assert(e, IsNil)
			c.Assert(res.StatusCode, Equals, http.StatusOK)
		}
	}

	// Close the leader and wait for a new one.
	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	newLeader := cluster.WaitLeader()

	// Make sure they proxy requests to the new leader.
	for name, s := range cluster.servers {
		if name != leader {
			testutil.WaitUntil(c, func(c *C) bool {
				res, e := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(e, IsNil)
				return res.StatusCode == http.StatusOK
			})
		}
	}

	// Close the new leader and then we have only one node.
	err = cluster.GetServer(newLeader).Stop()
	c.Assert(err, IsNil)

	// Request will fail with no leader.
	for name, s := range cluster.servers {
		if name != leader && name != newLeader {
			testutil.WaitUntil(c, func(c *C) bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				c.Assert(err, IsNil)
				return res.StatusCode == http.StatusInternalServerError
			})
		}
	}
}
