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

package api

import (
	"fmt"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testClusterStatsSuite{})

type testClusterStatsSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testClusterStatsSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testClusterStatsSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterStatsSuite) TestStatsInfo(c *C) {
	statsURL := s.urlPrefix + "/cluster_stats/node_info"
	res, err := http.Get(statsURL)
	c.Assert(err, IsNil)
	stats := []interface{}{}
	err = apiutil.ReadJSON(res.Body, stats)
	c.Assert(err, IsNil)
}
