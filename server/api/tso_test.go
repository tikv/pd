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

package api

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testTsoSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) {
		cfg.EnableLocalTSO = true
		cfg.Labels[config.ZoneLabel] = "dc-1"
	})
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testTsoSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTsoSuite) TestTransferAllocator(c *C) {
	cu := testutil.NewAPICheckerUtil(c)
	testutil.WaitUntil(c, func() bool {
		s.svr.GetTSOAllocatorManager().ClusterDCLocationChecker()
		_, err := s.svr.GetTSOAllocatorManager().GetAllocator("dc-1")
		return err == nil
	}, testutil.WithRetryTimes(5), testutil.WithSleepInterval(3*time.Second))
	addr := s.urlPrefix + "/tso/allocator/transfer/pd1?dcLocation=dc-1"
	err := cu.CheckPostJSON(testDialClient, addr, nil, cu.StatusOK())
	c.Assert(err, IsNil)
}
