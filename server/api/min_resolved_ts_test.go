// Copyright 2022 TiKV Project Authors.
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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

var _ = Suite(&testMinResolvedTSSuite{})

type testMinResolvedTSSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testMinResolvedTSSuite) SetUpSuite(c *C) {
	cluster.DefaultMinResolvedTSPersistenceInterval = time.Microsecond
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/highFrequencyClusterJobs", `return(true)`), IsNil)
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	r1 := newTestRegionInfo(7, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(c, s.svr, r1)
	r2 := newTestRegionInfo(8, 1, []byte("b"), []byte("c"))
	mustRegionHeartbeat(c, s.svr, r2)
}

func (s *testMinResolvedTSSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testMinResolvedTSSuite) TestMinResolvedTS(c *C) {
	url := s.urlPrefix + "/min-resolved-ts"
	rc := s.svr.GetRaftCluster()
	ts := uint64(233)
	rc.SetMinResolvedTS(1, ts)

	// no run job
	result := &minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      false,
		PersistInterval: typeutil.Duration{Duration: 0},
	}
	res, err := testDialClient.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	listResp := &minResolvedTS{}
	err = apiutil.ReadJSON(res.Body, listResp)
	c.Assert(err, IsNil)
	c.Assert(listResp, DeepEquals, result)

	// run job
	interval := typeutil.NewDuration(time.Microsecond)
	cfg := s.svr.GetRaftCluster().GetOpts().GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = interval
	s.svr.GetRaftCluster().GetOpts().SetPDServerConfig(cfg)
	time.Sleep(time.Millisecond)
	result = &minResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      true,
		PersistInterval: interval,
	}
	res, err = testDialClient.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	listResp = &minResolvedTS{}
	err = apiutil.ReadJSON(res.Body, listResp)
	c.Assert(err, IsNil)
	c.Assert(listResp, DeepEquals, result)
}
