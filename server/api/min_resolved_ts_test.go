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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
)

var _ = Suite(&testMinResolvedTSSuite{})

type testMinResolvedTSSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testMinResolvedTSSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (s *testMinResolvedTSSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testMinResolvedTSSuite) TestMinResolvedTS(c *C) {
	url := s.urlPrefix + "/min-resolved-ts"
	storage := s.svr.GetStorage()
	min := uint64(233)
	storage.SaveMinResolvedTS(min)
	result := &listMinResolvedTS{
		MinResolvedTS: min,
	}
	res, err := testDialClient.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	listResp := &listMinResolvedTS{}
	err = apiutil.ReadJSON(res.Body, listResp)
	c.Assert(err, IsNil)
	c.Assert(listResp, DeepEquals, result)
}
