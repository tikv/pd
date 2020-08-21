// Copyright 2020 TiKV Project Authors.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testServiceGCSafepointSuite{})

type testServiceGCSafepointSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testServiceGCSafepointSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
}

func (s *testServiceGCSafepointSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testServiceGCSafepointSuite) TestRegionStats(c *C) {
	sspURL := s.urlPrefix + "/gc/safepoint"

	storage := s.svr.GetStorage()
	list := &listServiceGCSafepoint{
		ServiceGCSafepoints: []*core.ServiceSafePoint{
			{
				ServiceID: "a",
				ExpiredAt: time.Now().Unix() + 10,
				SafePoint: 1,
			},
			{
				ServiceID: "b",
				ExpiredAt: time.Now().Unix() + 10,
				SafePoint: 2,
			},
			{
				ServiceID: "c",
				ExpiredAt: time.Now().Unix() + 10,
				SafePoint: 3,
			},
		},
		GCSafePoint: 1,
	}
	for _, ssp := range list.ServiceGCSafepoints {
		err := storage.SaveServiceGCSafePoint(ssp)
		c.Assert(err, IsNil)
	}
	storage.SaveGCSafePoint(1)

	res, err := testDialClient.Get(sspURL)
	c.Assert(err, IsNil)
	listResp := &listServiceGCSafepoint{}
	err = apiutil.ReadJSON(res.Body, listResp)
	c.Assert(err, IsNil)
	c.Assert(listResp, DeepEquals, list)

	res, err = doDelete(testDialClient, sspURL+"/a")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)

	left, err := storage.GetAllServiceGCSafePoints()
	c.Assert(err, IsNil)
	c.Assert(left, DeepEquals, list.ServiceGCSafepoints[1:])
}
