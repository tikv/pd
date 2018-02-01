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

package api

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testHealthAPISuite{})

type testHealthAPISuite struct {
	hc *http.Client
}

func (s *testHealthAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func checkSliceResponse(c *C, body []byte, cfgs []*server.Config) {
	got := []health{}
	json.Unmarshal(body, &got)

	c.Assert(len(got), Equals, len(cfgs))

	for _, h := range got {
		for _, cfg := range cfgs {
			if h.Name != cfg.Name {
				continue
			}
			relaxEqualStings(c, h.ClientUrls, strings.Split(cfg.ClientUrls, ","))
		}
	}
}

func (s *testHealthAPISuite) TestHealthSlice(c *C) {
	healths := []int{1, 3}

	for _, num := range healths {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/health"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkSliceResponse(c, buf, cfgs)
	}
}
