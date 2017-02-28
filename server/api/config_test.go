// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct {
	hc *http.Client
}

func (s *testConfigSuite) SetUpSuite(c *C) {
	s.hc = newUnixSocketClient()
}

func checkConfigResponse(c *C, body []byte, cfgs []*server.Config) {
	got := &server.Config{}
	err := json.Unmarshal(body, &got)
	c.Assert(err, IsNil)
}

func (s *testConfigSuite) TestConfigList(c *C) {
	numbers := []int{1, 3}
	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		parts := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/config"}
		addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkConfigResponse(c, buf, cfgs)
	}
}

func (s *testConfigSuite) TestConfigSchedule(c *C) {
	numbers := []int{1, 3}
	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		parts := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/config/schedule"}
		addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)

		sc := &server.ScheduleConfig{}
		err = json.Unmarshal(buf, sc)
		c.Assert(err, IsNil)

		sc.MaxStoreDownTime.Duration = time.Second
		postData, err := json.Marshal(sc)
		postURL := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/config"}
		postAddr := mustUnixAddrToHTTPAddr(c, strings.Join(postURL, ""))
		resp, err = s.hc.Post(postAddr, "application/json", bytes.NewBuffer(postData))
		c.Assert(err, IsNil)

		resp, err = s.hc.Get(addr)
		sc1 := &server.ScheduleConfig{}
		json.NewDecoder(resp.Body).Decode(sc1)

		c.Assert(*sc, Equals, *sc1)
	}
}
