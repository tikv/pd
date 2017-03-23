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
		sc := &server.ScheduleConfig{}
		readJSON(resp.Body, sc)

		sc.MaxStoreDownTime.Duration = time.Second
		postData, err := json.Marshal(sc)
		postURL := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/config/schedule"}
		postAddr := mustUnixAddrToHTTPAddr(c, strings.Join(postURL, ""))
		err = postJSON(s.hc, postAddr, postData)
		c.Assert(err, IsNil)

		resp, err = s.hc.Get(addr)
		sc1 := &server.ScheduleConfig{}
		readJSON(resp.Body, sc1)

		c.Assert(*sc, Equals, *sc1)
	}
}

func (s *testConfigSuite) TestConfigReplication(c *C) {
	numbers := []int{1, 3}
	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		parts := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/config/replicate"}
		addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)

		rc := &server.ReplicationConfig{}
		err = readJSON(resp.Body, rc)
		c.Assert(err, IsNil)

		rc.MaxReplicas = 5

		rc1 := map[string]int{"max-replicas": 5}
		postData, err := json.Marshal(rc1)
		postURL := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/config/replicate"}
		postAddr := mustUnixAddrToHTTPAddr(c, strings.Join(postURL, ""))
		err = postJSON(s.hc, postAddr, postData)
		c.Assert(err, IsNil)
		rc.LocationLabels = []string{"zone", "rack"}

		rc2 := map[string]string{"location-labels": "zone,rack"}
		postData, err = json.Marshal(rc2)
		err = postJSON(s.hc, postAddr, postData)

		resp, err = s.hc.Get(addr)
		rc3 := &server.ReplicationConfig{}
		err = readJSON(resp.Body, rc3)

		c.Assert(*rc, DeepEquals, *rc3)
	}
}
