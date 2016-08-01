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
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

func TestJoin(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMemberAPISuite{})

type testMemberAPISuite struct {
	hc *http.Client
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func correctListResponse(cfgs []*server.Config) []memberInfo {
	ans := make([]memberInfo, 0, len(cfgs))
	for _, c := range cfgs {
		ans = append(ans, memberInfo{
			Name:       c.Name,
			ClientURLs: strings.Split(c.ClientUrls, ","),
			PeerURLs:   strings.Split(c.PeerUrls, ","),
		})
	}
	return ans
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	dirs := make([]string, 0, 4)
	cfg := server.NewTestSingleConfig()
	dirs = append(dirs, cfg.DataDir)
	svr, err := server.NewServer(cfg)
	c.Assert(err, IsNil)
	defer svr.Close()
	go svr.Run()
	go ServeHTTP(cfg.HTTPAddr, svr)

	// wait http server
	time.Sleep(1 * time.Second)

	parts := []string{"http://", cfg.HTTPAddr, "/api/v1/members"}
	resp, err := s.hc.Get(strings.Join(parts, ""))
	c.Assert(err, IsNil)
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := []memberInfo{}
	json.Unmarshal(buf, &got)

	correct := correctListResponse([]*server.Config{cfg})
	c.Assert(got, DeepEquals, correct)

	cfgs := server.NewTestMultiConfig(3)
	for _, cfg := range cfgs {
		dirs = append(dirs, cfg.DataDir)

		go func(cfg *server.Config) {
			s, err := server.NewServer(cfg)
			c.Assert(err, IsNil)
			go s.Run()
			go ServeHTTP(cfg.HTTPAddr, s)
		}(cfg)
	}

	// wait etcds and http servers
	time.Sleep(5 * time.Second)

	// clean up
	defer func() {
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	}()

	parts = []string{"http://", cfgs[rand.Intn(len(cfgs))].HTTPAddr, "/api/v1/members"}
	resp, err = s.hc.Get(strings.Join(parts, ""))
	c.Assert(err, IsNil)
	buf, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got = []memberInfo{}
	json.Unmarshal(buf, &got)

	correct = correctListResponse(cfgs)
	c.Assert(got, DeepEquals, correct)
}
