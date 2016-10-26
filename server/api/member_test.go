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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testMemberAPISuite{})

type testMemberAPISuite struct {
	hc *http.Client
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	s.hc = newUnixSocketClient()
}

func relaxEqualStings(c *C, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	c.Assert(sortedStringA, Equals, sortedStringB)
}

func checkListResponse(c *C, body []byte, cfgs []*server.Config) {
	got := make(map[string][]*pdpb.PDMember)
	json.Unmarshal(body, &got)

	c.Assert(len(got["members"]), Equals, len(cfgs))

	for _, memb := range got["members"] {
		for _, cfg := range cfgs {
			if *memb.Name != cfg.Name {
				continue
			}

			relaxEqualStings(c, memb.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(c, memb.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	numbers := []int{1, 3}

	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		parts := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/members"}
		addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkListResponse(c, buf, cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberDelete(c *C) {
	cfgs, svrs, clean := mustNewCluster(c, 3)
	defer clean()

	target := rand.Intn(len(svrs))
	server := svrs[target]

	for i, cfg := range cfgs {
		if cfg.Name == server.Name() {
			cfgs = append(cfgs[:i], cfgs[i+1:]...)
			break
		}
	}
	for i, svr := range svrs {
		if svr.Name() == server.Name() {
			svrs = append(svrs[:i], svrs[i+1:]...)
			break
		}
	}
	clientURL := cfgs[rand.Intn(len(cfgs))].ClientUrls

	server.Close()
	time.Sleep(5 * time.Second)
	mustWaitLeader(c, svrs)

	var table = []struct {
		name    string
		checker Checker
		status  int
	}{
		{
			// delete a nonexistent pd
			name:    fmt.Sprintf("test-%d", rand.Int63()),
			checker: Equals,
			status:  http.StatusNotFound,
		},
		{
			// delete a pd randomly
			name:    server.Name(),
			checker: Equals,
			status:  http.StatusOK,
		},
		{
			// delete it again
			name:    server.Name(),
			checker: Equals,
			status:  http.StatusNotFound,
		},
	}

	for _, t := range table {
		parts := []string{clientURL, apiPrefix, "/api/v1/members/", t.name}
		addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
		req, err := http.NewRequest("DELETE", addr, nil)
		c.Assert(err, IsNil)
		resp, err := s.hc.Do(req)
		c.Assert(err, IsNil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			c.Assert(resp.StatusCode, t.checker, t.status)
		}
	}

	for i := 0; i < 10; i++ {
		parts := []string{clientURL, apiPrefix, "/api/v1/members"}
		addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusInternalServerError {
			time.Sleep(1)
			continue
		}
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkListResponse(c, buf, cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberLeader(c *C) {
	cfgs, svrs, clean := mustNewCluster(c, 3)
	defer clean()

	leader, err := svrs[0].GetLeader()
	c.Assert(err, IsNil)

	parts := []string{cfgs[rand.Intn(len(cfgs))].ClientUrls, apiPrefix, "/api/v1/leader"}
	addr := mustUnixAddrToHTTPAddr(c, strings.Join(parts, ""))
	c.Assert(err, IsNil)
	resp, err := s.hc.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	var got leaderInfo
	json.Unmarshal(buf, &got)
	c.Assert(got.Addr, Equals, leader.GetAddr())
	c.Assert(got.Pid, Equals, leader.GetPid())
}
