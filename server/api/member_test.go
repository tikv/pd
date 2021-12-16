// Copyright 2016 TiKV Project Authors.
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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testMemberAPISuite{})
var _ = Suite(&testResignAPISuite{})

type testMemberAPISuite struct {
	cfgs    []*config.Config
	servers []*server.Server
	clean   func()
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	s.cfgs, s.servers, s.clean = mustNewCluster(c, 3, func(cfg *config.Config) {
		cfg.EnableLocalTSO = true
		cfg.Labels = map[string]string{
			config.ZoneLabel: "dc-1",
		}
	})
}

func (s *testMemberAPISuite) TearDownSuite(c *C) {
	s.clean()
}

func relaxEqualStings(c *C, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	c.Assert(sortedStringA, Equals, sortedStringB)
}

func checkListResponse(c *C, body []byte, cfgs []*config.Config) {
	got := make(map[string][]*pdpb.Member)
	json.Unmarshal(body, &got)

	c.Assert(len(got["members"]), Equals, len(cfgs))

	for _, member := range got["members"] {
		for _, cfg := range cfgs {
			if member.GetName() != cfg.Name {
				continue
			}
			c.Assert(member.DcLocation, Equals, "dc-1")
			relaxEqualStings(c, member.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(c, member.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	for _, cfg := range s.cfgs {
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/members"
		resp, err := testDialClient.Get(addr)
		c.Assert(err, IsNil)
		buf, err := io.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		resp.Body.Close()
		checkListResponse(c, buf, s.cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberLeader(c *C) {
	leader := s.servers[0].GetLeader()
	addr := s.cfgs[rand.Intn(len(s.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
	resp, err := testDialClient.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	var got pdpb.Member
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	c.Assert(got.GetClientUrls(), DeepEquals, leader.GetClientUrls())
	c.Assert(got.GetMemberId(), Equals, leader.GetMemberId())
}

func (s *testMemberAPISuite) TestChangeLeaderPeerUrls(c *C) {
	leader := s.servers[0].GetLeader()
	addr := s.cfgs[rand.Intn(len(s.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
	resp, err := testDialClient.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	var got pdpb.Member
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	id := got.GetMemberId()
	peerUrls := got.GetPeerUrls()

	newPeerUrls := []string{"http://127.0.0.1:1111"}
	changeLeaderPeerUrls(c, leader, id, newPeerUrls)
	addr = s.cfgs[rand.Intn(len(s.cfgs))].ClientUrls + apiPrefix + "/api/v1/members"
	resp, err = testDialClient.Get(addr)
	c.Assert(err, IsNil)
	buf, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	resp.Body.Close()
	got1 := make(map[string]*pdpb.Member)
	json.Unmarshal(buf, &got1)
	c.Assert(got1["leader"].GetPeerUrls(), DeepEquals, newPeerUrls)
	c.Assert(got1["etcd_leader"].GetPeerUrls(), DeepEquals, newPeerUrls)

	// reset
	changeLeaderPeerUrls(c, leader, id, peerUrls)
}

func changeLeaderPeerUrls(c *C, leader *pdpb.Member, id uint64, urls []string) {
	data := map[string][]string{"peerURLs": urls}
	postData, err := json.Marshal(data)
	c.Assert(err, IsNil)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v2/members/%s", leader.GetClientUrls()[0], fmt.Sprintf("%x", id)), bytes.NewBuffer(postData))
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testDialClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, 204)
	resp.Body.Close()
}

type testResignAPISuite struct {
	cfgs    []*config.Config
	servers []*server.Server
	clean   func()
}

func (s *testResignAPISuite) SetUpSuite(c *C) {
	s.cfgs, s.servers, s.clean = mustNewCluster(c, 1)
}

func (s *testResignAPISuite) TearDownSuite(c *C) {
	s.clean()
}

func (s *testResignAPISuite) TestResignMyself(c *C) {
	addr := s.cfgs[0].ClientUrls + apiPrefix + "/api/v1/leader/resign"
	resp, err := testDialClient.Post(addr, "", nil)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
