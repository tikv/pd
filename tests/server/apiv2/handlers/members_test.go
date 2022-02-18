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

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

const membersPrefix = "/pd/api/v2/members"

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMembersAPISuite{})

type testMembersAPISuite struct {
	cancel       context.CancelFunc
	cluster      *tests.TestCluster
	leaderServer *tests.TestServer
}

func (s *testMembersAPISuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cancel = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels = map[string]string{
			config.ZoneLabel: "dc-1",
		}
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer.BootstrapCluster(), IsNil)
	s.cluster = cluster
}

func (s *testMembersAPISuite) TearDownSuite(c *C) {
	s.cancel()
	s.cluster.Destroy()
}

func (s *testMembersAPISuite) TestGetMembers(c *C) {
	for _, url := range s.cluster.GetConfig().GetClientURLs() {
		url += membersPrefix
		resp, err := dialClient.Get(url)
		c.Assert(err, IsNil)
		buf, err := io.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		resp.Body.Close()
		checkListResponse(c, buf, s.cluster)
	}
}

func (s *testMembersAPISuite) TestMembersLeader(c *C) {
	leader := s.cluster.GetServer(s.cluster.GetLeader()).GetServer()
	url := s.cluster.GetConfig().InitialServers[rand.Intn(len(s.cluster.GetServers()))].ClientURLs + membersPrefix
	resp, err := dialClient.Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	got := make(map[string]*pdpb.Member)
	// ignore the error since we only use some fields of the result
	json.Unmarshal(buf, &got)
	c.Assert(got["leader"].GetClientUrls(), DeepEquals, []string{leader.GetConfig().ClientUrls})
	c.Assert(got["leader"].GetMemberId(), Equals, leader.GetMember().ID())
	c.Assert(got["etcd_leader"].GetClientUrls(), DeepEquals, []string{leader.GetConfig().ClientUrls})
	c.Assert(got["etcd_leader"].GetMemberId(), Equals, leader.GetMember().ID())
}

func (s *testMembersAPISuite) TestUpdateLeaderPriority(c *C) {
	url := s.leaderServer.GetServer().GetAddr() + membersPrefix + "/pd1"
	data := map[string]float64{"leader_priority": 5}
	putData, err := json.Marshal(data)
	c.Assert(err, IsNil)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(putData))
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	req, err = http.NewRequest(http.MethodGet, url, nil)
	c.Assert(err, IsNil)
	resp, err = dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	var got *pdpb.Member
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	c.Assert(got.LeaderPriority, Equals, int32(5))
}

func relaxEqualStings(c *C, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	c.Assert(sortedStringA, Equals, sortedStringB)
}

func checkListResponse(c *C, body []byte, cluster *tests.TestCluster) {
	got := make(map[string][]*pdpb.Member)
	// ignore the error since we only use some fields of the result
	json.Unmarshal(body, &got)

	c.Assert(len(got["members"]), Equals, len(cluster.GetConfig().InitialServers))
	for _, member := range got["members"] {
		for _, s := range cluster.GetConfig().InitialServers {
			if member.GetName() != s.Name {
				continue
			}
			c.Assert(member.DcLocation, Equals, "dc-1")
			relaxEqualStings(c, member.ClientUrls, strings.Split(s.ClientURLs, ","))
			relaxEqualStings(c, member.PeerUrls, strings.Split(s.PeerURLs, ","))
		}
	}
}
