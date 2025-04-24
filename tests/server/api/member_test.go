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
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

type memberTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestMemberTestSuite(t *testing.T) {
	suite.Run(t, new(memberTestSuite))
}

func (suite *memberTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
	suite.env.PDCount = 3
}

func (suite *memberTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *memberTestSuite) TestMemberList() {
	suite.env.RunTest(suite.checkMemberList)
}

func (suite *memberTestSuite) checkMemberList(cluster *tests.TestCluster) {
	re := suite.Require()
	svrs := cluster.GetServers()

	for _, svr := range svrs {
		addr := svr.GetAddr() + api.APIPrefix + "/api/v1/members"
		resp, err := testDialClient.Get(addr)
		re.NoError(err)
		buf, err := io.ReadAll(resp.Body)
		re.NoError(err)
		resp.Body.Close()
		checkListResponse(re, buf, svrs)
	}
}

func (suite *memberTestSuite) TestMemberLeader() {
	suite.env.RunTest(suite.checkMemberLeader)
}

func (suite *memberTestSuite) checkMemberLeader(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer().GetLeader()
	svrs := cluster.GetServers()
	var addrs []string
	for _, svr := range svrs {
		addrs = append(addrs, svr.GetAddr()+api.APIPrefix+"/api/v1/leader")
	}

	addr := addrs[rand.Intn(len(addrs))]
	resp, err := testDialClient.Get(addr)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)

	var got pdpb.Member
	re.NoError(json.Unmarshal(buf, &got))
	re.Equal(leader.GetClientUrls(), got.GetClientUrls())
	re.Equal(leader.GetMemberId(), got.GetMemberId())
}

func (suite *memberTestSuite) TestChangeLeaderPeerUrls() {
	suite.env.RunTest(suite.checkChangeLeaderPeerUrls)
}

func (suite *memberTestSuite) checkChangeLeaderPeerUrls(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer().GetLeader()
	svrs := cluster.GetServers()
	var addrs []string
	for _, svr := range svrs {
		addrs = append(addrs, svr.GetAddr()+api.APIPrefix+"/api/v1/leader")
	}

	addr := addrs[rand.Intn(len(addrs))]
	resp, err := testDialClient.Get(addr)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)

	var got pdpb.Member
	re.NoError(json.Unmarshal(buf, &got))
	peerUrls := got.GetPeerUrls()

	newPeerUrls := []string{"http://127.0.0.1:1111"}
	suite.changeLeaderPeerUrls(leader, newPeerUrls)
	var addrs1 []string
	for _, svr := range svrs {
		addrs1 = append(addrs1, svr.GetAddr()+api.APIPrefix+"/api/v1/members")
	}
	addr = addrs1[rand.Intn(len(addrs1))]
	resp, err = testDialClient.Get(addr)
	re.NoError(err)
	buf, err = io.ReadAll(resp.Body)
	re.NoError(err)
	resp.Body.Close()
	got1 := make(map[string]*pdpb.Member)
	json.Unmarshal(buf, &got1)
	re.Equal(newPeerUrls, got1["leader"].GetPeerUrls())
	re.Equal(newPeerUrls, got1["etcd_leader"].GetPeerUrls())

	// reset
	suite.changeLeaderPeerUrls(leader, peerUrls)
}

func (suite *memberTestSuite) changeLeaderPeerUrls(leader *pdpb.Member, urls []string) {
	re := suite.Require()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: leader.GetClientUrls(),
	})
	re.NoError(err)
	_, err = cli.MemberUpdate(context.Background(), leader.GetMemberId(), urls)
	re.NoError(err)
	cli.Close()
}

func (suite *memberTestSuite) TestResignMyself() {
	suite.env.RunTest(suite.checkResignMyself)
}

func (suite *memberTestSuite) checkResignMyself(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	addr := leader.GetAddr() + api.APIPrefix + "/api/v1/leader/resign"
	resp, err := testDialClient.Post(addr, "", nil)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func relaxEqualStings(re *require.Assertions, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	re.Equal(sortedStringB, sortedStringA)
}

func checkListResponse(re *require.Assertions, body []byte, svrs map[string]*tests.TestServer) {
	got := make(map[string][]*pdpb.Member)
	json.Unmarshal(body, &got)
	re.Len(svrs, len(got["members"]))
	for _, member := range got["members"] {
		for _, svr := range svrs {
			if member.GetName() != svr.GetConfig().Name {
				continue
			}
			relaxEqualStings(re, member.ClientUrls, strings.Split(svr.GetConfig().ClientUrls, ","))
			relaxEqualStings(re, member.PeerUrls, strings.Split(svr.GetConfig().PeerUrls, ","))
		}
	}
}
