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

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type memberTestSuite struct {
	suite.Suite
	cfgs    []*config.Config
	servers []*server.Server
	clean   testutil.CleanupFunc
}

func TestMemberTestSuite(t *testing.T) {
	suite.Run(t, new(memberTestSuite))
}

func (suite *memberTestSuite) SetupSuite() {
	suite.cfgs, suite.servers, suite.clean = mustNewCluster(suite.Require(), 3, func(cfg *config.Config) {
		cfg.EnableLocalTSO = true
		cfg.Labels = map[string]string{
			config.ZoneLabel: "dc-1",
		}
	})
}

func (suite *memberTestSuite) TearDownSuite() {
	suite.clean()
}

func relaxEqualStings(re *require.Assertions, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	re.Equal(sortedStringB, sortedStringA)
}

func checkListResponse(re *require.Assertions, body []byte, cfgs []*config.Config) {
	got := make(map[string][]*pdpb.Member)
	json.Unmarshal(body, &got)
	re.Len(cfgs, len(got["members"]))
	for _, member := range got["members"] {
		for _, cfg := range cfgs {
			if member.GetName() != cfg.Name {
				continue
			}
			re.Equal("dc-1", member.DcLocation)
			relaxEqualStings(re, member.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(re, member.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (suite *memberTestSuite) TestMemberList() {
	re := suite.Require()
	for _, cfg := range suite.cfgs {
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/members"
		resp, err := testDialClient.Get(addr)
		re.NoError(err)
		buf, err := io.ReadAll(resp.Body)
		re.NoError(err)
		resp.Body.Close()
		checkListResponse(re, buf, suite.cfgs)
	}
}

func (suite *memberTestSuite) TestMemberLeader() {
	re := suite.Require()
	leader := suite.servers[0].GetLeader()
	addr := suite.cfgs[rand.Intn(len(suite.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
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
	re := suite.Require()
	leader := suite.servers[0].GetLeader()
	addr := suite.cfgs[rand.Intn(len(suite.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
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
	addr = suite.cfgs[rand.Intn(len(suite.cfgs))].ClientUrls + apiPrefix + "/api/v1/members"
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
	re := suite.Require()
	addr := suite.cfgs[0].ClientUrls + apiPrefix + "/api/v1/leader/resign"
	resp, err := testDialClient.Post(addr, "", nil)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
