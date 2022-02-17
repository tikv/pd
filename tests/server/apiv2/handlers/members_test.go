package handlers_test

import (
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

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMemberAPISuite{})

type testMemberAPISuite struct {
	cleanup context.CancelFunc
	cluster *tests.TestCluster
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
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
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	s.cluster = cluster
}

func (s *testMemberAPISuite) TearDownSuite(c *C) {
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	for _, url := range s.cluster.GetConfig().GetClientURLs() {
		addr := url + "/pd/api/v2/members"
		resp, err := dialClient.Get(addr)
		c.Assert(err, IsNil)
		buf, err := io.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		resp.Body.Close()
		checkListResponse(c, buf, s.cluster)
	}
}

func (s *testMemberAPISuite) TestMemberLeader(c *C) {
	leader := s.cluster.GetServer(s.cluster.GetLeader()).GetServer()
	addr := s.cluster.GetConfig().InitialServers[rand.Intn(len(s.cluster.GetServers()))].ClientURLs + "/pd/api/v2/members"
	resp, err := dialClient.Get(addr)
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
