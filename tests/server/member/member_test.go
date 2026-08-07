// Copyright 2018 TiKV Project Authors.
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

package member_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestMemberDelete(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leader := cluster.GetLeaderServer()
	var members []*tests.TestServer
	for _, s := range cluster.GetConfig().InitialServers {
		if s.Name != leaderName {
			members = append(members, cluster.GetServer(s.Name))
		}
	}
	re.Len(members, 2)

	var tables = []struct {
		path    string
		status  int
		members []*config.Config
	}{
		{path: "name/foobar", status: http.StatusNotFound},
		{path: "name/" + members[0].GetConfig().Name, members: []*config.Config{leader.GetConfig(), members[1].GetConfig()}},
		{path: "name/" + members[0].GetConfig().Name, status: http.StatusNotFound},
		{path: fmt.Sprintf("id/%d", members[1].GetServerID()), members: []*config.Config{leader.GetConfig()}},
	}

	for _, table := range tables {
		t.Log(time.Now(), "try to delete:", table.path)
		testutil.Eventually(re, func() bool {
			addr := leader.GetConfig().ClientUrls + "/pd/api/v1/members/" + table.path
			req, err := http.NewRequest(http.MethodDelete, addr, http.NoBody)
			re.NoError(err)
			res, err := tests.TestDialClient.Do(req)
			re.NoError(err)
			defer res.Body.Close()
			// Check by status.
			if table.status != 0 {
				if res.StatusCode != table.status {
					time.Sleep(time.Second)
					return false
				}
				return true
			}
			// Check by member list.
			re.NotEmpty(cluster.WaitLeader())
			if err = checkMemberList(re, leader.GetConfig().ClientUrls, table.members); err != nil {
				t.Logf("check member fail: %v", err)
				time.Sleep(time.Second)
				return false
			}
			return true
		})
	}
}

func checkMemberList(re *require.Assertions, clientURL string, configs []*config.Config) error {
	addr := clientURL + "/pd/api/v1/members"
	res, err := tests.TestDialClient.Get(addr)
	re.NoError(err)
	defer res.Body.Close()
	buf, err := io.ReadAll(res.Body)
	re.NoError(err)
	if res.StatusCode != http.StatusOK {
		return errors.Errorf("load members failed, status: %v, data: %q", res.StatusCode, buf)
	}
	data := &pdpb.GetMembersResponse{}
	err = json.Unmarshal(buf, &data)
	re.NoError(err)
	if len(data.GetMembers()) != len(configs) {
		return errors.Errorf("member length not match, %v vs %v", len(data.GetMembers()), len(configs))
	}
	for _, member := range data.GetMembers() {
		for _, cfg := range configs {
			if member.GetName() == cfg.Name {
				re.Equal([]string{cfg.ClientUrls}, member.ClientUrls)
				re.Equal([]string{cfg.PeerUrls}, member.PeerUrls)
			}
		}
	}
	return nil
}

func TestLeaderPriority(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.LeaderPriorityCheckInterval = typeutil.NewDuration(time.Second)
	})
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	re.NotEmpty(cluster.WaitLeader())

	leader, err := cluster.GetServer("pd1").GetEtcdLeader()
	re.NoError(err)
	server := cluster.GetServer(leader)
	addr := server.GetConfig().ClientUrls
	// PD leader should sync with etcd leader.
	testutil.Eventually(re, func() bool {
		leader, err := cluster.GetServer("pd1").GetEtcdLeader()
		if err != nil {
			return false
		}
		return cluster.GetLeader() == leader
	})
	// Bind a lower priority to current leader.
	post(t, re, addr+"/pd/api/v1/members/name/"+leader, `{"leader-priority": -1}`)

	// Wait etcd leader change.
	waitEtcdLeaderChange(re, server, leader)
	// PD leader should sync with etcd leader again.
	testutil.Eventually(re, func() bool {
		etcdLeader, err := server.GetEtcdLeader()
		if err != nil {
			return false
		}
		if cluster.GetLeader() == etcdLeader {
			return true
		}
		return false
	})
}

func post(t *testing.T, re *require.Assertions, url string, body string) {
	testutil.Eventually(re, func() bool {
		res, err := tests.TestDialClient.Post(url, "", bytes.NewBufferString(body)) // #nosec
		re.NoError(err)
		b, err := io.ReadAll(res.Body)
		res.Body.Close()
		re.NoError(err)
		t.Logf("post %s, status: %v res: %s", url, res.StatusCode, string(b))
		return res.StatusCode == http.StatusOK
	})
}

func waitEtcdLeaderChange(re *require.Assertions, server *tests.TestServer, old string) string {
	var leader string
	testutil.Eventually(re, func() bool {
		var err error
		leader, err = server.GetEtcdLeader()
		if err != nil {
			return false
		}
		return leader != old
	}, testutil.WithWaitFor(90*time.Second), testutil.WithTickInterval(time.Second))
	return leader
}

func TestLeaderResign(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader1 := cluster.WaitLeader()
	re.NotEmpty(leader1)
	addr1 := cluster.GetServer(leader1).GetConfig().ClientUrls

	post(t, re, addr1+"/pd/api/v1/leader/resign", "")
	leader2 := waitLeaderChange(re, cluster, leader1)
	t.Log("leader2:", leader2)
	addr2 := cluster.GetServer(leader2).GetConfig().ClientUrls
	post(t, re, addr2+"/pd/api/v1/leader/transfer/"+leader1, "")
	leader3 := waitLeaderChange(re, cluster, leader2)
	re.Equal(leader1, leader3)
}

func TestLeaderResignWithBlock(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader1 := cluster.WaitLeader()
	re.NotEmpty(leader1)
	addr1 := cluster.GetServer(leader1).GetConfig().ClientUrls

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/raftclusterIsBusy", `pause`))
	post(t, re, addr1+"/pd/api/v1/leader/resign", "")
	leader2 := waitLeaderChange(re, cluster, leader1)
	t.Log("leader2:", leader2)
	re.NotEqual(leader1, leader2)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/raftclusterIsBusy"))
}

func TestPDLeaderLostWhileEtcdLeaderIntact(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader1 := cluster.WaitLeader()
	re.NotEmpty(leader1)
	memberID := cluster.GetLeaderServer().GetLeader().GetMemberId()

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/leaderLoopCheckAgain", fmt.Sprintf("return(\"%d\")", memberID)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/exitCampaignLeader", fmt.Sprintf("return(\"%d\")", memberID)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/timeoutWaitPDLeader", `return(true)`))
	leader2 := waitLeaderChange(re, cluster, leader1)
	re.NotEqual(leader1, leader2)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/leaderLoopCheckAgain"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/exitCampaignLeader"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/timeoutWaitPDLeader"))
}

func waitLeaderChange(re *require.Assertions, cluster *tests.TestCluster, old string) string {
	var leader string
	testutil.Eventually(re, func() bool {
		leader = cluster.GetLeader()
		if leader == old || leader == "" {
			return false
		}
		return true
	})
	return leader
}

func TestMoveLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	originalLeader := cluster.WaitLeader()
	re.NotEmpty(originalLeader)
	originalLeaderServer := cluster.GetServer(originalLeader)
	re.NotNil(originalLeaderServer)

	// First, resign the original leader.
	err = originalLeaderServer.ResignLeaderWithRetry()
	re.NoError(err)
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(originalLeader, newLeader)
	newLeaderServer := cluster.GetServer(newLeader)
	re.NotNil(newLeaderServer)
	// Then, move leader back to the original leader.
	testutil.Eventually(re, func() bool {
		return newLeaderServer.MoveEtcdLeader(
			newLeaderServer.GetServerID(),
			originalLeaderServer.GetServerID(),
		) == nil
	})
	testutil.Eventually(re, func() bool {
		return originalLeaderServer.IsLeader()
	})
}

func TestCampaignLeaderFrequently(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	// the 1st time campaign leader.
	cluster.WaitLeader()
	leader := cluster.GetLeader()
	re.NotEmpty(cluster.GetLeader())

	// need to prevent 3 times(including the above 1st time) campaign leader in 5 min.
	for range 2 {
		cluster.GetLeaderServer().ResetPDLeader()
		re.NotEmpty(cluster.WaitLeader())
		re.Equal(leader, cluster.GetLeader())
	}
	// check for the 4th time.
	cluster.GetLeaderServer().ResetPDLeader()
	re.NotEmpty(cluster.WaitLeader())
	// PD leader should be different from before because etcd leader changed.
	re.NotEmpty(cluster.GetLeader())
	re.NotEqual(leader, cluster.GetLeader())
}

func TestGrantLeaseFailed(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 5)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeader()
	re.NotEmpty(cluster.GetLeader())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/skipGrantLeader", fmt.Sprintf("return(\"%s\")", leader)))

	for range 3 {
		cluster.GetLeaderServer().ResetPDLeader()
		re.NotEmpty(cluster.WaitLeader())
	}
	// PD leader should be different from before because etcd leader changed.
	re.NotEmpty(cluster.GetLeader())
	re.NotEqual(leader, cluster.GetLeader())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/skipGrantLeader"))
}

func TestGetLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	done := make(chan bool)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)

	// Send requests after server has started.
	go sendRequest(re, wg, done, leaderServer.GetAddr())
	time.Sleep(100 * time.Millisecond)

	re.NotNil(leaderServer.GetLeader())

	done <- true
	wg.Wait()
}

func sendRequest(re *require.Assertions, wg *sync.WaitGroup, done <-chan bool, addr string) {
	defer wg.Done()

	req := &pdpb.AllocIDRequest{Header: testutil.NewRequestHeader(0)}

	for {
		select {
		case <-done:
			return
		default:
			// We don't need to check the response and error,
			// just make sure the server will not panic.
			grpcPDClient, conn := testutil.MustNewGrpcClient(re, addr)
			if grpcPDClient != nil {
				_, _ = grpcPDClient.AllocID(context.Background(), req)
			}
			if conn != nil {
				conn.Close()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestElectionClientStaysOnLocalMember pins down the property that the PD
// leader's ability to notice its own failure rests on: the election client must
// only ever talk to this member's own etcd server.
//
// It is built from the local advertise client URLs, and only the health checker
// ever rewrites that list, so the property holds exactly as long as the checker
// stays disabled for this client. If it were enabled - as it is for the general
// server client, which this test also exercises so that a checker that never ran
// cannot make the assertion pass by accident - the client would follow the
// healthy members, and a member whose own etcd had stopped making progress would
// keep renewing its leader lease through a peer. See tikv/pd#7780, tikv/pd#10671
// and tikv/pd#10746.
func TestElectionClientStaysOnLocalMember(t *testing.T) {
	re := require.New(t)
	// Without this the health checker only ticks every 10s, which is longer than
	// the test is willing to wait for the contrast assertion below.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	for name, svr := range cluster.GetServers() {
		s := svr.GetServer()
		localURLs := strings.Split(s.GetConfig().AdvertiseClientUrls, ",")
		// The general server client is health checked, so it discovers its peers
		// and ends up with more endpoints than the member started with.
		testutil.Eventually(re, func() bool {
			return len(s.GetClient().Endpoints()) > len(localURLs)
		})
		// The election client must not have moved.
		re.Equal(localURLs, s.GetMember().Client().Endpoints(), name)
	}
}

// TestPDLeaderStepsDownWhenLeaseIsLostWithStaleEtcdLeaderView reproduces the
// shape of tikv/pd#10671 and pins down the property that keeps tikv/pd#7780 from
// being a correctness bug: a PD leader that has lost etcd leadership gives up
// because its lease is gone, not because it noticed the change.
//
// The test runs in two phases. First it blinds the member with the
// `staleEtcdLeaderView` failpoint - which freezes its cached etcd leader ID on
// itself, the defect tikv/pd#7780 describes - and moves etcd leadership away for
// real. That is the incident: a member holding a PD leadership it can no longer
// justify, with the colocation check in `campaignLeader` unable to notice. It
// must keep serving, which is what makes the second phase meaningful. Then the
// leader lease is taken away, standing in for the renewals a member with stuck
// storage can no longer complete, and the member must step down and let the new
// etcd leader take over.
//
// Several lease derived paths can be the proximate trigger, and the test
// deliberately does not care which: what is guarded is that losing the lease is
// sufficient on its own, with the etcd leader view offering no help at all.
func TestPDLeaderStepsDownWhenLeaseIsLostWithStaleEtcdLeaderView(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())

	oldLeaderName := cluster.WaitLeader()
	re.NotEmpty(oldLeaderName)
	oldLeaderServer := cluster.GetServer(oldLeaderName)
	oldLeader := oldLeaderServer.GetServer()
	var peer *tests.TestServer
	for name, svr := range cluster.GetServers() {
		if name != oldLeaderName {
			peer = svr
			break
		}
	}
	re.NotNil(peer)

	// Blind the member before anything else changes, so that its own colocation
	// check cannot be what makes it step down later.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/staleEtcdLeaderView",
		fmt.Sprintf("return(\"%s\")", oldLeaderName)))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/staleEtcdLeaderView"))
	}()
	re.Equal(oldLeader.GetMember().ID(), oldLeader.GetMember().GetEtcdLeader())

	// Now really move etcd leadership away. The member cannot see this, exactly
	// as it could not in the incident, and it also has to be true for a peer to
	// be allowed to campaign at all.
	testutil.Eventually(re, func() bool {
		return oldLeaderServer.MoveEtcdLeader(oldLeaderServer.GetServerID(), peer.GetServerID()) == nil
	})

	// Control: while the lease is alive and the view is blinded, nothing makes
	// the member give up, however wrong holding on has become. This is the state
	// tikv/pd#10671 was stuck in, and it is what the lease has to break out of -
	// without it the assertions below would pass for the wrong reason.
	time.Sleep(2 * time.Second)
	re.True(oldLeader.IsServing())
	re.Equal(oldLeaderName, cluster.GetLeader())

	// Take the lease away through the peer's client, leaving the old leader's own
	// code path untouched.
	lease := oldLeader.GetMember().GetLeadership().GetLease()
	re.NotNil(lease)
	leaseID := lease.GetID()
	re.NotEqual(clientv3.NoLease, leaseID)
	_, err = peer.GetServer().GetClient().Revoke(ctx, leaseID)
	re.NoError(err)

	// The lease is the only signal left, and it must be enough.
	testutil.Eventually(re, func() bool {
		return !oldLeader.IsServing() && oldLeader.GetRaftCluster() == nil
	}, testutil.WithWaitFor(30*time.Second))
	re.NotEqual(oldLeaderName, waitLeaderChange(re, cluster, oldLeaderName))
}
