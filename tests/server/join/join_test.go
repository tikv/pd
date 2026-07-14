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

package join_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"
	"github.com/tikv/pd/tests"
)

// TODO: enable it when we fix TestFailedAndDeletedPDJoinsPreviousCluster
// func TestMain(m *testing.M) {
// 	goleak.VerifyTestMain(m, testutil.LeakOptions...)
// }

func TestSimpleJoin(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	pd1 := cluster.GetServer("pd1")
	client := pd1.GetEtcdClient()
	members, err := etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 1)

	// Join the second PD.
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	err = pd2.Run()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	members, err = etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 2)

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join another PD.
	pd3, err := cluster.Join(ctx)
	re.NoError(err)
	err = pd3.Run()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	members, err = etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 3)
}

// A failed PD tries to join the previous cluster but it has been deleted
// during its downtime.
func TestFailedAndDeletedPDJoinsPreviousCluster(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.EtcdStartTimeout = 10 * time.Second
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	pd3 := cluster.GetServer("pd3")
	err = pd3.Stop()
	re.NoError(err)

	client := cluster.GetServer("pd1").GetEtcdClient()
	_, err = client.MemberRemove(context.TODO(), pd3.GetServerID())
	re.NoError(err)

	// The server should not successfully start.
	res := tests.RunServer(pd3)
	re.Error(<-res)

	members, err := etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 2)
}

// A deleted PD joins the previous cluster.
func TestDeletedPDJoinsPreviousCluster(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.EtcdStartTimeout = 10 * time.Second
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	pd3 := cluster.GetServer("pd3")
	client := cluster.GetServer("pd1").GetEtcdClient()
	_, err = client.MemberRemove(context.TODO(), pd3.GetServerID())
	re.NoError(err)

	err = pd3.Stop()
	re.NoError(err)

	// The server should not successfully start.
	res := tests.RunServer(pd3)
	re.Error(<-res)

	members, err := etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 2)
}

func TestFailedPDJoinsPreviousCluster(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	// Join the second PD.
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	re.NoError(pd2.Run())
	re.NoError(pd2.Stop())
	re.NoError(pd2.Destroy())
	re.Error(join.PrepareJoinCluster(pd2.GetConfig()))
}

// Reproduces https://github.com/tikv/pd/issues/10999: a new member can join
// and start up with a unique peer URL while advertising a client URL already
// owned by an existing member. Because --join points to a different string
// than the duplicated client URL, the self-join check is bypassed and etcd
// accepts the unique peer URL, so the member list ends up with two logical
// members sharing one client URL.
func TestJoinWithDuplicateClientURL(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	pd1 := cluster.GetServer("pd1")
	client := pd1.GetEtcdClient()
	dupClientURL := pd1.GetConfig().AdvertiseClientUrls

	// Join a new member whose advertise-client-urls duplicates pd1's, while its
	// peer URL stays unique. --join points to pd1's peer URL, a different string
	// than the duplicated client URL, so the `cfg.Join == cfg.AdvertiseClientUrls`
	// self-join check does not trigger.
	orphan, err := cluster.Join(ctx, func(conf *config.Config, _ string) {
		conf.AdvertiseClientUrls = dupClientURL
	})
	re.NoError(err)

	// The joining member starts successfully today — this is the bug. After a
	// fix it should be rejected before it can corrupt membership.
	re.NoError(orphan.Run())
	re.NotEmpty(cluster.WaitLeader())

	// Membership is now corrupted: two members advertise the same client URL.
	members, err := etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 2)
	dupOwners := make([]uint64, 0, 2)
	for _, m := range members.Members {
		for _, u := range m.ClientURLs {
			if u == dupClientURL {
				dupOwners = append(dupOwners, m.ID)
			}
		}
	}
	// Exactly one member should ever own a given client URL; here two do.
	re.Len(dupOwners, 2, "client URL %s is owned by members %v", dupClientURL, dupOwners)
}

// A PD joins itself.
func TestPDJoinsItself(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	// Get a server config and modify it to join itself
	cfg := cluster.GetConfig().InitialServers[0]
	serverCfg, err := cfg.Generate()
	re.NoError(err)
	serverCfg.Join = serverCfg.AdvertiseClientUrls
	re.Error(join.PrepareJoinCluster(serverCfg))
}
