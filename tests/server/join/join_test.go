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

// Regression test for https://github.com/tikv/pd/issues/10999: a member that
// tries to join while advertising a client URL already owned by another member
// must be rejected before it can corrupt membership — even though its peer URL
// is unique and --join points to a different string than the duplicated client
// URL, so the `cfg.Join == cfg.AdvertiseClientUrls` self-join check does not
// trigger.
func TestJoinWithDuplicateClientURL(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	pd1 := cluster.GetServer("pd1")
	client := pd1.GetEtcdClient()
	dupClientURL := pd1.GetConfig().AdvertiseClientUrls

	// PrepareJoinCluster (run inside cluster.Join) detects that dupClientURL is
	// already owned by pd1 and refuses the join before MemberAdd.
	_, err = cluster.Join(ctx, func(conf *config.Config, _ string) {
		conf.AdvertiseClientUrls = dupClientURL
	})
	re.Error(err)
	re.Contains(err.Error(), "already used by member")

	// Membership is untouched: the orphan never entered the member list.
	members, err := etcdutil.ListEtcdMembers(ctx, client)
	re.NoError(err)
	re.Len(members.Members, 1)
}

// Regression test for the restart path of https://github.com/tikv/pd/issues/10999:
// a member that restarts (its data directory already exists) after its
// advertise-client-urls was changed to a value owned by another member must
// still be rejected. The read-only conflict check runs on the restart-with-data
// path too, before the local etcd republishes its attributes — not only on a
// fresh join.
func TestRestartWithModifiedDuplicateClientURL(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	// Bring up a legitimate second member with its own unique client URL so it
	// has a data directory, then stop it to simulate a restart.
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	re.NoError(pd2.Run())
	re.NotEmpty(cluster.WaitLeader())
	re.NoError(pd2.Stop())

	dupClientURL := cluster.GetServer("pd1").GetConfig().AdvertiseClientUrls

	// Restart pd2 with its advertise-client-urls changed to pd1's. Its data
	// directory still exists, so PrepareJoinCluster takes the "data exists"
	// path — which must still run the uniqueness check and reject the restart.
	cfg := pd2.GetConfig()
	cfg.AdvertiseClientUrls = dupClientURL
	err = join.PrepareJoinCluster(cfg)
	re.Error(err)
	re.Contains(err.Error(), "already used by member")
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
