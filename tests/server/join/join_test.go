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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/server"
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
	_, err = os.Stat(filepath.Join(pd2.GetConfig().DataDir, "join"))
	re.False(os.IsNotExist(err))
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
	_, err = os.Stat(filepath.Join(pd3.GetConfig().DataDir, "join"))
	re.False(os.IsNotExist(err))
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
