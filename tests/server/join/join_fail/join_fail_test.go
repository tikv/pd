// Copyright 2019 TiKV Project Authors.
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

package join_fail_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestFailedPDJoinInStep1(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	// Join the second PD.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/join/addMemberFailed", `return`))
	_, err = cluster.Join(ctx)
	re.Error(err)
	re.Contains(err.Error(), "join failed")
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/join/addMemberFailed"))
}

func TestFailedToStartPDAfterSuccessfulJoin(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	client := cluster.GetServer("pd1").GetEtcdClient()
	pd2PeerURL := tempurl.Alloc()
	// Add a member to the cluster but don't start it to simulate a successful join with failed start.
	resp, err := client.MemberAdd(ctx, []string{pd2PeerURL})
	re.NoError(err)
	re.Len(resp.Members, 2)

	// Join the second PD and start it.
	pd2, err := cluster.Join(ctx, func(conf *config.Config, _ string) {
		conf.AdvertisePeerUrls = pd2PeerURL
	})
	re.NoError(err)
	re.NoError(pd2.Run())
	re.NotEmpty(cluster.WaitLeader())

	// Check that the new PD has joined the cluster.
	members, err := client.MemberList(ctx)
	re.NoError(err)
	re.Len(members.Members, 2)
	re.Equal(cluster.GetServer("pd1").GetClusterID(), pd2.GetClusterID())
	if !pd2.IsLeader() {
		// Check that PD2 can become the leader.
		re.NoError(cluster.ResignLeader())
		re.Equal(cluster.WaitLeader(), pd2.GetConfig().Name)
	}
}
