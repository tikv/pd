// Copyright 2020 TiKV Project Authors.
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

package election

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const defaultLeaseTimeout = 1

func TestLeadership(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()

	// Campaign the same leadership
	leadership1 := NewLeadership(client, "/test_leader", "test_leader_1")
	leadership2 := NewLeadership(client, "/test_leader", "test_leader_2")

	// leadership1 starts first and get the leadership
	err := leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	// leadership2 starts then and can not get the leadership
	err = leadership2.Campaign(defaultLeaseTimeout, "test_leader_2")
	re.Error(err)

	re.True(leadership1.Check())
	// leadership2 failed, so the check should return false
	re.False(leadership2.Check())

	// Sleep longer than the defaultLeaseTimeout to wait for the lease expires
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.False(leadership1.Check())
	re.False(leadership2.Check())

	// Delete the leader key and campaign for leadership1
	err = leadership1.DeleteLeaderKey()
	re.NoError(err)
	err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	re.True(leadership1.Check())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leadership1.Keep(ctx)

	// Sleep longer than the defaultLeaseTimeout
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.True(leadership1.Check())
	re.False(leadership2.Check())

	// Delete the leader key and re-campaign for leadership2
	err = leadership1.DeleteLeaderKey()
	re.NoError(err)
	err = leadership2.Campaign(defaultLeaseTimeout, "test_leader_2")
	re.NoError(err)
	re.True(leadership2.Check())
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go leadership2.Keep(ctx)

	// Sleep longer than the defaultLeaseTimeout
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.False(leadership1.Check())
	re.True(leadership2.Check())

	// Test resetting the leadership.
	leadership1.Reset()
	leadership2.Reset()
	re.False(leadership1.Check())
	re.False(leadership2.Check())

	// Try to keep the reset leadership.
	leadership1.Keep(ctx)
	leadership2.Keep(ctx)

	// Check the lease.
	lease1 := leadership1.getLease()
	re.NotNil(lease1)
	lease2 := leadership2.getLease()
	re.NotNil(lease2)

	re.True(lease1.IsExpired())
	re.True(lease2.IsExpired())
	re.NoError(lease1.Close())
	re.NoError(lease2.Close())
}

func TestExitWatch(t *testing.T) {
	re := require.New(t)
	leaderKey := "/test_leader"
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/fastTick", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick", "return(true)"))
	// Case1: close the client before the watch loop starts
	checkExitWatch(t, leaderKey, func(_ *embed.Etcd, client *clientv3.Client) func() {
		re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayWatcher", `pause`))
		client.Close()
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayWatcher"))
		return func() {}
	})
	// Case2: close the client when the watch loop is running
	checkExitWatch(t, leaderKey, func(_ *embed.Etcd, client *clientv3.Client) func() {
		// Wait for the watch loop to start
		time.Sleep(500 * time.Millisecond)
		client.Close()
		return func() {}
	})
	// Case3: delete the leader key
	checkExitWatch(t, leaderKey, func(_ *embed.Etcd, client *clientv3.Client) func() {
		leaderKey := leaderKey
		_, err := client.Delete(context.Background(), leaderKey)
		re.NoError(err)
		return func() {}
	})
	// Case4: close the server before the watch loop starts
	checkExitWatch(t, leaderKey, func(server *embed.Etcd, _ *clientv3.Client) func() {
		re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayWatcher", `pause`))
		server.Close()
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayWatcher"))
		return func() {}
	})
	// Case5: close the server when the watch loop is running
	checkExitWatch(t, leaderKey, func(server *embed.Etcd, _ *clientv3.Client) func() {
		// Wait for the watch loop to start
		time.Sleep(500 * time.Millisecond)
		server.Close()
		return func() {}
	})
	// Case6: transfer leader without client reconnection.
	checkExitWatch(t, leaderKey, func(server *embed.Etcd, client *clientv3.Client) func() {
		cfg1 := server.Config()
		etcd2 := etcdutil.MustAddEtcdMember(t, &cfg1, client)
		client2, err := etcdutil.CreateEtcdClient(nil, etcd2.Config().ListenClientUrls)
		re.NoError(err)
		// close the original leader
		server.Server.HardStop()
		// delete the leader key with the new client
		client2.Delete(context.Background(), leaderKey)
		return func() {
			etcd2.Close()
			client2.Close()
		}
	})
	// Case7: loss the quorum when the watch loop is running
	checkExitWatch(t, leaderKey, func(server *embed.Etcd, client *clientv3.Client) func() {
		cfg1 := server.Config()
		etcd2 := etcdutil.MustAddEtcdMember(t, &cfg1, client)
		cfg2 := etcd2.Config()
		etcd3 := etcdutil.MustAddEtcdMember(t, &cfg2, client)

		resp2, err := client.MemberList(context.Background())
		re.NoError(err)
		re.Len(resp2.Members, 3)

		etcd2.Server.HardStop()
		etcd3.Server.HardStop()
		return func() {}
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/fastTick"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/fastTick"))
}

func checkExitWatch(t *testing.T, leaderKey string, injectFunc func(server *embed.Etcd, client *clientv3.Client) func()) {
	re := require.New(t)
	servers, client1, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	client2, err := etcdutil.CreateEtcdClient(nil, servers[0].Config().ListenClientUrls)
	re.NoError(err)
	defer client2.Close()

	leadership1 := NewLeadership(client1, leaderKey, "test_leader_1")
	leadership2 := NewLeadership(client2, leaderKey, "test_leader_2")
	err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	resp, err := client2.Get(context.Background(), leaderKey)
	re.NoError(err)
	done := make(chan struct{})
	go func() {
		leadership2.Watch(context.Background(), resp.Header.Revision)
		done <- struct{}{}
	}()

	cleanFunc := injectFunc(servers[0], client2)
	defer cleanFunc()

	testutil.Eventually(re, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestRequestProgress(t *testing.T) {
	checkWatcherRequestProgress := func(injectWatchChanBlock bool) {
		re := require.New(t)
		fname := testutil.InitTempFileLogger("debug")
		defer os.RemoveAll(fname)
		servers, client1, clean := etcdutil.NewTestEtcdCluster(t, 1)
		defer clean()
		client2, err := etcdutil.CreateEtcdClient(nil, servers[0].Config().ListenClientUrls)
		re.NoError(err)
		defer client2.Close()

		leaderKey := "/test_leader"
		leadership1 := NewLeadership(client1, leaderKey, "test_leader_1")
		leadership2 := NewLeadership(client2, leaderKey, "test_leader_2")
		err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
		re.NoError(err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp, err := client2.Get(ctx, leaderKey)
		re.NoError(err)
		go func() {
			leadership2.Watch(ctx, resp.Header.Revision)
		}()

		if injectWatchChanBlock {
			failpoint.Enable("github.com/tikv/pd/pkg/election/watchChanBlock", "return(true)")
			testutil.Eventually(re, func() bool {
				b, _ := os.ReadFile(fname)
				l := string(b)
				return strings.Contains(l, "watch channel is blocked for a long time")
			})
			failpoint.Disable("github.com/tikv/pd/pkg/election/watchChanBlock")
		} else {
			testutil.Eventually(re, func() bool {
				b, _ := os.ReadFile(fname)
				l := string(b)
				return strings.Contains(l, "watcher receives progress notify in watch loop")
			})
		}
	}
	checkWatcherRequestProgress(false)
	checkWatcherRequestProgress(true)
}

func TestCampaignTimes(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	leadership := NewLeadership(client, "test_leader", "test_leader")

	// all the campaign times are within the timeout.
	campaignTimesRecordTimeout = 10 * time.Second
	defer func() {
		campaignTimesRecordTimeout = 5 * time.Minute
	}()
	for i := 0; i < 3; i++ {
		leadership.AddCampaignTimes()
		time.Sleep(100 * time.Millisecond)
	}
	re.Equal(3, leadership.GetCampaignTimesNum())

	// only the last 2 records are valid.
	campaignTimesRecordTimeout = 200 * time.Millisecond
	for i := 0; i < 3; i++ {
		leadership.AddCampaignTimes()
		time.Sleep(100 * time.Millisecond)
	}
	re.Equal(2, leadership.GetCampaignTimesNum())

	time.Sleep(200 * time.Millisecond)
	// need to wait for the next addCampaignTimes to update the campaign time.
	re.Equal(2, leadership.GetCampaignTimesNum())
	// check campaign leader frequency.
	leadership.AddCampaignTimes()
	re.Equal(1, leadership.GetCampaignTimesNum())
}
