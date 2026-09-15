// Copyright 2026 TiKV Project Authors.
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

package tests

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	serverconfig "github.com/tikv/pd/server/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestShouldRetryCurrentServers(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.True(shouldRetryCurrentServers(errors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.True(shouldRetryCurrentServers(errors.New("[PD:etcd:ErrStartEtcd]start etcd failed")))
	re.True(shouldRetryCurrentServers(errors.New("[PD:etcd:ErrStartEtcd]start etcd failed: listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.False(shouldRetryCurrentServers(errors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.False(shouldRetryCurrentServers(errors.New("Etcd cluster ID mismatch")))
	re.False(shouldRetryCurrentServers(errors.New("some other error")))
	re.False(shouldRetryCurrentServers(nil))
}

func TestClassifyInitialServersError(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.Equal(startServersRetryCurrent, classifyInitialServersError(errors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.Equal(startServersRetryCurrent, classifyInitialServersError(errors.New("[PD:etcd:ErrStartEtcd]start etcd failed")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(errors.New("[PD:etcd:ErrStartEtcd]start etcd failed: listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(errors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(errors.New("Etcd cluster ID mismatch")))
	re.Equal(startServersNoRetry, classifyInitialServersError(errors.New("some other error")))
	re.Equal(startServersNoRetry, classifyInitialServersError(nil))
}

func TestRunServerDoesNotBlockWithoutReceiver(t *testing.T) {
	t.Parallel()

	result := RunServer(&TestServer{state: Destroy})
	require.Eventually(t, func() bool {
		return len(result) == 1
	}, time.Second, 10*time.Millisecond)
	require.Error(t, <-result)
}

func TestRegenerateInitialServerURLsKeepsInitialClusterConsistent(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	config := newClusterConfig(3)
	cleanupClusterConfig(t, config)
	cluster := &TestCluster{
		config: config,
		opts: []ConfigOption{
			func(conf *serverconfig.Config, _ string) {
				conf.InitialClusterToken = "retry-token"
			},
		},
	}

	regenerateServerURLs := func(server *serverConfig) {
		server.ClientURLs = tempurl.Alloc()
		server.PeerURLs = tempurl.Alloc()
		server.AdvertiseClientURLs = server.ClientURLs
		server.AdvertisePeerURLs = server.PeerURLs
	}

	regenerateServerURLs(config.InitialServers[0])
	firstConf, err := config.InitialServers[0].Generate(WithGCTuner(false))
	re.NoError(err)
	regenerateServerURLs(config.InitialServers[1])
	secondConf, err := config.InitialServers[1].Generate(WithGCTuner(false))
	re.NoError(err)
	re.NotEqual(firstConf.InitialCluster, secondConf.InitialCluster)

	serverConfs, err := cluster.regenerateInitialServerConfigs(false)
	re.NoError(err)
	re.Len(serverConfs, len(config.InitialServers))

	expectedInitialCluster := config.getServerAddrs()
	for _, server := range config.InitialServers {
		re.Contains(expectedInitialCluster, server.PeerURLs)
	}
	for _, conf := range serverConfs {
		re.Equal(expectedInitialCluster, conf.InitialCluster)
		re.Equal("retry-token", conf.InitialClusterToken)
		re.False(conf.PDServerCfg.EnableGOGCTuner)
	}
}

func cleanupClusterConfig(t *testing.T, config *clusterConfig) {
	t.Helper()
	for _, server := range config.InitialServers {
		dataDir := server.DataDir
		t.Cleanup(func() {
			require.NoError(t, os.RemoveAll(dataDir))
		})
	}
}

func TestRunInitialServersRetriesPortConflict(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	// Fail the second member while the first waits for quorum. Startup must
	// observe the error out of order, cancel the first member, and join both
	// goroutines before destroying their data and retrying with new ports.
	conf := cluster.config.InitialServers[1]
	peerURL, err := url.Parse(conf.PeerURLs)
	re.NoError(err)
	listener, err := net.Listen("tcp", peerURL.Host)
	re.NoError(err)
	defer listener.Close()
	conflictingURL := conf.PeerURLs

	result := make(chan error, 1)
	go func() { result <- cluster.RunInitialServers() }()
	select {
	case err := <-result:
		re.NoError(err)
	case <-time.After(20 * time.Second):
		cancel()
		<-result
		t.Fatal("startup did not cancel the sibling waiting for quorum")
	}
	re.NotEqual(conflictingURL, conf.PeerURLs)
	for _, s := range cluster.servers {
		re.Equal(Running, s.State())
	}
	// The successful attempt must outlive the startup context.
	re.NotEmpty(cluster.WaitLeader())
	re.NoError(cluster.StopAll())
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
}

func TestRestartPreservesDataOnClientPortConflict(t *testing.T) {
	re := require.New(t)
	keypath.ResetClusterID()
	t.Cleanup(keypath.ResetClusterID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	clusterID := keypath.ClusterID()
	const markerKey = "/test/restart-port-conflict"
	before, err := cluster.GetEtcdClient().Put(ctx, markerKey, "preserved")
	re.NoError(err)
	re.NoError(cluster.StopAll())
	keypath.ResetClusterID()

	conf := cluster.config.InitialServers[1]
	clientURL, err := url.Parse(conf.ClientURLs)
	re.NoError(err)
	listener, err := net.Listen("tcp", clientURL.Host)
	re.NoError(err)
	defer listener.Close()
	oldClientURL, oldPeerURL := conf.ClientURLs, conf.PeerURLs
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	re.NotEqual(oldClientURL, conf.ClientURLs)
	after, err := cluster.GetEtcdClient().Get(ctx, markerKey)
	re.NoError(err)
	re.Len(after.Kvs, 1)
	re.Equal("preserved", string(after.Kvs[0].Value))
	re.Equal(before.Header.ClusterId, after.Header.ClusterId)
	re.Equal(clusterID, keypath.ClusterID())
	re.Equal(oldPeerURL, conf.PeerURLs)
}

func TestResignLeaderDoesNotResetLeaseInCaller(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	oldLeader := cluster.WaitLeader()
	re.NotEmpty(oldLeader)
	leader := cluster.GetServer(oldLeader)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/blockLeaseClose",
		fmt.Sprintf("return(%q)", "leader election@"+oldLeader)))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/blockLeaseClose"))
	}()
	start := time.Now()
	re.NoError(leader.ResignLeader())
	// The transfer API must not wait for the injected ten-second lease close.
	re.Less(time.Since(start), 10*time.Second)
	re.NotEmpty(cluster.WaitLeaderChange(oldLeader))
}
