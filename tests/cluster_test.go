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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	perrors "github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	serverconfig "github.com/tikv/pd/server/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestShouldRetryCurrentServers(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.True(shouldRetryCurrentServers(perrors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.True(shouldRetryCurrentServers(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed")))
	re.True(shouldRetryCurrentServers(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed: listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.False(shouldRetryCurrentServers(perrors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.False(shouldRetryCurrentServers(perrors.New("Etcd cluster ID mismatch")))
	re.False(shouldRetryCurrentServers(perrors.New("some other error")))
	re.False(shouldRetryCurrentServers(nil))
}

func TestClassifyInitialServersError(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	re.Equal(startServersRetryCurrent, classifyInitialServersError(perrors.New("[PD:server:ErrCancelStartEtcd]etcd start canceled")))
	re.Equal(startServersRetryCurrent, classifyInitialServersError(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(perrors.New("[PD:etcd:ErrStartEtcd]start etcd failed: listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(perrors.New("listen tcp 127.0.0.1:2379: bind: address already in use")))
	re.Equal(startServersRetryRecreate, classifyInitialServersError(perrors.New("Etcd cluster ID mismatch")))
	re.Equal(startServersNoRetry, classifyInitialServersError(perrors.New("some other error")))
	re.Equal(startServersNoRetry, classifyInitialServersError(nil))
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

	serverConfs, err := cluster.regenerateInitialServerConfigs()
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

func TestRunTasksFastErrorReturnsLaterFailureWithoutWaitingForEarlierTask(t *testing.T) {
	re := require.New(t)

	blocked := make(chan struct{})

	cleanupCalled := make(chan struct{}, 1)
	start := time.Now()
	err := runTasksFastError([]func() error{
		func() error {
			<-blocked
			return nil
		},
		func() error {
			return errors.New("address already in use")
		},
	}, func() {
		close(blocked)
		cleanupCalled <- struct{}{}
	})

	re.ErrorContains(err, "address already in use")
	re.Less(time.Since(start), 500*time.Millisecond)

	select {
	case <-cleanupCalled:
	default:
		t.Fatal("cleanup callback was not triggered")
	}
}
