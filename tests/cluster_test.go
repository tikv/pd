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
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"

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

type stubTestServer struct {
	state    atomic.Int32
	stopCh   chan struct{}
	runErr   error
	stopped  atomic.Bool
	finished atomic.Bool
}

func newStubTestServer() *stubTestServer {
	return &stubTestServer{
		stopCh: make(chan struct{}),
	}
}

func (s *stubTestServer) runWithStartSignal(started chan<- struct{}) error {
	s.state.Store(Running)
	if started != nil {
		close(started)
	}
	if s.runErr != nil {
		s.state.Store(Initial)
		s.finished.Store(true)
		return s.runErr
	}
	<-s.stopCh
	s.finished.Store(true)
	return nil
}

func (s *stubTestServer) Stop() error {
	if !s.state.CompareAndSwap(Running, Stop) {
		return errors.New("server is not running")
	}
	s.stopped.Store(true)
	close(s.stopCh)
	return nil
}

func (s *stubTestServer) State() int32 {
	return s.state.Load()
}

func TestRunServersWaitsForInFlightRuns(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	failedServer := newStubTestServer()
	failedServer.runErr = errors.New("start failed")
	blockedServer := newStubTestServer()

	// Start the failed server first to verify that RunServers waits until the
	// following server is stoppable before handling the failure.
	err := runTestServers([]testServerRunner{failedServer, blockedServer})
	re.EqualError(err, "start failed")
	re.True(blockedServer.stopped.Load())
	re.True(blockedServer.finished.Load())
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
