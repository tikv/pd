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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/utils/testutil"
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

func TestRegenerateInitialServerURLsKeepsInitialClusterConsistent(t *testing.T) {
	t.Parallel()

	re := require.New(t)
	config := newClusterConfig(3)
	oldPeerURLs := []string{
		"http://127.0.0.1:1",
		"http://127.0.0.1:2",
		"http://127.0.0.1:3",
	}
	for i, server := range config.InitialServers {
		server.PeerURLs = oldPeerURLs[i]
	}

	config.regenerateInitialServerURLs()
	initialCluster := config.getServerAddrs()
	for i, server := range config.InitialServers {
		re.NotEqual(oldPeerURLs[i], server.PeerURLs)
	}
	for _, server := range config.InitialServers {
		conf, err := server.Generate()
		re.NoError(err)
		re.Equal(initialCluster, conf.InitialCluster)
	}
}
