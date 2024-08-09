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

package server_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestUpdateAdvertiseUrls(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	// AdvertisePeerUrls should equals to PeerUrls.
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		re.Equal(conf.PeerURLs, serverConf.AdvertisePeerUrls)
		re.Equal(conf.ClientURLs, serverConf.AdvertiseClientUrls)
	}

	err = cluster.StopAll()
	re.NoError(err)

	// Change config will not affect peer urls.
	// Recreate servers with new peer URLs.
	for _, conf := range cluster.GetConfig().InitialServers {
		conf.AdvertisePeerURLs = conf.PeerURLs + "," + tempurl.Alloc()
	}
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf, err := conf.Generate()
		re.NoError(err)
		s, err := tests.NewTestServer(ctx, serverConf)
		re.NoError(err)
		cluster.GetServers()[conf.Name] = s
	}
	err = cluster.RunInitialServers()
	re.NoError(err)
	for _, conf := range cluster.GetConfig().InitialServers {
		serverConf := cluster.GetServer(conf.Name).GetConfig()
		re.Equal(conf.PeerURLs, serverConf.AdvertisePeerUrls)
	}
}

func TestClusterID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	clusterID := cluster.GetServer("pd1").GetClusterID()
	for _, s := range cluster.GetServers() {
		re.Equal(clusterID, s.GetClusterID())
	}

	// Restart all PDs.
	err = cluster.StopAll()
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)

	// All PDs should have the same cluster ID as before.
	for _, s := range cluster.GetServers() {
		re.Equal(clusterID, s.GetClusterID())
	}

	cluster2, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) { conf.InitialClusterToken = "foobar" })
	defer cluster2.Destroy()
	re.NoError(err)
	err = cluster2.RunInitialServers()
	re.NoError(err)
	clusterID2 := cluster2.GetServer("pd1").GetClusterID()
	re.NotEqual(clusterID, clusterID2)
}

func TestLeader(t *testing.T) {
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

	err = cluster.GetServer(leader1).Stop()
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return cluster.GetLeader() != leader1
	})
}

func TestRetryBootstrap(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader := cluster.WaitLeader()
	re.NotEmpty(leader)

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/saveRegionFailed", `return(true)`))
	leaderServer := cluster.GetServer(leader)
	re.NoError(leaderServer.BootstrapCluster())
	// First re-bootstrap should be success.
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/saveRegionFailed"))
	re.NoError(leaderServer.BootstrapCluster())
	// Second re-bootstrap should be failed.
	re.Error(leaderServer.BootstrapCluster())
}
