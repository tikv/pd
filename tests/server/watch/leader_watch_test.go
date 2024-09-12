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

package watch_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestWatcher(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) { conf.AutoCompactionRetention = "1s" })
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pd1 := cluster.GetLeaderServer()
	re.NotNil(pd1)

	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	err = pd2.Run()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	time.Sleep(5 * time.Second)
	pd3, err := cluster.Join(ctx)
	re.NoError(err)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayWatcher", `pause`))
	err = pd3.Run()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	time.Sleep(200 * time.Millisecond)
	re.Equal(pd1.GetConfig().Name, pd3.GetLeader().GetName())
	err = pd1.Stop()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	re.Equal(pd2.GetConfig().Name, pd2.GetLeader().GetName())
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayWatcher"))
	testutil.Eventually(re, func() bool {
		return pd3.GetLeader().GetName() == pd2.GetConfig().Name
	})
}

func TestWatcherCompacted(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) { conf.AutoCompactionRetention = "1s" })
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pd1 := cluster.GetLeaderServer()
	re.NotNil(pd1)
	client := pd1.GetEtcdClient()
	_, err = client.Put(context.Background(), "test", "v")
	re.NoError(err)
	// wait compaction
	time.Sleep(2 * time.Second)
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	err = pd2.Run()
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return pd2.GetLeader().GetName() == pd1.GetConfig().Name
	})
}
