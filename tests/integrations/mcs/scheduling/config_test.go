// Copyright 2023 TiKV Project Authors.
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

package scheduling

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/server/api"
)

type configTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
}

func TestConfig(t *testing.T) {
	suite.Run(t, &configTestSuite{})
}

func (suite *configTestSuite) SetupSuite() {
	re := suite.Require()

	schedulers.Register()
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
	// Force the coordinator to be prepared to initialize the schedulers.
	suite.pdLeaderServer.GetRaftCluster().GetCoordinator().GetPrepareChecker().SetPrepared()
}

func (suite *configTestSuite) TearDownSuite() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *configTestSuite) TestConfigWatch() {
	re := suite.Require()

	// Make sure the config is persisted before the watcher is created.
	persistConfig(re, suite.pdLeaderServer)
	// Create a config watcher.
	watcher, err := config.NewWatcher(
		suite.ctx,
		suite.pdLeaderServer.GetEtcdClient(),
		config.NewPersistConfig(config.NewConfig(), cache.NewStringTTL(suite.ctx, sc.DefaultGCInterval, sc.DefaultTTL)),
		endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil),
	)
	re.NoError(err)
	// Check the initial config value.
	re.Equal(uint64(sc.DefaultMaxReplicas), watcher.GetReplicationConfig().MaxReplicas)
	re.Equal(sc.DefaultSplitMergeInterval, watcher.GetScheduleConfig().SplitMergeInterval.Duration)
	re.Equal("0.0.0", watcher.GetClusterVersion().String())
	// Update the config and check if the scheduling config watcher can get the latest value.
	testutil.Eventually(re, func() bool {
		return watcher.GetReplicationConfig().MaxReplicas == 3
	})
	persistOpts := suite.pdLeaderServer.GetPersistOptions()
	persistOpts.SetMaxReplicas(5)
	persistConfig(re, suite.pdLeaderServer)
	testutil.Eventually(re, func() bool {
		return watcher.GetReplicationConfig().MaxReplicas == 5
	})
	persistOpts.SetSplitMergeInterval(2 * sc.DefaultSplitMergeInterval)
	persistConfig(re, suite.pdLeaderServer)
	testutil.Eventually(re, func() bool {
		return watcher.GetScheduleConfig().SplitMergeInterval.Duration == 2*sc.DefaultSplitMergeInterval
	})
	persistOpts.SetStoreConfig(&sc.StoreConfig{
		Coprocessor: sc.Coprocessor{
			RegionMaxSize: "144MiB",
		},
		Storage: sc.Storage{
			Engine: sc.RaftstoreV2,
		},
	})
	persistConfig(re, suite.pdLeaderServer)
	testutil.Eventually(re, func() bool {
		return watcher.GetStoreConfig().GetRegionMaxSize() == 144 &&
			watcher.GetStoreConfig().IsRaftKV2()
	})
	persistOpts.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	persistConfig(re, suite.pdLeaderServer)
	testutil.Eventually(re, func() bool {
		return watcher.GetClusterVersion().String() == "4.0.0"
	})
	watcher.Close()
}

// Manually trigger the config persistence in the PD API server side.
func persistConfig(re *require.Assertions, pdLeaderServer *tests.TestServer) {
	err := pdLeaderServer.GetPersistOptions().Persist(pdLeaderServer.GetServer().GetStorage())
	re.NoError(err)
}

func (suite *configTestSuite) TestSchedulerConfigWatch() {
	re := suite.Require()
	// Make sure the config is persisted before the watcher is created.
	persistConfig(re, suite.pdLeaderServer)
	// Create a config watcher.
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	watcher, err := config.NewWatcher(
		suite.ctx,
		suite.pdLeaderServer.GetEtcdClient(),
		config.NewPersistConfig(config.NewConfig(), cache.NewStringTTL(suite.ctx, sc.DefaultGCInterval, sc.DefaultTTL)),
		storage,
	)
	re.NoError(err)
	// Get all default scheduler names.
	var namesFromAPIServer []string
	testutil.Eventually(re, func() bool {
		namesFromAPIServer, _, _ = suite.pdLeaderServer.GetRaftCluster().GetStorage().LoadAllSchedulerConfigs()
		return len(namesFromAPIServer) == len(sc.DefaultSchedulers)
	})
	// Check all default schedulers' configs.
	var namesFromSchedulingServer []string
	testutil.Eventually(re, func() bool {
		namesFromSchedulingServer, _, err = storage.LoadAllSchedulerConfigs()
		re.NoError(err)
		return len(namesFromSchedulingServer) == len(namesFromAPIServer)
	})
	re.Equal(namesFromAPIServer, namesFromSchedulingServer)
	// Add a new scheduler.
	api.MustAddScheduler(re, suite.pdLeaderServer.GetAddr(), types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 1,
	})
	// Check the new scheduler's config.
	testutil.Eventually(re, func() bool {
		namesFromSchedulingServer, _, err = storage.LoadAllSchedulerConfigs()
		re.NoError(err)
		return slice.Contains(namesFromSchedulingServer, types.EvictLeaderScheduler.String())
	})
	assertEvictLeaderStoreIDs(re, storage, []uint64{1})
	// Update the scheduler by adding a store.
	err = suite.pdLeaderServer.GetServer().GetRaftCluster().PutMetaStore(
		&metapb.Store{
			Id:            2,
			Address:       "mock://2",
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
			Version:       "4.0.0",
		},
	)
	re.NoError(err)
	api.MustAddScheduler(re, suite.pdLeaderServer.GetAddr(), types.EvictLeaderScheduler.String(), map[string]any{
		"store_id": 2,
	})
	assertEvictLeaderStoreIDs(re, storage, []uint64{1, 2})
	// Update the scheduler by removing a store.
	api.MustDeleteScheduler(re, suite.pdLeaderServer.GetAddr(), fmt.Sprintf("%s-%d", types.EvictLeaderScheduler.String(), 1))
	assertEvictLeaderStoreIDs(re, storage, []uint64{2})
	// Delete the scheduler.
	api.MustDeleteScheduler(re, suite.pdLeaderServer.GetAddr(), types.EvictLeaderScheduler.String())
	// Check the removed scheduler's config.
	testutil.Eventually(re, func() bool {
		namesFromSchedulingServer, _, err = storage.LoadAllSchedulerConfigs()
		re.NoError(err)
		return !slice.Contains(namesFromSchedulingServer, types.EvictLeaderScheduler.String())
	})
	watcher.Close()
}

func assertEvictLeaderStoreIDs(
	re *require.Assertions, storage *endpoint.StorageEndpoint, storeIDs []uint64,
) {
	var evictLeaderCfg struct {
		StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	}
	testutil.Eventually(re, func() bool {
		cfg, err := storage.LoadSchedulerConfig(types.EvictLeaderScheduler.String())
		re.NoError(err)
		err = schedulers.DecodeConfig([]byte(cfg), &evictLeaderCfg)
		re.NoError(err)
		return len(evictLeaderCfg.StoreIDWithRanges) == len(storeIDs)
	})
	// Validate the updated scheduler's config.
	for _, storeID := range storeIDs {
		re.Contains(evictLeaderCfg.StoreIDWithRanges, storeID)
	}
}
