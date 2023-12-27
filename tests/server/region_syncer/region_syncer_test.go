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

package syncer_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	v, _ := i.allocator.Alloc()
	return v
}

func TestRegionSyncer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/regionStorageFastFlush", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/syncer/noFastExitSync", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/syncer/disableClientStreaming", `return(true)`))

	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer func() {
		cluster.Destroy()
		cancel()
	}()
	re.NoError(err)

	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()

	re.NoError(leaderServer.BootstrapCluster())
	rc := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(rc)
	followerServer := cluster.GetServer(cluster.GetFollower())

	testutil.Eventually(re, func() bool {
		return !followerServer.GetServer().DirectlyGetRaftCluster().GetRegionSyncer().IsRunning()
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/syncer/disableClientStreaming"))
	re.True(cluster.WaitRegionSyncerClientsReady(2))
	testutil.Eventually(re, func() bool {
		return followerServer.GetServer().DirectlyGetRaftCluster().GetRegionSyncer().IsRunning()
	})

	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	// merge case
	// region2 -> region1 -> region0
	// merge A to B will increases version to max(versionA, versionB)+1, but does not increase conver
	// region0 version is max(1, max(1, 1)+1)+1=3
	regions[0] = regions[0].Clone(core.WithEndKey(regions[2].GetEndKey()), core.WithIncVersion(), core.WithIncVersion())
	err = rc.HandleRegionHeartbeat(regions[0])
	re.NoError(err)

	// merge case
	// region3 -> region4
	// merge A to B will increases version to max(versionA, versionB)+1, but does not increase conver
	// region4 version is max(1, 1)+1=2
	regions[4] = regions[3].Clone(core.WithEndKey(regions[4].GetEndKey()), core.WithIncVersion())
	err = rc.HandleRegionHeartbeat(regions[4])
	re.NoError(err)

	// merge case
	// region0 -> region4
	// merge A to B will increases version to max(versionA, versionB)+1, but does not increase conver
	// region4 version is max(3, 2)+1=4
	regions[4] = regions[0].Clone(core.WithEndKey(regions[4].GetEndKey()), core.WithIncVersion())
	err = rc.HandleRegionHeartbeat(regions[4])
	re.NoError(err)
	regions = regions[4:]
	regionLen = len(regions)

	// change the statistics of regions
	for i := 0; i < len(regions); i++ {
		idx := uint64(i)
		regions[i] = regions[i].Clone(
			core.SetWrittenBytes(idx+10),
			core.SetWrittenKeys(idx+20),
			core.SetReadBytes(idx+30),
			core.SetReadKeys(idx+40))
		err = rc.HandleRegionHeartbeat(regions[i])
		re.NoError(err)
	}

	// change the leader of region
	for i := 0; i < len(regions); i++ {
		regions[i] = regions[i].Clone(core.WithLeader(regions[i].GetPeers()[1]))
		err = rc.HandleRegionHeartbeat(regions[i])
		re.NoError(err)
	}

	// ensure flush to region storage, we use a duration larger than the
	// region storage flush rate limit (3s).
	time.Sleep(4 * time.Second)

	// test All regions have been synchronized to the cache of followerServer
	re.NotNil(followerServer)
	cacheRegions := leaderServer.GetServer().GetBasicCluster().GetRegions()
	re.Len(cacheRegions, regionLen)
	testutil.Eventually(re, func() bool {
		assert := assert.New(t)
		for _, region := range cacheRegions {
			r := followerServer.GetServer().GetBasicCluster().GetRegion(region.GetID())
			if !(assert.Equal(region.GetMeta(), r.GetMeta()) &&
				assert.Equal(region.GetStat(), r.GetStat()) &&
				assert.Equal(region.GetLeader(), r.GetLeader()) &&
				assert.Equal(region.GetBuckets(), r.GetBuckets())) {
				return false
			}
		}
		return true
	})

	err = leaderServer.Stop()
	re.NoError(err)
	cluster.WaitLeader()
	leaderServer = cluster.GetLeaderServer()
	testutil.Eventually(re, func() bool {
		return !leaderServer.GetServer().GetRaftCluster().GetRegionSyncer().IsRunning()
	})
	re.NotNil(leaderServer)
	loadRegions := leaderServer.GetServer().GetRaftCluster().GetRegions()
	re.Len(loadRegions, regionLen)
	for _, region := range regions {
		r := leaderServer.GetRegionInfoByID(region.GetID())
		re.Equal(region.GetMeta(), r.GetMeta())
		re.Equal(region.GetStat(), r.GetStat())
		re.Equal(region.GetLeader(), r.GetLeader())
		re.Equal(region.GetBuckets(), r.GetBuckets())
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/syncer/noFastExitSync"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/regionStorageFastFlush"))
}

func TestFullSyncWithAddMember(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	rc := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(rc)
	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	// ensure flush to region storage
	time.Sleep(3 * time.Second)
	// restart pd1
	err = leaderServer.Stop()
	re.NoError(err)
	err = leaderServer.Run()
	re.NoError(err)
	re.Equal("pd1", cluster.WaitLeader())

	// join new PD
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	re.NoError(pd2.Run())
	re.Equal("pd1", cluster.WaitLeader())
	// waiting for synchronization to complete
	time.Sleep(3 * time.Second)
	re.NoError(cluster.ResignLeader())
	re.Equal("pd2", cluster.WaitLeader())
	loadRegions := pd2.GetServer().GetRaftCluster().GetRegions()
	re.Len(loadRegions, regionLen)
}

func TestPrepareChecker(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	rc := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(rc)
	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}

	// ensure flush to region storage
	time.Sleep(3 * time.Second)
	re.True(leaderServer.GetRaftCluster().IsPrepared())

	// join new PD
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	err = pd2.Run()
	re.NoError(err)
	// waiting for synchronization to complete
	time.Sleep(3 * time.Second)
	err = cluster.ResignLeader()
	re.NoError(err)
	re.Equal("pd2", cluster.WaitLeader())
	leaderServer = cluster.GetLeaderServer()
	rc = leaderServer.GetServer().GetRaftCluster()
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	time.Sleep(time.Second)
	re.True(rc.IsPrepared())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
}

// ref: https://github.com/tikv/pd/issues/6988
func TestPrepareCheckerWithTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", `return(true)`))
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	rc := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(rc)
	regionLen := 100
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	// ensure flush to region storage
	time.Sleep(3 * time.Second)
	re.True(leaderServer.GetRaftCluster().IsPrepared())

	// join new PD
	pd2, err := cluster.Join(ctx)
	re.NoError(err)
	err = pd2.Run()
	re.NoError(err)
	// waiting for synchronization to complete
	time.Sleep(3 * time.Second)
	err = cluster.ResignLeader()
	re.NoError(err)
	re.Equal("pd2", cluster.WaitLeader())

	// transfer leader to pd1, can start coordinator immediately.
	err = cluster.ResignLeader()
	re.NoError(err)
	re.Equal("pd1", cluster.WaitLeader())
	re.True(rc.IsPrepared())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker"))
}

func initRegions(regionLen int) []*core.RegionInfo {
	allocator := &idAllocator{allocator: mockid.NewIDAllocator()}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		r := &metapb.Region{
			Id: allocator.alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers: []*metapb.Peer{
				{Id: allocator.alloc(), StoreId: uint64(1)},
				{Id: allocator.alloc(), StoreId: uint64(2)},
				{Id: allocator.alloc(), StoreId: uint64(3)},
			},
		}
		region := core.NewRegionInfo(r, r.Peers[0], core.SetSource(core.Heartbeat))
		// Here is used to simulate the upgrade process.
		if i < regionLen/2 {
			buckets := &metapb.Buckets{
				RegionId: r.Id,
				Keys:     [][]byte{r.StartKey, r.EndKey},
				Version:  1,
			}
			region.UpdateBuckets(buckets, region.GetBuckets())
		}
		regions = append(regions, region)
	}
	return regions
}
