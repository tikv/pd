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

package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/informer"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
)

func TestInformer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create the cluster.
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	servers := cluster.GetServers()
	endpoints := make([]string, 0, len(servers))
	for _, s := range servers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}

	client, err := pd.NewClientWithContext(
		ctx, endpoints,
		pd.SecurityOption{},
	)
	re.NoError(err)

	rc := leaderServer.GetServer().GetRaftCluster()
	re.NotNil(rc)

	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		re.NoError(err)
	}

	store := core.NewRegionTree()
	stopCh := make(chan struct{})
	regionInformer := informer.NewInformer(
		&informer.ListWatch{
			ListFunc: func(options informer.Options) ([]interface{}, uint64, error) {
				ctx := context.WithValue(context.TODO(), pd.ResourceKey{}, "regionstat")
				ctx = context.WithValue(ctx, pd.NameKey{}, "test")
				return client.List(ctx)
			},
			WatchFunc: func(options informer.Options) (informer.Interface, error) {
				ctx := context.WithValue(context.TODO(), pd.RevisionKey{}, options.Revision)
				ctx = context.WithValue(ctx, pd.ResourceKey{}, "regionstat")
				ctx = context.WithValue(ctx, pd.NameKey{}, "test")
				return client.Watch(ctx)
			},
		},
		store,
	)
	go regionInformer.Run(stopCh)
	defer close(stopCh)
	time.Sleep(2 * time.Second)
	lister := informer.NewGenericLister(store)
	rs, err := lister.List(fmt.Sprintf("%20d", 0), fmt.Sprintf("%20d", 200), 200)
	re.NoError(err)
	re.Len(rs, 110)
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
	time.Sleep(2 * time.Second)
	regions = regions[4:]
	regionLen = len(regions)
	rs, err = lister.List(fmt.Sprintf("%20d", 0), fmt.Sprintf("%20d", 200), 200)
	re.NoError(err)
	re.Len(rs, regionLen)
}

func initRegions(regionLen int) []*core.RegionInfo {
	allocator := &idAllocator{allocator: mockid.NewIDAllocator()}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := 1; i <= regionLen; i++ {
		r := &metapb.Region{
			Id: allocator.alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
			Peers: []*metapb.Peer{
				{Id: allocator.alloc(), StoreId: uint64(1)},
				{Id: allocator.alloc(), StoreId: uint64(2)},
				{Id: allocator.alloc(), StoreId: uint64(3)},
			},
		}
		region := core.NewRegionInfo(r, r.Peers[0])
		buckets := &metapb.Buckets{
			RegionId: r.Id,
			Keys:     [][]byte{r.StartKey, r.EndKey},
			Version:  1,
		}
		region.UpdateBuckets(buckets, region.GetBuckets())
		regions = append(regions, region)
	}
	return regions
}
