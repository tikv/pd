// Copyright 2025 TiKV Project Authors.
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
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestRouterClientEnabledSuite(t *testing.T) {
	suite.Run(t, &routerClientSuite{routerClientEnabled: true})
}

func TestRouterClientDisabledSuite(t *testing.T) {
	suite.Run(t, &routerClientSuite{routerClientEnabled: false})
}

type routerClientSuite struct {
	suite.Suite
	ctx             context.Context
	clean           context.CancelFunc
	cluster         *tests.TestCluster
	client          pd.Client
	grpcPDClient    pdpb.PDClient
	regionHeartbeat pdpb.PD_RegionHeartbeatClient
	reportBucket    pdpb.PD_ReportBucketsClient

	routerClientEnabled bool
}

func (suite *routerClientSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.ctx, suite.clean = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 3)
	re.NoError(err)
	endpoints := runServer(re, suite.cluster)
	re.Len(endpoints, 3)

	re.NotEmpty(suite.cluster.WaitLeader())
	leader := suite.cluster.GetLeaderServer()
	suite.grpcPDClient = testutil.MustNewGrpcClient(re, leader.GetAddr())
	suite.client = setupCli(suite.ctx, re, endpoints, opt.WithEnableRouterClient(suite.routerClientEnabled))

	suite.regionHeartbeat, err = suite.grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	suite.reportBucket, err = suite.grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)
	cluster := suite.cluster.GetLeaderServer().GetRaftCluster()
	re.NotNil(cluster)
	cluster.GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)
}

// TearDownSuite cleans up the test cluster and client.
func (suite *routerClientSuite) TearDownSuite() {
	suite.client.Close()
	suite.clean()
	suite.cluster.Destroy()
}

func (suite *routerClientSuite) TestGetRegion() {
	re := suite.Require()
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}
	err := suite.regionHeartbeat.Send(req)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"))
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader) &&
			r.Buckets == nil
	})
	breq := &pdpb.ReportBucketsRequest{
		Header: newHeader(),
		Buckets: &metapb.Buckets{
			RegionId:   regionID,
			Version:    1,
			Keys:       [][]byte{[]byte("a"), []byte("z")},
			PeriodInMs: 2000,
			Stats: &metapb.BucketStats{
				ReadBytes:  []uint64{1},
				ReadKeys:   []uint64{1},
				ReadQps:    []uint64{1},
				WriteBytes: []uint64{1},
				WriteKeys:  []uint64{1},
				WriteQps:   []uint64{1},
			},
		},
	}
	re.NoError(suite.reportBucket.Send(breq))
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), opt.WithBuckets())
		re.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets != nil
	})
	suite.cluster.GetLeaderServer().GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(false)

	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), opt.WithBuckets())
		re.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets == nil
	})
	suite.cluster.GetLeaderServer().GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/grpcClientClosed", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/useForwardRequest", `return(true)`))
	re.NoError(suite.reportBucket.Send(breq))
	re.Error(suite.reportBucket.RecvMsg(breq))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/grpcClientClosed"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/useForwardRequest"))
}

func (suite *routerClientSuite) TestGetPrevRegion() {
	re := suite.Require()
	regionLen := 10
	regions := make([]*metapb.Region, 0, regionLen)
	for i := range regionLen {
		regionID := regionIDAllocator.alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, r)
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(),
			Region: r,
			Leader: peers[0],
		}
		err := suite.regionHeartbeat.Send(req)
		re.NoError(err)
	}
	for i := range 20 {
		testutil.Eventually(re, func() bool {
			r, err := suite.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			re.NoError(err)
			if i > 0 && i < regionLen {
				return reflect.DeepEqual(peers[0], r.Leader) &&
					reflect.DeepEqual(regions[i-1], r.Meta)
			}
			return r == nil
		})
	}
}

func (suite *routerClientSuite) TestGetRegionByID() {
	re := suite.Require()
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}
	err := suite.regionHeartbeat.Send(req)
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegionByID(context.Background(), regionID)
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader)
	})

	// test WithCallerComponent
	testutil.Eventually(re, func() bool {
		r, err := suite.client.
			WithCallerComponent(caller.GetComponent(0)).
			GetRegionByID(context.Background(), regionID)
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader)
	})
}

func (suite *routerClientSuite) TestGetRegionConcurrently() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	suite.dispatchConcurrentRequests(ctx, re, &wg)
	wg.Wait()
}

func (suite *routerClientSuite) dispatchConcurrentRequests(ctx context.Context, re *require.Assertions, wg *sync.WaitGroup) {
	regions := make([]*metapb.Region, 0, 2)
	for i := range 2 {
		regionID := regionIDAllocator.alloc()
		region := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		re.NoError(suite.regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
			Header: newHeader(),
			Region: region,
			Leader: peers[0],
		}))
		regions = append(regions, region)
	}

	const concurrency = 1000

	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			var (
				r                   *router.Region
				err                 error
				seed                = rand.Intn(100)
				allowFollowerHandle = seed%2 == 0
			)
			// Randomly sleep to avoid the concurrent requests to be dispatched at the same time.
			time.Sleep(time.Duration(seed) * time.Millisecond)
			switch seed % 3 {
			case 0:
				region := regions[0]
				testutil.Eventually(re, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetRegion(ctx, region.GetStartKey(), opt.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetRegion(ctx, region.GetStartKey())
					}
					if err != nil {
						re.ErrorContains(err, context.Canceled.Error())
					}
					if r == nil {
						return false
					}
					return reflect.DeepEqual(region, r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				})
			case 1:
				testutil.Eventually(re, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetPrevRegion(ctx, regions[1].GetStartKey(), opt.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetPrevRegion(ctx, regions[1].GetStartKey())
					}
					if err != nil {
						re.ErrorContains(err, context.Canceled.Error())
					}
					if r == nil {
						return false
					}
					return reflect.DeepEqual(regions[0], r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				})
			case 2:
				region := regions[0]
				testutil.Eventually(re, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetRegionByID(ctx, region.GetId(), opt.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetRegionByID(ctx, region.GetId())
					}
					if err != nil {
						re.ErrorContains(err, context.Canceled.Error())
					}
					if r == nil {
						return false
					}
					return reflect.DeepEqual(region, r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				})
			}
		}()
	}
}

func (suite *routerClientSuite) TestDynamicallyEnableRouterClient() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	for _, enabled := range []bool{!suite.routerClientEnabled, suite.routerClientEnabled} {
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		wg.Wait()
		err := suite.client.UpdateOption(opt.EnableRouterClient, enabled)
		re.NoError(err)
	}
}

func (suite *routerClientSuite) TestConcurrentlyEnableRouterClient() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	// Concurrently enable and disable the router client.
	for _, enabled := range []bool{!suite.routerClientEnabled, suite.routerClientEnabled} {
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		// Switch the router client option immediately right after the concurrent requests dispatch.
		err := suite.client.UpdateOption(opt.EnableRouterClient, enabled)
		re.NoError(err)
		select {
		case <-time.After(time.Second):
			// Let the bullet fly for a while.
		case <-ctx.Done():
		}
	}
	wg.Wait()
}

func (suite *routerClientSuite) TestConcurrentlyEnableFollowerHandle() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	// Wait for the region syncer on the follower to be running.
	testutil.Eventually(re, func() bool {
		running := true
		for _, s := range suite.cluster.GetServers() {
			if s.IsLeader() {
				continue
			}
			running = running && s.GetServer().DirectlyGetRaftCluster().GetRegionSyncer().IsRunning()
		}
		return running
	})

	wg := sync.WaitGroup{}
	// Concurrently enable and disable the follower handle.
	for _, enabled := range []bool{true, false} {
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		// Switch the follower handle option immediately right after the concurrent requests dispatch.
		err := suite.client.UpdateOption(opt.EnableFollowerHandle, enabled)
		re.NoError(err)
		select {
		case <-time.After(time.Second):
			// Let the bullet fly for a while.
		case <-ctx.Done():
		}
	}
}
