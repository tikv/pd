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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

func TestRouterClientEnabledSuite(t *testing.T) {
	suite.Run(t, &routerClientSuite{routerClientEnabled: true})
}

func TestRouterClientDisabledSuite(t *testing.T) {
	suite.Run(t, &routerClientSuite{routerClientEnabled: false})
}

type routerClientSuite struct {
	suite.Suite
	cleanup         testutil.CleanupFunc
	ctx             context.Context
	clean           context.CancelFunc
	srv             *server.Server
	client          pd.Client
	grpcPDClient    pdpb.PDClient
	regionHeartbeat pdpb.PD_RegionHeartbeatClient
	reportBucket    pdpb.PD_ReportBucketsClient

	routerClientEnabled bool
}

func (suite *routerClientSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.srv, suite.cleanup, err = server.NewTestServer(re, assertutil.CheckerWithNilAssert(re))
	re.NoError(err)
	suite.grpcPDClient = testutil.MustNewGrpcClient(re, suite.srv.GetAddr())

	server.MustWaitLeader(re, []*server.Server{suite.srv})
	bootstrapServer(re, newHeader(), suite.grpcPDClient)

	suite.ctx, suite.clean = context.WithCancel(context.Background())
	suite.client = setupCli(suite.ctx, re, suite.srv.GetEndpoints(), opt.WithEnableRouterClient(suite.routerClientEnabled))

	suite.regionHeartbeat, err = suite.grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	suite.reportBucket, err = suite.grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)
	cluster := suite.srv.GetRaftCluster()
	re.NotNil(cluster)
	cluster.GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)
}

// TearDownSuite cleans up the test cluster and client.
func (suite *routerClientSuite) TearDownSuite() {
	suite.client.Close()
	suite.clean()
	suite.cleanup()
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
	suite.srv.GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(false)

	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), opt.WithBuckets())
		re.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets == nil
	})
	suite.srv.GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)

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
			switch rand.Intn(3) {
			case 0:
				region := regions[0]
				testutil.Eventually(re, func() bool {
					r, err := suite.client.GetRegion(ctx, region.GetStartKey())
					re.NoError(err)
					if r == nil {
						return false
					}
					return reflect.DeepEqual(region, r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				})
			case 1:
				testutil.Eventually(re, func() bool {
					r, err := suite.client.GetPrevRegion(ctx, regions[1].GetStartKey())
					re.NoError(err)
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
					r, err := suite.client.GetRegionByID(ctx, region.GetId())
					re.NoError(err)
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
		err := suite.client.UpdateOption(opt.EnableRouterClient, enabled)
		re.NoError(err)
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		wg.Wait()
	}
}

func (suite *routerClientSuite) TestConcurrentlyEnableRouterClient() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	suite.dispatchConcurrentRequests(ctx, re, &wg)
	// Concurrently enable and disable the router client.
	for _, enabled := range []bool{!suite.routerClientEnabled, suite.routerClientEnabled} {
		err := suite.client.UpdateOption(opt.EnableRouterClient, enabled)
		re.NoError(err)
	}
	wg.Wait()
}
