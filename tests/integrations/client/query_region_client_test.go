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

package client_test

import (
	"context"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestQueryRegionClientEnabledSuite(t *testing.T) {
	suite.Run(t, &queryRegionClientSuite{queryRegionEnabled: true})
}

func TestQueryRegionClientDisabledSuite(t *testing.T) {
	suite.Run(t, &queryRegionClientSuite{queryRegionEnabled: false})
}

type queryRegionClientSuite struct {
	suite.Suite
	ctx                context.Context
	cancel             context.CancelFunc
	cluster            *tests.TestCluster
	client             pd.Client
	regionHeartbeat    pdpb.PD_RegionHeartbeatClient
	reportBucket       pdpb.PD_ReportBucketsClient
	queryRegionEnabled bool
}

func (suite *queryRegionClientSuite) SetupSuite() {
	re := suite.Require()
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 3)
	re.NoError(err)
	endpoints := runServer(re, suite.cluster)
	re.NotEmpty(suite.cluster.WaitLeader())
	leader := suite.cluster.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	suite.regionHeartbeat, err = grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	suite.reportBucket, err = grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)
	opts := make([]pd.ClientOption, 0, 1)
	if suite.queryRegionEnabled {
		opts = append(opts, pd.WithEnableQueryRegion())
	}
	suite.client = setupCli(suite.ctx, re, endpoints, opts...)
	re.NoError(suite.client.UpdateOption(pd.EnableFollowerHandle, true))
	cluster := leader.GetRaftCluster()
	re.NotNil(cluster)
	cluster.GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)
}

func (suite *queryRegionClientSuite) TearDownSuite() {
	suite.client.Close()
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *queryRegionClientSuite) TestGetRegion() {
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
	re.NoError(suite.regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}))
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
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		re.NoError(err)
		return r != nil && r.Buckets != nil
	})
	suite.cluster.GetLeaderServer().GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(false)
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		re.NoError(err)
		return r != nil && r.Buckets == nil
	})
	suite.cluster.GetLeaderServer().GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)
}

func (suite *queryRegionClientSuite) TestGetPrevRegion() {
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
		re.NoError(suite.regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
			Header: newHeader(),
			Region: r,
			Leader: peers[0],
		}))
	}
	for i := range 20 {
		testutil.Eventually(re, func() bool {
			r, err := suite.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			re.NoError(err)
			if i > 0 && i < regionLen {
				if r == nil {
					return false
				}
				return reflect.DeepEqual(peers[0], r.Leader) &&
					reflect.DeepEqual(regions[i-1], r.Meta)
			}
			return r == nil
		})
	}
}

func (suite *queryRegionClientSuite) TestGetRegionByID() {
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
	re.NoError(suite.regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}))

	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegionByID(context.Background(), regionID)
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader)
	})
}

func (suite *queryRegionClientSuite) TestGetRegionConcurrently() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	suite.dispatchConcurrentRequests(ctx, re, &wg)
	wg.Wait()
}

func (suite *queryRegionClientSuite) dispatchConcurrentRequests(ctx context.Context, re *require.Assertions, wg *sync.WaitGroup) {
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
				r                   *pd.Region
				err                 error
				seed                = rand.IntN(100)
				allowFollowerHandle = seed%2 == 0
			)
			time.Sleep(time.Duration(seed) * time.Millisecond)
			switch seed % 3 {
			case 0:
				region := regions[0]
				testutil.Eventually(re, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetRegion(ctx, region.GetStartKey(), pd.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetRegion(ctx, region.GetStartKey())
					}
					if err != nil {
						if strings.Contains(err.Error(), "region not found") {
							return false
						}
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
						r, err = suite.client.GetPrevRegion(ctx, regions[1].GetStartKey(), pd.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetPrevRegion(ctx, regions[1].GetStartKey())
					}
					if err != nil {
						if strings.Contains(err.Error(), "region not found") {
							return false
						}
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
						r, err = suite.client.GetRegionByID(ctx, region.GetId(), pd.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetRegionByID(ctx, region.GetId())
					}
					if err != nil {
						if strings.Contains(err.Error(), "region not found") {
							return false
						}
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

func (suite *queryRegionClientSuite) TestDynamicallyEnableQueryRegion() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	for _, enabled := range []bool{!suite.queryRegionEnabled, suite.queryRegionEnabled} {
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		wg.Wait()
		re.NoError(suite.client.UpdateOption(pd.EnableQueryRegion, enabled))
	}
}

func (suite *queryRegionClientSuite) TestConcurrentlyEnableQueryRegion() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	for _, enabled := range []bool{!suite.queryRegionEnabled, suite.queryRegionEnabled} {
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		re.NoError(suite.client.UpdateOption(pd.EnableQueryRegion, enabled))
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
	wg.Wait()
}

func (suite *queryRegionClientSuite) TestConcurrentlyEnableFollowerHandle() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

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
	for _, enabled := range []bool{false, true} {
		suite.dispatchConcurrentRequests(ctx, re, &wg)
		re.NoError(suite.client.UpdateOption(pd.EnableFollowerHandle, enabled))
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
	wg.Wait()
}

func TestQueryRegionClientHeaderError(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	srv := cluster.GetLeaderServer().GetServer()

	client := setupCli(ctx, re, srv.GetEndpoints(), pd.WithEnableQueryRegion())
	defer client.Close()

	r, err := client.GetRegion(ctx, []byte("a"))
	re.ErrorContains(err, pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
	re.Nil(r)
}
