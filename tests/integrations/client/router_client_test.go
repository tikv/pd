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
	"fmt"
	"math"
	"math/rand/v2"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	conn            *grpc.ClientConn
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
	suite.grpcPDClient, suite.conn = testutil.MustNewGrpcClient(re, leader.GetAddr())
	suite.client = setupCli(suite.ctx, re, endpoints,
		opt.WithEnableRouterClient(suite.routerClientEnabled),
		opt.WithEnableFollowerHandle(true))

	suite.regionHeartbeat, err = suite.grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	suite.reportBucket, err = suite.grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)
	cluster := suite.cluster.GetLeaderServer().GetRaftCluster()
	re.NotNil(cluster)
	cluster.GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)
}

func newTestBuckets(regionID uint64, keys ...[]byte) *metapb.Buckets {
	return &metapb.Buckets{
		RegionId:   regionID,
		Version:    1,
		Keys:       keys,
		PeriodInMs: 2000,
		Stats: &metapb.BucketStats{
			ReadBytes:  []uint64{1},
			ReadKeys:   []uint64{1},
			ReadQps:    []uint64{1},
			WriteBytes: []uint64{1},
			WriteKeys:  []uint64{1},
			WriteQps:   []uint64{1},
		},
	}
}

// TearDownSuite cleans up the test cluster and client.
func (suite *routerClientSuite) TearDownSuite() {
	suite.client.Close()
	_ = suite.regionHeartbeat.CloseSend()
	_ = suite.reportBucket.CloseSend()
	if suite.conn != nil {
		_ = suite.conn.Close()
	}
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
	r, err := suite.client.GetRegion(context.Background(), nil)
	re.NoError(err)
	re.NotNil(r)
	re.Equal(regionID, r.Meta.GetId())
	breq := &pdpb.ReportBucketsRequest{
		Header:  newHeader(),
		Buckets: newTestBuckets(regionID, []byte("a"), []byte("z")),
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
				// In this case, the region must not be nil.
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

	r, err := suite.client.GetRegionByID(context.Background(), 0)
	re.NoError(err)
	re.Nil(r)

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
	as := assert.New(suite.T())
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
				seed                = rand.IntN(100)
				allowFollowerHandle = seed%2 == 0
			)
			// Randomly sleep to avoid the concurrent requests to be dispatched at the same time.
			time.Sleep(time.Duration(seed) * time.Millisecond)
			switch seed % 3 {
			case 0:
				region := regions[0]
				if !testutil.EventuallyWithAssert(as, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetRegion(ctx, region.GetStartKey(), opt.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetRegion(ctx, region.GetStartKey())
					}
					if err != nil {
						if strings.Contains(err.Error(), "region not found") {
							return false
						}
						as.Contains(err.Error(), context.Canceled.Error())
					}
					if r == nil {
						return false
					}
					return reflect.DeepEqual(region, r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				}) {
					return
				}
			case 1:
				if !testutil.EventuallyWithAssert(as, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetPrevRegion(ctx, regions[1].GetStartKey(), opt.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetPrevRegion(ctx, regions[1].GetStartKey())
					}
					if err != nil {
						if strings.Contains(err.Error(), "region not found") {
							return false
						}
						as.Contains(err.Error(), context.Canceled.Error())
					}
					if r == nil {
						return false
					}
					return reflect.DeepEqual(regions[0], r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				}) {
					return
				}
			case 2:
				region := regions[0]
				if !testutil.EventuallyWithAssert(as, func() bool {
					if allowFollowerHandle {
						r, err = suite.client.GetRegionByID(ctx, region.GetId(), opt.WithAllowFollowerHandle())
					} else {
						r, err = suite.client.GetRegionByID(ctx, region.GetId())
					}
					if err != nil {
						if strings.Contains(err.Error(), "region not found") {
							return false
						}
						as.Contains(err.Error(), context.Canceled.Error())
					}
					if r == nil {
						return false
					}
					return reflect.DeepEqual(region, r.Meta) &&
						reflect.DeepEqual(peers[0], r.Leader) &&
						r.Buckets == nil
				}) {
					return
				}
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
	for _, enabled := range []bool{false, true} {
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

func (suite *routerClientSuite) TestQueryRegionFollowerFallbackMatchesUnarySemantics() {
	if !suite.routerClientEnabled {
		suite.T().Skip("QueryRegion is disabled")
	}
	re := suite.Require()
	unaryClient := setupCli(suite.ctx, re, suite.cluster.GetLeaderServer().GetServer().GetEndpoints(),
		opt.WithEnableRouterClient(false))
	defer unaryClient.Close()
	reportBucket, err := suite.grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)
	defer func() {
		re.NoError(reportBucket.CloseSend())
	}()

	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id:          regionID,
		StartKey:    []byte("follower-fallback-a"),
		EndKey:      []byte("follower-fallback-b"),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       peers,
	}
	re.NoError(suite.regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}))
	nextRegion := &metapb.Region{
		Id:          regionIDAllocator.alloc(),
		StartKey:    region.GetEndKey(),
		EndKey:      []byte("follower-fallback-c"),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       peers,
	}
	re.NoError(suite.regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: nextRegion,
		Leader: peers[0],
	}))
	testutil.Eventually(re, func() bool {
		cluster := suite.cluster.GetLeaderServer().GetRaftCluster()
		return cluster.GetRegion(region.GetId()) != nil &&
			cluster.GetRegion(nextRegion.GetId()) != nil
	})
	buckets := newTestBuckets(regionID, region.GetStartKey(), region.GetEndKey())
	re.NoError(reportBucket.Send(&pdpb.ReportBucketsRequest{
		Header:  newHeader(),
		Buckets: buckets,
	}))
	testutil.Eventually(re, func() bool {
		got := suite.cluster.GetLeaderServer().GetRaftCluster().GetRegion(regionID)
		return got != nil && reflect.DeepEqual(buckets, got.GetBuckets())
	})

	follower := suite.cluster.GetServer(suite.cluster.GetFollower())
	re.NotNil(follower)
	// Query a running syncer so the follower returns a successful cache miss.
	testutil.Eventually(re, func() bool {
		cluster := follower.GetServer().DirectlyGetRaftCluster()
		return cluster.GetRegionSyncer().IsRunning() &&
			cluster.GetRegion(regionID) != nil && cluster.GetRegion(nextRegion.GetId()) != nil
	})
	re.NoError(failpoint.Enable(
		"github.com/tikv/pd/client/clients/router/forceUseFollower",
		fmt.Sprintf("return(%q)", follower.GetAddr()),
	))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/router/forceUseFollower"))
	}()
	getRegion := func(client pd.Client, options ...opt.GetRegionOption) (*router.Region, error) {
		return client.GetRegion(context.Background(), region.GetStartKey(), options...)
	}
	getPrevRegion := func(client pd.Client, options ...opt.GetRegionOption) (*router.Region, error) {
		return client.GetPrevRegion(context.Background(), nextRegion.GetStartKey(), options...)
	}
	getRegionByID := func(client pd.Client, options ...opt.GetRegionOption) (*router.Region, error) {
		return client.GetRegionByID(context.Background(), region.GetId(), options...)
	}
	queries := []struct {
		name string
		get  func(pd.Client, ...opt.GetRegionOption) (*router.Region, error)
	}{
		{name: "GetRegion", get: getRegion},
		{name: "GetPrevRegion", get: getPrevRegion},
		{name: "GetRegionByID", get: getRegionByID},
	}
	leaderURL, err := url.Parse(suite.cluster.GetLeaderServer().GetAddr())
	re.NoError(err)
	followerURL, err := url.Parse(follower.GetAddr())
	re.NoError(err)
	notFound := &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND}
	notBootstrapped := &pdpb.Error{Type: pdpb.ErrorType_NOT_BOOTSTRAPPED}
	unknown := &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN}
	for _, scenario := range []struct {
		name                           string
		allowFollower                  bool
		followerMissing, leaderMissing bool
		followerError, leaderError     *pdpb.Error
		leaderTransportError           bool
		wantAttempts                   []bool
	}{
		{name: "leader hit", wantAttempts: []bool{true}},
		{name: "follower hit", allowFollower: true, wantAttempts: []bool{false}},
		{name: "follower miss", allowFollower: true, followerMissing: true, wantAttempts: []bool{false, true}},
		{name: "leader miss", leaderMissing: true, wantAttempts: []bool{true}},
		{name: "both miss", allowFollower: true, followerMissing: true, leaderMissing: true, wantAttempts: []bool{false, true}},
		{name: "leader region not found", leaderError: notFound, wantAttempts: []bool{true}},
		{name: "leader not bootstrapped", leaderError: notBootstrapped, wantAttempts: []bool{true}},
		{name: "follower region not found", allowFollower: true, followerError: notFound, wantAttempts: []bool{false, true}},
		{name: "follower not bootstrapped", allowFollower: true, followerError: notBootstrapped, wantAttempts: []bool{false, true}},
		{name: "follower unknown error", allowFollower: true, followerError: unknown, wantAttempts: []bool{false, true}},
		{name: "miss then leader region not found", allowFollower: true, followerMissing: true, leaderError: notFound, wantAttempts: []bool{false, true}},
		{name: "miss then leader not bootstrapped", allowFollower: true, followerMissing: true, leaderError: notBootstrapped, wantAttempts: []bool{false, true}},
		{name: "both header errors", allowFollower: true, followerError: notFound, leaderError: unknown, wantAttempts: []bool{false, true}},
		{name: "leader transport error", leaderTransportError: true, wantAttempts: []bool{true}},
		{name: "miss then leader transport error", allowFollower: true, followerMissing: true, leaderTransportError: true, wantAttempts: []bool{false, true}},
	} {
		for _, query := range queries {
			for _, withBuckets := range []bool{false, true} {
				suite.Run(fmt.Sprintf("%s/%s/buckets=%t", scenario.name, query.name, withBuckets), func() {
					re := suite.Require()
					var unaryRegion *router.Region
					var unaryErr error
					for _, enabled := range []bool{false, true} {
						recorder := &regionRPCRecorder{
							leaderTarget: leaderURL.Host, followerTarget: followerURL.Host,
							followerOnly: scenario.allowFollower && !enabled,
							fault: func(isLeader bool, method string, request, response any) error {
								missing, headerErr := scenario.followerMissing, scenario.followerError
								if isLeader {
									missing, headerErr = scenario.leaderMissing, scenario.leaderError
									if scenario.leaderTransportError {
										return status.Error(codes.Unavailable, "injected leader receive error")
									}
								}
								replaceRegionResponse(method, request, response, isLeader, missing, headerErr)
								return nil
							},
						}
						client := setupCli(suite.ctx, re, suite.cluster.GetLeaderServer().GetServer().GetEndpoints(),
							opt.WithEnableRouterClient(enabled), opt.WithEnableFollowerHandle(true), opt.WithForwardingOption(false),
							opt.WithGRPCDialOptions(grpc.WithUnaryInterceptor(recorder.unary), grpc.WithStreamInterceptor(recorder.stream)))
						suite.T().Cleanup(client.Close)
						if recorder.followerOnly {
							testutil.Eventually(re, func() bool {
								clients := client.GetServiceDiscovery().GetAllServiceClients()
								if len(clients) != 3 {
									return false
								}
								for _, candidate := range clients {
									if candidate.Available() != (candidate.GetURL() == follower.GetAddr()) {
										return false
									}
								}
								return true
							})
						}
						options := []opt.GetRegionOption{opt.WithAllowPDLeaderOnly()}
						if scenario.allowFollower {
							options = []opt.GetRegionOption{opt.WithAllowFollowerHandle()}
						}
						if withBuckets {
							options = append(options, opt.WithBuckets())
						}
						got, err := query.get(client, options...)
						re.Equal(scenario.wantAttempts, recorder.attempts(), "router enabled=%t", enabled)
						switch {
						case scenario.leaderTransportError:
							re.Equal(codes.Unavailable, status.Code(err))
							re.Nil(got)
						case scenario.leaderError != nil:
							re.ErrorContains(err, scenario.leaderError.String())
							re.Nil(got)
						default:
							re.NoError(err)
							if scenario.leaderMissing {
								re.Nil(got)
							} else {
								re.NotNil(got)
								re.Equal(region, got.Meta)
								if withBuckets && scenario.wantAttempts[len(scenario.wantAttempts)-1] {
									re.Equal(buckets, got.Buckets)
								} else {
									re.Nil(got.Buckets)
								}
							}
						}
						if !enabled {
							unaryRegion, unaryErr = got, err
						} else {
							re.Equal(unaryRegion, got)
							if unaryErr != nil {
								re.EqualError(err, unaryErr.Error())
							}
						}
					}
				})
			}
		}
	}
	// Also exercise the real server's successful sparse-response path, without
	// client-side response replacement, against the authoritative unary result.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/queryRegionFollowerCacheMiss", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/queryRegionFollowerCacheMiss"))
	}()
	for _, query := range queries {
		unaryRegion, err := query.get(unaryClient, opt.WithAllowPDLeaderOnly(), opt.WithBuckets())
		re.NoError(err, query.name)
		queryRegion, err := query.get(suite.client, opt.WithAllowFollowerHandle(), opt.WithBuckets())
		re.NoError(err, query.name)
		re.Equal(unaryRegion, queryRegion, query.name)
		re.NotNil(queryRegion, query.name)
		re.Equal(region, queryRegion.Meta, query.name)
		re.Equal(buckets, queryRegion.Buckets, query.name)
	}
	// A follower miss is inconclusive, but a leader miss must remain a
	// successful nil result, including ID zero, just like the unary APIs.
	for _, id := range []uint64{0, regionIDAllocator.alloc(), math.MaxUint64} {
		unaryRegion, err := unaryClient.GetRegionByID(suite.ctx, id, opt.WithAllowPDLeaderOnly())
		re.NoError(err)
		re.Nil(unaryRegion)
		queryRegion, err := suite.client.GetRegionByID(suite.ctx, id, opt.WithAllowFollowerHandle())
		re.NoError(err)
		re.Equal(unaryRegion, queryRegion)
	}
}

func TestRouterClientHeaderError(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	srv := cluster.GetLeaderServer().GetServer()

	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("router=%t", enabled), func(t *testing.T) {
			re := require.New(t)
			client := setupCli(ctx, re, srv.GetEndpoints(), opt.WithEnableRouterClient(enabled))
			defer client.Close()

			r, err := client.GetRegion(ctx, []byte("a"))
			re.ErrorContains(err, pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
			re.Nil(r)
			r, err = client.GetPrevRegion(ctx, []byte("a"))
			re.ErrorContains(err, pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
			re.Nil(r)
			r, err = client.GetRegionByID(ctx, 0)
			re.ErrorContains(err, pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
			re.Nil(r)
		})
	}
}
