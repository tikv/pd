// Copyright 2016 TiKV Project Authors.
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

package pd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/tsoutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestTSLessEqual(t *testing.T) {
	re := require.New(t)
	re.True(tsoutil.TSLessEqual(9, 9, 9, 9))
	re.True(tsoutil.TSLessEqual(8, 9, 9, 8))
	re.False(tsoutil.TSLessEqual(9, 8, 8, 9))
	re.False(tsoutil.TSLessEqual(9, 8, 9, 6))
	re.True(tsoutil.TSLessEqual(9, 6, 9, 8))
}

const testClientURL = "tmp://test.url:5255"

func TestClientCtx(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	cli, err := NewClientWithContext(ctx, caller.TestComponent,
		[]string{testClientURL}, SecurityOption{})
	re.Error(err)
	defer cli.Close()
	re.Less(time.Since(start), time.Second*5)
}

func TestClientWithRetry(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	cli, err := NewClientWithContext(context.TODO(), caller.TestComponent,
		[]string{testClientURL}, SecurityOption{}, opt.WithMaxErrorRetry(5))
	re.Error(err)
	defer cli.Close()
	re.Less(time.Since(start), time.Second*10)
}

func TestIsRetryableGetTSError(t *testing.T) {
	re := require.New(t)

	re.True(isRetryableGetTSError(fmt.Errorf("%s", errs.NotLeaderErr)))
	re.True(isRetryableGetTSError(fmt.Errorf("%s", errs.MismatchCalleeIDErr)))
	re.True(isRetryableGetTSError(perrors.WithStack(errs.ErrClientTSOStreamClosed)))
	re.False(isRetryableGetTSError(errors.New("other error")))
}

func TestRoundUpDurationToSeconds(t *testing.T) {
	re := require.New(t)
	re.Equal(int64(0), roundUpDurationToSeconds(0))
	re.Equal(int64(1), roundUpDurationToSeconds(time.Millisecond))
	re.Equal(int64(1), roundUpDurationToSeconds(time.Second))
	re.Equal(int64(3600), roundUpDurationToSeconds(time.Hour))
	re.Equal(int64(3601), roundUpDurationToSeconds(time.Hour+1))
	// time.Duration(9223372036854775807) -> 9223372036.854... secs -(round up)-> 9223372037
	re.Equal(int64(9223372037), roundUpDurationToSeconds(math.MaxInt64-1))
	re.Equal(int64(math.MaxInt64), roundUpDurationToSeconds(math.MaxInt64))
}

func TestSaturatingStdDurationFromSeconds(t *testing.T) {
	re := require.New(t)

	re.Equal(time.Second*2, saturatingStdDurationFromSeconds(2))
	re.Equal(time.Duration(0), saturatingStdDurationFromSeconds(-2))
	re.Equal(time.Hour, saturatingStdDurationFromSeconds(3600))
	re.Equal(time.Duration(math.MaxInt64), saturatingStdDurationFromSeconds(1<<34))
	re.Equal((1<<33)*time.Second, saturatingStdDurationFromSeconds(1<<33))
	re.Equal(9223372036*time.Second, saturatingStdDurationFromSeconds(9223372036))
	re.Equal(time.Duration(math.MaxInt64), saturatingStdDurationFromSeconds(9223372037))
	re.Equal(time.Duration(math.MaxInt64), saturatingStdDurationFromSeconds(math.MaxInt64))
}

func TestPBToGCStateWithGlobalGCBarriers(t *testing.T) {
	reqStartTime := time.Now()
	base := &pdpb.GCState{
		TxnSafePoint: 10,
		GcSafePoint:  9,
	}

	absent := pbToGCStateWithGlobalGCBarriers(
		base,
		nil,
		reqStartTime,
		true,
	)
	require.False(t, absent.HasGlobalGCBarriers())
	_, err := absent.GetGlobalGCBarriers()
	require.Error(t, err)

	empty := pbToGCStateWithGlobalGCBarriers(
		base,
		&pdpb.GlobalGCBarriersInfo{},
		reqStartTime,
		true,
	)
	require.True(t, empty.HasGlobalGCBarriers())
	barriers, err := empty.GetGlobalGCBarriers()
	require.NoError(t, err)
	require.Empty(t, barriers)

	nonEmpty := pbToGCStateWithGlobalGCBarriers(
		base,
		&pdpb.GlobalGCBarriersInfo{
			Barriers: []*pdpb.GlobalGCBarrierInfo{
				{
					BarrierId:  "backup",
					BarrierTs:  20,
					TtlSeconds: 60,
				},
			},
		},
		reqStartTime,
		true,
	)
	require.True(t, nonEmpty.HasGlobalGCBarriers())
	barriers, err = nonEmpty.GetGlobalGCBarriers()
	require.NoError(t, err)
	require.Equal(t, "backup", barriers[0].BarrierID)
	require.Equal(t, uint64(20), barriers[0].BarrierTS)
	require.Equal(t, time.Minute, barriers[0].TTL)
}

func TestIsKeyspaceUsingKeyspaceLevelGC(t *testing.T) {
	tests := []struct {
		name string
		meta *keyspacepb.KeyspaceMeta
		want bool
	}{
		{name: "nil metadata", meta: nil, want: false},
		{name: "nil config", meta: &keyspacepb.KeyspaceMeta{}, want: false},
		{name: "empty config", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{}}, want: false},
		{name: "native keyspace level GC", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "keyspace_level"}}, want: true},
		{name: "unified GC", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "unified"}}, want: false},
		{name: "unified GC overrides safe point version v2", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "unified", "safe_point_version": "v2"}}, want: false},
		{name: "invalid GC management type", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "111111"}}, want: false},
		{name: "safe point version v2", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": "v2"}}, want: true},
		{name: "uppercase safe point version", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": "V2"}}, want: false},
		{name: "padded safe point version", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": " v2 "}}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			meta := test.meta

			// When
			got := IsKeyspaceUsingKeyspaceLevelGC(meta)

			// Then
			require.Equal(t, test.want, got)
		})
	}
}

func TestIsQueryRegionUnimplemented(t *testing.T) {
	re := require.New(t)
	unimplementedErr := status.Error(codes.Unimplemented, "unknown method QueryRegion for service pdpb.PD")
	re.True(isQueryRegionUnimplemented(unimplementedErr))
	// The router client wraps the stream error with a stack.
	re.True(isQueryRegionUnimplemented(perrors.WithStack(unimplementedErr)))
	re.False(isQueryRegionUnimplemented(nil))
	re.False(isQueryRegionUnimplemented(status.Error(codes.Unavailable, "unavailable")))
	re.False(isQueryRegionUnimplemented(perrors.WithStack(errs.ErrClientRouterConnectionTimeout)))
}

// testLegacyRegionServer serves the unary Region RPCs but does not implement
// QueryRegion, like a PD server that predates it.
type testLegacyRegionServer struct {
	pdpb.UnimplementedPDServer
	leader *pdpb.Member
	region *pdpb.GetRegionResponse

	getRegionCount     atomic.Int32
	getPrevRegionCount atomic.Int32
	getRegionByIDCount atomic.Int32
}

func (s *testLegacyRegionServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	return &pdpb.GetMembersResponse{
		Header:  &pdpb.ResponseHeader{},
		Members: []*pdpb.Member{s.leader},
		Leader:  s.leader,
	}, nil
}

func (s *testLegacyRegionServer) GetRegion(context.Context, *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	s.getRegionCount.Add(1)
	return s.region, nil
}

func (s *testLegacyRegionServer) GetPrevRegion(context.Context, *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	s.getPrevRegionCount.Add(1)
	return s.region, nil
}

func (s *testLegacyRegionServer) GetRegionByID(context.Context, *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	s.getRegionByIDCount.Add(1)
	return s.region, nil
}

func TestRegionQueriesFallBackWhenQueryRegionIsUnimplemented(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	url := "http://" + listener.Addr().String()
	regionServer := &testLegacyRegionServer{
		leader: &pdpb.Member{MemberId: 1, ClientUrls: []string{url}},
		region: &pdpb.GetRegionResponse{
			Header: &pdpb.ResponseHeader{},
			Region: &metapb.Region{Id: 2, StartKey: []byte("a"), EndKey: []byte("z")},
			Leader: &metapb.Peer{Id: 3, StoreId: 4},
		},
	}
	server := grpc.NewServer()
	pdpb.RegisterPDServer(server, regionServer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	discovery := sd.NewDefaultServiceDiscovery(ctx, cancel, []string{url}, nil)
	defer discovery.Close()
	re.NoError(discovery.CheckMemberChanged())
	option := opt.NewOption()
	option.InitMetrics = false
	routerClient := router.NewClient(ctx, discovery, nil, option)
	defer routerClient.Close()
	cli := &client{
		inner: &innerClient{
			serviceDiscovery: discovery,
			routerClient:     routerClient,
			ctx:              ctx,
			option:           option,
		},
	}

	region, err := cli.GetRegion(ctx, []byte("b"))
	re.NoError(err)
	re.Equal(uint64(2), region.Meta.GetId())
	re.Equal(int32(1), regionServer.getRegionCount.Load())
	region, err = cli.GetPrevRegion(ctx, []byte("z"))
	re.NoError(err)
	re.Equal(uint64(2), region.Meta.GetId())
	re.Equal(int32(1), regionServer.getPrevRegionCount.Load())
	region, err = cli.GetRegionByID(ctx, 2)
	re.NoError(err)
	re.Equal(uint64(2), region.Meta.GetId())
	re.Equal(int32(1), regionServer.getRegionByIDCount.Load())
}
