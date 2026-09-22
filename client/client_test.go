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
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/tsoutil"
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

// attributionServer checks the actual QueryRegion wire header against the
// component encoded in each test query, including batches of different kinds.
type attributionServer struct {
	pdpb.UnimplementedPDServer
}

func (*attributionServer) QueryRegion(stream pdpb.PD_QueryRegionServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetHeader().GetClusterId() != 0 || req.GetHeader().GetCallerId() != string(caller.GetCallerID()) {
			return errors.New("incorrect cluster or caller ID")
		}
		component := req.GetHeader().GetCallerComponent()
		resp := &pdpb.QueryRegionResponse{RegionsById: make(map[uint64]*pdpb.RegionResponse)}
		for _, keys := range [][][]byte{req.GetKeys(), req.GetPrevKeys()} {
			for _, key := range keys {
				if string(key) != component {
					return errors.New("incorrect key query component")
				}
			}
		}
		for _, id := range req.GetIds() {
			expected := strconv.FormatUint(id, 10)
			if id == 0 {
				expected = ""
			}
			if component != expected {
				return errors.New("incorrect ID query component")
			}
		}
		for range req.GetKeys() {
			resp.KeyIdMap = append(resp.KeyIdMap, 42)
		}
		for range req.GetPrevKeys() {
			resp.PrevKeyIdMap = append(resp.PrevKeyIdMap, 42)
		}
		for _, id := range append(req.GetIds(), 42) {
			resp.RegionsById[id] = &pdpb.RegionResponse{Region: &metapb.Region{Id: id}, Leader: &metapb.Peer{Id: 1}}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func TestRouterCallerAttribution(t *testing.T) {
	re := require.New(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	server := grpc.NewServer()
	pdpb.RegisterPDServer(server, &attributionServer{})
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		re.NoError(<-serveErr)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr := "http://" + listener.Addr().String()
	conn, err := grpcutil.GetClientConn(ctx, addr, nil)
	re.NoError(err)
	t.Cleanup(func() { re.NoError(conn.Close()) })
	discovery := newTestServiceDiscovery(addr, conn)
	option := opt.NewOption()
	routerClient := router.NewClient(ctx, discovery, nil, option)
	t.Cleanup(routerClient.Close)
	cli := &client{callerComponent: "parent", inner: &innerClient{option: option}}
	cli.inner.routerClient = routerClient

	// Shared wrappers exercise all public APIs concurrently and repeatedly,
	// including an empty component after requests have returned to the pool.
	var wg sync.WaitGroup
	results := make(chan error, 120)
	for id := range uint64(4) {
		component := caller.Component(strconv.FormatUint(id, 10))
		if id == 0 {
			component = ""
		}
		wrapped := cli.WithCallerComponent(component)
		wg.Go(func() {
			for range 10 {
				for method := range 3 {
					var region *router.Region
					var err error
					// An outer context must not override the wrapper's identity.
					requestCtx := caller.WithComponent(ctx, "outer")
					switch method {
					case 0:
						region, err = wrapped.GetRegion(requestCtx, []byte(component))
					case 1:
						region, err = wrapped.GetPrevRegion(requestCtx, []byte(component))
					case 2:
						region, err = wrapped.GetRegionByID(requestCtx, id)
					}
					expectedID := uint64(42)
					if method == 2 {
						expectedID = id
					}
					if err == nil && (region == nil || region.Meta.GetId() != expectedID) {
						err = errors.New("incorrect region result")
					}
					results <- err
				}
			}
		})
	}
	wg.Wait()
	close(results)
	for err := range results {
		re.NoError(err)
	}
}
