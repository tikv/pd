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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/routerpb"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
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
		{name: "empty mode suppresses legacy v2", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "", "safe_point_version": "v2"}}, want: false},
		{name: "unknown mode suppresses legacy v2", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "invalid", "safe_point_version": "v2"}}, want: false},
		{name: "native mode overrides legacy v1", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"gc_management_type": "keyspace_level", "safe_point_version": "v1"}}, want: true},
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

func TestIsRetryableGetAllStoresError(t *testing.T) {
	re := require.New(t)

	re.False(isRetryableGetAllStoresError(nil))
	re.True(isRetryableGetAllStoresError(errs.ErrClientGetProtoClient))
	re.True(isRetryableGetAllStoresError(perrors.WithStack(errs.ErrClientGetProtoClient)))
	re.True(isRetryableGetAllStoresError(fmt.Errorf("%s", errs.NotLeaderErr)))
	re.True(isRetryableGetAllStoresError(fmt.Errorf("%s", errs.NoLeaderErr)))
	re.True(isRetryableGetAllStoresError(fmt.Errorf("%s", errs.NotPrimaryErr)))
	re.True(isRetryableGetAllStoresError(fmt.Errorf("%s", errs.NotServedErr)))
	re.True(isRetryableGetAllStoresError(fmt.Errorf("%s", errs.MismatchCalleeIDErr)))
	re.True(isRetryableGetAllStoresError(status.Error(codes.Unavailable, "connection reset")))
	re.True(isRetryableGetAllStoresError(status.Error(codes.DeadlineExceeded, "rpc timeout")))
	re.True(isRetryableGetAllStoresError(errors.New((&pdpb.Error{
		Type:    pdpb.ErrorType_UNKNOWN,
		Message: "PD server is not leader",
	}).String())))

	re.False(isRetryableGetAllStoresError(status.Error(codes.InvalidArgument, "invalid request")))
	re.False(isRetryableGetAllStoresError(status.Error(codes.PermissionDenied, "denied")))
	re.False(isRetryableGetAllStoresError(errors.New("other error")))
	re.False(isRetryableGetAllStoresError(errors.New((&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	}).String())))
}

type getAllStoresTestPDServer struct {
	pdpb.UnimplementedPDServer
	handler func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error)
}

func (s *getAllStoresTestPDServer) GetAllStores(
	ctx context.Context,
	req *pdpb.GetAllStoresRequest,
) (*pdpb.GetAllStoresResponse, error) {
	return s.handler(ctx, req)
}

type getAllStoresTestRouterServer struct {
	routerpb.UnimplementedRouterServer
	handler func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error)
}

func (s *getAllStoresTestRouterServer) GetAllStores(
	ctx context.Context,
	req *pdpb.GetAllStoresRequest,
) (*pdpb.GetAllStoresResponse, error) {
	return s.handler(ctx, req)
}

type getAllStoresServiceClient struct {
	sd.ServiceClient
	conn     *grpc.ClientConn
	isLeader bool
}

func (c *getAllStoresServiceClient) GetClientConn() *grpc.ClientConn { return c.conn }
func (*getAllStoresServiceClient) BuildGRPCTargetContext(ctx context.Context, _ bool) context.Context {
	return ctx
}
func (c *getAllStoresServiceClient) IsConnectedToLeader() bool { return c.isLeader }
func (c *getAllStoresServiceClient) Available() bool           { return c.conn != nil }
func (c *getAllStoresServiceClient) NeedRetry(pdErr *pdpb.Error, err error) bool {
	if c.isLeader {
		return false
	}
	return err != nil || pdErr != nil
}

type getAllStoresServiceDiscovery struct {
	*testServiceDiscovery
	mu              sync.RWMutex
	serviceClient   sd.ServiceClient
	onCheckMemberCh func(checkCount int)
	checks          atomic.Int32
}

func (d *getAllStoresServiceDiscovery) GetServiceClient() sd.ServiceClient {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.serviceClient
}

func (d *getAllStoresServiceDiscovery) setServiceClient(sc sd.ServiceClient) {
	d.mu.Lock()
	d.serviceClient = sc
	d.mu.Unlock()
}

func (d *getAllStoresServiceDiscovery) ScheduleCheckMemberChanged() {
	n := int(d.checks.Add(1))
	if d.onCheckMemberCh != nil {
		d.onCheckMemberCh(n)
	}
}

func startGetAllStoresBufconnServer(
	t *testing.T,
	register func(*grpc.Server),
) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveErr; !errors.Is(err, grpc.ErrServerStopped) {
			require.NoError(t, err)
		}
		require.NoError(t, listener.Close())
	})
	conn, err := grpc.NewClient("passthrough:///get-all-stores-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram().GetSampleCount()
}

func TestGetAllStoresRetryOnLeaderChange(t *testing.T) {
	expectedStores := []*metapb.Store{{Id: 1, Address: "127.0.0.1:20160"}}

	for _, tc := range []struct {
		name            string
		oldLeaderResult func() (*pdpb.GetAllStoresResponse, error)
	}{
		{
			name: "grpc not leader error",
			oldLeaderResult: func() (*pdpb.GetAllStoresResponse, error) {
				return nil, status.Error(codes.Unavailable, errs.NotLeaderErr)
			},
		},
		{
			name: "response header not leader error",
			oldLeaderResult: func() (*pdpb.GetAllStoresResponse, error) {
				return &pdpb.GetAllStoresResponse{
					Header: &pdpb.ResponseHeader{
						Error: &pdpb.Error{
							Type:    pdpb.ErrorType_UNKNOWN,
							Message: errs.NotLeaderErr,
						},
					},
				}, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			var oldCalls, newCalls atomic.Int32
			var gotExcludeTombstone atomic.Bool

			oldConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
				pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
					handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
						oldCalls.Add(1)
						return tc.oldLeaderResult()
					},
				})
			})
			newConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
				pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
					handler: func(_ context.Context, req *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
						newCalls.Add(1)
						gotExcludeTombstone.Store(req.GetExcludeTombstoneStores())
						return &pdpb.GetAllStoresResponse{
							Header: &pdpb.ResponseHeader{},
							Stores: expectedStores,
						}, nil
					},
				})
			})

			oldSC := &getAllStoresServiceClient{conn: oldConn, isLeader: true}
			newSC := &getAllStoresServiceClient{conn: newConn, isLeader: true}
			discovery := &getAllStoresServiceDiscovery{
				testServiceDiscovery: newTestServiceDiscovery("http://old-leader", oldConn),
				serviceClient:        oldSC,
			}
			// Switch to the new leader after the second refresh signal to cover multiple retries.
			discovery.onCheckMemberCh = func(checkCount int) {
				if checkCount >= 2 {
					discovery.setServiceClient(newSC)
				}
			}

			option := opt.NewOption()
			option.Timeout = 2 * time.Second
			c := &client{inner: &innerClient{serviceDiscovery: discovery, option: option}}

			cmdBefore := histogramSampleCount(t, metrics.CmdDurationGetAllStores)
			failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)

			stores, err := c.GetAllStores(t.Context(), opt.WithExcludeTombstone())
			re.NoError(err)
			re.Len(stores, 1)
			re.Equal(expectedStores[0].GetId(), stores[0].GetId())
			re.True(gotExcludeTombstone.Load())
			re.Equal(int32(2), oldCalls.Load())
			re.Equal(int32(1), newCalls.Load())
			re.Equal(int32(2), discovery.checks.Load())
			re.Equal(cmdBefore+1, histogramSampleCount(t, metrics.CmdDurationGetAllStores))
			re.Equal(failedBefore, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
		})
	}
}

func TestGetAllStoresRetryOnTransientNilServiceClient(t *testing.T) {
	re := require.New(t)
	expectedStores := []*metapb.Store{{Id: 2, Address: "127.0.0.1:20161"}}

	leaderConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
		pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
			handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
				return &pdpb.GetAllStoresResponse{
					Header: &pdpb.ResponseHeader{},
					Stores: expectedStores,
				}, nil
			},
		})
	})
	leaderSC := &getAllStoresServiceClient{conn: leaderConn, isLeader: true}
	discovery := &getAllStoresServiceDiscovery{
		testServiceDiscovery: newTestServiceDiscovery("http://leader", leaderConn),
		serviceClient:        nil,
	}
	discovery.onCheckMemberCh = func(checkCount int) {
		if checkCount >= 1 {
			discovery.setServiceClient(leaderSC)
		}
	}

	option := opt.NewOption()
	option.Timeout = 2 * time.Second
	c := &client{inner: &innerClient{serviceDiscovery: discovery, option: option}}

	failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)
	stores, err := c.GetAllStores(t.Context())
	re.NoError(err)
	re.Len(stores, 1)
	re.Equal(expectedStores[0].GetId(), stores[0].GetId())
	re.Equal(int32(1), discovery.checks.Load())
	re.Equal(failedBefore, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
}

func TestGetAllStoresRouterFallback(t *testing.T) {
	expectedStores := []*metapb.Store{{Id: 3, Address: "127.0.0.1:20162"}}

	t.Run("falls back to PD leader immediately", func(t *testing.T) {
		re := require.New(t)
		var routerCalls, leaderCalls atomic.Int32

		routerConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			routerpb.RegisterRouterServer(s, &getAllStoresTestRouterServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					routerCalls.Add(1)
					return nil, status.Error(codes.Unavailable, "router unavailable")
				},
			})
		})
		leaderConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					leaderCalls.Add(1)
					return &pdpb.GetAllStoresResponse{
						Header: &pdpb.ResponseHeader{},
						Stores: expectedStores,
					}, nil
				},
			})
		})

		pdDiscovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://leader", leaderConn),
			serviceClient:        &getAllStoresServiceClient{conn: leaderConn, isLeader: true},
		}
		routerDiscovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://router", routerConn),
			serviceClient:        &getAllStoresServiceClient{conn: routerConn, isLeader: false},
		}

		option := opt.NewOption()
		option.Timeout = 2 * time.Second
		c := &client{
			inner: &innerClient{
				serviceDiscovery: pdDiscovery,
				option:           option,
			},
		}
		c.inner.msDiscovery = routerDiscovery

		failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)
		stores, err := c.GetAllStores(t.Context(), opt.WithAllowRouterServiceHandleStoreRequest())
		re.NoError(err)
		re.Len(stores, 1)
		re.Equal(expectedStores[0].GetId(), stores[0].GetId())
		re.Equal(int32(1), routerCalls.Load())
		re.Equal(int32(1), leaderCalls.Load())
		re.Equal(int32(0), pdDiscovery.checks.Load())
		re.Equal(failedBefore, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
	})

	t.Run("retries after fallback hits stale PD leader", func(t *testing.T) {
		re := require.New(t)
		var routerCalls, oldLeaderCalls, newLeaderCalls atomic.Int32

		routerConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			routerpb.RegisterRouterServer(s, &getAllStoresTestRouterServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					routerCalls.Add(1)
					return &pdpb.GetAllStoresResponse{
						Header: &pdpb.ResponseHeader{
							Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "router stale"},
						},
					}, nil
				},
			})
		})
		oldLeaderConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					oldLeaderCalls.Add(1)
					return nil, status.Error(codes.Unavailable, errs.NotLeaderErr)
				},
			})
		})
		newLeaderConn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					newLeaderCalls.Add(1)
					return &pdpb.GetAllStoresResponse{
						Header: &pdpb.ResponseHeader{},
						Stores: expectedStores,
					}, nil
				},
			})
		})

		newLeaderSC := &getAllStoresServiceClient{conn: newLeaderConn, isLeader: true}
		pdDiscovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://old-leader", oldLeaderConn),
			serviceClient:        &getAllStoresServiceClient{conn: oldLeaderConn, isLeader: true},
		}
		pdDiscovery.onCheckMemberCh = func(checkCount int) {
			if checkCount >= 1 {
				pdDiscovery.setServiceClient(newLeaderSC)
			}
		}
		routerDiscovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://router", routerConn),
			serviceClient:        &getAllStoresServiceClient{conn: routerConn, isLeader: false},
		}

		option := opt.NewOption()
		option.Timeout = 2 * time.Second
		c := &client{
			inner: &innerClient{
				serviceDiscovery: pdDiscovery,
				option:           option,
			},
		}
		c.inner.msDiscovery = routerDiscovery

		failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)
		stores, err := c.GetAllStores(t.Context(), opt.WithAllowRouterServiceHandleStoreRequest())
		re.NoError(err)
		re.Len(stores, 1)
		re.Equal(expectedStores[0].GetId(), stores[0].GetId())
		re.Equal(int32(2), routerCalls.Load())
		re.Equal(int32(1), oldLeaderCalls.Load())
		re.Equal(int32(1), newLeaderCalls.Load())
		re.Equal(int32(1), pdDiscovery.checks.Load())
		re.Equal(failedBefore, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
	})
}

func TestGetAllStoresNonRetryableErrorReturnsImmediately(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     func() (*pdpb.GetAllStoresResponse, error)
		wantErrMsg string
		wantChecks int32
	}{
		{
			name: "grpc invalid argument",
			result: func() (*pdpb.GetAllStoresResponse, error) {
				return nil, status.Error(codes.InvalidArgument, "invalid store filter")
			},
			wantErrMsg: "invalid store filter",
			wantChecks: 1, // respForErr triggers ScheduleCheckMemberChanged on gRPC errors.
		},
		{
			name: "header not bootstrapped",
			result: func() (*pdpb.GetAllStoresResponse, error) {
				return &pdpb.GetAllStoresResponse{
					Header: &pdpb.ResponseHeader{
						Error: &pdpb.Error{
							Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
							Message: "cluster is not bootstrapped",
						},
					},
				}, nil
			},
			wantErrMsg: "cluster is not bootstrapped",
			wantChecks: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			var calls atomic.Int32
			conn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
				pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
					handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
						calls.Add(1)
						return tc.result()
					},
				})
			})
			discovery := &getAllStoresServiceDiscovery{
				testServiceDiscovery: newTestServiceDiscovery("http://leader", conn),
				serviceClient:        &getAllStoresServiceClient{conn: conn, isLeader: true},
			}
			option := opt.NewOption()
			option.Timeout = 2 * time.Second
			c := &client{inner: &innerClient{serviceDiscovery: discovery, option: option}}

			cmdBefore := histogramSampleCount(t, metrics.CmdDurationGetAllStores)
			failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)

			stores, err := c.GetAllStores(t.Context())
			re.Nil(stores)
			re.ErrorContains(err, tc.wantErrMsg)
			re.Equal(int32(1), calls.Load())
			re.Equal(tc.wantChecks, discovery.checks.Load())
			re.Equal(cmdBefore+1, histogramSampleCount(t, metrics.CmdDurationGetAllStores))
			re.Equal(failedBefore+1, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
		})
	}
}

func TestGetAllStoresContextTimeoutAndCancellation(t *testing.T) {
	t.Run("request timeout aborts retries on DeadlineExceeded and records failure once", func(t *testing.T) {
		re := require.New(t)
		var calls atomic.Int32
		conn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					calls.Add(1)
					return nil, status.Error(codes.DeadlineExceeded, "upstream deadline exceeded")
				},
			})
		})
		discovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://leader", conn),
			serviceClient:        &getAllStoresServiceClient{conn: conn, isLeader: true},
		}
		option := opt.NewOption()
		option.Timeout = 180 * time.Millisecond
		c := &client{inner: &innerClient{serviceDiscovery: discovery, option: option}}

		cmdBefore := histogramSampleCount(t, metrics.CmdDurationGetAllStores)
		failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)

		start := time.Now()
		stores, err := c.GetAllStores(t.Context())
		elapsed := time.Since(start)

		re.Nil(stores)
		re.Error(err)
		re.Equal(codes.DeadlineExceeded, status.Code(err))
		re.GreaterOrEqual(calls.Load(), int32(2))
		re.Less(elapsed, time.Second)
		re.Equal(cmdBefore+1, histogramSampleCount(t, metrics.CmdDurationGetAllStores))
		re.Equal(failedBefore+1, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
	})

	t.Run("expired outer context does not retry after DeadlineExceeded", func(t *testing.T) {
		re := require.New(t)
		var calls atomic.Int32
		conn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
				handler: func(reqCtx context.Context, _ *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					calls.Add(1)
					<-reqCtx.Done()
					return nil, status.FromContextError(reqCtx.Err()).Err()
				},
			})
		})
		discovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://leader", conn),
			serviceClient:        &getAllStoresServiceClient{conn: conn, isLeader: true},
		}
		option := opt.NewOption()
		option.Timeout = 80 * time.Millisecond
		c := &client{inner: &innerClient{serviceDiscovery: discovery, option: option}}

		failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)
		start := time.Now()
		stores, err := c.GetAllStores(t.Context())
		elapsed := time.Since(start)

		re.Nil(stores)
		re.Error(err)
		re.Equal(codes.DeadlineExceeded, status.Code(err))
		re.Equal(int32(1), calls.Load())
		re.Less(elapsed, 500*time.Millisecond)
		re.Equal(failedBefore+1, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
	})

	t.Run("caller cancellation aborts backoff wait promptly", func(t *testing.T) {
		re := require.New(t)
		var calls atomic.Int32
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		conn := startGetAllStoresBufconnServer(t, func(s *grpc.Server) {
			pdpb.RegisterPDServer(s, &getAllStoresTestPDServer{
				handler: func(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
					if calls.Add(1) == 1 {
						time.AfterFunc(15*time.Millisecond, cancel)
					}
					return nil, status.Error(codes.Unavailable, errs.NotLeaderErr)
				},
			})
		})
		discovery := &getAllStoresServiceDiscovery{
			testServiceDiscovery: newTestServiceDiscovery("http://leader", conn),
			serviceClient:        &getAllStoresServiceClient{conn: conn, isLeader: true},
		}
		option := opt.NewOption()
		option.Timeout = 5 * time.Second
		c := &client{inner: &innerClient{serviceDiscovery: discovery, option: option}}

		cmdBefore := histogramSampleCount(t, metrics.CmdDurationGetAllStores)
		failedBefore := histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores)

		start := time.Now()
		stores, err := c.GetAllStores(ctx)
		elapsed := time.Since(start)

		re.Nil(stores)
		re.Error(err)
		re.Equal(int32(1), calls.Load())
		re.Less(elapsed, 500*time.Millisecond)
		re.Equal(cmdBefore+1, histogramSampleCount(t, metrics.CmdDurationGetAllStores))
		re.Equal(failedBefore+1, histogramSampleCount(t, metrics.CmdFailedDurationGetAllStores))
	})
}
