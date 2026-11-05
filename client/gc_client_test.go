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

package pd

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	sd "github.com/tikv/pd/client/servicediscovery"
)

type gcReadErrorServer struct {
	pdpb.UnimplementedPDServer
	rpcErr    error
	headerErr *pdpb.Error
}

func (s *gcReadErrorServer) GetGCState(context.Context, *pdpb.GetGCStateRequest) (*pdpb.GetGCStateResponse, error) {
	if s.rpcErr != nil {
		return nil, s.rpcErr
	}
	return &pdpb.GetGCStateResponse{
		Header:  &pdpb.ResponseHeader{Error: s.headerErr},
		GcState: &pdpb.GCState{KeyspaceScope: wrapKeyspaceScope(42), IsKeyspaceLevelGc: true},
	}, nil
}

func (s *gcReadErrorServer) GetAllKeyspacesGCStates(context.Context, *pdpb.GetAllKeyspacesGCStatesRequest) (*pdpb.GetAllKeyspacesGCStatesResponse, error) {
	if s.rpcErr != nil {
		return nil, s.rpcErr
	}
	return &pdpb.GetAllKeyspacesGCStatesResponse{Header: &pdpb.ResponseHeader{Error: s.headerErr}}, nil
}

type gcReadServiceClient struct {
	sd.ServiceClient
	conn *grpc.ClientConn
}

func (c *gcReadServiceClient) GetClientConn() *grpc.ClientConn { return c.conn }
func (*gcReadServiceClient) BuildGRPCTargetContext(ctx context.Context, _ bool) context.Context {
	return ctx
}

type gcReadServiceDiscovery struct {
	*testServiceDiscovery
	serviceClient sd.ServiceClient
	checks        int
}

func (d *gcReadServiceDiscovery) GetServiceClient() sd.ServiceClient { return d.serviceClient }
func (d *gcReadServiceDiscovery) ScheduleCheckMemberChanged()        { d.checks++ }

func TestGCReadErrorsScheduleMemberDiscovery(t *testing.T) {
	sampleCount := func(observer prometheus.Observer) uint64 {
		metric := &dto.Metric{}
		require.NoError(t, observer.(prometheus.Metric).Write(metric))
		return metric.GetHistogram().GetSampleCount()
	}
	for _, api := range []struct {
		name              string
		read              func(context.Context, gc.GCStatesClient) error
		cmdDuration       prometheus.Observer
		cmdFailedDuration prometheus.Observer
	}{
		{
			name: "GetGCState",
			read: func(ctx context.Context, c gc.GCStatesClient) error {
				_, err := c.GetGCState(ctx)
				return err
			},
			cmdDuration:       metrics.CmdDurationGetGCState,
			cmdFailedDuration: metrics.CmdFailedDurationGetGCState,
		},
		{
			name: "GetAllKeyspacesGCStates",
			read: func(ctx context.Context, c gc.GCStatesClient) error {
				_, err := c.GetAllKeyspacesGCStates(ctx)
				return err
			},
			cmdDuration:       metrics.CmdDurationGetAllKeyspacesGCStates,
			cmdFailedDuration: metrics.CmdFailedDurationGetAllKeyspacesGCStates,
		},
	} {
		t.Run(api.name, func(t *testing.T) {
			for _, testCase := range []struct {
				name       string
				server     *gcReadErrorServer
				wantCode   codes.Code
				wantChecks int
			}{
				{
					name:     "success",
					server:   &gcReadErrorServer{},
					wantCode: codes.OK,
				},
				{
					name:       "leader loss RPC error",
					server:     &gcReadErrorServer{rpcErr: status.Error(codes.Unavailable, "not leader")},
					wantCode:   codes.Unavailable,
					wantChecks: 1,
				},
				{
					name: "application header error",
					server: &gcReadErrorServer{headerErr: &pdpb.Error{
						Type: pdpb.ErrorType_UNKNOWN, Message: "gc state read failed",
					}},
					wantCode: codes.Unknown,
				},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					listener := bufconn.Listen(1 << 20)
					server := grpc.NewServer()
					pdpb.RegisterPDServer(server, testCase.server)
					serveErr := make(chan error, 1)
					go func() { serveErr <- server.Serve(listener) }()
					t.Cleanup(func() {
						server.Stop()
						if err := <-serveErr; !errors.Is(err, grpc.ErrServerStopped) {
							require.NoError(t, err)
						}
					})
					conn, err := grpc.NewClient("passthrough:///gc-read-test",
						grpc.WithTransportCredentials(insecure.NewCredentials()),
						grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
							return listener.DialContext(ctx)
						}))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, conn.Close()) })
					discovery := &gcReadServiceDiscovery{
						testServiceDiscovery: newTestServiceDiscovery("http://gc-read-test", conn),
						serviceClient:        &gcReadServiceClient{conn: conn},
					}
					c := &client{inner: &innerClient{serviceDiscovery: discovery, option: opt.NewOption()}}
					cmdCount := sampleCount(api.cmdDuration)
					failedCount := sampleCount(api.cmdFailedDuration)
					err = api.read(t.Context(), c.GetGCStatesClient(42))
					if testCase.wantCode == codes.OK {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
						failedCount++
					}
					require.Equal(t, testCase.wantCode, status.Code(err))
					if testCase.server.headerErr != nil {
						require.ErrorContains(t, err, "gc state read failed")
					}
					require.Equal(t, testCase.wantChecks, discovery.checks)
					require.Equal(t, cmdCount+1, sampleCount(api.cmdDuration), "each call records one command duration")
					require.Equal(t, failedCount, sampleCount(api.cmdFailedDuration), "only failed calls record a failed duration")
				})
			}
		})
	}
}

func TestPBToGCStatePreservesKeyspaceLevelGC(t *testing.T) {
	requestStart := time.Unix(100, 0)
	for _, testCase := range []struct {
		name              string
		isKeyspaceLevelGC bool
		excludeBarriers   bool
	}{
		{name: "keyspace-level-with-barriers", isKeyspaceLevelGC: true},
		{
			name:              "keyspace-level-without-barriers",
			isKeyspaceLevelGC: true,
			excludeBarriers:   true,
		},
		{name: "unified-with-barriers", isKeyspaceLevelGC: false},
		{
			name:              "unified-without-barriers",
			isKeyspaceLevelGC: false,
			excludeBarriers:   true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pbState := &pdpb.GCState{
				KeyspaceScope:     wrapKeyspaceScope(42),
				IsKeyspaceLevelGc: testCase.isKeyspaceLevelGC,
				TxnSafePoint:      100,
				GcSafePoint:       90,
				GcBarriers: []*pdpb.GCBarrierInfo{
					{BarrierId: "backup", BarrierTs: 95, TtlSeconds: 60},
				},
			}

			state := pbToGCState(
				pbState,
				requestStart,
				testCase.excludeBarriers,
			)
			require.Equal(t, testCase.isKeyspaceLevelGC,
				state.IsKeyspaceLevelGC)
			require.Equal(t, uint32(42), state.KeyspaceID)
			require.Equal(t, uint64(100), state.TxnSafePoint)
			require.Equal(t, uint64(90), state.GCSafePoint)
			require.Equal(t, !testCase.excludeBarriers,
				state.HasGCBarriers())
		})
	}
}

func TestPBToGCStateWithGlobalBarriersPreservesKeyspaceLevelGC(t *testing.T) {
	requestStart := time.Unix(100, 0)
	state := pbToGCStateWithGlobalGCBarriers(
		&pdpb.GCState{
			KeyspaceScope:     wrapKeyspaceScope(42),
			IsKeyspaceLevelGc: true,
			TxnSafePoint:      100,
			GcSafePoint:       90,
		},
		&pdpb.GlobalGCBarriersInfo{
			Barriers: []*pdpb.GlobalGCBarrierInfo{
				{BarrierId: "snapshot", BarrierTs: 95, TtlSeconds: 60},
			},
		},
		requestStart,
		true,
	)

	require.True(t, state.IsKeyspaceLevelGC)
	require.False(t, state.HasGCBarriers())
	require.True(t, state.HasGlobalGCBarriers())
	barriers, err := state.GetGlobalGCBarriers()
	require.NoError(t, err)
	require.Len(t, barriers, 1)
	require.Equal(t, "snapshot", barriers[0].BarrierID)
}
