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

package grpcutil

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
)

type testService struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (*testService) EmptyCall(context.Context, *grpc_testing.Empty) (*grpc_testing.Empty, error) {
	return &grpc_testing.Empty{}, nil
}

func TestGRPCClientMetricsExportAfterOneRPC(t *testing.T) {
	EnableGRPCClientMetrics()

	metricsSrv := httptest.NewServer(promhttp.Handler())
	t.Cleanup(metricsSrv.Close)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	gs := grpc.NewServer()
	grpc_testing.RegisterTestServiceServer(gs, &testService{})
	go gs.Serve(l)
	t.Cleanup(gs.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := GetClientConn(ctx, "http://"+l.Addr().String(), nil)
	require.NoError(t, err)
	defer conn.Close()

	cli := grpc_testing.NewTestServiceClient(conn)
	_, err = cli.EmptyCall(ctx, &grpc_testing.Empty{})
	require.NoError(t, err)

	resp, err := http.Get(metricsSrv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	metrics := string(body)

	require.Contains(t, metrics, "grpc_client_started_total")
	require.Contains(t, metrics, "grpc_client_handling_seconds")
	require.True(t, strings.Contains(metrics, "grpc_client_handled_total") || strings.Contains(metrics, "grpc_client_handled_latency_seconds"),
		"expected grpc_client_* metrics families to be exported after one RPC")
}
