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

package router

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/pkg/batch"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

// TestProcessRequestsClosesConnOnRejectedStream covers a pre-opened QueryRegion
// stream rejected by an endpoint which does not provide the PD service: Send
// returns io.EOF, and the connection must still be closed to be recreated.
func TestProcessRequestsClosesConnOnRejectedStream(t *testing.T) {
	re := require.New(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cc, err := grpcutil.GetClientConn(ctx, "http://"+lis.Addr().String(), nil,
		grpcutil.ExpectedServiceDialOptions("pdpb.PD")...)
	re.NoError(err)
	defer cc.Close()
	stream, err := pdpb.NewPDClient(cc).QueryRegion(ctx)
	re.NoError(err)
	// Wait for the stream to be rejected.
	re.Eventually(func() bool {
		return stream.Send(&pdpb.QueryRegionRequest{}) != nil
	}, 5*time.Second, 10*time.Millisecond)

	c := &Cli{
		svcDiscovery:    sd.NewMockServiceDiscovery(nil, nil),
		batchController: batch.NewController[*Request](1, requestFinisher(nil, nil), nil),
	}
	_, err = c.processRequestsInner(stream.Send, stream.Recv, false, false)
	re.Equal(codes.Unimplemented, status.Code(err))
	re.Equal(connectivity.Shutdown, cc.GetState())
}
