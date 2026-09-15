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

package server

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
)

type tokenProxyTestServer struct {
	rmpb.UnimplementedResourceManagerServer
	handle func(rmpb.ResourceManager_AcquireTokenBucketsServer) error
}

func (s *tokenProxyTestServer) AcquireTokenBuckets(stream rmpb.ResourceManager_AcquireTokenBucketsServer) error {
	return s.handle(stream)
}

func newTokenProxyTestConn(t *testing.T, service rmpb.ResourceManagerServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	transport := grpc.NewServer()
	rmpb.RegisterResourceManagerServer(transport, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = transport.Serve(listener)
	}()
	conn, err := grpc.NewClient("passthrough:///token-proxy-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		transport.Stop()
		_ = listener.Close()
		<-done
	})
	return conn
}

func TestResourceGroupProxyTokenStream(t *testing.T) {
	for _, mode := range []string{"backend-error", "backend-eof", "round-trip", "client-cancel"} {
		t.Run(mode, func(t *testing.T) {
			re := require.New(t)
			received := make(chan struct{})
			backendDone := make(chan struct{})
			backend := &tokenProxyTestServer{handle: func(stream rmpb.ResourceManager_AcquireTokenBucketsServer) error {
				defer close(backendDone)
				if _, err := stream.Recv(); err != nil {
					return err
				}
				close(received)
				switch mode {
				case "backend-error":
					return status.Error(codes.Unavailable, "backend unavailable")
				case "backend-eof":
					return nil
				case "client-cancel":
					<-stream.Context().Done()
					return stream.Context().Err()
				default:
					for {
						if err := stream.Send(&rmpb.TokenBucketsResponse{Responses: []*rmpb.TokenBucketResponse{{ResourceGroupName: "test"}}}); err != nil {
							return err
						}
						if _, err := stream.Recv(); err != nil {
							if err == io.EOF {
								return nil
							}
							return err
						}
					}
				}
			}}
			backendConn := newTokenProxyTestConn(t, backend)
			pd := &Server{ctx: context.Background(), member: member.NewMember(nil, nil, 1)}
			pd.SetServicePrimaryAddr(constant.ResourceManagerServiceName, "backend")
			pd.clientConns.Store("backend", backendConn)
			proxyConn := newTokenProxyTestConn(t, &resourceGroupProxyServer{GrpcServer: &GrpcServer{Server: pd}})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := rmpb.NewResourceManagerClient(proxyConn).AcquireTokenBuckets(ctx)
			re.NoError(err)
			re.NoError(stream.Send(&rmpb.TokenBucketsRequest{}))
			select {
			case <-received:
			case <-ctx.Done():
				t.Fatal("backend did not receive the token request")
			}
			switch mode {
			case "backend-error":
				_, err = stream.Recv()
				re.Equal(codes.Unavailable, status.Code(err))
				re.Contains(err.Error(), "backend unavailable")
			case "backend-eof":
				_, err = stream.Recv()
				re.ErrorIs(err, io.EOF)
			case "client-cancel":
				cancel()
				_, err = stream.Recv()
				re.Equal(codes.Canceled, status.Code(err))
			default:
				for i := range 2 {
					if i > 0 {
						re.NoError(stream.Send(&rmpb.TokenBucketsRequest{}))
					}
					resp, err := stream.Recv()
					re.NoError(err)
					re.Len(resp.Responses, 1)
					re.Equal("test", resp.Responses[0].ResourceGroupName)
				}
				re.NoError(stream.CloseSend())
				_, err = stream.Recv()
				re.ErrorIs(err, io.EOF)
			}
			select {
			case <-backendDone:
			case <-time.After(3 * time.Second):
				t.Fatal("backend token stream did not stop")
			}
		})
	}
}
