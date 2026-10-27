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

package grpcutil

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// startGRPCServer starts a gRPC server which only registers the health service,
// simulating an endpoint that is owned by another kind of service.
func startGRPCServer(t *testing.T, register func(*grpc.Server)) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	if register != nil {
		register(srv)
	}
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)
	return "http://" + lis.Addr().String()
}

func dialWithExpectedService(t *testing.T, url, service string) *grpc.ClientConn {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	cc, err := GetClientConn(ctx, url, nil, ExpectedServiceDialOptions(service)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = cc.Close()
	})
	return cc
}

func TestExpectedServiceClosesConnOnUnknownService(t *testing.T) {
	re := require.New(t)
	url := startGRPCServer(t, nil)
	cc := dialWithExpectedService(t, url, "pdpb.PD")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.Equal(connectivity.Shutdown, cc.GetState())
}

func TestExpectedServiceClosesConnOnUnknownServiceStream(t *testing.T) {
	re := require.New(t)
	url := startGRPCServer(t, nil)
	cc := dialWithExpectedService(t, url, "pdpb.PD")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := pdpb.NewPDClient(cc).Tso(ctx)
	re.NoError(err)
	_, err = stream.Recv()
	re.Equal(codes.Unimplemented, status.Code(err))
	re.Equal(connectivity.Shutdown, cc.GetState())
}

// TestExpectedServiceClosesConnOnStreamSendEOF covers a pre-opened stream: once
// the server rejects it, Send returns io.EOF and only Recv reports the status.
func TestExpectedServiceClosesConnOnStreamSendEOF(t *testing.T) {
	re := require.New(t)
	url := startGRPCServer(t, nil)
	cc := dialWithExpectedService(t, url, "pdpb.PD")

	stream, err := pdpb.NewPDClient(cc).QueryRegion(t.Context())
	re.NoError(err)
	var sendErr error
	re.Eventually(func() bool {
		sendErr = stream.Send(&pdpb.QueryRegionRequest{})
		return sendErr != nil
	}, 5*time.Second, 10*time.Millisecond)
	re.ErrorIs(sendErr, io.EOF)
	re.NotEqual(connectivity.Shutdown, cc.GetState())

	err = ResolveStreamSendError(sendErr, func() error {
		_, err := stream.Recv()
		return err
	})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.Equal(connectivity.Shutdown, cc.GetState())
}

func TestResolveStreamSendError(t *testing.T) {
	re := require.New(t)
	recvErr := status.Error(codes.Unimplemented, "unknown service pdpb.PD")
	recvCalled := false
	recv := func(err error) func() error {
		return func() error {
			recvCalled = true
			return err
		}
	}

	re.NoError(ResolveStreamSendError(nil, recv(recvErr)))
	re.False(recvCalled)
	otherErr := status.Error(codes.Unavailable, "unavailable")
	re.Equal(otherErr, ResolveStreamSendError(otherErr, recv(recvErr)))
	re.False(recvCalled)
	re.Equal(recvErr, ResolveStreamSendError(io.EOF, recv(recvErr)))
	re.True(recvCalled)
	// Keep io.EOF if the stream status can not be got by receiving.
	re.Equal(io.EOF, ResolveStreamSendError(io.EOF, recv(nil)))
}

func TestExpectedServiceKeepsConnOnUnknownMethod(t *testing.T) {
	re := require.New(t)
	url := startGRPCServer(t, nil)
	cc := dialWithExpectedService(t, url, healthpb.Health_ServiceDesc.ServiceName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cc.Invoke(ctx, "/grpc.health.v1.Health/NoSuchMethod",
		&healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.NotEqual(connectivity.Shutdown, cc.GetState())
}

func TestExpectedServiceKeepsConnOnOtherUnknownService(t *testing.T) {
	re := require.New(t)
	// The endpoint is a real PD, but it does not provide the keyspace service.
	url := startGRPCServer(t, func(s *grpc.Server) {
		pdpb.RegisterPDServer(s, &pdpb.UnimplementedPDServer{})
	})
	cc := dialWithExpectedService(t, url, "pdpb.PD")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := keyspacepb.NewKeyspaceClient(cc).LoadKeyspace(ctx, &keyspacepb.LoadKeyspaceRequest{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.NotEqual(connectivity.Shutdown, cc.GetState())
	// An unimplemented method of the expected service must not close the connection either.
	_, err = pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.NotEqual(connectivity.Shutdown, cc.GetState())
}

func TestGetOrCreateGRPCConnRecreatesShutdownConn(t *testing.T) {
	re := require.New(t)
	url := startGRPCServer(t, nil)
	clientConns := &sync.Map{}

	cc, err := GetOrCreateGRPCConn(context.Background(), clientConns, url, nil)
	re.NoError(err)
	re.NoError(cc.Close())

	newCC, err := GetOrCreateGRPCConn(context.Background(), clientConns, url, nil)
	re.NoError(err)
	defer newCC.Close()
	re.NotSame(cc, newCC)
	re.NotEqual(connectivity.Shutdown, newCC.GetState())
	cached, ok := clientConns.Load(url)
	re.True(ok)
	re.Same(newCC, cached)
}

// TestUnknownServiceErrorFormat pins the message returned by the gRPC server for
// an unregistered service, which isUnknownServiceError relies on. If it fails
// after upgrading gRPC, update unknownServiceErrorPrefix accordingly, otherwise
// the connections reaching another service will never be closed.
func TestUnknownServiceErrorFormat(t *testing.T) {
	re := require.New(t)
	url := startGRPCServer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	cc, err := GetClientConn(ctx, url, nil)
	re.NoError(err)
	defer cc.Close()

	const formatChangedMsg = "gRPC changed the unknown service error format, update unknownServiceErrorPrefix"
	checkUnknownServiceError := func(err error) {
		s, ok := status.FromError(err)
		re.True(ok, formatChangedMsg)
		re.Equal(codes.Unimplemented, s.Code(), formatChangedMsg)
		re.Equal(unknownServiceErrorPrefix+"pdpb.PD", s.Message(), formatChangedMsg)
		re.True(isUnknownServiceError(err, "pdpb.PD"))
		re.False(isUnknownServiceError(err, "tsopb.TSO"))
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	_, err = pdpb.NewPDClient(cc).GetMembers(callCtx, &pdpb.GetMembersRequest{})
	checkUnknownServiceError(err)

	stream, err := pdpb.NewPDClient(cc).Tso(callCtx)
	re.NoError(err)
	_, err = stream.Recv()
	checkUnknownServiceError(err)

	// An unknown method of a registered service must not be treated as an unknown service.
	err = cc.Invoke(callCtx, "/grpc.health.v1.Health/NoSuchMethod",
		&healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.False(isUnknownServiceError(err, healthpb.Health_ServiceDesc.ServiceName))
}
