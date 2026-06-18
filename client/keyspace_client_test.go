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
	"sync/atomic"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	clienterrs "github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
)

type testKeyspaceServer struct {
	keyspacepb.UnimplementedKeyspaceServer

	meta     *keyspacepb.KeyspaceMeta
	respType pdpb.ErrorType
	respMsg  string
	rpcErr   error
	lastID   atomic.Uint32
}

func (s *testKeyspaceServer) LoadKeyspaceByID(
	_ context.Context,
	req *keyspacepb.LoadKeyspaceByIDRequest,
) (*keyspacepb.LoadKeyspaceResponse, error) {
	s.lastID.Store(req.GetId())
	if s.rpcErr != nil {
		return nil, s.rpcErr
	}
	resp := &keyspacepb.LoadKeyspaceResponse{
		Header: &pdpb.ResponseHeader{},
	}
	if s.respMsg != "" {
		resp.Header.Error = &pdpb.Error{
			Type:    s.respType,
			Message: s.respMsg,
		}
		return resp, nil
	}
	resp.Keyspace = s.meta
	return resp, nil
}

func startTestKeyspaceServer(t *testing.T, keyspaceServer *testKeyspaceServer) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	keyspacepb.RegisterKeyspaceServer(server, keyspaceServer)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()

	addr := "http://" + listener.Addr().String()
	cleanup := func() {
		server.Stop()
		_ = listener.Close()
		<-done
	}
	return addr, cleanup
}

func newTestKeyspaceClient(t *testing.T, ctx context.Context, addr string) *client {
	t.Helper()

	conn, err := grpcutil.GetClientConn(ctx, addr, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	option := opt.NewOption()
	option.InitMetrics = false
	return &client{
		inner: &innerClient{
			serviceDiscovery: newTestServiceDiscovery(addr, conn),
			ctx:              ctx,
			option:           option,
		},
	}
}

func TestLoadKeyspaceByID(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	expected := &keyspacepb.KeyspaceMeta{
		Id:    42,
		Name:  "test-keyspace",
		State: keyspacepb.KeyspaceState_ENABLED,
	}
	keyspaceServer := &testKeyspaceServer{meta: expected}
	addr, cleanup := startTestKeyspaceServer(t, keyspaceServer)
	t.Cleanup(cleanup)

	client := newTestKeyspaceClient(t, ctx, addr)
	span, tracedCtx := opentracing.StartSpanFromContext(ctx, "test-load-keyspace-by-id")
	defer span.Finish()
	loaded, err := client.LoadKeyspaceByID(tracedCtx, expected.GetId())
	re.NoError(err)
	re.Equal(expected, loaded)
	re.Equal(expected.GetId(), keyspaceServer.lastID.Load())
}

func TestLoadKeyspaceByIDReturnsHeaderError(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	keyspaceServer := &testKeyspaceServer{
		respType: pdpb.ErrorType_ENTRY_NOT_FOUND,
		respMsg:  "keyspace does not exist",
	}
	addr, cleanup := startTestKeyspaceServer(t, keyspaceServer)
	t.Cleanup(cleanup)

	client := newTestKeyspaceClient(t, ctx, addr)
	loaded, err := client.LoadKeyspaceByID(ctx, 42)
	re.Nil(loaded)
	re.ErrorContains(err, "Load keyspace id 42 failed")
	re.ErrorContains(err, "keyspace does not exist")
	re.Equal(uint32(42), keyspaceServer.lastID.Load())
}

func TestLoadKeyspaceByIDReturnsProtoClientError(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	option := opt.NewOption()
	client := &client{
		inner: &innerClient{
			serviceDiscovery: &testServiceDiscovery{},
			ctx:              ctx,
			option:           option,
		},
	}

	loaded, err := client.LoadKeyspaceByID(ctx, 42)
	re.Nil(loaded)
	re.True(clienterrs.ErrClientGetProtoClient.Equal(err))
}

func TestLoadKeyspaceByIDReturnsRPCError(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	keyspaceServer := &testKeyspaceServer{
		rpcErr: errors.New("load keyspace by id failed"),
	}
	addr, cleanup := startTestKeyspaceServer(t, keyspaceServer)
	t.Cleanup(cleanup)

	client := newTestKeyspaceClient(t, ctx, addr)
	loaded, err := client.LoadKeyspaceByID(ctx, 42)
	re.Nil(loaded)
	re.ErrorContains(err, "load keyspace by id failed")
	re.Equal(uint32(42), keyspaceServer.lastID.Load())
}
