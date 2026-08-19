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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/kvproto/pkg/tsopb"
)

func TestUnaryRPCsReturnUnavailableWhenServerIsNotRunning(t *testing.T) {
	service := &Service{Server: &Server{}}
	listener := bufconn.Listen(1024 * 1024)
	transport := grpc.NewServer()
	tsopb.RegisterTSOServer(transport, service)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- transport.Serve(listener)
	}()
	t.Cleanup(func() {
		transport.Stop()
		require.NoError(t, <-serveErr)
	})
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := tsopb.NewTSOClient(conn)

	group, err := client.FindGroupByKeyspaceID(context.Background(), &tsopb.FindGroupByKeyspaceIDRequest{})
	require.Nil(t, group)
	require.Equal(t, codes.Unavailable, status.Code(err))

	minTS, err := client.GetMinTS(context.Background(), &tsopb.GetMinTSRequest{})
	require.Nil(t, minTS)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestResolveServingRevision(t *testing.T) {
	revision, loaded := resolveServingRevision(10, 12, 8)
	require.Equal(t, uint64(12), revision)
	require.True(t, loaded)

	revision, loaded = resolveServingRevision(10, 8, 12)
	require.Equal(t, uint64(10), revision)
	require.True(t, loaded)

	revision, loaded = resolveServingRevision(12, 8, 10)
	require.Equal(t, uint64(8), revision)
	require.False(t, loaded)
}
