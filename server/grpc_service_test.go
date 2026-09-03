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
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestValidatePDForwardedHostWithoutLeader(t *testing.T) {
	grpcServer := &GrpcServer{Server: &Server{member: member.NewMember(nil, nil, 1)}}
	err := grpcServer.validatePDForwardedHost("http://127.0.0.1:2379")
	require.ErrorIs(t, err, errs.ErrNotLeader)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestIsSamePDClientURL(t *testing.T) {
	testCases := []struct {
		name   string
		first  string
		second string
		same   bool
	}{
		{name: "same HTTP URL", first: "http://127.0.0.1:2379", second: "http://127.0.0.1:2379", same: true},
		{name: "same HTTPS URL", first: "https://127.0.0.1:2379", second: "https://127.0.0.1:2379", same: true},
		{name: "HTTP to HTTPS", first: "http://127.0.0.1:2379", second: "https://127.0.0.1:2379", same: true},
		{name: "HTTPS to HTTP", first: "https://127.0.0.1:2379", second: "http://127.0.0.1:2379", same: true},
		{name: "uppercase scheme", first: "HTTP://127.0.0.1:2379", second: "https://127.0.0.1:2379"},
		{name: "different host", first: "http://127.0.0.1:2379", second: "https://127.0.0.2:2379"},
		{name: "different port", first: "http://127.0.0.1:2379", second: "https://127.0.0.1:2380"},
		{name: "different path", first: "http://127.0.0.1:2379", second: "https://127.0.0.1:2379/path"},
		{name: "different query", first: "http://127.0.0.1:2379", second: "https://127.0.0.1:2379?query=value"},
		{name: "nested scheme", first: "http://127.0.0.1:2379", second: "https://http://127.0.0.1:2379"},
		{name: "missing scheme", first: "http://127.0.0.1:2379", second: "127.0.0.1:2379"},
		{name: "unsupported scheme", first: "http://127.0.0.1:2379", second: "ftp://127.0.0.1:2379"},
		{name: "same unsupported URL", first: "ftp://127.0.0.1:2379", second: "ftp://127.0.0.1:2379"},
		{name: "empty URLs"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.same, isSamePDClientURL(testCase.first, testCase.second))
		})
	}
}

func TestNewSchedulingAskBatchSplitRequestPreservesReason(t *testing.T) {
	re := require.New(t)
	req := newSchedulingAskBatchSplitRequest(&pdpb.AskBatchSplitRequest{
		Header:     &pdpb.RequestHeader{ClusterId: 1, SenderId: 2},
		Region:     &metapb.Region{Id: 100},
		SplitCount: 3,
		Reason:     pdpb.SplitReason_LOAD,
	})
	re.Equal(uint64(1), req.GetHeader().GetClusterId())
	re.Equal(uint64(2), req.GetHeader().GetSenderId())
	re.Equal(uint64(100), req.GetRegion().GetId())
	re.Equal(uint32(3), req.GetSplitCount())
	re.Equal(pdpb.SplitReason_LOAD, req.GetReason())
}

func TestConvertSchedulingHeaderPreservesError(t *testing.T) {
	testCases := []struct {
		name string
		in   *schedulingpb.Error
		want *pdpb.Error
	}{
		{
			name: "ok",
		},
		{
			name: "not bootstrapped",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_NOT_BOOTSTRAPPED, Message: "cluster is not initialized"},
			want: &pdpb.Error{Type: pdpb.ErrorType_NOT_BOOTSTRAPPED, Message: "cluster is not initialized"},
		},
		{
			name: "already bootstrapped",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_ALREADY_BOOTSTRAPPED, Message: "cluster is already bootstrapped"},
			want: &pdpb.Error{Type: pdpb.ErrorType_ALREADY_BOOTSTRAPPED, Message: "cluster is already bootstrapped"},
		},
		{
			name: "invalid value",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_INVALID_VALUE, Message: "bad request"},
			want: &pdpb.Error{Type: pdpb.ErrorType_INVALID_VALUE, Message: "bad request"},
		},
		{
			name: "region not found",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_UNKNOWN, Message: "region not found"},
			want: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND, Message: "region not found"},
		},
		{
			name: "unknown",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_CLUSTER_MISMATCHED, Message: "cluster mismatch"},
			want: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "cluster mismatch"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			header := convertHeader(&schedulingpb.ResponseHeader{
				ClusterId: 1,
				Error:     testCase.in,
			})
			re.Equal(uint64(1), header.GetClusterId())
			re.Equal(testCase.want, header.GetError())
		})
	}
}

func TestServiceDiscoveryRPCsReturnUnavailableWhenServerIsNotRunning(t *testing.T) {
	grpcServer := &GrpcServer{Server: &Server{
		serviceMiddlewarePersistOptions: config.NewServiceMiddlewarePersistOptions(&config.ServiceMiddlewareConfig{}),
	}}
	listener := bufconn.Listen(1024 * 1024)
	transport := grpc.NewServer()
	pdpb.RegisterPDServer(transport, grpcServer)
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
	client := pdpb.NewPDClient(conn)

	members, err := client.GetMembers(context.Background(), &pdpb.GetMembersRequest{})
	require.Nil(t, members)
	require.Equal(t, codes.Unavailable, status.Code(err))

	clusterInfo, err := client.GetClusterInfo(context.Background(), &pdpb.GetClusterInfoRequest{})
	require.Nil(t, clusterInfo)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestGetMembersErrorResult(t *testing.T) {
	notStarted := errors.WithStack(errs.ErrServerNotStarted.FastGenByArgs())
	response, err := getMembersErrorResult(notStarted)
	require.Nil(t, response)
	require.Equal(t, codes.Unavailable, status.Code(err))

	internalErr := errors.New("failed to load members")
	response, err = getMembersErrorResult(internalErr)
	require.NoError(t, err)
	require.Equal(t, pdpb.ErrorType_UNKNOWN, response.GetHeader().GetError().GetType())
	require.Equal(t, internalErr.Error(), response.GetHeader().GetError().GetMessage())
}

func TestDialAddress(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	// A dialable address is valid.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	defer func() { re.NoError(listener.Close()) }()
	re.NoError(dialAddress(ctx, listener.Addr().String()))

	// mock:// addresses are accepted without dialing.
	re.NoError(dialAddress(ctx, "mock://tikv-1:1"))

	// TiKV registers its store address before it starts listening on it, so
	// a refused connection still proves the address is valid.
	notYetListening, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	notYetListeningAddr := notYetListening.Addr().String()
	re.NoError(notYetListening.Close())
	re.NoError(dialAddress(ctx, notYetListeningAddr))

	// An address that cannot be resolved at all is invalid.
	re.Error(dialAddress(ctx, "unreachable-host.test.invalid:12345"))

	// A malformed address with an empty host or port must be rejected
	// before it reaches DialContext.
	re.Error(dialAddress(ctx, ":1234"))
	re.Error(dialAddress(ctx, "host:"))
}

func TestValidateStoreAddress(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	defer func() { re.NoError(listener.Close()) }()

	re.NoError(validateStoreAddress(ctx, &metapb.Store{
		Address:       listener.Addr().String(),
		StatusAddress: "mock://tikv-1-status:1",
		PeerAddress:   "mock://tikv-1-peer:1",
	}))

	re.Error(validateStoreAddress(ctx, &metapb.Store{
		Address: "unreachable-host.test.invalid:12345",
	}))
}
