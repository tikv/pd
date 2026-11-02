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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/pkg/retry"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestGetCallerID(t *testing.T) {
	re := require.New(t)
	tests := []struct {
		name          string
		advertiseAddr string
		expectedHost  string
	}{
		{
			name:          "valid http address",
			advertiseAddr: "http://127.0.0.1:2379",
			expectedHost:  "127.0.0.1:2379",
		},
		{
			name:          "valid https address",
			advertiseAddr: "https://127.0.0.1:2379",
			expectedHost:  "127.0.0.1:2379",
		},
		{
			name:          "valid address without scheme",
			advertiseAddr: "localhost:2379",
			expectedHost:  "localhost:2379",
		},
		{
			name:          "empty address",
			advertiseAddr: "",
			expectedHost:  "",
		},
		{
			name:          "valid IPv4 address without scheme",
			advertiseAddr: "127.0.0.1:2379",
			expectedHost:  "127.0.0.1:2379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			actual := GetCalleeID(tt.advertiseAddr)
			re.Equal(tt.expectedHost, actual)
		})
	}
}

func TestGetOrCreateGRPCConnClosesInjectedFailure(t *testing.T) {
	re := require.New(t)
	const (
		addr          = "http://127.0.0.1:1"
		failpointName = "github.com/tikv/pd/client/pkg/utils/grpcutil/unreachableNetwork2"
	)
	re.NoError(failpoint.Enable(failpointName, `return("http://127.0.0.1:1")`))
	t.Cleanup(func() {
		re.NoError(failpoint.Disable(failpointName))
	})

	conn, err := GetOrCreateGRPCConn(context.Background(), &sync.Map{}, addr, nil)
	re.Nil(conn)
	re.EqualError(err, "unreachable network")
}

func TestUnaryBackofferInterceptor(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		withBackoffer bool
		rpcErr        error
		headerErr     *pdpb.Error
		wantAttempts  int
		wantCode      codes.Code
	}{
		{
			name:          "retries unavailable RPC",
			withBackoffer: true,
			rpcErr:        status.Error(codes.Unavailable, "not leader"),
			wantAttempts:  2,
			wantCode:      codes.OK,
		},
		{
			name:          "header error is an RPC success",
			withBackoffer: true,
			headerErr:     &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "gc state read failed"},
			wantAttempts:  1,
			wantCode:      codes.OK,
		},
		{
			name:         "no backoffer preserves RPC error",
			rpcErr:       status.Error(codes.Unavailable, "not leader"),
			wantAttempts: 1,
			wantCode:     codes.Unavailable,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if testCase.withBackoffer {
				bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, 100*time.Millisecond)
				bo.SetRetryableChecker(func(err error) bool { return status.Code(err) == codes.Unavailable }, true)
				ctx = retry.WithBackoffer(ctx, bo)
			}
			attempts := 0
			response := &pdpb.GetGCStateResponse{}
			invoker := func(_ context.Context, _ string, _, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				attempts++
				if attempts == 1 && testCase.rpcErr != nil {
					return testCase.rpcErr
				}
				reply.(*pdpb.GetGCStateResponse).Header = &pdpb.ResponseHeader{Error: testCase.headerErr}
				return nil
			}
			err := UnaryBackofferInterceptor()(ctx, "/pdpb.PD/GetGCState", nil, response, nil, invoker)
			require.Equal(t, testCase.wantCode, status.Code(err))
			require.Equal(t, testCase.wantAttempts, attempts)
			if testCase.headerErr != nil {
				require.Equal(t, testCase.headerErr, response.GetHeader().GetError())
			}
		})
	}
}
