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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/utils/testutil"
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
