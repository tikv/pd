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

package client_test

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/grpcutil"
)

// regionRPCRecorder observes real Region RPCs before the public client's retry
// decision. Faults replace received responses, not the client's routing or retry
// implementation. Health injection makes the selected follower deterministic;
// forwarding is disabled so unary fallback still connects directly to the leader.
type regionRPCRecorder struct {
	leaderTarget   string
	followerTarget string
	followerOnly   bool
	fault          func(bool, string, any, any) error
	mu             sync.Mutex
	leaderAttempts []bool
}

func (r *regionRPCRecorder) receive(target, method string, request, response any) error {
	if target != r.leaderTarget && target != r.followerTarget {
		return status.Errorf(codes.Internal, "unexpected Region RPC target %s", target)
	}
	isLeader := target == r.leaderTarget
	r.mu.Lock()
	r.leaderAttempts = append(r.leaderAttempts, isLeader)
	r.mu.Unlock()
	return r.fault(isLeader, method, request, response)
}

func (r *regionRPCRecorder) attempts() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.leaderAttempts...)
}

func (r *regionRPCRecorder) unary(
	ctx context.Context, method string, request, response any, conn *grpc.ClientConn,
	invoke grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	if r.followerOnly && method == "/grpc.health.v1.Health/Check" && conn.Target() != r.followerTarget {
		return status.Error(codes.Unavailable, "parity test selects one follower")
	}
	if err := invoke(ctx, method, request, response, conn, opts...); err != nil {
		return err
	}
	if _, ok := response.(*pdpb.GetRegionResponse); !ok {
		return nil
	}
	return r.receive(conn.Target(), method, request, response)
}

func (r *regionRPCRecorder) stream(
	ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string,
	streamer grpc.Streamer, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	stream, err := streamer(ctx, desc, conn, method, opts...)
	if err != nil || method != "/pdpb.PD/QueryRegion" {
		return stream, err
	}
	return &regionRecordingStream{ClientStream: stream, recorder: r, target: conn.Target()}, nil
}

type regionRecordingStream struct {
	grpc.ClientStream
	recorder *regionRPCRecorder
	target   string
	request  any
}

func (s *regionRecordingStream) SendMsg(request any) error {
	s.request = request
	return s.ClientStream.SendMsg(request)
}

func (s *regionRecordingStream) RecvMsg(response any) error {
	if err := s.ClientStream.RecvMsg(response); err != nil {
		return err
	}
	return s.recorder.receive(s.target, "/pdpb.PD/QueryRegion", s.request, response)
}

// replaceRegionResponse uses the server's real missing-result encoders. In
// particular, a follower miss is a unary header error but a successful sparse
// QueryRegion response. Header errors deliberately retain any hit payload to
// verify that the client rejects the entire invalid response.
func replaceRegionResponse(method string, request, response any, isLeader, missing bool, headerErr *pdpb.Error) {
	switch resp := response.(type) {
	case *pdpb.GetRegionResponse:
		if missing {
			empty := core.NewBasicCluster()
			switch req := request.(type) {
			case *pdpb.GetRegionRequest:
				get := grpcutil.GetRegion
				if method == "/pdpb.PD/GetPrevRegion" {
					get = grpcutil.GetPrevRegion
				}
				result, _ := get(empty, req, !isLeader)
				*resp = *result
			case *pdpb.GetRegionByIDRequest:
				result, _ := grpcutil.GetRegionByID(empty, req, !isLeader)
				*resp = *result
			}
		}
		if headerErr != nil {
			resp.Header = &pdpb.ResponseHeader{Error: headerErr}
		}
	case *pdpb.QueryRegionResponse:
		if missing {
			*resp = *grpcutil.QueryRegion(core.NewBasicCluster(), request.(*pdpb.QueryRegionRequest))
		}
		if headerErr != nil {
			resp.Header = &pdpb.ResponseHeader{Error: headerErr}
		}
	}
}
