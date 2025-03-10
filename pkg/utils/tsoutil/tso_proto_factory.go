// Copyright 2023 TiKV Project Authors.
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

package tsoutil

import (
	"context"

	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"

	"github.com/tikv/pd/pkg/utils/grpcutil"
)

// ProtoFactory is the abstract factory for creating tso related data structures defined in the grpc service
type ProtoFactory interface {
	createForwardStream(ctx context.Context, client *grpc.ClientConn) (stream, context.CancelFunc, error)
}

// TSOProtoFactory is the abstract factory for creating tso related data structures defined in the TSO grpc service
type TSOProtoFactory struct {
}

// PDProtoFactory is the abstract factory for creating tso related data structures defined in the PD grpc service
type PDProtoFactory struct {
}

func (*TSOProtoFactory) createForwardStream(ctx context.Context, clientConn *grpc.ClientConn) (stream, context.CancelFunc, error) {
	done := make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	go grpcutil.CheckStream(cctx, cancel, done)
	forwardStream, err := tsopb.NewTSOClient(clientConn).Tso(cctx)
	done <- struct{}{}
	return &tsoStream{forwardStream}, cancel, err
}

func (*PDProtoFactory) createForwardStream(ctx context.Context, clientConn *grpc.ClientConn) (stream, context.CancelFunc, error) {
	done := make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	go grpcutil.CheckStream(cctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(clientConn).Tso(cctx)
	done <- struct{}{}
	return &pdStream{forwardStream}, cancel, err
}

type stream interface {
	// process sends a request and receives the response through the stream
	process(clusterID uint64, count, keyspaceID, keyspaceGroupID uint32) (response, error)
}

type tsoStream struct {
	stream tsopb.TSO_TsoClient
}

// process sends a request and receives the response through the stream
func (s *tsoStream) process(clusterID uint64, count, keyspaceID, keyspaceGroupID uint32) (response, error) {
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       clusterID,
			KeyspaceId:      keyspaceID,
			KeyspaceGroupId: keyspaceGroupID,
		},
		Count: count,
	}
	if err := s.stream.Send(req); err != nil {
		return nil, err
	}
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type pdStream struct {
	stream pdpb.PD_TsoClient
}

// process sends a request and receives the response through the stream
func (s *pdStream) process(clusterID uint64, count, _, _ uint32) (response, error) {
	req := &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Count: count,
	}
	if err := s.stream.Send(req); err != nil {
		return nil, err
	}
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return resp, nil
}
