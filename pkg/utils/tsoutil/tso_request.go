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

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/tikv/pd/pkg/mcs/utils"
	"google.golang.org/grpc"
)

// Request is an interface wrapping tsopb.TsoRequest and pdpb.TsoRequest
// so that they can be generally handled by the TSO dispatcher
type Request interface {
	// getForwardedHost returns the forwarded host
	getForwardedHost() string
	// getClientConn returns the grpc client connection
	getClientConn() *grpc.ClientConn
	// getCount returns the count of timestamps to retrieve
	getCount() uint32
	// process sends request and receive response via stream.
	// count defines the count of timestamps to retrieve.
	process(ctx context.Context, forwardStream stream, count uint32, tsoProtoFactory ProtoFactory) (response, error)
	// sendResponseAsync sends the response back to the sender of the request
	sendResponseAsync(countSum, physical, firstLogical int64, suffixBits uint32) int64
	// sendErrorResponseAsync creates an tso error response and sends it back to the client asynchronously.
	sendErrorResponseAsync(err error)
	// isResponseSent returns whether the response has been sent
	isResponseSent() bool
}

// response is an interface wrapping tsopb.TsoResponse and pdpb.TsoResponse
type response interface {
	GetTimestamp() *pdpb.Timestamp
}

// TSOProtoRequest wraps the request and stream channel in the TSO grpc service
type TSOProtoRequest struct {
	grpcSvrStreamCtx context.Context
	forwardedHost    string
	clientConn       *grpc.ClientConn
	request          *tsopb.TsoRequest
	responseCh       chan *tsopb.TsoResponse
	responseSent     bool
}

// NewTSOProtoRequest creates a TSOProtoRequest and returns as a Request
func NewTSOProtoRequest(
	grpcSvrStreamCtx context.Context, forwardedHost string, clientConn *grpc.ClientConn,
	request *tsopb.TsoRequest, responseCh chan *tsopb.TsoResponse) Request {
	tsoRequest := &TSOProtoRequest{
		grpcSvrStreamCtx: grpcSvrStreamCtx,
		forwardedHost:    forwardedHost,
		clientConn:       clientConn,
		request:          request,
		responseCh:       responseCh,
	}
	return tsoRequest
}

// getForwardedHost returns the forwarded host
func (r *TSOProtoRequest) getForwardedHost() string {
	return r.forwardedHost
}

// getClientConn returns the grpc client connection
func (r *TSOProtoRequest) getClientConn() *grpc.ClientConn {
	return r.clientConn
}

// getCount returns the count of timestamps to retrieve
func (r *TSOProtoRequest) getCount() uint32 {
	return r.request.GetCount()
}

// process sends request and receive response via stream.
// count defines the count of timestamps to retrieve.
func (r *TSOProtoRequest) process(
	ctx context.Context, forwardStream stream, count uint32, tsoProtoFactory ProtoFactory,
) (response, error) {
	return forwardStream.process(ctx, r.request.GetHeader().GetClusterId(), count,
		r.request.GetHeader().GetKeyspaceId(), r.request.GetHeader().GetKeyspaceGroupId(), r.request.GetDcLocation())
}

// sendResponseAsync sends the response back to the sender of the request
func (r *TSOProtoRequest) sendResponseAsync(countSum, physical, firstLogical int64, suffixBits uint32) int64 {
	count := r.request.GetCount()
	countSum += int64(count)
	response := &tsopb.TsoResponse{
		Header: &tsopb.ResponseHeader{ClusterId: r.request.GetHeader().GetClusterId()},
		Count:  count,
		Timestamp: &pdpb.Timestamp{
			Physical:   physical,
			Logical:    addLogical(firstLogical, countSum, suffixBits),
			SuffixBits: suffixBits,
		},
	}
	// Asynchronously send response back to the client. Though responseCh is a buffered channel
	// with size 1, in TSO streaming process routine, it calls stream.Recv() followed by stream.Send()
	// in a loop and strictly follows this order, so the responseCh is always empty and the outputting
	// the response to the channel is always non-blocking.
	select {
	case <-r.grpcSvrStreamCtx.Done():
	case r.responseCh <- response:
		r.responseSent = true
	}
	return countSum
}

// sendErrorResponseAsync creates an tso error response and sends it back to the client asynchronously.
func (r *TSOProtoRequest) sendErrorResponseAsync(err error) {
	if r.isResponseSent() {
		return
	}
	response := &tsopb.TsoResponse{
		Header: &tsopb.ResponseHeader{
			ClusterId: r.request.GetHeader().GetClusterId(),
			Error: &tsopb.Error{
				Type:    tsopb.ErrorType_UNKNOWN,
				Message: err.Error(),
			},
		},
		Timestamp: &pdpb.Timestamp{},
		Count:     0,
	}
	// Asynchronously send response back to the client.
	select {
	case <-r.grpcSvrStreamCtx.Done():
	case r.responseCh <- response:
		r.responseSent = true
	}
}

// isResponseSent returns whether the response has been sent
func (r *TSOProtoRequest) isResponseSent() bool {
	return r.responseSent
}

// PDProtoRequest wraps the request and stream channel in the PD grpc service
type PDProtoRequest struct {
	grpcSvrStreamCtx context.Context
	forwardedHost    string
	clientConn       *grpc.ClientConn
	request          *pdpb.TsoRequest
	responseCh       chan *pdpb.TsoResponse
	responseSent     bool
}

// NewPDProtoRequest creates a PDProtoRequest and returns as a Request
func NewPDProtoRequest(
	grpcSvrStreamCtx context.Context, forwardedHost string, clientConn *grpc.ClientConn,
	request *pdpb.TsoRequest, responseCh chan *pdpb.TsoResponse,
) Request {
	tsoRequest := &PDProtoRequest{
		grpcSvrStreamCtx: grpcSvrStreamCtx,
		forwardedHost:    forwardedHost,
		clientConn:       clientConn,
		request:          request,
		responseCh:       responseCh,
	}
	return tsoRequest
}

// getForwardedHost returns the forwarded host
func (r *PDProtoRequest) getForwardedHost() string {
	return r.forwardedHost
}

// getClientConn returns the grpc client connection
func (r *PDProtoRequest) getClientConn() *grpc.ClientConn {
	return r.clientConn
}

// getCount returns the count of timestamps to retrieve
func (r *PDProtoRequest) getCount() uint32 {
	return r.request.GetCount()
}

// process sends request and receive response via stream.
// count defines the count of timestamps to retrieve.
func (r *PDProtoRequest) process(
	ctx context.Context, forwardStream stream, count uint32, tsoProtoFactory ProtoFactory,
) (response, error) {
	return forwardStream.process(ctx, r.request.GetHeader().GetClusterId(), count,
		utils.DefaultKeyspaceID, utils.DefaultKeyspaceGroupID, r.request.GetDcLocation())
}

// sendResponseAsync sends the response back to the sender of the request
func (r *PDProtoRequest) sendResponseAsync(countSum, physical, firstLogical int64, suffixBits uint32) int64 {
	count := r.request.GetCount()
	countSum += int64(count)
	response := &pdpb.TsoResponse{
		Header: &pdpb.ResponseHeader{ClusterId: r.request.GetHeader().GetClusterId()},
		Count:  count,
		Timestamp: &pdpb.Timestamp{
			Physical:   physical,
			Logical:    addLogical(firstLogical, countSum, suffixBits),
			SuffixBits: suffixBits,
		},
	}
	// Asynchronously send response back to the client. Though responseCh is a buffered channel
	// with size 1, in TSO streaming process routine, it calls stream.Recv() followed by stream.Send()
	// in a loop and strictly follows this order, so the responseCh is always empty and the outputting
	// the response to the channel is always non-blocking.
	select {
	case <-r.grpcSvrStreamCtx.Done():
	case r.responseCh <- response:
		r.responseSent = true
	}
	return countSum
}

// sendErrorResponseAsync creates an tso error response and sends it back to the client asynchronously.
func (r *PDProtoRequest) sendErrorResponseAsync(err error) {
	if r.isResponseSent() {
		return
	}
	response := &pdpb.TsoResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: r.request.GetHeader().GetClusterId(),
			Error: &pdpb.Error{
				Type:    pdpb.ErrorType_UNKNOWN,
				Message: err.Error(),
			},
		},
		Timestamp: &pdpb.Timestamp{},
		Count:     0,
	}
	// Asynchronously send response back to the client.
	select {
	case <-r.grpcSvrStreamCtx.Done():
	case r.responseCh <- response:
		r.responseSent = true
	}
}

// isResponseSent returns whether the response has been sent
func (r *PDProtoRequest) isResponseSent() bool {
	return r.responseSent
}
