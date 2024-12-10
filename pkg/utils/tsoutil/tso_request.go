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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"google.golang.org/grpc"
)

// Request is an interface wrapping tsopb.TsoRequest and pdpb.TsoRequest so
// they can be generally handled by the TSO dispatcher
type Request interface {
	// getForwardedHost returns the forwarded host
	getForwardedHost() string
	// getClientConn returns the grpc client connection
	getClientConn() *grpc.ClientConn
	// getCount returns the count of timestamps to retrieve
	getCount() uint32
	// process sends request and receive response via stream.
	// count defines the count of timestamps to retrieve.
	process(forwardStream stream, count uint32) (tsoResp, error)
	// postProcess sends the response back to the sender of the request
	postProcess(countSum, physical, firstLogical int64, suffixBits uint32) (int64, error)
}

// response is an interface wrapping tsopb.TsoResponse and pdpb.TsoResponse
type response interface {
	GetTimestamp() *pdpb.Timestamp
}

// TSOProtoRequest wraps the request and stream channel in the TSO grpc service
type TSOProtoRequest struct {
	forwardedHost string
	clientConn    *grpc.ClientConn
	request       *tsopb.TsoRequest
	stream        tsopb.TSO_TsoServer
}

// NewTSOProtoRequest creates a TSOProtoRequest and returns as a Request
func NewTSOProtoRequest(forwardedHost string, clientConn *grpc.ClientConn, request *tsopb.TsoRequest, stream tsopb.TSO_TsoServer) Request {
	tsoRequest := &TSOProtoRequest{
		forwardedHost: forwardedHost,
		clientConn:    clientConn,
		request:       request,
		stream:        stream,
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
func (r *TSOProtoRequest) process(forwardStream stream, count uint32) (tsoResp, error) {
	return forwardStream.process(r.request.GetHeader().GetClusterId(), count,
		r.request.GetHeader().GetKeyspaceId(), r.request.GetHeader().GetKeyspaceGroupId())
}

// postProcess sends the response back to the sender of the request
func (r *TSOProtoRequest) postProcess(countSum, physical, firstLogical int64, suffixBits uint32) (int64, error) {
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
	// Send back to the client.
	if err := r.stream.Send(response); err != nil {
		return countSum, err
	}
	return countSum, nil
}

// PDProtoRequest wraps the request and stream channel in the PD grpc service
type PDProtoRequest struct {
	forwardedHost string
	clientConn    *grpc.ClientConn
	request       *pdpb.TsoRequest
	stream        pdpb.PD_TsoServer
}

// NewPDProtoRequest creates a PDProtoRequest and returns as a Request
func NewPDProtoRequest(forwardedHost string, clientConn *grpc.ClientConn, request *pdpb.TsoRequest, stream pdpb.PD_TsoServer) Request {
	tsoRequest := &PDProtoRequest{
		forwardedHost: forwardedHost,
		clientConn:    clientConn,
		request:       request,
		stream:        stream,
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
func (r *PDProtoRequest) process(forwardStream stream, count uint32) (tsoResp, error) {
	return forwardStream.process(r.request.GetHeader().GetClusterId(), count,
		constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
}

// postProcess sends the response back to the sender of the request
func (r *PDProtoRequest) postProcess(countSum, physical, firstLogical int64, suffixBits uint32) (int64, error) {
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
	// Send back to the client.
	if err := r.stream.Send(response); err != nil {
		return countSum, err
	}
	return countSum, nil
}
