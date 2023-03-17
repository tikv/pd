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
	"github.com/tikv/pd/pkg/mcs/utils"
	"google.golang.org/grpc"
)

// TSORequest is an interface wrapping tsopb.TsoRequest and pdpb.TsoRequest so
// they can be generally handled by the TSO dispatcher
type TSORequest interface {
	// getForwardedHost returns the forwarded host
	getForwardedHost() string
	// getClientConn returns the grpc client connection
	getClientConn() *grpc.ClientConn
	// getCount returns the count of timestamps to retrieve
	getCount() uint32
	// process sends request and receive response via stream.
	// count defins the count of timestamps to retrieve.
	process(forwardStream tsoStream, count uint32, tsoProtoFactory ProtoFactory) (tsoResp, error)
	// postProcess sends the response back to the sender of the request
	postProcess(countSum, physical, firstLogical int64, suffixBits uint32) (int64, error)
}

type tsoResponse interface {
	GetTimestamp() *pdpb.Timestamp
}

// TSOProtoTSORequest wraps the request and stream channel in the TSO grpc service
type TSOProtoTSORequest struct {
	forwardedHost string
	clientConn    *grpc.ClientConn
	request       *tsopb.TsoRequest
	stream        tsopb.TSO_TsoServer
}

// NewTSOProtoTSORequest creats a TSOProtoTSORequest and returns as a TSORequest
func NewTSOProtoTSORequest(forwardedHost string, clientConn *grpc.ClientConn, request *tsopb.TsoRequest, stream tsopb.TSO_TsoServer) TSORequest {
	tsoRequest := &TSOProtoTSORequest{
		forwardedHost: forwardedHost,
		clientConn:    clientConn,
		request:       request,
		stream:        stream,
	}
	return tsoRequest
}

// getForwardedHost returns the forwarded host
func (r *TSOProtoTSORequest) getForwardedHost() string {
	return r.forwardedHost
}

// getClientConn returns the grpc client connection
func (r *TSOProtoTSORequest) getClientConn() *grpc.ClientConn {
	return r.clientConn
}

// getCount returns the count of timestamps to retrieve
func (r *TSOProtoTSORequest) getCount() uint32 {
	return r.request.GetCount()
}

// process sends request and receive response via stream.
// count defins the count of timestamps to retrieve.
func (r *TSOProtoTSORequest) process(forwardStream tsoStream, count uint32, tsoProtoFactory ProtoFactory) (tsoResp, error) {
	return forwardStream.process(r.request.GetHeader().GetClusterId(), count,
		r.request.GetHeader().GetKeyspaceId(), r.request.GetHeader().GetKeyspaceGroupId(), r.request.GetDcLocation())
}

// postProcess sends the response back to the sender of the request
func (r *TSOProtoTSORequest) postProcess(countSum, physical, firstLogical int64, suffixBits uint32) (int64, error) {
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

// PDProtoTSORequest wraps the request and stream channel in the PD grpc service
type PDProtoTSORequest struct {
	forwardedHost string
	clientConn    *grpc.ClientConn
	request       *pdpb.TsoRequest
	stream        pdpb.PD_TsoServer
}

// NewPDProtoTSORequest creats a PDProtoTSORequest and returns as a TSORequest
func NewPDProtoTSORequest(forwardedHost string, clientConn *grpc.ClientConn, request *pdpb.TsoRequest, stream pdpb.PD_TsoServer) TSORequest {
	tsoRequest := &PDProtoTSORequest{
		forwardedHost: forwardedHost,
		clientConn:    clientConn,
		request:       request,
		stream:        stream,
	}
	return tsoRequest
}

// getForwardedHost returns the forwarded host
func (r *PDProtoTSORequest) getForwardedHost() string {
	return r.forwardedHost
}

// getClientConn returns the grpc client connection
func (r *PDProtoTSORequest) getClientConn() *grpc.ClientConn {
	return r.clientConn
}

// getCount returns the count of timestamps to retrieve
func (r *PDProtoTSORequest) getCount() uint32 {
	return r.request.GetCount()
}

// process sends request and receive response via stream.
// count defins the count of timestamps to retrieve.
func (r *PDProtoTSORequest) process(forwardStream tsoStream, count uint32, tsoProtoFactory ProtoFactory) (tsoResp, error) {
	return forwardStream.process(r.request.GetHeader().GetClusterId(), count,
		utils.DefaultKeyspaceID, utils.DefaultKeySpaceGroupID, r.request.GetDcLocation())
}

// postProcess sends the response back to the sender of the request
func (r *PDProtoTSORequest) postProcess(countSum, physical, firstLogical int64, suffixBits uint32) (int64, error) {
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
