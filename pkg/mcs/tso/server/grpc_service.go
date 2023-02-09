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

package server

import (
	"context"
	"net/http"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/server"
	"google.golang.org/grpc"
)

var _ tsopb.TSOServer = (*Service)(nil)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(srv *Service) (http.Handler, server.APIServiceGroup) {
	return dummyRestService{}, server.APIServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// Service is the gRPC service for TSO.
type Service struct {
	ctx    context.Context
	server *Server
	// settings
}

// NewService creates a new TSO service.
func NewService(svr *Server) registry.RegistrableService {
	return &Service{
		ctx:    svr.Context(),
		server: svr,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	tsopb.RegisterTSOServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	handler, group := SetUpRestHandler(s)
	server.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

// GetMembers implements gRPC PDServer.
func (s *Service) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	return nil, nil
}

// Tso returns a stream of timestamps
func (s *Service) Tso(stream tsopb.TSO_TsoServer) error {
	return nil
}

// SyncMaxTS will check whether MaxTS is the biggest one among all Local TSOs this PD is holding when skipCheck is set,
// and write it into all Local TSO Allocators then if it's indeed the biggest one.
func (s *Service) SyncMaxTS(_ context.Context, request *pdpb.SyncMaxTSRequest) (*pdpb.SyncMaxTSResponse, error) {
	return nil, nil
}

// GetDCLocationInfo gets the dc-location info of the given dc-location from PD leader's TSO allocator manager.
func (s *Service) GetDCLocationInfo(ctx context.Context, request *pdpb.GetDCLocationInfoRequest) (*pdpb.GetDCLocationInfoResponse, error) {
	return nil, nil
}

// SetExternalTimestamp sets a given external timestamp to perform stale read.
func (s *Service) SetExternalTimestamp(ctx context.Context, request *pdpb.SetExternalTimestampRequest) (*pdpb.SetExternalTimestampResponse, error) {
	return nil, nil
}

// GetExternalTimestamp gets the saved external timstamp.
func (s *Service) GetExternalTimestamp(ctx context.Context, request *pdpb.GetExternalTimestampRequest) (*pdpb.GetExternalTimestampResponse, error) {
	return nil, nil
}
