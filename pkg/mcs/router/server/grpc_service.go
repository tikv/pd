// Copyright 2025 TiKV Project Authors.
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

	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/routerpb"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(*Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (dummyRestService) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// ConfigProvider is used to get router config from the given
// `bs.server` without modifying its interface.
type ConfigProvider any

// Service is the router grpc service.
type Service struct {
	*Server
}

// NewService creates a new router service.
func NewService[T ConfigProvider](svr bs.Server) registry.RegistrableService {
	server, ok := svr.(*Server)
	if !ok {
		log.Fatal("create router server failed")
	}
	return &Service{
		Server: server,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	routerpb.RegisterRouterServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) error {
	handler, group := SetUpRestHandler(s)
	return apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

// BatchScanRegions implements the BatchScanRegions RPC method.
func (*Service) BatchScanRegions(context.Context, *pdpb.BatchScanRegionsRequest) (*pdpb.BatchScanRegionsResponse, error) {
	return &pdpb.BatchScanRegionsResponse{}, nil
}

// ScanRegions implements the ScanRegions RPC method.
func (*Service) ScanRegions(context.Context, *pdpb.ScanRegionsRequest) (*pdpb.ScanRegionsResponse, error) {
	return &pdpb.ScanRegionsResponse{}, nil
}

// GetRegion implements the GetRegion RPC method.
func (*Service) GetRegion(context.Context, *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	return &pdpb.GetRegionResponse{}, nil
}

// GetAllStores implements the GetAllStores RPC method.
func (*Service) GetAllStores(context.Context, *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	return &pdpb.GetAllStoresResponse{}, nil
}

// GetStore implements the GetStore RPC method.
func (*Service) GetStore(context.Context, *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	return &pdpb.GetStoreResponse{}, nil
}

// GetPrevRegion implements the GetPrevRegion RPC method.
func (*Service) GetPrevRegion(context.Context, *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	return &pdpb.GetRegionResponse{}, nil
}

// GetRegionByID implements the GetRegionByID RPC method.
func (*Service) GetRegionByID(context.Context, *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	return &pdpb.GetRegionResponse{}, nil
}

// QueryRegion implements the QueryRegion RPC method.
func (*Service) QueryRegion(routerpb.Router_QueryRegionServer) error {
	return nil
}
