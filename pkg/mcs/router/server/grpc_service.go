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
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/routerpb"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	gh "github.com/tikv/pd/pkg/grpc"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keypath"
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
func (s *Service) BatchScanRegions(_ctx context.Context, request *pdpb.BatchScanRegionsRequest) (*pdpb.BatchScanRegionsResponse, error) {
	resp, err := gh.BatchScanRegions(s.GetBasicCluster(), request, false)
	metering.IncRegionRequestCounter("BatchScanRegions", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// ScanRegions implements the ScanRegions RPC method.
func (s *Service) ScanRegions(_ctx context.Context, request *pdpb.ScanRegionsRequest) (*pdpb.ScanRegionsResponse, error) {
	resp, err := gh.ScanRegions(s.GetBasicCluster(), request, false)
	metering.IncRegionRequestCounter("ScanRegions", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// GetRegion implements the GetRegion RPC method.
func (s *Service) GetRegion(_ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	resp, err := gh.GetRegion(s.GetBasicCluster(), request, false)
	metering.IncRegionRequestCounter("GetRegion", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// GetAllStores implements the GetAllStores RPC method.
func (s *Service) GetAllStores(_ctx context.Context, request *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	resp, err := gh.GetAllStores(s.GetBasicCluster(), request)
	metering.IncRegionRequestCounter("GetAllStores", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// GetStore implements the GetStore RPC method.
func (s *Service) GetStore(_ctx context.Context, request *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	resp, err := gh.GetStore(s.GetBasicCluster(), request)
	metering.IncRegionRequestCounter("GetStore", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// GetPrevRegion implements the GetPrevRegion RPC method.
func (s *Service) GetPrevRegion(_ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	resp, err := gh.GetPrevRegion(s.GetBasicCluster(), request, false)
	metering.IncRegionRequestCounter("GetPrevRegion", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// GetRegionByID implements the GetRegionByID RPC method.
func (s *Service) GetRegionByID(_ctx context.Context, request *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	resp, err := gh.GetRegionByID(s.GetBasicCluster(), request, false)
	metering.IncRegionRequestCounter("GetRegionByID", request.GetHeader(), resp.GetHeader().GetError(), regionRequestCounter)
	return resp, err
}

// QueryRegion implements the QueryRegion RPC method.
func (s *Service) QueryRegion(stream routerpb.Router_QueryRegionServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if clusterID := keypath.ClusterID(); request.GetHeader().GetClusterId() != clusterID {
			return errs.ErrMismatchClusterID(clusterID, request.GetHeader().GetClusterId())
		}

		cluster := s.GetBasicCluster()
		start := time.Now()
		needBuckets := request.GetNeedBuckets()
		keyIDMap, prevKeyIDMap, regionsByID := cluster.QueryRegions(
			request.GetKeys(),
			request.GetPrevKeys(),
			request.GetIds(),
			needBuckets,
		)
		queryRegionDuration.Observe(time.Since(start).Seconds())
		// Build the response and send it to the client.
		response := &pdpb.QueryRegionResponse{
			Header:       gh.WrapHeader(),
			KeyIdMap:     keyIDMap,
			PrevKeyIdMap: prevKeyIDMap,
			RegionsById:  regionsByID,
		}
		gh.IncRegionRequestCounter("QueryRegion", request.Header, response.Header.Error, regionRequestCounter)
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}
