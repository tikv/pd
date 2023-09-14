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

	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC errors
var (
	ErrNotStarted        = status.Errorf(codes.Unavailable, "server not started")
	ErrClusterMismatched = status.Errorf(codes.Unavailable, "cluster mismatched")
)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(srv *Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// ConfigProvider is used to get scheduling config from the given
// `bs.server` without modifying its interface.
type ConfigProvider interface{}

// Service is the scheduling grpc service.
type Service struct {
	*Server
}

// NewService creates a new TSO service.
func NewService[T ConfigProvider](svr bs.Server) registry.RegistrableService {
	server, ok := svr.(*Server)
	if !ok {
		log.Fatal("create scheduling server failed")
	}
	return &Service{
		Server: server,
	}
}

// StoreHeartbeat implements gRPC PDServer.
func (s *Service) StoreHeartbeat(ctx context.Context, request *schedulingpb.StoreHeartbeatRequest) (*schedulingpb.StoreHeartbeatResponse, error) {
	c := s.GetCluster()
	if c == nil {
		// TODO: add metrics
		log.Info("cluster isn't initialized")
		return &schedulingpb.StoreHeartbeatResponse{Header: &schedulingpb.ResponseHeader{ClusterId: s.clusterID}}, nil
	}

	if c.GetStore(request.GetStats().GetStoreId()) == nil {
		s.metaWatcher.GetStoreWatcher().ForceLoad()
	}

	// TODO: add metrics
	if err := c.HandleStoreHeartbeat(request); err != nil {
		log.Error("handle store heartbeat failed", zap.Error(err))
	}
	return &schedulingpb.StoreHeartbeatResponse{Header: &schedulingpb.ResponseHeader{ClusterId: s.clusterID}}, nil
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	schedulingpb.RegisterSchedulingServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	handler, group := SetUpRestHandler(s)
	apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}
