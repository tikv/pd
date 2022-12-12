// Copyright 2020 TiKV Project Authors.
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

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/server"
	"google.golang.org/grpc"
)

// SetUpRestService is a hook to sets up the REST service.
var SetUpRestService = func(srv *Service) (http.Handler, server.ServiceGroup) {
	return dummyRestService{}, server.ServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
	return
}

// Service is the gRPC service for resource manager.
type Service struct {
	ctx     context.Context
	manager *Manager
	// settings
}

// NewService creates a new resource manager service.
func NewService(svr *server.Server) registry.RegistrableService {
	manager := NewManager(svr)
	return &Service{
		ctx:     svr.Context(),
		manager: manager,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	rmpb.RegisterResourceManagerServer(g, s)
}

func (s *Service) RegisterRESTServer(userDefineHandlers map[string]http.Handler) {
	hander, group := SetUpRestService(s)
	server.RegisterUserDefinedHandlers(userDefineHandlers, &group, hander)
}

func (s *Service) GetManager() *Manager {
	return s.manager
}

// AcquireTokenBuckets implements ResourceManagerServer.AcquireTokenBuckets.
func (s *Service) AcquireTokenBuckets(stream rmpb.ResourceManager_AcquireTokenBucketsServer) error {
	for {
		select {
		case <-s.ctx.Done():
			return errors.New("server closed")
		default:
		}
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		for _, req := range request.Requests {
			tag := &tipb.ResourceGroupTag{}
			if err := tag.Unmarshal(req.ResourceGroupTag); err != nil {
				return err
			}
			rg := s.manager.GetResourceGroup(string(tag.GroupName))
			if rg == nil {
				return errors.New("resource group not found")
			}
			// TODO: implement
		}
	}
}

// GetResourceGroup implements ResourceManagerServer.GetResourceGroup.
func (s *Service) GetResourceGroup(ctx context.Context, req *rmpb.GetResourceGroupRequest) (*rmpb.GetResourceGroupResponse, error) {
	tag := &tipb.ResourceGroupTag{}
	if err := tag.Unmarshal(req.ResourceGroupTag); err != nil {
		return nil, err
	}
	// TODO: implement
	return nil, errors.New("not implemented")
}

// ListResourceGroups implements ResourceManagerServer.ListResourceGroups.
func (s *Service) ListResourceGroups(ctx context.Context, req *rmpb.ListResourceGroupsRequest) (*rmpb.ListResourceGroupsResponse, error) {
	return nil, errors.New("not implemented")
}

// AddResourceGroup implements ResourceManagerServer.AddResourceGroup.
func (s *Service) AddResourceGroup(ctx context.Context, req *rmpb.AddResourceGroupRequest) (*rmpb.AddResourceGroupRespose, error) {
	return nil, errors.New("not implemented")
}
