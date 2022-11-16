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

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/server"
	"google.golang.org/grpc"
)

// Service is the gRPC service for resource manager.
type Service struct {
	ctx     context.Context
	manager *Manager
	// settings
}

// NewService creates a new resource manager service.
func NewService(svr *server.Server) registry.GRPCService {
	manager := NewManager(svr)
	return &Service{
		ctx:     svr.Context(),
		manager: manager,
	}
}

// RegisterService registers the service to gRPC server.
func (s *Service) RegisterService(g *grpc.Server) {
	rmpb.RegisterResourceManagerServer(g, s)
}

// AcquireTokenBuckets implements ResourceManagerServer.AcquireTokenBuckets.
func (s *Service) AcquireTokenBuckets(stream rmpb.ResourceManager_AcquireTokenBucketsServer) error {
	_, cancel := context.WithCancel(stream.Context())
	defer cancel()
	return errors.New("not implemented")
	// for {
	// 	select {
	// 	case <-ctx.Done():
	// 		break
	// 	case <-s.ctx.Done():
	// 		break
	// 	}
	// 	req, err := stream.Recv()
	// 	if err == io.EOF {
	// 		return nil
	// 	}
	// 	if err != nil {
	// 		return errors.WithStack(err)
	// 	}
	// 	for _, request := range req.Requests {
	// 		tag := &tipb.ResourceGroupTag{}
	// 		if err := tag.Unmarshal(request.ResourceGroupTag); err != nil {
	// 			return err
	// 		}
	// 		rg := s.manager.GetResourceGroup(string(tag.Name.GroupName))
	// 		if rg == nil {
	// 			return errors.New("resource group not found")
	// 		}
	// 		// TODO: implement
	// 	}
	// }
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
