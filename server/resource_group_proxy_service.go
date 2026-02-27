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
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	rm_server "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/logutil"
)

type resourceGroupProxyServer struct {
	*GrpcServer
	metadataManager *rm_server.Manager
}

func (s *resourceGroupProxyServer) getResourceManagerDelegateClient(ctx context.Context) (resource_manager.ResourceManagerClient, error) {
	forwardedHost, ok := s.GetServicePrimaryAddr(ctx, constant.ResourceManagerServiceName)
	if !ok || forwardedHost == "" {
		return nil, status.Errorf(codes.Unavailable, "resource manager service is not available")
	}
	if s.isLocalRequest(forwardedHost) {
		return nil, status.Errorf(codes.Unavailable, "Microservice mode should not support resource manager requests")
	}
	client, err := s.getDelegateClient(ctx, forwardedHost)
	if err != nil {
		return nil, err
	}
	return resource_manager.NewResourceManagerClient(client), nil
}

func (s *resourceGroupProxyServer) closeClient(ctx context.Context) {
	forwardedHost, ok := s.GetServicePrimaryAddr(ctx, constant.ResourceManagerServiceName)
	if !ok || forwardedHost == "" {
		log.Warn("resource manager service address is not found when closing delegate client")
		return
	}
	s.closeDelegateClient(forwardedHost)
}

// ListResourceGroups implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) ListResourceGroups(ctx context.Context, req *resource_manager.ListResourceGroupsRequest) (*resource_manager.ListResourceGroupsResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.ListResourceGroups(ctx, req)
	if err != nil {
		s.closeClient(ctx)
		return nil, err
	}
	return resp, nil
}

// GetResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) GetResourceGroup(ctx context.Context, req *resource_manager.GetResourceGroupRequest) (*resource_manager.GetResourceGroupResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.GetResourceGroup(ctx, req)
	if err != nil {
		s.closeClient(ctx)
		return nil, err
	}
	return resp, nil
}

// AddResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) AddResourceGroup(_ context.Context, req *resource_manager.PutResourceGroupRequest) (*resource_manager.PutResourceGroupResponse, error) {
	if req.GetGroup() == nil {
		return nil, status.Error(codes.InvalidArgument, "resource group is required")
	}
	if s.metadataManager == nil {
		return nil, status.Error(codes.Internal, "resource group metadata manager is not initialized")
	}
	if err := s.metadataManager.AddResourceGroup(req.GetGroup()); err != nil {
		return nil, err
	}
	return &resource_manager.PutResourceGroupResponse{Body: "Success!"}, nil
}

// ModifyResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) ModifyResourceGroup(_ context.Context, req *resource_manager.PutResourceGroupRequest) (*resource_manager.PutResourceGroupResponse, error) {
	if req.GetGroup() == nil {
		return nil, status.Error(codes.InvalidArgument, "resource group is required")
	}
	if s.metadataManager == nil {
		return nil, status.Error(codes.Internal, "resource group metadata manager is not initialized")
	}
	if err := s.metadataManager.ModifyResourceGroup(req.GetGroup()); err != nil {
		return nil, err
	}
	return &resource_manager.PutResourceGroupResponse{Body: "Success!"}, nil
}

// DeleteResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) DeleteResourceGroup(_ context.Context, req *resource_manager.DeleteResourceGroupRequest) (*resource_manager.DeleteResourceGroupResponse, error) {
	if s.metadataManager == nil {
		return nil, status.Error(codes.Internal, "resource group metadata manager is not initialized")
	}
	if err := s.metadataManager.DeleteResourceGroup(
		rm_server.ExtractKeyspaceID(req.GetKeyspaceId()),
		req.GetResourceGroupName(),
	); err != nil {
		return nil, err
	}
	return &resource_manager.DeleteResourceGroupResponse{Body: "Success!"}, nil
}

// AcquireTokenBuckets implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) AcquireTokenBuckets(stream resource_manager.ResourceManager_AcquireTokenBucketsServer) error {
	delegateClient, err := s.getResourceManagerDelegateClient(stream.Context())
	if err != nil {
		return err
	}
	delegateStream, err := delegateClient.AcquireTokenBuckets(stream.Context())
	if err != nil {
		s.closeClient(stream.Context())
		return err
	}

	errCh := make(chan error, 1)
	var reportOnce sync.Once
	reportErr := func(err error) {
		if err != nil {
			s.closeClient(stream.Context())
		}
		reportOnce.Do(func() {
			errCh <- err
		})
	}
	var wg sync.WaitGroup
	wg.Add(1)

	// client -> server
	go func() {
		defer logutil.LogPanic()
		defer wg.Done()
		defer func() { _ = delegateStream.CloseSend() }()
		for {
			in, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					// The client has finished sending messages.
					break
				}
				reportErr(err)
				return
			}
			if err := delegateStream.Send(in); err != nil {
				reportErr(err)
				return
			}
		}
	}()

	// server -> client
	for {
		out, err := delegateStream.Recv()
		if err != nil {
			if err == io.EOF {
				// The server has finished sending messages.
				break
			}
			reportErr(err)
			break
		}
		if err := stream.Send(out); err != nil {
			reportErr(err)
			break
		}
	}

	wg.Wait()
	reportErr(nil)

	return <-errCh
}
