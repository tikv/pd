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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

type resourceGroupProxyServer struct {
	*GrpcServer
}

func (s *resourceGroupProxyServer) getResourceManagerDelegateClient(ctx context.Context) (resource_manager.ResourceManagerClient, error) {
	forwardedHost, ok := s.GetServicePrimaryAddr(ctx, constant.ResourceManagerServiceName)
	if !ok || forwardedHost == "" {
		return nil, status.Errorf(codes.Unavailable, "resource manager service is not available")
	}
	if s.isLocalRequest(forwardedHost) {
		return nil, status.Errorf(codes.Unavailable, "API mode should not support resource manager requests")
	}
	client, err := s.getDelegateClient(ctx, forwardedHost)
	if err != nil {
		return nil, err
	}
	return resource_manager.NewResourceManagerClient(client), nil
}

// ListResourceGroups implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) ListResourceGroups(ctx context.Context, req *resource_manager.ListResourceGroupsRequest) (*resource_manager.ListResourceGroupsResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.ListResourceGroups(ctx, req)
}

// GetResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) GetResourceGroup(ctx context.Context, req *resource_manager.GetResourceGroupRequest) (*resource_manager.GetResourceGroupResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.GetResourceGroup(ctx, req)
}

// AddResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) AddResourceGroup(ctx context.Context, req *resource_manager.PutResourceGroupRequest) (*resource_manager.PutResourceGroupResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.AddResourceGroup(ctx, req)
}

// ModifyResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) ModifyResourceGroup(ctx context.Context, req *resource_manager.PutResourceGroupRequest) (*resource_manager.PutResourceGroupResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.ModifyResourceGroup(ctx, req)
}

// DeleteResourceGroup implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) DeleteResourceGroup(ctx context.Context, req *resource_manager.DeleteResourceGroupRequest) (*resource_manager.DeleteResourceGroupResponse, error) {
	client, err := s.getResourceManagerDelegateClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.DeleteResourceGroup(ctx, req)
}

// AcquireTokenBuckets implements the resource_manager.ResourceManagerServer interface.
func (s *resourceGroupProxyServer) AcquireTokenBuckets(stream resource_manager.ResourceManager_AcquireTokenBucketsServer) error {
	delegateClient, err := s.getResourceManagerDelegateClient(stream.Context())
	if err != nil {
		return err
	}
	delegateStream, err := delegateClient.AcquireTokenBuckets(stream.Context())
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)

	// client -> server
	go func() {
		defer func() { _ = delegateStream.CloseSend() }()
		for {
			in, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					// The client has finished sending messages.
					break
				}
				errCh <- err
				return
			}
			if err := delegateStream.Send(in); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// server -> client
	go func() {
		for {
			out, err := delegateStream.Recv()
			if err != nil {
				if err == io.EOF {
					// The server has finished sending messages.
					break
				}
				errCh <- err
				return
			}
			if err := stream.Send(out); err != nil {
				errCh <- err
				return
			}
		}
	}()

	return <-errCh
}
