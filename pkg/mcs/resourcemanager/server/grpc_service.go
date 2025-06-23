// Copyright 2022 TiKV Project Authors.
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

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

var _ rmpb.ResourceManagerServer = (*Service)(nil)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(*Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (dummyRestService) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// Service is the gRPC service for resource manager.
type Service struct {
	ctx     context.Context
	manager *Manager
	// settings
}

// NewService creates a new resource manager service.
func NewService[T ConfigProvider](svr bs.Server) registry.RegistrableService {
	manager := NewManager[T](svr)

	return &Service{
		ctx:     svr.Context(),
		manager: manager,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	rmpb.RegisterResourceManagerServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) error {
	handler, group := SetUpRestHandler(s)
	return apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

// GetManager returns the resource manager.
func (s *Service) GetManager() *Manager {
	return s.manager
}

func (s *Service) checkServing() error {
	if s.manager == nil || s.manager.srv == nil || !s.manager.srv.IsServing() {
		return errs.ErrNotLeader
	}
	return nil
}

// GetResourceGroup implements ResourceManagerServer.GetResourceGroup.
func (s *Service) GetResourceGroup(_ context.Context, req *rmpb.GetResourceGroupRequest) (*rmpb.GetResourceGroupResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	rg, err := s.manager.GetResourceGroup(ExtractKeyspaceID(req.GetKeyspaceId()), req.ResourceGroupName, req.WithRuStats)
	if err != nil {
		return nil, err
	}
	if rg == nil {
		return nil, errs.ErrResourceGroupNotExists.FastGenByArgs(req.ResourceGroupName)
	}
	return &rmpb.GetResourceGroupResponse{
		Group: rg.IntoProtoResourceGroup(),
	}, nil
}

// ListResourceGroups implements ResourceManagerServer.ListResourceGroups.
func (s *Service) ListResourceGroups(_ context.Context, req *rmpb.ListResourceGroupsRequest) (*rmpb.ListResourceGroupsResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	groups, err := s.manager.GetResourceGroupList(ExtractKeyspaceID(req.GetKeyspaceId()), req.WithRuStats)
	if err != nil {
		return nil, err
	}
	resp := &rmpb.ListResourceGroupsResponse{
		Groups: make([]*rmpb.ResourceGroup, 0, len(groups)),
	}
	for _, group := range groups {
		resp.Groups = append(resp.Groups, group.IntoProtoResourceGroup())
	}
	return resp, nil
}

// AddResourceGroup implements ResourceManagerServer.AddResourceGroup.
func (s *Service) AddResourceGroup(_ context.Context, req *rmpb.PutResourceGroupRequest) (*rmpb.PutResourceGroupResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	err := s.manager.AddResourceGroup(req.GetGroup())
	if err != nil {
		return nil, err
	}
	return &rmpb.PutResourceGroupResponse{Body: "Success!"}, nil
}

// DeleteResourceGroup implements ResourceManagerServer.DeleteResourceGroup.
func (s *Service) DeleteResourceGroup(_ context.Context, req *rmpb.DeleteResourceGroupRequest) (*rmpb.DeleteResourceGroupResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	err := s.manager.DeleteResourceGroup(ExtractKeyspaceID(req.GetKeyspaceId()), req.ResourceGroupName)
	if err != nil {
		return nil, err
	}
	return &rmpb.DeleteResourceGroupResponse{Body: "Success!"}, nil
}

// ModifyResourceGroup implements ResourceManagerServer.ModifyResourceGroup.
func (s *Service) ModifyResourceGroup(_ context.Context, req *rmpb.PutResourceGroupRequest) (*rmpb.PutResourceGroupResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	err := s.manager.ModifyResourceGroup(req.GetGroup())
	if err != nil {
		return nil, err
	}
	return &rmpb.PutResourceGroupResponse{Body: "Success!"}, nil
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
		failpoint.Inject("acquireFailed", func() {
			err = errors.New("error")
		})
		if err != nil {
			return errors.WithStack(err)
		}
		if err := s.checkServing(); err != nil {
			return err
		}
		var (
			targetPeriodMs = request.GetTargetRequestPeriodMs()
			clientUniqueID = request.GetClientUniqueId()
			resps          = &rmpb.TokenBucketsResponse{}
			logFields      = make([]zap.Field, 2)
		)
		for _, req := range request.Requests {
			keyspaceID := ExtractKeyspaceID(req.GetKeyspaceId())
			resourceGroupName := req.GetResourceGroupName()
			logFields[0] = zap.Uint32("keyspace-id", keyspaceID)
			logFields[1] = zap.String("resource-group", resourceGroupName)
			// Get keyspace resource group manager to apply service limit later.
			krgm := s.manager.getKeyspaceResourceGroupManager(keyspaceID)
			if krgm == nil {
				log.Warn("keyspace resource group manager not found", logFields...)
				continue
			}
			// Get the resource group from manager to acquire token buckets.
			rg, err := s.manager.GetMutableResourceGroup(keyspaceID, resourceGroupName)
			if rg == nil {
				log.Warn("resource group not found", append(logFields, zap.Error(err))...)
				continue
			}
			// Send the consumption to update the metrics.
			err = s.manager.dispatchConsumption(req)
			if err != nil {
				return err
			}
			if req.GetIsBackground() {
				continue
			}
			now := time.Now()
			resp := &rmpb.TokenBucketResponse{
				ResourceGroupName: rg.Name,
			}
			switch rg.Mode {
			case rmpb.GroupMode_RUMode:
				var tokens *rmpb.GrantedRUTokenBucket
				for _, re := range req.GetRuItems().GetRequestRU() {
					if re.Type == rmpb.RequestUnitType_RU {
						tokens = rg.RequestRU(now, re.Value, targetPeriodMs, clientUniqueID, krgm.getServiceLimiter())
					}
					if tokens == nil {
						continue
					}
					resp.GrantedRUTokens = append(resp.GrantedRUTokens, tokens)
				}
			case rmpb.GroupMode_RawMode:
				log.Warn("not supports the resource type",
					append(logFields, zap.String("mode", rmpb.GroupMode_name[int32(rmpb.GroupMode_RawMode)]))...)
				continue
			}
			log.Debug("finish token request from", logFields...)
			resps.Responses = append(resps.Responses, resp)
		}
		if err := stream.Send(resps); err != nil {
			return errors.WithStack(err)
		}
	}
}
