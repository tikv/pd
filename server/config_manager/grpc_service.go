// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package configmanager

import (
	"context"

	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var notLeaderError = status.Errorf(codes.Unavailable, "not leader")

// Create implements gRPC PDServer.
func (c *ConfigManager) Create(ctx context.Context, request *configpb.CreateRequest) (*configpb.CreateResponse, error) {
	if err := c.validateComponentRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if !c.svr.GetConfig().EnableDynamicConfig {
		component, componentID := request.Component, request.ComponentId
		lc, err := NewLocalConfig(request.Config, request.Version)
		if err != nil {
			log.Error("failed to update component config", zap.String("component", component), zap.String("component-id", componentID))
		}
		c.Lock()
		if localCfgs, ok := c.LocalCfgs[component]; ok {
			localCfgs[componentID] = lc
		} else {
			c.LocalCfgs[component] = make(map[string]*LocalConfig)
			c.LocalCfgs[component][componentID] = lc
		}
		c.Unlock()
		c.Persist(c.svr.GetStorage())
		return &configpb.CreateResponse{
			Header:  c.componentHeader(),
			Status:  &configpb.Status{Code: configpb.StatusCode_OK},
			Version: request.Version,
			Config:  request.Config,
		}, nil
	}

	version, config, status := c.CreateConfig(request.GetVersion(), request.GetComponent(), request.GetComponentId(), request.GetConfig())
	if status.GetCode() == configpb.StatusCode_OK {
		log.Info("component has registered", zap.String("component", request.GetComponent()), zap.String("component-id", request.GetComponentId()))
		c.Persist(c.svr.GetStorage())
	}

	return &configpb.CreateResponse{
		Header:  c.componentHeader(),
		Status:  status,
		Version: version,
		Config:  config,
	}, nil
}

// GetAll implements gRPC PDServer.
func (c *ConfigManager) GetAll(ctx context.Context, request *configpb.GetAllRequest) (*configpb.GetAllResponse, error) {
	if err := c.validateComponentRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if !c.svr.GetConfig().EnableDynamicConfig {
		return &configpb.GetAllResponse{
			Header: c.componentHeader(),
			Status: &configpb.Status{Code: configpb.StatusCode_OK},
		}, nil
	}

	localConfigs, status := c.GetAllConfig(ctx)
	return &configpb.GetAllResponse{
		Header:       c.componentHeader(),
		Status:       status,
		LocalConfigs: localConfigs,
	}, nil
}

// Get implements gRPC PDServer.
func (c *ConfigManager) Get(ctx context.Context, request *configpb.GetRequest) (*configpb.GetResponse, error) {
	if err := c.validateComponentRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if !c.svr.GetConfig().EnableDynamicConfig {
		return &configpb.GetResponse{
			Header: c.componentHeader(),
			Status: &configpb.Status{Code: configpb.StatusCode_OK},
		}, nil
	}

	version, config, status := c.GetConfig(request.GetVersion(), request.GetComponent(), request.GetComponentId())

	return &configpb.GetResponse{
		Header:  c.componentHeader(),
		Status:  status,
		Version: version,
		Config:  config,
	}, nil
}

// Update implements gRPC PDServer.
func (c *ConfigManager) Update(ctx context.Context, request *configpb.UpdateRequest) (*configpb.UpdateResponse, error) {
	if err := c.validateComponentRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if !c.svr.GetConfig().EnableDynamicConfig {
		return &configpb.UpdateResponse{
			Header: c.componentHeader(),
			Status: &configpb.Status{Code: configpb.StatusCode_OK},
		}, nil
	}

	version, status := c.UpdateConfig(request.GetKind(), request.GetVersion(), request.GetEntries())
	if status.GetCode() == configpb.StatusCode_OK {
		log.Info("config has updated in config manager", zap.Reflect("entries", request.GetEntries()))
		c.Persist(c.svr.GetStorage())
	}

	return &configpb.UpdateResponse{
		Header:  c.componentHeader(),
		Status:  status,
		Version: version,
	}, nil
}

// Delete implements gRPC PDServer.
func (c *ConfigManager) Delete(ctx context.Context, request *configpb.DeleteRequest) (*configpb.DeleteResponse, error) {
	if err := c.validateComponentRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if !c.svr.GetConfig().EnableDynamicConfig {
		return &configpb.DeleteResponse{
			Header: c.componentHeader(),
			Status: &configpb.Status{Code: configpb.StatusCode_OK},
		}, nil
	}

	status := c.DeleteConfig(request.GetKind(), request.GetVersion())
	if status.GetCode() == configpb.StatusCode_OK {
		c.Persist(c.svr.GetStorage())
	}

	return &configpb.DeleteResponse{
		Header: c.componentHeader(),
		Status: status,
	}, nil
}

func (c *ConfigManager) componentHeader() *configpb.Header {
	return &configpb.Header{ClusterId: c.svr.ClusterID()}
}

func (c *ConfigManager) validateComponentRequest(header *configpb.Header) error {
	if c.svr.IsClosed() || !c.svr.GetMember().IsLeader() {
		return errors.WithStack(notLeaderError)
	}
	clusterID := c.svr.ClusterID()
	if header.GetClusterId() != clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", clusterID, header.GetClusterId())
	}
	return nil
}
