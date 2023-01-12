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

package pd

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type actionType int

const (
	add    actionType = 0
	modify actionType = 1
)

// ResourceManagerClient manages resource group info and token request.
type ResourceManagerClient interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
}

// resourceManagerClient gets the ResourceManager client of current PD leader.
func (c *client) resourceManagerClient() rmpb.ResourceManagerClient {
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		return rmpb.NewResourceManagerClient(cc.(*grpc.ClientConn))
	}
	return nil
}

// ListResourceGroups loads and returns all metadata of resource groups.
func (c *client) ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	req := &rmpb.ListResourceGroupsRequest{}
	resp, err := c.resourceManagerClient().ListResourceGroups(ctx, req)
	if err != nil {
		return nil, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errors.Errorf("[resource_manager]" + resErr.Message)
	}
	return resp.GetGroups(), nil
}

func (c *client) GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error) {
	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
	}
	resp, err := c.resourceManagerClient().GetResourceGroup(ctx, req)
	if err != nil {
		return nil, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errors.Errorf("[resource_manager]" + resErr.Message)
	}
	return resp.GetGroup(), nil
}

func (c *client) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	return c.putResourceGroup(ctx, metaGroup, add)
}

func (c *client) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	return c.putResourceGroup(ctx, metaGroup, modify)
}

func (c *client) putResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup, typ actionType) (str string, err error) {
	req := &rmpb.PutResourceGroupRequest{
		Group: metaGroup,
	}
	var resp *rmpb.PutResourceGroupResponse
	switch typ {
	case add:
		resp, err = c.resourceManagerClient().AddResourceGroup(ctx, req)
	case modify:
		resp, err = c.resourceManagerClient().ModifyResourceGroup(ctx, req)
	}
	if err != nil {
		return str, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return str, errors.Errorf("[resource_manager]" + resErr.Message)
	}
	str = resp.GetBody()
	return
}

func (c *client) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	req := &rmpb.DeleteResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
	}
	resp, err := c.resourceManagerClient().DeleteResourceGroup(ctx, req)
	if err != nil {
		return "", err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return "", errors.Errorf("[resource_manager]" + resErr.Message)
	}
	return resp.GetBody(), nil
}

func (c *client) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	req := &tokenRequest{
		done:       make(chan error, 1),
		requestCtx: ctx,
		clientCtx:  c.ctx,
		Requeset:   request,
	}
	c.tokenDispatcher.tokenBatchController.tokenRequestCh <- req
	grantedTokens, err := req.Wait()
	if err != nil {
		return nil, err
	}
	return grantedTokens, err
}

type tokenRequest struct {
	clientCtx    context.Context
	requestCtx   context.Context
	done         chan error
	Requeset     *rmpb.TokenBucketsRequest
	TokenBuckets []*rmpb.TokenBucketResponse
}

func (req *tokenRequest) Wait() (tokenBuckets []*rmpb.TokenBucketResponse, err error) {
	select {
	case err = <-req.done:
		err = errors.WithStack(err)
		if err != nil {
			return nil, err
		}
		tokenBuckets = req.TokenBuckets
		return
	case <-req.requestCtx.Done():
		return nil, errors.WithStack(req.requestCtx.Err())
	case <-req.clientCtx.Done():
		return nil, errors.WithStack(req.clientCtx.Err())
	}
}

type tokenBatchController struct {
	tokenRequestCh chan *tokenRequest
}

func newTokenBatchController(tokenRequestCh chan *tokenRequest) *tokenBatchController {
	return &tokenBatchController{
		tokenRequestCh: tokenRequestCh,
	}
}

type tokenDispatcher struct {
	dispatcherCancel     context.CancelFunc
	tokenBatchController *tokenBatchController
}

type resourceManagerConnectionContext struct {
	stream rmpb.ResourceManager_AcquireTokenBucketsClient
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *client) createTokenispatcher() {
	dispatcherCtx, dispatcherCancel := context.WithCancel(c.ctx)
	dispatcher := &tokenDispatcher{
		dispatcherCancel: dispatcherCancel,
		tokenBatchController: newTokenBatchController(
			make(chan *tokenRequest, 1)),
	}
	go c.handleResourceTokenDispatcher(dispatcherCtx, dispatcher.tokenBatchController)
	c.tokenDispatcher = dispatcher
}

func (c *client) handleResourceTokenDispatcher(dispatcherCtx context.Context, tbc *tokenBatchController) {
	var connection resourceManagerConnectionContext
	if err := c.tryResourceManagerConnect(dispatcherCtx, &connection); err != nil {
		log.Warn("get stream error", zap.Error(err))
	}

	for {
		var firstRequest *tokenRequest
		select {
		case <-dispatcherCtx.Done():
			return
		case firstRequest = <-tbc.tokenRequestCh:
		}
		stream, streamCtx, cancel := connection.stream, connection.ctx, connection.cancel
		if stream == nil {
			c.tryResourceManagerConnect(dispatcherCtx, &connection)
			firstRequest.done <- errors.Errorf("no stream")
			continue
		}
		select {
		case <-streamCtx.Done():
			log.Info("[resource_manager] resource manager stream is canceled")
			cancel()
			connection.stream = nil
			continue
		default:
		}
		err := c.processTokenRequests(stream, firstRequest)
		if err != nil {
			log.Info("processTokenRequests error", zap.Error(err))
			cancel()
			connection.stream = nil
		}
	}
}

func (c *client) processTokenRequests(stream rmpb.ResourceManager_AcquireTokenBucketsClient, t *tokenRequest) error {
	req := t.Requeset
	if err := stream.Send(req); err != nil {
		err = errors.WithStack(err)
		t.done <- err
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		t.done <- err
		return err
	}
	if resp.GetError() != nil {
		return errors.Errorf("[resource_manager]" + resp.GetError().Message)
	}
	tokenBuckets := resp.GetResponses()
	t.TokenBuckets = tokenBuckets
	t.done <- nil
	return nil
}

func (c *client) tryResourceManagerConnect(ctx context.Context, connection *resourceManagerConnectionContext) error {
	var (
		err    error
		stream rmpb.ResourceManager_AcquireTokenBucketsClient
	)
	for i := 0; i < maxRetryTimes; i++ {
		cctx, cancel := context.WithCancel(ctx)
		stream, err = c.resourceManagerClient().AcquireTokenBuckets(cctx)
		if err == nil && stream != nil {
			connection.cancel = cancel
			connection.ctx = cctx
			connection.stream = stream
			return nil
		}
		cancel()
		select {
		case <-ctx.Done():
			return err
		case <-time.After(retryInterval):
		}
	}
	return err
}
