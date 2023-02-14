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
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type actionType int

const (
	add                     actionType = 0
	modify                  actionType = 1
	groupSettingsPathPrefix            = "resource_group/settings"
	// errNotLeaderMsg is returned when the requested server is not the leader.
	errNotLeaderMsg = "not leader"
)

// ResourceManagerClient manages resource group info and token request.
type ResourceManagerClient interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	WatchResourceGroup(ctx context.Context, revision int64) (chan []*rmpb.ResourceGroup, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
}

// resourceManagerClient gets the ResourceManager client of current PD leader.
func (c *client) resourceManagerClient() rmpb.ResourceManagerClient {
	if cc, err := c.getOrCreateGRPCConn(c.GetLeaderAddr()); err == nil {
		return rmpb.NewResourceManagerClient(cc)
	}
	return nil
}

// gRPCErrorHandler is used to handle the gRPC error returned by the resource manager service.
func (c *client) gRPCErrorHandler(err error) {
	if strings.Contains(err.Error(), errNotLeaderMsg) {
		c.ScheduleCheckLeader()
	}
}

// ListResourceGroups loads and returns all metadata of resource groups.
func (c *client) ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	req := &rmpb.ListResourceGroupsRequest{}
	resp, err := c.resourceManagerClient().ListResourceGroups(ctx, req)
	if err != nil {
		c.gRPCErrorHandler(err)
		return nil, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errors.Errorf("[resource_manager] %s", resErr.Message)
	}
	return resp.GetGroups(), nil
}

func (c *client) GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error) {
	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
	}
	resp, err := c.resourceManagerClient().GetResourceGroup(ctx, req)
	if err != nil {
		c.gRPCErrorHandler(err)
		return nil, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errors.Errorf("[resource_manager] %s", resErr.Message)
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
		c.gRPCErrorHandler(err)
		return str, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return str, errors.Errorf("[resource_manager] %s", resErr.Message)
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
		c.gRPCErrorHandler(err)
		return "", err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return "", errors.Errorf("[resource_manager] %s", resErr.Message)
	}
	return resp.GetBody(), nil
}

// WatchResourceGroup [just for TEST] watches resource groups changes.
// It returns a stream of slices of resource groups.
// The first message in stream contains all current resource groups,
// all subsequent messages contains new events[PUT/DELETE] for all resource groups.
func (c *client) WatchResourceGroup(ctx context.Context, revision int64) (chan []*rmpb.ResourceGroup, error) {
	configChan, err := c.WatchGlobalConfig(ctx, groupSettingsPathPrefix, revision)
	if err != nil {
		return nil, err
	}
	resourceGroupWatcherChan := make(chan []*rmpb.ResourceGroup)
	go func() {
		defer func() {
			close(resourceGroupWatcherChan)
			if r := recover(); r != nil {
				log.Error("[pd] panic in ResourceManagerClient `WatchResourceGroups`", zap.Any("error", r))
				return
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-configChan:
				if !ok {
					return
				}
				groups := make([]*rmpb.ResourceGroup, 0, len(res))
				for _, item := range res {
					switch item.EventType {
					case pdpb.EventType_PUT:
						group := &rmpb.ResourceGroup{}
						if err := proto.Unmarshal([]byte(item.Value), group); err != nil {
							return
						}
						groups = append(groups, group)
					case pdpb.EventType_DELETE:
						continue
					}
				}
				resourceGroupWatcherChan <- groups
			}
		}
	}()
	return resourceGroupWatcherChan, err
}

func (c *client) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	req := &tokenRequest{
		done:       make(chan error, 1),
		requestCtx: ctx,
		clientCtx:  c.ctx,
		Request:    request,
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
	Request      *rmpb.TokenBucketsRequest
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

func (cc *resourceManagerConnectionContext) reset() {
	cc.stream = nil
	if cc.cancel != nil {
		cc.cancel()
	}
}

func (c *client) createTokenDispatcher() {
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
	var (
		connection   resourceManagerConnectionContext
		firstRequest *tokenRequest
		stream       rmpb.ResourceManager_AcquireTokenBucketsClient
		streamCtx    context.Context
		toReconnect  bool
		err          error
	)
	if err = c.tryResourceManagerConnect(dispatcherCtx, &connection); err != nil {
		log.Warn("[resource_manager] get token stream error", zap.Error(err))
	}
	for {
		// Fetch the request from the channel.
		select {
		case <-dispatcherCtx.Done():
			return
		case firstRequest = <-tbc.tokenRequestCh:
		}
		// Try to get a stream connection.
		stream, streamCtx = connection.stream, connection.ctx
		select {
		case <-c.updateTokenConnectionCh:
			toReconnect = true
		default:
			toReconnect = stream == nil
		}
		// If the stream is nil or the leader has changed, try to reconnect.
		if toReconnect {
			connection.reset()
			c.tryResourceManagerConnect(dispatcherCtx, &connection)
			log.Info("[resource_manager] token leader may change, try to reconnect the stream")
			stream, streamCtx = connection.stream, connection.ctx
		}
		// If the stream is still nil, return an error.
		if stream == nil {
			firstRequest.done <- errors.Errorf("failed to get the stream connection")
			c.ScheduleCheckLeader()
			connection.reset()
			continue
		}
		select {
		case <-streamCtx.Done():
			connection.reset()
			log.Info("[resource_manager] token stream is canceled")
			continue
		default:
		}
		if err = c.processTokenRequests(stream, firstRequest); err != nil {
			c.ScheduleCheckLeader()
			connection.reset()
			log.Info("[resource_manager] token request error", zap.Error(err))
		}
	}
}

func (c *client) processTokenRequests(stream rmpb.ResourceManager_AcquireTokenBucketsClient, t *tokenRequest) error {
	req := t.Request
	if err := stream.Send(req); err != nil {
		err = errors.WithStack(err)
		t.done <- err
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		c.gRPCErrorHandler(err)
		err = errors.WithStack(err)
		t.done <- err
		return err
	}
	if resp.GetError() != nil {
		return errors.Errorf("[resource_manager] %s", resp.GetError().Message)
	}
	t.TokenBuckets = resp.GetResponses()
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

func (tbc *tokenBatchController) revokePendingTokenRequest(err error) {
	for i := 0; i < len(tbc.tokenRequestCh); i++ {
		req := <-tbc.tokenRequestCh
		req.done <- err
	}
}
