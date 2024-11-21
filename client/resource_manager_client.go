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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

type actionType int

const (
	add                        actionType = 0
	modify                     actionType = 1
	groupSettingsPathPrefix               = "resource_group/settings"
	controllerConfigPathPrefix            = "resource_group/controller"
)

// GroupSettingsPathPrefixBytes is used to watch or get resource groups.
var GroupSettingsPathPrefixBytes = []byte(groupSettingsPathPrefix)

// ControllerConfigPathPrefixBytes is used to watch or get controller config.
var ControllerConfigPathPrefixBytes = []byte(controllerConfigPathPrefix)

// ResourceManagerClient manages resource group info and token request.
type ResourceManagerClient interface {
	ListResourceGroups(ctx context.Context, opts ...GetResourceGroupOption) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...GetResourceGroupOption) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
	Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error)
}

// GetResourceGroupOp represents available options when getting resource group.
type GetResourceGroupOp struct {
	withRUStats bool
}

// GetResourceGroupOption configures GetResourceGroupOp.
type GetResourceGroupOption func(*GetResourceGroupOp)

// WithRUStats specifies to return resource group with ru statistics data.
func WithRUStats(op *GetResourceGroupOp) {
	op.withRUStats = true
}

// resourceManagerClient gets the ResourceManager client of current PD leader.
func (c *innerClient) resourceManagerClient() (rmpb.ResourceManagerClient, error) {
	cc, err := c.getOrCreateGRPCConn()
	if err != nil {
		return nil, err
	}
	return rmpb.NewResourceManagerClient(cc), nil
}

// ListResourceGroups loads and returns all metadata of resource groups.
func (c *client) ListResourceGroups(ctx context.Context, ops ...GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	cc, err := c.inner.resourceManagerClient()
	if err != nil {
		return nil, err
	}
	getOp := &GetResourceGroupOp{}
	for _, op := range ops {
		op(getOp)
	}
	req := &rmpb.ListResourceGroupsRequest{
		WithRuStats: getOp.withRUStats,
	}
	resp, err := cc.ListResourceGroups(ctx, req)
	if err != nil {
		c.inner.gRPCErrorHandler(err)
		return nil, errs.ErrClientListResourceGroup.FastGenByArgs(err.Error())
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errs.ErrClientListResourceGroup.FastGenByArgs(resErr.Message)
	}
	return resp.GetGroups(), nil
}

// GetResourceGroup implements the ResourceManagerClient interface.
func (c *client) GetResourceGroup(ctx context.Context, resourceGroupName string, ops ...GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	cc, err := c.inner.resourceManagerClient()
	if err != nil {
		return nil, err
	}
	getOp := &GetResourceGroupOp{}
	for _, op := range ops {
		op(getOp)
	}
	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
		WithRuStats:       getOp.withRUStats,
	}
	resp, err := cc.GetResourceGroup(ctx, req)
	if err != nil {
		c.inner.gRPCErrorHandler(err)
		return nil, &errs.ErrClientGetResourceGroup{ResourceGroupName: resourceGroupName, Cause: err.Error()}
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, &errs.ErrClientGetResourceGroup{ResourceGroupName: resourceGroupName, Cause: resErr.Message}
	}
	return resp.GetGroup(), nil
}

// AddResourceGroup implements the ResourceManagerClient interface.
func (c *client) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	return c.putResourceGroup(ctx, metaGroup, add)
}

// ModifyResourceGroup implements the ResourceManagerClient interface.
func (c *client) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	return c.putResourceGroup(ctx, metaGroup, modify)
}

func (c *client) putResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup, typ actionType) (string, error) {
	cc, err := c.inner.resourceManagerClient()
	if err != nil {
		return "", err
	}
	req := &rmpb.PutResourceGroupRequest{
		Group: metaGroup,
	}
	var resp *rmpb.PutResourceGroupResponse
	switch typ {
	case add:
		resp, err = cc.AddResourceGroup(ctx, req)
	case modify:
		resp, err = cc.ModifyResourceGroup(ctx, req)
	}
	if err != nil {
		c.inner.gRPCErrorHandler(err)
		return "", err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return "", errors.Errorf("[resource_manager] %s", resErr.Message)
	}
	return resp.GetBody(), nil
}

// DeleteResourceGroup implements the ResourceManagerClient interface.
func (c *client) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	cc, err := c.inner.resourceManagerClient()
	if err != nil {
		return "", err
	}
	req := &rmpb.DeleteResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
	}
	resp, err := cc.DeleteResourceGroup(ctx, req)
	if err != nil {
		c.inner.gRPCErrorHandler(err)
		return "", err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return "", errors.Errorf("[resource_manager] %s", resErr.Message)
	}
	return resp.GetBody(), nil
}

// LoadResourceGroups implements the ResourceManagerClient interface.
func (c *client) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	resp, err := c.Get(ctx, GroupSettingsPathPrefixBytes, opt.WithPrefix())
	if err != nil {
		return nil, 0, err
	}
	if resp.Header.Error != nil {
		return nil, resp.Header.Revision, errors.New(resp.Header.Error.Message)
	}
	groups := make([]*rmpb.ResourceGroup, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		group := &rmpb.ResourceGroup{}
		if err := proto.Unmarshal(item.Value, group); err != nil {
			continue
		}
		groups = append(groups, group)
	}
	return groups, resp.Header.Revision, nil
}

// AcquireTokenBuckets implements the ResourceManagerClient interface.
func (c *client) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	req := &tokenRequest{
		done:       make(chan error, 1),
		requestCtx: ctx,
		clientCtx:  c.inner.ctx,
		Request:    request,
	}
	c.inner.tokenDispatcher.tokenBatchController.tokenRequestCh <- req
	grantedTokens, err := req.wait()
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

func (req *tokenRequest) wait() (tokenBuckets []*rmpb.TokenBucketResponse, err error) {
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

func (c *innerClient) createTokenDispatcher() {
	dispatcherCtx, dispatcherCancel := context.WithCancel(c.ctx)
	dispatcher := &tokenDispatcher{
		dispatcherCancel: dispatcherCancel,
		tokenBatchController: newTokenBatchController(
			make(chan *tokenRequest, 1)),
	}
	c.wg.Add(1)
	go c.handleResourceTokenDispatcher(dispatcherCtx, dispatcher.tokenBatchController)
	c.tokenDispatcher = dispatcher
}

func (c *innerClient) handleResourceTokenDispatcher(dispatcherCtx context.Context, tbc *tokenBatchController) {
	defer func() {
		log.Info("[resource manager] exit resource token dispatcher")
		c.wg.Done()
	}()
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
			if err := c.tryResourceManagerConnect(dispatcherCtx, &connection); err != nil {
				log.Error("[resource_manager] try to connect token leader failed", errs.ZapError(err))
			}
			log.Info("[resource_manager] token leader may change, try to reconnect the stream")
			stream, streamCtx = connection.stream, connection.ctx
		}
		// If the stream is still nil, return an error.
		if stream == nil {
			firstRequest.done <- errors.Errorf("failed to get the stream connection")
			c.pdSvcDiscovery.ScheduleCheckMemberChanged()
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
			c.pdSvcDiscovery.ScheduleCheckMemberChanged()
			connection.reset()
			log.Info("[resource_manager] token request error", zap.Error(err))
		}
	}
}

func (c *innerClient) processTokenRequests(stream rmpb.ResourceManager_AcquireTokenBucketsClient, t *tokenRequest) error {
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

func (c *innerClient) tryResourceManagerConnect(ctx context.Context, connection *resourceManagerConnectionContext) error {
	var (
		err    error
		stream rmpb.ResourceManager_AcquireTokenBucketsClient
	)
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for range maxRetryTimes {
		cc, err := c.resourceManagerClient()
		if err != nil {
			continue
		}
		cctx, cancel := context.WithCancel(ctx)
		stream, err = cc.AcquireTokenBuckets(cctx)
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
		case <-ticker.C:
		}
	}
	return err
}

func (tbc *tokenBatchController) revokePendingTokenRequest(err error) {
	for range len(tbc.tokenRequestCh) {
		req := <-tbc.tokenRequestCh
		req.done <- err
	}
}
