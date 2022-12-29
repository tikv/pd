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

// KeyspaceClient manages keyspace metadata.
type ResourceManagerClient interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) (string, error)
	ModifyResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
}

// leaderClient gets the client of current PD leader.
func (c *client) resouceManagerClient() rmpb.ResourceManagerClient {
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		return rmpb.NewResourceManagerClient(cc.(*grpc.ClientConn))
	}
	return nil
}

// ListResourceGroups loads and returns target keyspace's metadata.
func (c *client) ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	req := &rmpb.ListResourceGroupsRequest{}
	resp, err := c.resouceManagerClient().ListResourceGroups(ctx, req)
	if err != nil {
		return nil, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errors.Errorf("[pd]" + resErr.Message)
	}
	return resp.GetGroups(), nil
}

func (c *client) GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error) {
	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
	}
	resp, err := c.resouceManagerClient().GetResourceGroup(ctx, req)
	if err != nil {
		return nil, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return nil, errors.Errorf("[pd]" + resErr.Message)
	}
	return resp.GetGroup(), nil
}

func (c *client) AddResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) (string, error) {
	return c.putResourceGroup(ctx, resourceGroupName, settings, 0 /* type add resource group */)
}

func (c *client) ModifyResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) (string, error) {
	return c.putResourceGroup(ctx, resourceGroupName, settings, 1 /* type modify resource group */)
}

func (c *client) putResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings, typ int) (str string, err error) {
	group := &rmpb.ResourceGroup{
		Name:     resourceGroupName,
		Settings: settings,
	}
	req := &rmpb.PutResourceGroupRequest{
		Group: group,
	}
	var resp *rmpb.PutResourceGroupResponse
	if typ == 0 {
		resp, err = c.resouceManagerClient().AddResourceGroup(ctx, req)
	} else {
		resp, err = c.resouceManagerClient().ModifyResourceGroup(ctx, req)
	}
	if err != nil {
		return str, err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return str, errors.Errorf("[pd]" + resErr.Message)
	}
	str = resp.GetBody()
	return
}

func (c *client) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	req := &rmpb.DeleteResourceGroupRequest{
		ResourceGroupName: resourceGroupName,
	}
	resp, err := c.resouceManagerClient().DeleteResourceGroup(ctx, req)
	if err != nil {
		return "", err
	}
	resErr := resp.GetError()
	if resErr != nil {
		return "", errors.Errorf("[pd]" + resErr.Message)
	}
	return resp.GetBody(), nil
}

func (c *client) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	req := &tokenRequest{
		done:       make(chan error, 1),
		requestCtx: ctx,
		clientCtx:  c.ctx,
	}
	req.Requeset = request
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
	go c.handleResouceTokenDispatcher(dispatcherCtx, dispatcher.tokenBatchController)
	c.tokenDispatcher = dispatcher
}

func (c *client) handleResouceTokenDispatcher(dispatcherCtx context.Context, tbc *tokenBatchController) {
	var connection resourceManagerConnectionContext
	if err := c.tryResourceManagerConnect(dispatcherCtx, &connection); err != nil {
		log.Warn("get stream error", zap.Error(err))
	}

	for {
		var firstTSORequest *tokenRequest
		select {
		case <-dispatcherCtx.Done():
			return
		case firstTSORequest = <-tbc.tokenRequestCh:
		}
		stream, streamCtx, cancel := connection.stream, connection.ctx, connection.cancel
		if stream == nil {
			c.tryResourceManagerConnect(dispatcherCtx, &connection)
			c.finishTokenRequest(firstTSORequest, nil, errors.Errorf("no stream"))
			continue
		}
		select {
		case <-streamCtx.Done():
			log.Info("[pd] resource manager stream is canceled")
			cancel()
			stream = nil
			continue
		default:
		}
		err := c.processTokenRequests(stream, firstTSORequest)
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
		c.finishTokenRequest(t, nil, err)
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		c.finishTokenRequest(t, nil, err)
		return err
	}
	if resp.GetError() != nil {
		return errors.Errorf("[pd]" + resp.GetError().Message)
	}
	tokenBuckets := resp.GetResponses()
	c.finishTokenRequest(t, tokenBuckets, nil)
	return nil
}

func (c *client) finishTokenRequest(t *tokenRequest, tokenBuckets []*rmpb.TokenBucketResponse, err error) {
	t.TokenBuckets = tokenBuckets
	t.done <- err
}

func (c *client) tryResourceManagerConnect(ctx context.Context, connection *resourceManagerConnectionContext) error {
	var (
		err    error
		stream rmpb.ResourceManager_AcquireTokenBucketsClient
	)
	for i := 0; i < maxRetryTimes; i++ {
		cctx, cancel := context.WithCancel(ctx)
		stream, err = c.resouceManagerClient().AcquireTokenBuckets(cctx)
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
