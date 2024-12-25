package pd

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	cb "github.com/tikv/pd/client/pkg/circuitbreaker"
	sd "github.com/tikv/pd/client/servicediscovery"
)

const (
	dispatchRetryDelay = 50 * time.Millisecond
	dispatchRetryCount = 2
)

type innerClient struct {
	keyspaceID               uint32
	svrUrls                  []string
	pdSvcDiscovery           sd.ServiceDiscovery
	tokenDispatcher          *tokenDispatcher
	regionMetaCircuitBreaker *cb.CircuitBreaker[*pdpb.GetRegionResponse]

	// For service mode switching.
	serviceModeKeeper

	// For internal usage.
	updateTokenConnectionCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	tlsCfg *tls.Config
	option *opt.Option
}

func (c *innerClient) init(updateKeyspaceIDCb sd.UpdateKeyspaceIDFunc) error {
	c.pdSvcDiscovery = sd.NewPDServiceDiscovery(
		c.ctx, c.cancel, &c.wg, c.setServiceMode,
		updateKeyspaceIDCb, c.keyspaceID, c.svrUrls, c.tlsCfg, c.option)
	if err := c.setup(); err != nil {
		c.cancel()
		if c.pdSvcDiscovery != nil {
			c.pdSvcDiscovery.Close()
		}
		return err
	}
	c.regionMetaCircuitBreaker = cb.NewCircuitBreaker[*pdpb.GetRegionResponse]("region_meta", c.option.RegionMetaCircuitBreakerSettings)

	return nil
}

func (c *innerClient) setServiceMode(newMode pdpb.ServiceMode) {
	c.Lock()
	defer c.Unlock()

	if c.option.UseTSOServerProxy {
		// If we are using TSO server proxy, we always use PD_SVC_MODE.
		newMode = pdpb.ServiceMode_PD_SVC_MODE
	}

	if newMode == c.serviceMode {
		return
	}
	log.Info("[pd] changing service mode",
		zap.String("old-mode", c.serviceMode.String()),
		zap.String("new-mode", newMode.String()))
	c.resetTSOClientLocked(newMode)
	oldMode := c.serviceMode
	c.serviceMode = newMode
	log.Info("[pd] service mode changed",
		zap.String("old-mode", oldMode.String()),
		zap.String("new-mode", newMode.String()))
}

// Reset a new TSO client.
func (c *innerClient) resetTSOClientLocked(mode pdpb.ServiceMode) {
	// Re-create a new TSO client.
	var (
		newTSOCli          *tso.Cli
		newTSOSvcDiscovery sd.ServiceDiscovery
	)
	switch mode {
	case pdpb.ServiceMode_PD_SVC_MODE:
		newTSOCli = tso.NewClient(c.ctx, c.option,
			c.pdSvcDiscovery, &tso.PDStreamBuilderFactory{})
	case pdpb.ServiceMode_API_SVC_MODE:
		newTSOSvcDiscovery = sd.NewTSOServiceDiscovery(
			c.ctx, c, c.pdSvcDiscovery,
			c.keyspaceID, c.tlsCfg, c.option)
		// At this point, the keyspace group isn't known yet. Starts from the default keyspace group,
		// and will be updated later.
		newTSOCli = tso.NewClient(c.ctx, c.option,
			newTSOSvcDiscovery, &tso.MSStreamBuilderFactory{})
		if err := newTSOSvcDiscovery.Init(); err != nil {
			log.Error("[pd] failed to initialize tso service discovery. keep the current service mode",
				zap.Strings("svr-urls", c.svrUrls),
				zap.String("current-mode", c.serviceMode.String()),
				zap.Error(err))
			return
		}
	case pdpb.ServiceMode_UNKNOWN_SVC_MODE:
		log.Warn("[pd] intend to switch to unknown service mode, just return")
		return
	}
	newTSOCli.Setup()
	// Replace the old TSO client.
	oldTSOClient := c.tsoClient
	c.tsoClient = newTSOCli
	oldTSOClient.Close()
	// Replace the old TSO service discovery if needed.
	oldTSOSvcDiscovery := c.tsoSvcDiscovery
	// If newTSOSvcDiscovery is nil, that's expected, as it means we are switching to PD service mode and
	// no tso microservice discovery is needed.
	c.tsoSvcDiscovery = newTSOSvcDiscovery
	// Close the old TSO service discovery safely after both the old client and service discovery are replaced.
	if oldTSOSvcDiscovery != nil {
		// We are switching from API service mode to PD service mode, so delete the old tso microservice discovery.
		oldTSOSvcDiscovery.Close()
	}
}

func (c *innerClient) scheduleUpdateTokenConnection() {
	select {
	case c.updateTokenConnectionCh <- struct{}{}:
	default:
	}
}

func (c *innerClient) getServiceMode() pdpb.ServiceMode {
	c.RLock()
	defer c.RUnlock()
	return c.serviceMode
}

func (c *innerClient) getTSOClient() *tso.Cli {
	c.RLock()
	defer c.RUnlock()
	return c.tsoClient
}

func (c *innerClient) close() {
	c.cancel()
	c.wg.Wait()

	c.serviceModeKeeper.close()
	c.pdSvcDiscovery.Close()

	if c.tokenDispatcher != nil {
		tokenErr := errors.WithStack(errs.ErrClosing)
		c.tokenDispatcher.tokenBatchController.revokePendingTokenRequest(tokenErr)
		c.tokenDispatcher.dispatcherCancel()
	}
}

func (c *innerClient) setup() error {
	// Init the metrics.
	if c.option.InitMetrics {
		metrics.InitAndRegisterMetrics(c.option.MetricsLabels)
	}

	// Init the client base.
	if err := c.pdSvcDiscovery.Init(); err != nil {
		return err
	}

	// Register callbacks
	c.pdSvcDiscovery.AddServingURLSwitchedCallback(c.scheduleUpdateTokenConnection)

	// Create dispatchers
	c.createTokenDispatcher()
	return nil
}

// getClientAndContext returns the leader pd client and the original context. If leader is unhealthy, it returns
// follower pd client and the context which holds forward information.
func (c *innerClient) getRegionAPIClientAndContext(ctx context.Context, allowFollower bool) (sd.ServiceClient, context.Context) {
	var serviceClient sd.ServiceClient
	if allowFollower {
		serviceClient = c.pdSvcDiscovery.GetServiceClientByKind(sd.UniversalAPIKind)
		if serviceClient != nil {
			return serviceClient, serviceClient.BuildGRPCTargetContext(ctx, !allowFollower)
		}
	}
	serviceClient = c.pdSvcDiscovery.GetServiceClient()
	if serviceClient == nil || serviceClient.GetClientConn() == nil {
		return nil, ctx
	}
	return serviceClient, serviceClient.BuildGRPCTargetContext(ctx, !allowFollower)
}

// gRPCErrorHandler is used to handle the gRPC error returned by the resource manager service.
func (c *innerClient) gRPCErrorHandler(err error) {
	if errs.IsLeaderChange(err) {
		c.pdSvcDiscovery.ScheduleCheckMemberChanged()
	}
}

func (c *innerClient) getOrCreateGRPCConn() (*grpc.ClientConn, error) {
	cc, err := c.pdSvcDiscovery.GetOrCreateGRPCConn(c.pdSvcDiscovery.GetServingURL())
	if err != nil {
		return nil, err
	}
	return cc, err
}

func (c *innerClient) dispatchTSORequestWithRetry(ctx context.Context) tso.TSFuture {
	var (
		retryable bool
		err       error
		req       *tso.Request
	)
	for i := range dispatchRetryCount {
		// Do not delay for the first time.
		if i > 0 {
			time.Sleep(dispatchRetryDelay)
		}
		// Get the tsoClient each time, as it may be initialized or switched during the process.
		tsoClient := c.getTSOClient()
		if tsoClient == nil {
			err = errs.ErrClientGetTSO.FastGenByArgs("tso client is nil")
			continue
		}
		// Get a new request from the pool if it's not from the current pool.
		if !req.IsFrom(tsoClient.GetRequestPool()) {
			req = tsoClient.GetTSORequest(ctx)
		}
		retryable, err = tsoClient.DispatchRequest(req)
		if !retryable {
			break
		}
	}
	if err != nil {
		if req == nil {
			return tso.NewRequestFastFail(err)
		}
		req.TryDone(err)
	}
	return req
}

func isOverloaded(err error) cb.Overloading {
	switch status.Code(errors.Cause(err)) {
	case codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
		return cb.Yes
	default:
		return cb.No
	}
}
