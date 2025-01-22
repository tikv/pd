// Copyright 2024 TiKV Project Authors.
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
	"crypto/tls"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	sd "github.com/tikv/pd/client/servicediscovery"
)

const (
	dispatchRetryDelay = 50 * time.Millisecond
	dispatchRetryCount = 2
)

type innerClient struct {
	keyspaceID       uint32
	svrUrls          []string
	serviceDiscovery sd.ServiceDiscovery
	tokenDispatcher  *tokenDispatcher

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
	c.serviceDiscovery = sd.NewServiceDiscovery(
		c.ctx, c.cancel, &c.wg, c.setServiceMode,
		updateKeyspaceIDCb, c.keyspaceID, c.svrUrls, c.tlsCfg, c.option)
	if err := c.setup(); err != nil {
		c.cancel()
		if c.serviceDiscovery != nil {
			c.serviceDiscovery.Close()
		}
		return err
	}

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
			c.serviceDiscovery, &tso.PDStreamBuilderFactory{})
	case pdpb.ServiceMode_API_SVC_MODE:
		newTSOSvcDiscovery = sd.NewTSOServiceDiscovery(
			c.ctx, c, c.serviceDiscovery,
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
	// If newTSOSvcDiscovery is nil, that's expected, as it means we are switching to PD mode and
	// no tso microservice discovery is needed.
	c.tsoSvcDiscovery = newTSOSvcDiscovery
	// Close the old TSO service discovery safely after both the old client and service discovery are replaced.
	if oldTSOSvcDiscovery != nil {
		// We are switching from PD service mode to PD mode, so delete the old tso microservice discovery.
		oldTSOSvcDiscovery.Close()
	}
}

func (c *innerClient) scheduleUpdateTokenConnection(string) error {
	select {
	case c.updateTokenConnectionCh <- struct{}{}:
	default:
	}
	return nil
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
	c.serviceDiscovery.Close()

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
	if err := c.serviceDiscovery.Init(); err != nil {
		return err
	}

	// Register callbacks
	c.serviceDiscovery.AddLeaderSwitchedCallback(c.scheduleUpdateTokenConnection)

	// Create dispatchers
	c.createTokenDispatcher()
	return nil
}

// getClientAndContext returns the leader pd client and the original context. If leader is unhealthy, it returns
// follower pd client and the context which holds forward information.
func (c *innerClient) getRegionAPIClientAndContext(ctx context.Context, allowFollower bool) (sd.ServiceClient, context.Context) {
	var serviceClient sd.ServiceClient
	if allowFollower {
		serviceClient = c.serviceDiscovery.GetServiceClientByKind(sd.UniversalAPIKind)
		if serviceClient != nil {
			return serviceClient, serviceClient.BuildGRPCTargetContext(ctx, !allowFollower)
		}
	}
	serviceClient = c.serviceDiscovery.GetServiceClient()
	if serviceClient == nil || serviceClient.GetClientConn() == nil {
		return nil, ctx
	}
	return serviceClient, serviceClient.BuildGRPCTargetContext(ctx, !allowFollower)
}

// gRPCErrorHandler is used to handle the gRPC error returned by the resource manager service.
func (c *innerClient) gRPCErrorHandler(err error) {
	if errs.IsLeaderChange(err) {
		c.serviceDiscovery.ScheduleCheckMemberChanged()
	}
}

func (c *innerClient) getOrCreateGRPCConn() (*grpc.ClientConn, error) {
	cc, err := c.serviceDiscovery.GetOrCreateGRPCConn(c.serviceDiscovery.GetServingURL())
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
