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
	"fmt"
	"math/rand"
	"runtime/trace"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	tsoDispatcherCheckInterval = time.Minute
	// defaultMaxTSOBatchSize is the default max size of the TSO request batch.
	defaultMaxTSOBatchSize = 10000
	// retryInterval and maxRetryTimes are used to control the retry interval and max retry times.
	retryInterval = 500 * time.Millisecond
	maxRetryTimes = 6
)

// TSOClient is the client used to get timestamps.
type TSOClient interface {
	// GetTS gets a timestamp from PD or TSO microservice.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetTSAsync gets a timestamp from PD or TSO microservice, without block the caller.
	GetTSAsync(ctx context.Context) TSFuture
	// GetLocalTS gets a local timestamp from PD or TSO microservice.
	GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error)
	// GetLocalTSAsync gets a local timestamp from PD or TSO microservice, without block the caller.
	GetLocalTSAsync(ctx context.Context, dcLocation string) TSFuture
	// GetMinTS gets a timestamp from PD or the minimal timestamp across all keyspace groups from
	// the TSO microservice.
	GetMinTS(ctx context.Context) (int64, int64, error)
}

type tsoClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *option

	svcDiscovery ServiceDiscovery
	tsoStreamBuilderFactory
	// tsoAllocators defines the mapping {dc-location -> TSO allocator leader URL}
	tsoAllocators sync.Map // Store as map[string]string
	// tsoAllocServingURLSwitchedCallback will be called when any global/local
	// tso allocator leader is switched.
	tsoAllocServingURLSwitchedCallback []func()

	// tsoReqPool is the pool to recycle `*tsoRequest`.
	tsoReqPool *sync.Pool
	// tsoDispatcher is used to dispatch different TSO requests to
	// the corresponding dc-location TSO channel.
	tsoDispatcher sync.Map // Same as map[string]*tsoDispatcher

	checkTSODispatcherCh chan struct{}
}

// newTSOClient returns a new TSO client.
func newTSOClient(
	ctx context.Context, option *option,
	svcDiscovery ServiceDiscovery, factory tsoStreamBuilderFactory,
) *tsoClient {
	ctx, cancel := context.WithCancel(ctx)
	c := &tsoClient{
		ctx:                     ctx,
		cancel:                  cancel,
		option:                  option,
		svcDiscovery:            svcDiscovery,
		tsoStreamBuilderFactory: factory,
		tsoReqPool: &sync.Pool{
			New: func() any {
				return &tsoRequest{
					done:     make(chan error, 1),
					physical: 0,
					logical:  0,
				}
			},
		},
		checkTSODispatcherCh: make(chan struct{}, 1),
	}

	eventSrc := svcDiscovery.(tsoAllocatorEventSource)
	eventSrc.SetTSOLocalServURLsUpdatedCallback(c.updateTSOLocalServURLs)
	eventSrc.SetTSOGlobalServURLUpdatedCallback(c.updateTSOGlobalServURL)
	c.svcDiscovery.AddServiceURLsSwitchedCallback(c.scheduleUpdateAllTSOConnectionCtxs)

	return c
}

func (c *tsoClient) getOption() *option { return c.option }

func (c *tsoClient) getServiceDiscovery() ServiceDiscovery { return c.svcDiscovery }

func (c *tsoClient) setup() {
	if err := c.svcDiscovery.CheckMemberChanged(); err != nil {
		log.Warn("[tso] failed to check member changed", errs.ZapError(err))
	}
	c.updateTSODispatcher()

	// Start the daemons.
	c.wg.Add(1)
	go c.tsoDispatcherCheckLoop()
}

func (c *tsoClient) tsoDispatcherCheckLoop() {
	log.Info("[tso] start tso dispatcher check loop")
	defer log.Info("[tso] exit tso dispatcher check loop")
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	ticker := time.NewTicker(tsoDispatcherCheckInterval)
	defer ticker.Stop()
	for {
		c.updateTSODispatcher()
		select {
		case <-ticker.C:
		case <-c.checkTSODispatcherCh:
		case <-loopCtx.Done():
			return
		}
	}
}

// close closes the TSO client
func (c *tsoClient) close() {
	if c == nil {
		return
	}
	log.Info("[tso] closing tso client")

	c.cancel()
	c.wg.Wait()

	log.Info("[tso] close tso client")
	c.closeTSODispatcher()
	log.Info("[tso] tso client is closed")
}

func (c *tsoClient) scheduleCheckTSODispatcher() {
	select {
	case c.checkTSODispatcherCh <- struct{}{}:
	default:
	}
}

// scheduleUpdateAllTSOConnectionCtxs update the TSO connection contexts for all dc-locations.
func (c *tsoClient) scheduleUpdateAllTSOConnectionCtxs() {
	c.tsoDispatcher.Range(func(_, dispatcher any) bool {
		dispatcher.(*tsoDispatcher).scheduleUpdateConnectionCtxs()
		return true
	})
}

// scheduleUpdateTSOConnectionCtxs update the TSO connection contexts for the given dc-location.
func (c *tsoClient) scheduleUpdateTSOConnectionCtxs(dcLocation string) {
	dispatcher, ok := c.getTSODispatcher(dcLocation)
	if !ok {
		return
	}
	dispatcher.scheduleUpdateConnectionCtxs()
}

// TSO Follower Proxy only supports the Global TSO proxy now.
func (c *tsoClient) allowTSOFollowerProxy(dc string) bool {
	return dc == globalDCLocation && c.option.getEnableTSOFollowerProxy()
}

func (c *tsoClient) getTSORequest(ctx context.Context, dcLocation string) *tsoRequest {
	req := c.tsoReqPool.Get().(*tsoRequest)
	// Set needed fields in the request before using it.
	req.start = time.Now()
	req.pool = c.tsoReqPool
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	req.physical = 0
	req.logical = 0
	req.dcLocation = dcLocation
	req.streamID = ""
	return req
}

func (c *tsoClient) getTSODispatcher(dcLocation string) (*tsoDispatcher, bool) {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok || dispatcher == nil {
		return nil, false
	}
	return dispatcher.(*tsoDispatcher), true
}

// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
func (c *tsoClient) GetTSOAllocators() *sync.Map {
	return &c.tsoAllocators
}

// GetTSOAllocatorServingURLByDCLocation returns the tso allocator of the given dcLocation
func (c *tsoClient) GetTSOAllocatorServingURLByDCLocation(dcLocation string) (string, bool) {
	url, exist := c.tsoAllocators.Load(dcLocation)
	if !exist {
		return "", false
	}
	return url.(string), true
}

// GetTSOAllocatorClientConnByDCLocation returns the TSO allocator gRPC client connection of the given dcLocation.
func (c *tsoClient) GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string) {
	url, ok := c.tsoAllocators.Load(dcLocation)
	if !ok {
		log.Fatal("[tso] the allocator leader should exist", zap.String("dc-location", dcLocation))
	}
	cc, ok := c.svcDiscovery.GetClientConns().Load(url)
	if !ok {
		return nil, url.(string)
	}
	return cc.(*grpc.ClientConn), url.(string)
}

// AddTSOAllocatorServingURLSwitchedCallback adds callbacks which will be called
// when any global/local tso allocator service endpoint is switched.
func (c *tsoClient) AddTSOAllocatorServingURLSwitchedCallback(callbacks ...func()) {
	c.tsoAllocServingURLSwitchedCallback = append(c.tsoAllocServingURLSwitchedCallback, callbacks...)
}

func (c *tsoClient) updateTSOLocalServURLs(allocatorMap map[string]string) error {
	if len(allocatorMap) == 0 {
		return nil
	}

	updated := false

	// Switch to the new one
	for dcLocation, url := range allocatorMap {
		if len(url) == 0 {
			continue
		}
		oldURL, exist := c.GetTSOAllocatorServingURLByDCLocation(dcLocation)
		if exist && url == oldURL {
			continue
		}
		updated = true
		if _, err := c.svcDiscovery.GetOrCreateGRPCConn(url); err != nil {
			log.Warn("[tso] failed to connect dc tso allocator serving url",
				zap.String("dc-location", dcLocation),
				zap.String("serving-url", url),
				errs.ZapError(err))
			return err
		}
		c.tsoAllocators.Store(dcLocation, url)
		log.Info("[tso] switch dc tso local allocator serving url",
			zap.String("dc-location", dcLocation),
			zap.String("new-url", url),
			zap.String("old-url", oldURL))
		// Should trigger the update of the connection contexts once the allocator leader is switched.
		c.scheduleUpdateTSOConnectionCtxs(dcLocation)
	}

	// Garbage collection of the old TSO allocator primaries
	c.gcAllocatorServingURL(allocatorMap)

	if updated {
		c.scheduleCheckTSODispatcher()
	}

	return nil
}

func (c *tsoClient) updateTSOGlobalServURL(url string) error {
	c.tsoAllocators.Store(globalDCLocation, url)
	log.Info("[tso] switch dc tso global allocator serving url",
		zap.String("dc-location", globalDCLocation),
		zap.String("new-url", url))
	c.scheduleUpdateTSOConnectionCtxs(globalDCLocation)
	c.scheduleCheckTSODispatcher()
	return nil
}

func (c *tsoClient) gcAllocatorServingURL(curAllocatorMap map[string]string) {
	// Clean up the old TSO allocators
	c.tsoAllocators.Range(func(dcLocationKey, _ any) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if _, exist := curAllocatorMap[dcLocation]; !exist {
			log.Info("[tso] delete unused tso allocator", zap.String("dc-location", dcLocation))
			c.tsoAllocators.Delete(dcLocation)
		}
		return true
	})
}

// backupClientConn gets a grpc client connection of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *tsoClient) backupClientConn() (*grpc.ClientConn, string) {
	urls := c.svcDiscovery.GetBackupURLs()
	if len(urls) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for range urls {
		url := urls[rand.Intn(len(urls))]
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(url); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return cc, url
		}
	}
	return nil, ""
}

// tsoConnectionContext is used to store the context of a TSO stream connection.
type tsoConnectionContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	// Current URL of the stream connection.
	streamURL string
	// Current stream to send gRPC requests.
	stream *tsoStream
}

// updateConnectionCtxs will choose the proper way to update the connections for the given dc-location.
// It will return a bool to indicate whether the update is successful.
func (c *tsoClient) updateConnectionCtxs(ctx context.Context, dc string, connectionCtxs *sync.Map) bool {
	// Normal connection creating, it will be affected by the `enableForwarding`.
	createTSOConnection := c.tryConnectToTSO
	if c.allowTSOFollowerProxy(dc) {
		createTSOConnection = c.tryConnectToTSOWithProxy
	}
	if err := createTSOConnection(ctx, dc, connectionCtxs); err != nil {
		log.Error("[tso] update connection contexts failed", zap.String("dc", dc), errs.ZapError(err))
		return false
	}
	return true
}

// tryConnectToTSO will try to connect to the TSO allocator leader. If the connection becomes unreachable
// and enableForwarding is true, it will create a new connection to a follower to do the forwarding,
// while a new daemon will be created also to switch back to a normal leader connection ASAP the
// connection comes back to normal.
func (c *tsoClient) tryConnectToTSO(
	ctx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) error {
	var (
		networkErrNum  uint64
		err            error
		stream         *tsoStream
		url            string
		cc             *grpc.ClientConn
		updateAndClear = func(newURL string, connectionCtx *tsoConnectionContext) {
			// Only store the `connectionCtx` if it does not exist before.
			if connectionCtx != nil {
				connectionCtxs.LoadOrStore(newURL, connectionCtx)
			}
			// Remove all other `connectionCtx`s.
			connectionCtxs.Range(func(url, cc any) bool {
				if url.(string) != newURL {
					cc.(*tsoConnectionContext).cancel()
					connectionCtxs.Delete(url)
				}
				return true
			})
		}
	)

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	// Retry several times before falling back to the follower when the network problem happens
	for range maxRetryTimes {
		c.svcDiscovery.ScheduleCheckMemberChanged()
		cc, url = c.GetTSOAllocatorClientConnByDCLocation(dc)
		if _, ok := connectionCtxs.Load(url); ok {
			// Just trigger the clean up of the stale connection contexts.
			updateAndClear(url, nil)
			return nil
		}
		if cc != nil {
			cctx, cancel := context.WithCancel(ctx)
			stream, err = c.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, c.option.timeout)
			failpoint.Inject("unreachableNetwork", func() {
				stream = nil
				err = status.New(codes.Unavailable, "unavailable").Err()
			})
			if stream != nil && err == nil {
				updateAndClear(url, &tsoConnectionContext{cctx, cancel, url, stream})
				return nil
			}

			if err != nil && c.option.enableForwarding {
				// The reason we need to judge if the error code is equal to "Canceled" here is that
				// when we create a stream we use a goroutine to manually control the timeout of the connection.
				// There is no need to wait for the transport layer timeout which can reduce the time of unavailability.
				// But it conflicts with the retry mechanism since we use the error code to decide if it is caused by network error.
				// And actually the `Canceled` error can be regarded as a kind of network error in some way.
				if rpcErr, ok := status.FromError(err); ok && (isNetworkError(rpcErr.Code()) || rpcErr.Code() == codes.Canceled) {
					networkErrNum++
				}
			}
			cancel()
		} else {
			networkErrNum++
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}

	if networkErrNum == maxRetryTimes {
		// encounter the network error
		backupClientConn, backupURL := c.backupClientConn()
		if backupClientConn != nil {
			log.Info("[tso] fall back to use follower to forward tso stream", zap.String("dc", dc), zap.String("follower-url", backupURL))
			forwardedHost, ok := c.GetTSOAllocatorServingURLByDCLocation(dc)
			if !ok {
				return errors.Errorf("cannot find the allocator leader in %s", dc)
			}

			// create the follower stream
			cctx, cancel := context.WithCancel(ctx)
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
			stream, err = c.tsoStreamBuilderFactory.makeBuilder(backupClientConn).build(cctx, cancel, c.option.timeout)
			if err == nil {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addr := trimHTTPPrefix(backupURL)
				// the goroutine is used to check the network and change back to the original stream
				go c.checkAllocator(ctx, cancel, dc, forwardedHostTrim, addr, url, updateAndClear)
				requestForwarded.WithLabelValues(forwardedHostTrim, addr).Set(1)
				updateAndClear(backupURL, &tsoConnectionContext{cctx, cancel, backupURL, stream})
				return nil
			}
			cancel()
		}
	}
	return err
}

func (c *tsoClient) checkAllocator(
	ctx context.Context,
	forwardCancel context.CancelFunc,
	dc, forwardedHostTrim, addr, url string,
	updateAndClear func(newAddr string, connectionCtx *tsoConnectionContext),
) {
	defer func() {
		// cancel the forward stream
		forwardCancel()
		requestForwarded.WithLabelValues(forwardedHostTrim, addr).Set(0)
	}()
	cc, u := c.GetTSOAllocatorClientConnByDCLocation(dc)
	var healthCli healthpb.HealthClient
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		// the pd/allocator leader change, we need to re-establish the stream
		if u != url {
			log.Info("[tso] the leader of the allocator leader is changed", zap.String("dc", dc), zap.String("origin", url), zap.String("new", u))
			return
		}
		if healthCli == nil && cc != nil {
			healthCli = healthpb.NewHealthClient(cc)
		}
		if healthCli != nil {
			healthCtx, healthCancel := context.WithTimeout(ctx, c.option.timeout)
			resp, err := healthCli.Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
			failpoint.Inject("unreachableNetwork", func() {
				resp.Status = healthpb.HealthCheckResponse_UNKNOWN
			})
			healthCancel()
			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				// create a stream of the original allocator
				cctx, cancel := context.WithCancel(ctx)
				stream, err := c.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, c.option.timeout)
				if err == nil && stream != nil {
					log.Info("[tso] recover the original tso stream since the network has become normal", zap.String("dc", dc), zap.String("url", url))
					updateAndClear(url, &tsoConnectionContext{cctx, cancel, url, stream})
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// To ensure we can get the latest allocator leader
			// and once the leader is changed, we can exit this function.
			cc, u = c.GetTSOAllocatorClientConnByDCLocation(dc)
		}
	}
}

// tryConnectToTSOWithProxy will create multiple streams to all the service endpoints to work as
// a TSO proxy to reduce the pressure of the main serving service endpoint.
func (c *tsoClient) tryConnectToTSOWithProxy(
	ctx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) error {
	tsoStreamBuilders := c.getAllTSOStreamBuilders()
	leaderAddr := c.svcDiscovery.GetServingURL()
	forwardedHost, ok := c.GetTSOAllocatorServingURLByDCLocation(dc)
	if !ok {
		return errors.Errorf("cannot find the allocator leader in %s", dc)
	}
	// GC the stale one.
	connectionCtxs.Range(func(addr, cc any) bool {
		addrStr := addr.(string)
		if _, ok := tsoStreamBuilders[addrStr]; !ok {
			log.Info("[tso] remove the stale tso stream",
				zap.String("dc", dc),
				zap.String("addr", addrStr))
			cc.(*tsoConnectionContext).cancel()
			connectionCtxs.Delete(addr)
		}
		return true
	})
	// Update the missing one.
	for addr, tsoStreamBuilder := range tsoStreamBuilders {
		if _, ok = connectionCtxs.Load(addr); ok {
			continue
		}
		log.Info("[tso] try to create tso stream",
			zap.String("dc", dc), zap.String("addr", addr))
		cctx, cancel := context.WithCancel(ctx)
		// Do not proxy the leader client.
		if addr != leaderAddr {
			log.Info("[tso] use follower to forward tso stream to do the proxy",
				zap.String("dc", dc), zap.String("addr", addr))
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
		}
		// Create the TSO stream.
		stream, err := tsoStreamBuilder.build(cctx, cancel, c.option.timeout)
		if err == nil {
			if addr != leaderAddr {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addrTrim := trimHTTPPrefix(addr)
				requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(1)
			}
			connectionCtxs.Store(addr, &tsoConnectionContext{cctx, cancel, addr, stream})
			continue
		}
		log.Error("[tso] create the tso stream failed",
			zap.String("dc", dc), zap.String("addr", addr), errs.ZapError(err))
		cancel()
	}
	return nil
}

// getAllTSOStreamBuilders returns a TSO stream builder for every service endpoint of TSO leader/followers
// or of keyspace group primary/secondaries.
func (c *tsoClient) getAllTSOStreamBuilders() map[string]tsoStreamBuilder {
	var (
		addrs          = c.svcDiscovery.GetServiceURLs()
		streamBuilders = make(map[string]tsoStreamBuilder, len(addrs))
		cc             *grpc.ClientConn
		err            error
	)
	for _, addr := range addrs {
		if len(addrs) == 0 {
			continue
		}
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			streamBuilders[addr] = c.tsoStreamBuilderFactory.makeBuilder(cc)
		}
	}
	return streamBuilders
}

func (c *tsoClient) createTSODispatcher(dcLocation string) {
	dispatcher := newTSODispatcher(c.ctx, dcLocation, defaultMaxTSOBatchSize, c)
	if _, ok := c.tsoDispatcher.LoadOrStore(dcLocation, dispatcher); !ok {
		// Create a new dispatcher for the dc-location to handle the TSO requests.
		c.wg.Add(1)
		go dispatcher.handleDispatcher(&c.wg)
	} else {
		dispatcher.close()
	}
}

func (c *tsoClient) closeTSODispatcher() {
	c.tsoDispatcher.Range(func(_, dispatcherInterface any) bool {
		if dispatcherInterface != nil {
			dispatcherInterface.(*tsoDispatcher).close()
		}
		return true
	})
}

func (c *tsoClient) updateTSODispatcher() {
	// Set up the new TSO dispatcher and batch controller.
	c.GetTSOAllocators().Range(func(dcLocationKey, _ any) bool {
		dcLocation := dcLocationKey.(string)
		if _, ok := c.getTSODispatcher(dcLocation); !ok {
			c.createTSODispatcher(dcLocation)
		}
		return true
	})
	// Clean up the unused TSO dispatcher
	c.tsoDispatcher.Range(func(dcLocationKey, dispatcher any) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if _, exist := c.GetTSOAllocators().Load(dcLocation); !exist {
			log.Info("[tso] delete unused tso dispatcher", zap.String("dc-location", dcLocation))
			c.tsoDispatcher.Delete(dcLocation)
			dispatcher.(*tsoDispatcher).close()
		}
		return true
	})
}

// dispatchRequest will send the TSO request to the corresponding TSO dispatcher.
func (c *tsoClient) dispatchRequest(request *tsoRequest) (bool, error) {
	dispatcher, ok := c.getTSODispatcher(request.dcLocation)
	if !ok {
		err := errs.ErrClientGetTSO.FastGenByArgs(fmt.Sprintf("unknown dc-location %s to the client", request.dcLocation))
		log.Error("[tso] dispatch tso request error", zap.String("dc-location", request.dcLocation), errs.ZapError(err))
		c.svcDiscovery.ScheduleCheckMemberChanged()
		// New dispatcher could be created in the meantime, which is retryable.
		return true, err
	}

	defer trace.StartRegion(request.requestCtx, "pdclient.tsoReqEnqueue").End()
	select {
	case <-request.requestCtx.Done():
		// Caller cancelled the request, no need to retry.
		return false, request.requestCtx.Err()
	case <-request.clientCtx.Done():
		// Client is closed, no need to retry.
		return false, request.clientCtx.Err()
	case <-c.ctx.Done():
		// tsoClient is closed due to the PD service mode switch, which is retryable.
		return true, c.ctx.Err()
	default:
		// This failpoint will increase the possibility that the request is sent to a closed dispatcher.
		failpoint.Inject("delayDispatchTSORequest", func() {
			time.Sleep(time.Second)
		})
		dispatcher.push(request)
	}
	// Check the contexts again to make sure the request is not been sent to a closed dispatcher.
	// Never retry on these conditions to prevent unexpected data race.
	select {
	case <-request.requestCtx.Done():
		return false, request.requestCtx.Err()
	case <-request.clientCtx.Done():
		return false, request.clientCtx.Err()
	case <-c.ctx.Done():
		return false, c.ctx.Err()
	default:
	}
	return false, nil
}
