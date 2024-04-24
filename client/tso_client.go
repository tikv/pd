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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
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
	// dc-location -> deadline
	tsDeadline sync.Map // Same as map[string]chan deadline
	// dc-location -> *tsoInfo while the tsoInfo is the last TSO info
	lastTSOInfoMap sync.Map // Same as map[string]*tsoInfo

	checkTSDeadlineCh         chan struct{}
	checkTSODispatcherCh      chan struct{}
	updateTSOConnectionCtxsCh chan struct{}
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
		checkTSDeadlineCh:         make(chan struct{}),
		checkTSODispatcherCh:      make(chan struct{}, 1),
		updateTSOConnectionCtxsCh: make(chan struct{}, 1),
	}

	eventSrc := svcDiscovery.(tsoAllocatorEventSource)
	eventSrc.SetTSOLocalServURLsUpdatedCallback(c.updateTSOLocalServURLs)
	eventSrc.SetTSOGlobalServURLUpdatedCallback(c.updateTSOGlobalServURL)
	c.svcDiscovery.AddServiceURLsSwitchedCallback(c.scheduleUpdateTSOConnectionCtxs)

	return c
}

func (c *tsoClient) Setup() {
	c.svcDiscovery.CheckMemberChanged()
	c.updateTSODispatcher()

	// Start the daemons.
	c.wg.Add(2)
	go c.tsoDispatcherCheckLoop()
	go c.tsCancelLoop()
}

// Close closes the TSO client
func (c *tsoClient) Close() {
	if c == nil {
		return
	}
	log.Info("closing tso client")

	c.cancel()
	c.wg.Wait()

	log.Info("close tso client")
	c.closeTSODispatcher()
	log.Info("tso client is closed")
}

func (c *tsoClient) scheduleCheckTSDeadline() {
	select {
	case c.checkTSDeadlineCh <- struct{}{}:
	default:
	}
}

func (c *tsoClient) scheduleCheckTSODispatcher() {
	select {
	case c.checkTSODispatcherCh <- struct{}{}:
	default:
	}
}

func (c *tsoClient) scheduleUpdateTSOConnectionCtxs() {
	select {
	case c.updateTSOConnectionCtxsCh <- struct{}{}:
	default:
	}
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
	return req
}

func (c *tsoClient) getTSODispatcher(dcLocation string) (*tsoDispatcher, bool) {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok {
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

// GetTSOAllocatorClientConnByDCLocation returns the tso allocator grpc client connection
// of the given dcLocation
func (c *tsoClient) GetTSOAllocatorClientConnByDCLocation(dcLocation string) (*grpc.ClientConn, string) {
	url, ok := c.tsoAllocators.Load(dcLocation)
	if !ok {
		panic(fmt.Sprintf("the allocator leader in %s should exist", dcLocation))
	}
	// todo: if we support local tso forward, we should get or create client conns.
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
	c.scheduleUpdateTSOConnectionCtxs()
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
	for i := 0; i < len(urls); i++ {
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

type tsoConnectionContext struct {
	streamURL string
	// Current stream to send gRPC requests, pdpb.PD_TsoClient for a leader/follower in the PD cluster,
	// or tsopb.TSO_TsoClient for a primary/secondary in the TSO cluster
	stream tsoStream
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *tsoClient) updateTSOConnectionCtxs(updaterCtx context.Context, dc string, connectionCtxs *sync.Map) bool {
	// Normal connection creating, it will be affected by the `enableForwarding`.
	createTSOConnection := c.tryConnectToTSO
	if c.allowTSOFollowerProxy(dc) {
		createTSOConnection = c.tryConnectToTSOWithProxy
	}
	if err := createTSOConnection(updaterCtx, dc, connectionCtxs); err != nil {
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
		stream         tsoStream
		url            string
		cc             *grpc.ClientConn
		updateAndClear = func(newURL string, connectionCtx *tsoConnectionContext) {
			// Only store the connectionCtx if it does not exist before.
			connectionCtxs.LoadOrStore(newURL, connectionCtx)
			// Remove all other connection contexts.
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
	for i := 0; i < maxRetryTimes; i++ {
		c.svcDiscovery.ScheduleCheckMemberChanged()
		cc, url = c.GetTSOAllocatorClientConnByDCLocation(dc)
		if _, ok := connectionCtxs.Load(url); ok {
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
				updateAndClear(url, &tsoConnectionContext{url, stream, cctx, cancel})
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
				updateAndClear(backupURL, &tsoConnectionContext{backupURL, stream, cctx, cancel})
				return nil
			}
			cancel()
		}
	}
	return err
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
			connectionCtxs.Store(addr, &tsoConnectionContext{addr, stream, cctx, cancel})
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

type tsoInfo struct {
	tsoServer           string
	reqKeyspaceGroupID  uint32
	respKeyspaceGroupID uint32
	respReceivedAt      time.Time
	physical            int64
	logical             int64
}

func (c *tsoClient) processRequests(
	stream tsoStream, dcLocation string, tbc *tsoBatchController,
) error {
	requests := tbc.getCollectedRequests()
	// nolint
	for _, req := range requests {
		defer trace.StartRegion(req.requestCtx, "pdclient.tsoReqSend").End()
		if span := opentracing.SpanFromContext(req.requestCtx); span != nil && span.Tracer() != nil {
			span = span.Tracer().StartSpan("pdclient.processRequests", opentracing.ChildOf(span.Context()))
			defer span.Finish()
		}
	}

	count := int64(len(requests))
	reqKeyspaceGroupID := c.svcDiscovery.GetKeyspaceGroupID()
	respKeyspaceGroupID, physical, logical, suffixBits, err := stream.processRequests(
		c.svcDiscovery.GetClusterID(), c.svcDiscovery.GetKeyspaceID(), reqKeyspaceGroupID,
		dcLocation, count, tbc.batchStartTime)
	if err != nil {
		tbc.finishCollectedRequests(0, 0, 0, err)
		return err
	}
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	firstLogical := tsoutil.AddLogical(logical, -count+1, suffixBits)
	curTSOInfo := &tsoInfo{
		tsoServer:           stream.getServerURL(),
		reqKeyspaceGroupID:  reqKeyspaceGroupID,
		respKeyspaceGroupID: respKeyspaceGroupID,
		respReceivedAt:      time.Now(),
		physical:            physical,
		logical:             tsoutil.AddLogical(firstLogical, count-1, suffixBits),
	}
	c.compareAndSwapTS(dcLocation, curTSOInfo, physical, firstLogical)
	tbc.finishCollectedRequests(physical, firstLogical, suffixBits, nil)
	return nil
}

func (c *tsoClient) compareAndSwapTS(
	dcLocation string,
	curTSOInfo *tsoInfo,
	physical, firstLogical int64,
) {
	val, loaded := c.lastTSOInfoMap.LoadOrStore(dcLocation, curTSOInfo)
	if !loaded {
		return
	}
	lastTSOInfo := val.(*tsoInfo)
	if lastTSOInfo.respKeyspaceGroupID != curTSOInfo.respKeyspaceGroupID {
		log.Info("[tso] keyspace group changed",
			zap.String("dc-location", dcLocation),
			zap.Uint32("old-group-id", lastTSOInfo.respKeyspaceGroupID),
			zap.Uint32("new-group-id", curTSOInfo.respKeyspaceGroupID))
	}

	// The TSO we get is a range like [largestLogical-count+1, largestLogical], so we save the last TSO's largest logical
	// to compare with the new TSO's first logical. For example, if we have a TSO resp with logical 10, count 5, then
	// all TSOs we get will be [6, 7, 8, 9, 10]. lastTSOInfo.logical stores the logical part of the largest ts returned
	// last time.
	if tsoutil.TSLessEqual(physical, firstLogical, lastTSOInfo.physical, lastTSOInfo.logical) {
		log.Panic("[tso] timestamp fallback",
			zap.String("dc-location", dcLocation),
			zap.Uint32("keyspace", c.svcDiscovery.GetKeyspaceID()),
			zap.String("last-ts", fmt.Sprintf("(%d, %d)", lastTSOInfo.physical, lastTSOInfo.logical)),
			zap.String("cur-ts", fmt.Sprintf("(%d, %d)", physical, firstLogical)),
			zap.String("last-tso-server", lastTSOInfo.tsoServer),
			zap.String("cur-tso-server", curTSOInfo.tsoServer),
			zap.Uint32("last-keyspace-group-in-request", lastTSOInfo.reqKeyspaceGroupID),
			zap.Uint32("cur-keyspace-group-in-request", curTSOInfo.reqKeyspaceGroupID),
			zap.Uint32("last-keyspace-group-in-response", lastTSOInfo.respKeyspaceGroupID),
			zap.Uint32("cur-keyspace-group-in-response", curTSOInfo.respKeyspaceGroupID),
			zap.Time("last-response-received-at", lastTSOInfo.respReceivedAt),
			zap.Time("cur-response-received-at", curTSOInfo.respReceivedAt))
	}
	lastTSOInfo.tsoServer = curTSOInfo.tsoServer
	lastTSOInfo.reqKeyspaceGroupID = curTSOInfo.reqKeyspaceGroupID
	lastTSOInfo.respKeyspaceGroupID = curTSOInfo.respKeyspaceGroupID
	lastTSOInfo.respReceivedAt = curTSOInfo.respReceivedAt
	lastTSOInfo.physical = curTSOInfo.physical
	lastTSOInfo.logical = curTSOInfo.logical
}
