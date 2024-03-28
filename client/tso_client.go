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
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

type tsoRequest struct {
	start      time.Time
	clientCtx  context.Context
	requestCtx context.Context
	done       chan error
	physical   int64
	logical    int64
	dcLocation string
}

var tsoReqPool = sync.Pool{
	New: func() any {
		return &tsoRequest{
			done:     make(chan error, 1),
			physical: 0,
			logical:  0,
		}
	},
}

func (req *tsoRequest) tryDone(err error) {
	select {
	case req.done <- err:
	default:
	}
}

type tsoClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *option

	ServiceDiscovery
	tsoStreamBuilderFactory
	// tsoAllocators defines the mapping {dc-location -> TSO allocator leader URL}
	tsoAllocators sync.Map // Store as map[string]string
	// tsoAllocServingURLSwitchedCallback will be called when any global/local
	// tso allocator leader is switched.
	tsoAllocServingURLSwitchedCallback []func()

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
		ctx:                       ctx,
		cancel:                    cancel,
		option:                    option,
		ServiceDiscovery:          svcDiscovery,
		tsoStreamBuilderFactory:   factory,
		checkTSDeadlineCh:         make(chan struct{}),
		checkTSODispatcherCh:      make(chan struct{}, 1),
		updateTSOConnectionCtxsCh: make(chan struct{}, 1),
	}

	eventSrc := svcDiscovery.(tsoAllocatorEventSource)
	eventSrc.SetTSOLocalServURLsUpdatedCallback(c.updateTSOLocalServURLs)
	eventSrc.SetTSOGlobalServURLUpdatedCallback(c.updateGlobalDispatcher)
	c.ServiceDiscovery.AddServiceURLsSwitchedCallback(c.scheduleUpdateTSOConnectionCtxs)

	return c
}

func (c *tsoClient) Setup() {
	c.ServiceDiscovery.CheckMemberChanged()
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
	c.tsoDispatcher.Range(func(_, dispatcherInterface any) bool {
		if dispatcherInterface != nil {
			dispatcher := dispatcherInterface.(*tsoDispatcher)
			dispatcher.dispatcherCancel()
			dispatcher.tsoBatchController.clear()
		}
		return true
	})

	log.Info("tso client is closed")
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
	cc, ok := c.ServiceDiscovery.GetClientConns().Load(url)
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
		if _, err := c.ServiceDiscovery.GetOrCreateGRPCConn(url); err != nil {
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

func (c *tsoClient) updateGlobalDispatcher(url string) error {
	if d, ok := c.tsoDispatcher.Load(globalDCLocation); ok {
		dispatcher := d.(*tsoDispatcher)
		old := dispatcher.targetURL.Load()
		if old != url {
			dispatcher.targetURL.Store(url)
			dispatcher.dispatcherCancel()
		}
	}
	c.tsoAllocators.Store(globalDCLocation, url)
	log.Info("[tso] switch dc tso global allocator serving url",
		zap.String("dc-location", globalDCLocation),
		zap.String("new-url", url))
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

// TSO Follower Proxy only supports the Global TSO proxy now.
func (c *tsoClient) allowTSOFollowerProxy(dc string) bool {
	return dc == globalDCLocation && c.option.getEnableTSOFollowerProxy()
}

// backupClientConn gets a grpc client connection of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *tsoClient) backupClientConn() (*grpc.ClientConn, string) {
	urls := c.ServiceDiscovery.GetBackupURLs()
	if len(urls) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for i := 0; i < len(urls); i++ {
		url := urls[rand.Intn(len(urls))]
		if cc, err = c.ServiceDiscovery.GetOrCreateGRPCConn(url); err != nil {
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

// getAllTSOStreamBuilders returns a TSO stream builder for every service endpoint of TSO leader/followers
// or of keyspace group primary/secondaries.
func (c *tsoClient) getAllTSOStreamBuilders() map[string]tsoStreamBuilder {
	var (
		addrs          = c.ServiceDiscovery.GetServiceURLs()
		streamBuilders = make(map[string]tsoStreamBuilder, len(addrs))
		cc             *grpc.ClientConn
		err            error
	)
	for _, addr := range addrs {
		if len(addrs) == 0 {
			continue
		}
		if cc, err = c.ServiceDiscovery.GetOrCreateGRPCConn(addr); err != nil {
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

func (c *tsoClient) processRequests(
	stream tsoStream, dcLocation string, tbc *tsoBatchController,
) error {
	requests := tbc.getCollectedRequests()
	for _, req := range requests {
		defer trace.StartRegion(req.requestCtx, "pdclient.tsoReqSend").End()
		if span := opentracing.SpanFromContext(req.requestCtx); span != nil && span.Tracer() != nil {
			span = span.Tracer().StartSpan("pdclient.processRequests", opentracing.ChildOf(span.Context()))
			defer span.Finish()
		}
	}

	count := int64(len(requests))
	reqKeyspaceGroupID := c.ServiceDiscovery.GetKeyspaceGroupID()

	select {
	case <-stream.(*pdTSOStream).stream.Context().Done():
		fmt.Println("stream context done before sleep")
	default:
		fmt.Println("stream context not done before sleep")
	}
	failpoint.Inject("waitBeforeProcessTSO", func() {
		time.Sleep(time.Second * 5)
	})
	select {
	case <-stream.(*pdTSOStream).stream.Context().Done():
		fmt.Println("stream context done after sleep")
	default:
		fmt.Println("stream context not done after sleep")
	}

	respKeyspaceGroupID, physical, logical, suffixBits, err := stream.processRequests(
		c.ServiceDiscovery.GetClusterID(), c.ServiceDiscovery.GetKeyspaceID(), reqKeyspaceGroupID,
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

type tsoInfo struct {
	tsoServer           string
	reqKeyspaceGroupID  uint32
	respKeyspaceGroupID uint32
	respReceivedAt      time.Time
	physical            int64
	logical             int64
}

// TSFuture is a future which promises to return a TSO.
type TSFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

func (req *tsoRequest) Wait() (physical int64, logical int64, err error) {
	// If tso command duration is observed very high, the reason could be it
	// takes too long for Wait() be called.
	start := time.Now()
	cmdDurationTSOAsyncWait.Observe(start.Sub(req.start).Seconds())
	select {
	case err = <-req.done:
		defer trace.StartRegion(req.requestCtx, "pdclient.tsoReqDone").End()
		err = errors.WithStack(err)
		defer tsoReqPool.Put(req)
		if err != nil {
			cmdFailDurationTSO.Observe(time.Since(req.start).Seconds())
			return 0, 0, err
		}
		physical, logical = req.physical, req.logical
		now := time.Now()
		cmdDurationWait.Observe(now.Sub(start).Seconds())
		cmdDurationTSO.Observe(now.Sub(req.start).Seconds())
		return
	case <-req.requestCtx.Done():
		return 0, 0, errors.WithStack(req.requestCtx.Err())
	case <-req.clientCtx.Done():
		return 0, 0, errors.WithStack(req.clientCtx.Err())
	}
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
			zap.Uint32("keyspace", c.ServiceDiscovery.GetKeyspaceID()),
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
