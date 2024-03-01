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
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/client/timerpool"
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type tsoDispatcher struct {
	dispatcherCancel   context.CancelFunc
	tsoBatchController *tsoBatchController
}

type tsoInfo struct {
	tsoServer           string
	reqKeyspaceGroupID  uint32
	respKeyspaceGroupID uint32
	respReceivedAt      time.Time
	physical            int64
	logical             int64
}

const (
	tsLoopDCCheckInterval  = time.Minute
	defaultMaxTSOBatchSize = 10000 // should be higher if client is sending requests in burst
	retryInterval          = 500 * time.Millisecond
	maxRetryTimes          = 6
)

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

func (c *tsoClient) dispatchRequest(ctx context.Context, dcLocation string, request *tsoRequest) error {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok {
		err := errs.ErrClientGetTSO.FastGenByArgs(fmt.Sprintf("unknown dc-location %s to the client", dcLocation))
		log.Error("[tso] dispatch tso request error", zap.String("dc-location", dcLocation), errs.ZapError(err))
		c.svcDiscovery.ScheduleCheckMemberChanged()
		return err
	}

	defer trace.StartRegion(request.requestCtx, "tsoReqEnqueue").End()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case dispatcher.(*tsoDispatcher).tsoBatchController.tsoRequestCh <- request:
	}
	return nil
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
		defer trace.StartRegion(req.requestCtx, "tsoReqDone").End()
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

func (c *tsoClient) updateTSODispatcher() {
	// Set up the new TSO dispatcher and batch controller.
	c.GetTSOAllocators().Range(func(dcLocationKey, _ any) bool {
		dcLocation := dcLocationKey.(string)
		if !c.checkTSODispatcher(dcLocation) {
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
			dispatcher.(*tsoDispatcher).dispatcherCancel()
			c.tsoDispatcher.Delete(dcLocation)
		}
		return true
	})
}

type deadline struct {
	timer  *time.Timer
	done   chan struct{}
	cancel context.CancelFunc
}

func newTSDeadline(
	timeout time.Duration,
	done chan struct{},
	cancel context.CancelFunc,
) *deadline {
	timer := timerpool.GlobalTimerPool.Get(timeout)
	return &deadline{
		timer:  timer,
		done:   done,
		cancel: cancel,
	}
}

func (c *tsoClient) tsCancelLoop() {
	defer c.wg.Done()

	tsCancelLoopCtx, tsCancelLoopCancel := context.WithCancel(c.ctx)
	defer tsCancelLoopCancel()

	ticker := time.NewTicker(tsLoopDCCheckInterval)
	defer ticker.Stop()
	for {
		// Watch every dc-location's tsDeadlineCh
		c.GetTSOAllocators().Range(func(dcLocation, _ any) bool {
			c.watchTSDeadline(tsCancelLoopCtx, dcLocation.(string))
			return true
		})
		select {
		case <-c.checkTSDeadlineCh:
			continue
		case <-ticker.C:
			continue
		case <-tsCancelLoopCtx.Done():
			log.Info("exit tso requests cancel loop")
			return
		}
	}
}

func (c *tsoClient) watchTSDeadline(ctx context.Context, dcLocation string) {
	if _, exist := c.tsDeadline.Load(dcLocation); !exist {
		tsDeadlineCh := make(chan *deadline, 1)
		c.tsDeadline.Store(dcLocation, tsDeadlineCh)
		go func(dc string, tsDeadlineCh <-chan *deadline) {
			for {
				select {
				case d := <-tsDeadlineCh:
					select {
					case <-d.timer.C:
						log.Error("[tso] tso request is canceled due to timeout", zap.String("dc-location", dc), errs.ZapError(errs.ErrClientGetTSOTimeout))
						d.cancel()
						timerpool.GlobalTimerPool.Put(d.timer)
					case <-d.done:
						timerpool.GlobalTimerPool.Put(d.timer)
					case <-ctx.Done():
						timerpool.GlobalTimerPool.Put(d.timer)
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(dcLocation, tsDeadlineCh)
	}
}

func (c *tsoClient) scheduleCheckTSDeadline() {
	select {
	case c.checkTSDeadlineCh <- struct{}{}:
	default:
	}
}

func (c *tsoClient) tsoDispatcherCheckLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	ticker := time.NewTicker(tsLoopDCCheckInterval)
	defer ticker.Stop()
	for {
		c.updateTSODispatcher()
		select {
		case <-ticker.C:
		case <-c.checkTSODispatcherCh:
		case <-loopCtx.Done():
			log.Info("exit tso dispatcher loop")
			return
		}
	}
}

func (c *tsoClient) checkAllocator(
	dispatcherCtx context.Context,
	forwardCancel context.CancelFunc,
	dc, forwardedHostTrim, addrTrim, url string,
	updateAndClear func(newAddr string, connectionCtx *tsoConnectionContext)) {
	defer func() {
		// cancel the forward stream
		forwardCancel()
		requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(0)
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
			healthCtx, healthCancel := context.WithTimeout(dispatcherCtx, c.option.timeout)
			resp, err := healthCli.Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
			failpoint.Inject("unreachableNetwork", func() {
				resp.Status = healthpb.HealthCheckResponse_UNKNOWN
			})
			healthCancel()
			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				// create a stream of the original allocator
				cctx, cancel := context.WithCancel(dispatcherCtx)
				stream, err := c.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, c.option.timeout)
				if err == nil && stream != nil {
					log.Info("[tso] recover the original tso stream since the network has become normal", zap.String("dc", dc), zap.String("url", url))
					updateAndClear(url, &tsoConnectionContext{url, stream, cctx, cancel})
					return
				}
			}
		}
		select {
		case <-dispatcherCtx.Done():
			return
		case <-ticker.C:
			// To ensure we can get the latest allocator leader
			// and once the leader is changed, we can exit this function.
			cc, u = c.GetTSOAllocatorClientConnByDCLocation(dc)
		}
	}
}

func (c *tsoClient) checkTSODispatcher(dcLocation string) bool {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok || dispatcher == nil {
		return false
	}
	return true
}

func (c *tsoClient) createTSODispatcher(dcLocation string) {
	dispatcherCtx, dispatcherCancel := context.WithCancel(c.ctx)
	dispatcher := &tsoDispatcher{
		dispatcherCancel: dispatcherCancel,
		tsoBatchController: newTSOBatchController(
			make(chan *tsoRequest, defaultMaxTSOBatchSize*2),
			defaultMaxTSOBatchSize),
	}
	failpoint.Inject("shortDispatcherChannel", func() {
		dispatcher = &tsoDispatcher{
			dispatcherCancel: dispatcherCancel,
			tsoBatchController: newTSOBatchController(
				make(chan *tsoRequest, 1),
				defaultMaxTSOBatchSize),
		}
	})

	if _, ok := c.tsoDispatcher.LoadOrStore(dcLocation, dispatcher); !ok {
		// Successfully stored the value. Start the following goroutine.
		// Each goroutine is responsible for handling the tso stream request for its dc-location.
		// The only case that will make the dispatcher goroutine exit
		// is that the loopCtx is done, otherwise there is no circumstance
		// this goroutine should exit.
		c.wg.Add(1)
		go c.handleDispatcher(dispatcherCtx, dcLocation, dispatcher.tsoBatchController)
		log.Info("[tso] tso dispatcher created", zap.String("dc-location", dcLocation))
	} else {
		dispatcherCancel()
	}
}

func (c *tsoClient) handleDispatcher(
	dispatcherCtx context.Context,
	dc string,
	tbc *tsoBatchController) {
	var (
		err        error
		streamAddr string
		stream     tsoStream
		streamCtx  context.Context
		cancel     context.CancelFunc
		// addr -> connectionContext
		connectionCtxs sync.Map
		opts           []opentracing.StartSpanOption
	)
	defer func() {
		log.Info("[tso] exit tso dispatcher", zap.String("dc-location", dc))
		// Cancel all connections.
		connectionCtxs.Range(func(_, cc any) bool {
			cc.(*tsoConnectionContext).cancel()
			return true
		})
		c.wg.Done()
	}()
	// Call updateTSOConnectionCtxs once to init the connectionCtxs first.
	c.updateTSOConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
	// Only the Global TSO needs to watch the updateTSOConnectionCtxsCh to sense the
	// change of the cluster when TSO Follower Proxy is enabled.
	// TODO: support TSO Follower Proxy for the Local TSO.
	if dc == globalDCLocation {
		go func() {
			var updateTicker = &time.Ticker{}
			setNewUpdateTicker := func(ticker *time.Ticker) {
				if updateTicker.C != nil {
					updateTicker.Stop()
				}
				updateTicker = ticker
			}
			// Set to nil before returning to ensure that the existing ticker can be GC.
			defer setNewUpdateTicker(nil)

			for {
				select {
				case <-dispatcherCtx.Done():
					return
				case <-c.option.enableTSOFollowerProxyCh:
					enableTSOFollowerProxy := c.option.getEnableTSOFollowerProxy()
					log.Info("[tso] tso follower proxy status changed",
						zap.String("dc-location", dc),
						zap.Bool("enable", enableTSOFollowerProxy))
					if enableTSOFollowerProxy && updateTicker.C == nil {
						// Because the TSO Follower Proxy is enabled,
						// the periodic check needs to be performed.
						setNewUpdateTicker(time.NewTicker(memberUpdateInterval))
					} else if !enableTSOFollowerProxy && updateTicker.C != nil {
						// Because the TSO Follower Proxy is disabled,
						// the periodic check needs to be turned off.
						setNewUpdateTicker(&time.Ticker{})
					} else {
						// The status of TSO Follower Proxy does not change, and updateTSOConnectionCtxs is not triggered
						continue
					}
				case <-updateTicker.C:
				case <-c.updateTSOConnectionCtxsCh:
				}
				c.updateTSOConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
			}
		}()
	}

	// Loop through each batch of TSO requests and send them for processing.
	streamLoopTimer := time.NewTimer(c.option.timeout)
	defer streamLoopTimer.Stop()
	bo := retry.InitialBackoffer(updateMemberBackOffBaseTime, updateMemberTimeout, updateMemberBackOffBaseTime)
tsoBatchLoop:
	for {
		select {
		case <-dispatcherCtx.Done():
			return
		default:
		}
		// Start to collect the TSO requests.
		maxBatchWaitInterval := c.option.getMaxTSOBatchWaitInterval()
		if err = tbc.fetchPendingRequests(dispatcherCtx, maxBatchWaitInterval); err != nil {
			if err == context.Canceled {
				log.Info("[tso] stop fetching the pending tso requests due to context canceled",
					zap.String("dc-location", dc))
			} else {
				log.Error("[tso] fetch pending tso requests error",
					zap.String("dc-location", dc),
					zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
			}
			return
		}
		if maxBatchWaitInterval >= 0 {
			tbc.adjustBestBatchSize()
		}
		// Stop the timer if it's not stopped.
		if !streamLoopTimer.Stop() {
			select {
			case <-streamLoopTimer.C: // try to drain from the channel
			default:
			}
		}
		// We need be careful here, see more details in the comments of Timer.Reset.
		// https://pkg.go.dev/time@master#Timer.Reset
		streamLoopTimer.Reset(c.option.timeout)
		// Choose a stream to send the TSO gRPC request.
	streamChoosingLoop:
		for {
			connectionCtx := c.chooseStream(&connectionCtxs)
			if connectionCtx != nil {
				streamAddr, stream, streamCtx, cancel = connectionCtx.streamAddr, connectionCtx.stream, connectionCtx.ctx, connectionCtx.cancel
			}
			// Check stream and retry if necessary.
			if stream == nil {
				log.Info("[tso] tso stream is not ready", zap.String("dc", dc))
				if c.updateTSOConnectionCtxs(dispatcherCtx, dc, &connectionCtxs) {
					continue streamChoosingLoop
				}
				timer := time.NewTimer(retryInterval)
				select {
				case <-dispatcherCtx.Done():
					timer.Stop()
					return
				case <-streamLoopTimer.C:
					err = errs.ErrClientCreateTSOStream.FastGenByArgs(errs.RetryTimeoutErr)
					log.Error("[tso] create tso stream error", zap.String("dc-location", dc), errs.ZapError(err))
					c.svcDiscovery.ScheduleCheckMemberChanged()
					c.finishRequest(tbc.getCollectedRequests(), 0, 0, 0, errors.WithStack(err))
					timer.Stop()
					continue tsoBatchLoop
				case <-timer.C:
					timer.Stop()
					continue streamChoosingLoop
				}
			}
			select {
			case <-streamCtx.Done():
				log.Info("[tso] tso stream is canceled", zap.String("dc", dc), zap.String("stream-addr", streamAddr))
				// Set `stream` to nil and remove this stream from the `connectionCtxs` due to being canceled.
				connectionCtxs.Delete(streamAddr)
				cancel()
				stream = nil
				continue
			default:
				break streamChoosingLoop
			}
		}
		done := make(chan struct{})
		dl := newTSDeadline(c.option.timeout, done, cancel)
		tsDeadlineCh, ok := c.tsDeadline.Load(dc)
		for !ok || tsDeadlineCh == nil {
			c.scheduleCheckTSDeadline()
			time.Sleep(time.Millisecond * 100)
			tsDeadlineCh, ok = c.tsDeadline.Load(dc)
		}
		select {
		case <-dispatcherCtx.Done():
			return
		case tsDeadlineCh.(chan *deadline) <- dl:
		}
		opts = extractSpanReference(tbc, opts[:0])
		err = c.processRequests(stream, dc, tbc, opts)
		close(done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err != nil {
			select {
			case <-dispatcherCtx.Done():
				return
			default:
			}
			c.svcDiscovery.ScheduleCheckMemberChanged()
			log.Error("[tso] getTS error after processing requests",
				zap.String("dc-location", dc),
				zap.String("stream-addr", streamAddr),
				zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
			// Set `stream` to nil and remove this stream from the `connectionCtxs` due to error.
			connectionCtxs.Delete(streamAddr)
			cancel()
			stream = nil
			// Because ScheduleCheckMemberChanged is asynchronous, if the leader changes, we better call `updateMember` ASAP.
			if IsLeaderChange(err) {
				if err := bo.Exec(dispatcherCtx, c.svcDiscovery.CheckMemberChanged); err != nil {
					select {
					case <-dispatcherCtx.Done():
						return
					default:
					}
				}
				// Because the TSO Follower Proxy could be configured online,
				// If we change it from on -> off, background updateTSOConnectionCtxs
				// will cancel the current stream, then the EOF error caused by cancel()
				// should not trigger the updateTSOConnectionCtxs here.
				// So we should only call it when the leader changes.
				c.updateTSOConnectionCtxs(dispatcherCtx, dc, &connectionCtxs)
			}
		}
	}
}

// TSO Follower Proxy only supports the Global TSO proxy now.
func (c *tsoClient) allowTSOFollowerProxy(dc string) bool {
	return dc == globalDCLocation && c.option.getEnableTSOFollowerProxy()
}

// chooseStream uses the reservoir sampling algorithm to randomly choose a connection.
// connectionCtxs will only have only one stream to choose when the TSO Follower Proxy is off.
func (c *tsoClient) chooseStream(connectionCtxs *sync.Map) (connectionCtx *tsoConnectionContext) {
	idx := 0
	connectionCtxs.Range(func(_, cc any) bool {
		j := rand.Intn(idx + 1)
		if j < 1 {
			connectionCtx = cc.(*tsoConnectionContext)
		}
		idx++
		return true
	})
	return connectionCtx
}

type tsoConnectionContext struct {
	streamAddr string
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
	dispatcherCtx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) error {
	var (
		networkErrNum uint64
		err           error
		stream        tsoStream
		url           string
		cc            *grpc.ClientConn
	)
	updateAndClear := func(newAddr string, connectionCtx *tsoConnectionContext) {
		if cc, loaded := connectionCtxs.LoadOrStore(newAddr, connectionCtx); loaded {
			// If the previous connection still exists, we should close it first.
			cc.(*tsoConnectionContext).cancel()
			connectionCtxs.Store(newAddr, connectionCtx)
		}
		connectionCtxs.Range(func(addr, cc any) bool {
			if addr.(string) != newAddr {
				cc.(*tsoConnectionContext).cancel()
				connectionCtxs.Delete(addr)
			}
			return true
		})
	}
	// retry several times before falling back to the follower when the network problem happens

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for i := 0; i < maxRetryTimes; i++ {
		c.svcDiscovery.ScheduleCheckMemberChanged()
		cc, url = c.GetTSOAllocatorClientConnByDCLocation(dc)
		if cc != nil {
			cctx, cancel := context.WithCancel(dispatcherCtx)
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
		case <-dispatcherCtx.Done():
			return err
		case <-ticker.C:
		}
	}

	if networkErrNum == maxRetryTimes {
		// encounter the network error
		backupClientConn, addr := c.backupClientConn()
		if backupClientConn != nil {
			log.Info("[tso] fall back to use follower to forward tso stream", zap.String("dc", dc), zap.String("addr", addr))
			forwardedHost, ok := c.GetTSOAllocatorServingAddrByDCLocation(dc)
			if !ok {
				return errors.Errorf("cannot find the allocator leader in %s", dc)
			}

			// create the follower stream
			cctx, cancel := context.WithCancel(dispatcherCtx)
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
			stream, err = c.tsoStreamBuilderFactory.makeBuilder(backupClientConn).build(cctx, cancel, c.option.timeout)
			if err == nil {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addrTrim := trimHTTPPrefix(addr)
				// the goroutine is used to check the network and change back to the original stream
				go c.checkAllocator(dispatcherCtx, cancel, dc, forwardedHostTrim, addrTrim, url, updateAndClear)
				requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(1)
				updateAndClear(addr, &tsoConnectionContext{addr, stream, cctx, cancel})
				return nil
			}
			cancel()
		}
	}
	return err
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

// tryConnectToTSOWithProxy will create multiple streams to all the service endpoints to work as
// a TSO proxy to reduce the pressure of the main serving service endpoint.
func (c *tsoClient) tryConnectToTSOWithProxy(dispatcherCtx context.Context, dc string, connectionCtxs *sync.Map) error {
	tsoStreamBuilders := c.getAllTSOStreamBuilders()
	leaderAddr := c.svcDiscovery.GetServingAddr()
	forwardedHost, ok := c.GetTSOAllocatorServingAddrByDCLocation(dc)
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
		cctx, cancel := context.WithCancel(dispatcherCtx)
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

func extractSpanReference(tbc *tsoBatchController, opts []opentracing.StartSpanOption) []opentracing.StartSpanOption {
	for _, req := range tbc.getCollectedRequests() {
		if span := opentracing.SpanFromContext(req.requestCtx); span != nil {
			opts = append(opts, opentracing.ChildOf(span.Context()))
		}
	}
	return opts
}

func (c *tsoClient) processRequests(
	stream tsoStream, dcLocation string, tbc *tsoBatchController, opts []opentracing.StartSpanOption,
) error {
	if len(opts) > 0 {
		span := opentracing.StartSpan("pdclient.processRequests", opts...)
		defer span.Finish()
	}

	requests := tbc.getCollectedRequests()
	for _, req := range requests {
		defer trace.StartRegion(req.requestCtx, "tsoReqSend").End()
	}
	count := int64(len(requests))
	reqKeyspaceGroupID := c.svcDiscovery.GetKeyspaceGroupID()
	respKeyspaceGroupID, physical, logical, suffixBits, err := stream.processRequests(
		c.svcDiscovery.GetClusterID(), c.svcDiscovery.GetKeyspaceID(), reqKeyspaceGroupID,
		dcLocation, requests, tbc.batchStartTime)
	if err != nil {
		c.finishRequest(requests, 0, 0, 0, err)
		return err
	}
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	firstLogical := tsoutil.AddLogical(logical, -count+1, suffixBits)
	curTSOInfo := &tsoInfo{
		tsoServer:           stream.getServerAddr(),
		reqKeyspaceGroupID:  reqKeyspaceGroupID,
		respKeyspaceGroupID: respKeyspaceGroupID,
		respReceivedAt:      time.Now(),
		physical:            physical,
		logical:             tsoutil.AddLogical(firstLogical, count-1, suffixBits),
	}
	c.compareAndSwapTS(dcLocation, curTSOInfo, physical, firstLogical)
	c.finishRequest(requests, physical, firstLogical, suffixBits, nil)
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

func (c *tsoClient) finishRequest(requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32, err error) {
	for i := 0; i < len(requests); i++ {
		if span := opentracing.SpanFromContext(requests[i].requestCtx); span != nil {
			span.Finish()
		}
		requests[i].physical, requests[i].logical = physical, tsoutil.AddLogical(firstLogical, int64(i), suffixBits)
		defer trace.StartRegion(requests[i].requestCtx, "tsoReqDequeue").End()
		requests[i].done <- err
	}
}
