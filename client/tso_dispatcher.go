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
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/client/timerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type dispatcherProvider interface {
	allowTSOFollowerProxy(string) bool
	GetClientConns() *sync.Map
	backupClientConn() (*grpc.ClientConn, string)
	getAllTSOStreamBuilders() map[string]tsoStreamBuilder
	ScheduleCheckMemberChanged()
}

type tsoDispatcher struct {
	dispatcherCtx           context.Context
	dispatcherCancel        context.CancelFunc
	tsoBatchController      *tsoBatchController
	checkConnectionCh       chan struct{}
	checkConnectionResultCh chan bool

	dc       string
	provider dispatcherProvider
	option   *option
	tsoStreamBuilderFactory

	// url -> connectionContext
	connectionCtxs sync.Map
	targetURL      atomic.String
}

func (d *tsoDispatcher) getTSOAllocatorClientConn() (*grpc.ClientConn, string) {
	url := d.targetURL.Load()
	// todo: if we support local tso forward, we should get or create client conns.
	cc, ok := d.provider.GetClientConns().Load(url)
	if !ok {
		return nil, url
	}
	return cc.(*grpc.ClientConn), url
}

func (d *tsoDispatcher) allowTSOFollowerProxy() bool {
	return d.provider.allowTSOFollowerProxy(d.dc)
}

func (d *tsoDispatcher) checkAllocator(
	dispatcherCtx context.Context,
	forwardCtx context.Context,
	forwardCancel context.CancelFunc,
	dc, forwardedHostTrim, addr, url string,
	updateAndClear func(newAddr string, connectionCtx *tsoConnectionContext)) {
	defer func() {
		// cancel the forward stream
		forwardCancel()
		requestForwarded.WithLabelValues(forwardedHostTrim, addr).Set(0)
	}()
	cc, u := d.getTSOAllocatorClientConn()
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
			healthCtx, healthCancel := context.WithTimeout(dispatcherCtx, d.option.timeout)
			resp, err := healthCli.Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
			failpoint.Inject("unreachableNetwork", func() {
				resp.Status = healthpb.HealthCheckResponse_UNKNOWN
			})
			healthCancel()
			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				// create a stream of the original allocator
				cctx, cancel := context.WithCancel(dispatcherCtx)
				stream, err := d.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, d.option.timeout)
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
		case <-forwardCtx.Done():
			return
		case <-ticker.C:
			// To ensure we can get the latest allocator leader
			// and once the leader is changed, we can exit this function.
			cc, u = d.getTSOAllocatorClientConn()
		}
	}
}

func (d *tsoDispatcher) scheduleCheckConnectionContext() {
	select {
	case d.checkConnectionCh <- struct{}{}:
	default:
	}
}

func (d *tsoDispatcher) sendCheckConnectionContextResult(result bool) {
	select {
	case d.checkConnectionResultCh <- result:
	default:
	}
}

type tsoConnectionContext struct {
	streamURL string
	// Current stream to send gRPC requests, pdpb.PD_TsoClient for a leader/follower in the PD cluster,
	// or tsopb.TSO_TsoClient for a primary/secondary in the TSO cluster
	stream tsoStream
	ctx    context.Context
	cancel context.CancelFunc
}

func (d *tsoDispatcher) updateTSOConnectionCtxs() {
	select {
	case <-d.checkConnectionResultCh:
	default:
	}
	// Normal connection creating, it will be affected by the `enableForwarding`.
	createTSOConnection := d.tryConnectToTSO
	if d.allowTSOFollowerProxy() {
		createTSOConnection = d.tryConnectToTSOWithProxy
	}
	if err := createTSOConnection(); err != nil {
		log.Error("[tso] update connection contexts failed", zap.String("dc", d.dc), errs.ZapError(err))
		d.sendCheckConnectionContextResult(false)
		return
	}
	d.sendCheckConnectionContextResult(true)
}

// tryConnectToTSO will try to connect to the TSO allocator leader. If the connection becomes unreachable
// and enableForwarding is true, it will create a new connection to a follower to do the forwarding,
// while a new daemon will be created also to switch back to a normal leader connection ASAP the
// connection comes back to normal.
func (d *tsoDispatcher) tryConnectToTSO() error {
	var (
		networkErrNum  uint64
		err            error
		stream         tsoStream
		url            string
		cc             *grpc.ClientConn
		dc             = d.dc
		connectionCtxs = &d.connectionCtxs
		dispatcherCtx  = d.dispatcherCtx
	)
	updateAndClear := func(newURL string, connectionCtx *tsoConnectionContext) {
		if cc, loaded := connectionCtxs.LoadOrStore(newURL, connectionCtx); loaded {
			// If the previous connection still exists, we should close it first.
			cc.(*tsoConnectionContext).cancel()
			connectionCtxs.Store(newURL, connectionCtx)
		}
		connectionCtxs.Range(func(url, cc any) bool {
			if url.(string) != newURL {
				cc.(*tsoConnectionContext).cancel()
				connectionCtxs.Delete(url)
			}
			return true
		})
	}

	// retry several times before falling back to the follower when the network problem happens
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for i := 0; i < maxRetryTimes; i++ {
		// We don't need to check the merber info for the first time.
		// Because we have already triggered the check before calling this function.
		if i > 0 {
			d.provider.ScheduleCheckMemberChanged()
		}
		cc, url = d.getTSOAllocatorClientConn()
		if cc != nil {
			cctx, cancel := context.WithCancel(dispatcherCtx)
			stream, err = d.tsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, d.option.timeout)
			failpoint.Inject("unreachableNetwork", func() {
				stream = nil
				err = status.New(codes.Unavailable, "unavailable").Err()
			})
			if stream != nil && err == nil {
				updateAndClear(url, &tsoConnectionContext{url, stream, cctx, cancel})
				return nil
			}

			if err != nil && d.option.enableForwarding {
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
		backupClientConn, backupURL := d.provider.backupClientConn()
		if backupClientConn != nil {
			log.Info("[tso] fall back to use follower to forward tso stream", zap.String("dc", dc), zap.String("follower-url", backupURL))
			forwardedHost := d.targetURL.Load()
			if len(forwardedHost) == 0 {
				return errors.Errorf("cannot find the allocator leader in %s", dc)
			}

			// create the follower stream
			cctx, cancel := context.WithCancel(dispatcherCtx)
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
			stream, err = d.tsoStreamBuilderFactory.makeBuilder(backupClientConn).build(cctx, cancel, d.option.timeout)
			if err == nil {
				forwardedHostTrim := trimHTTPPrefix(forwardedHost)
				addr := trimHTTPPrefix(backupURL)
				// the goroutine is used to check the network and change back to the original stream
				go d.checkAllocator(dispatcherCtx, cctx, cancel, dc, forwardedHostTrim, addr, url, updateAndClear)
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
// Note: If some follower proxy streams are canceled but at least one stream is still alive,
// this method may be not triggered. This PR(PR number to be updated) does not change this behavior.
func (d *tsoDispatcher) tryConnectToTSOWithProxy() error {
	var (
		dc             = d.dc
		connectionCtxs = &d.connectionCtxs
		dispatcherCtx  = d.dispatcherCtx
	)
	tsoStreamBuilders := d.provider.getAllTSOStreamBuilders()
	forwardedHost := d.targetURL.Load()
	if len(forwardedHost) == 0 {
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
		if _, ok := connectionCtxs.Load(addr); ok {
			continue
		}
		log.Info("[tso] try to create tso stream",
			zap.String("dc", dc), zap.String("addr", addr))
		cctx, cancel := context.WithCancel(dispatcherCtx)
		// Do not proxy the leader client.
		if addr != forwardedHost {
			log.Info("[tso] use follower to forward tso stream to do the proxy",
				zap.String("dc", dc), zap.String("addr", addr))
			cctx = grpcutil.BuildForwardContext(cctx, forwardedHost)
		}
		// Create the TSO stream.
		stream, err := tsoStreamBuilder.build(cctx, cancel, d.option.timeout)
		if err == nil {
			if addr != forwardedHost {
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

func (c *tsoClient) dispatchRequest(request *tsoRequest) (bool, error) {
	dispatcher, ok := c.tsoDispatcher.Load(request.dcLocation)
	if !ok {
		err := errs.ErrClientGetTSO.FastGenByArgs(fmt.Sprintf("unknown dc-location %s to the client", request.dcLocation))
		log.Error("[tso] dispatch tso request error", zap.String("dc-location", request.dcLocation), errs.ZapError(err))
		c.ServiceDiscovery.ScheduleCheckMemberChanged()
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
		dispatcher.(*tsoDispatcher).tsoBatchController.tsoRequestCh <- request
	}
	return false, nil
}

func (c *tsoClient) updateTSODispatcher() {
	// Set up the new TSO dispatcher and batch controller.
	c.GetTSOAllocators().Range(func(dcLocationKey, url any) bool {
		dcLocation := dcLocationKey.(string)
		targetURL := url.(string)
		dispatcher, ok := c.checkTSODispatcher(dcLocation)
		if !ok {
			c.createTSODispatcher(dcLocation, targetURL)
		} else if dispatcher.targetURL.Load() != targetURL {
			dispatcher.targetURL.Store(targetURL)
			dispatcher.scheduleCheckConnectionContext()
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

func (c *tsoClient) checkTSODispatcher(dcLocation string) (*tsoDispatcher, bool) {
	dispatcher, ok := c.tsoDispatcher.Load(dcLocation)
	if !ok || dispatcher == nil {
		return nil, false
	}
	return dispatcher.(*tsoDispatcher), true
}

func (c *tsoClient) createTSODispatcher(dcLocation, url string) {
	dispatcherCtx, dispatcherCancel := context.WithCancel(c.ctx)
	dispatcher := &tsoDispatcher{
		dc:               dcLocation,
		dispatcherCtx:    dispatcherCtx,
		dispatcherCancel: dispatcherCancel,
		tsoBatchController: newTSOBatchController(
			make(chan *tsoRequest, defaultMaxTSOBatchSize*2),
			defaultMaxTSOBatchSize),
		checkConnectionCh:       make(chan struct{}, 1),
		checkConnectionResultCh: make(chan bool, 1),
		provider:                c,
		option:                  c.option,
		tsoStreamBuilderFactory: c.tsoStreamBuilderFactory,
	}
	dispatcher.targetURL.Store(url)
	failpoint.Inject("shortDispatcherChannel", func() {
		dispatcher.tsoBatchController = newTSOBatchController(
			make(chan *tsoRequest, 1),
			defaultMaxTSOBatchSize)
	})

	if _, ok := c.tsoDispatcher.LoadOrStore(dcLocation, dispatcher); !ok {
		// Successfully stored the value. Start the following goroutine.
		// Each goroutine is responsible for handling the tso stream request for its dc-location.
		// The only case that will make the dispatcher goroutine exit
		// is that the loopCtx is done, otherwise there is no circumstance
		// this goroutine should exit.
		c.wg.Add(1)
		go c.handleDispatcher(dispatcher)
		log.Info("[tso] tso dispatcher created", zap.String("dc-location", dcLocation))
	} else {
		dispatcherCancel()
	}
}

func (c *tsoClient) handleDispatcher(dispatcher *tsoDispatcher) {
	var (
		err           error
		streamURL     string
		stream        tsoStream
		streamCtx     context.Context
		cancel        context.CancelFunc
		dc            = dispatcher.dc
		tbc           = dispatcher.tsoBatchController
		dispatcherCtx = dispatcher.dispatcherCtx
	)
	defer func() {
		log.Info("[tso] exit tso dispatcher", zap.String("dc-location", dc))
		// Cancel all connections.
		dispatcher.connectionCtxs.Range(func(_, cc any) bool {
			cc.(*tsoConnectionContext).cancel()
			return true
		})
		c.wg.Done()
	}()
	// Call updateTSOConnectionCtxs once to init the connectionCtxs first.
	dispatcher.updateTSOConnectionCtxs()
	// Watch the updateTSOConnectionCtxsCh to sense the
	// change of the cluster when TSO Follower Proxy is enabled or members are changed.
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
			case <-dispatcher.checkConnectionCh:
			}
			dispatcher.updateTSOConnectionCtxs()
		}
	}()

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
		// Once the TSO requests are collected, must make sure they could be finished or revoked eventually,
		// otherwise the upper caller may get blocked on waiting for the results.
		if err = tbc.fetchPendingRequests(dispatcherCtx, maxBatchWaitInterval); err != nil {
			// Finish the collected requests if the fetch failed.
			tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(err))
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
			connectionCtx := chooseStream(&dispatcher.connectionCtxs)
			if connectionCtx != nil {
				streamURL, stream, streamCtx, cancel = connectionCtx.streamURL, connectionCtx.stream, connectionCtx.ctx, connectionCtx.cancel
			}
			// Check stream and retry if necessary.
			if stream == nil {
				log.Info("[tso] tso stream is not ready", zap.String("dc", dc))
				dispatcher.scheduleCheckConnectionContext()
				timer := time.NewTimer(retryInterval)
				for {
					select {
					case <-dispatcherCtx.Done():
						// Finish the collected requests if the context is canceled.
						tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(dispatcherCtx.Err()))
						timer.Stop()
						return
					case <-streamLoopTimer.C:
						err = errs.ErrClientCreateTSOStream.FastGenByArgs(errs.RetryTimeoutErr)
						log.Error("[tso] create tso stream error", zap.String("dc-location", dc), errs.ZapError(err))
						c.ServiceDiscovery.ScheduleCheckMemberChanged()
						// Finish the collected requests if the stream is failed to be created.
						tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(err))
						timer.Stop()
						continue tsoBatchLoop
					case <-timer.C:
						timer.Stop()
						continue streamChoosingLoop
					case update := <-dispatcher.checkConnectionResultCh:
						if update {
							continue streamChoosingLoop
						}
					}
				}
			}
			select {
			case <-streamCtx.Done():
				log.Info("[tso] tso stream is canceled", zap.String("dc", dc), zap.String("stream-url", streamURL))
				// Set `stream` to nil and remove this stream from the `connectionCtxs` due to being canceled.
				dispatcher.connectionCtxs.Delete(streamURL)
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
			// Finish the collected requests if the context is canceled.
			tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(dispatcherCtx.Err()))
			return
		case tsDeadlineCh.(chan *deadline) <- dl:
		}
		// processRequests guarantees that the collected requests could be finished properly.
		err = c.processRequests(stream, dc, tbc)
		close(done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err != nil {
			select {
			case <-dispatcherCtx.Done():
				return
			default:
			}
			c.ServiceDiscovery.ScheduleCheckMemberChanged()
			log.Error("[tso] getTS error after processing requests",
				zap.String("dc-location", dc),
				zap.String("stream-url", streamURL),
				zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
			// Set `stream` to nil and remove this stream from the `connectionCtxs` due to error.
			dispatcher.connectionCtxs.Delete(streamURL)
			cancel()
			stream = nil
			// Because ScheduleCheckMemberChanged is asynchronous, if the leader changes, we better call `updateMember` ASAP.
			if IsLeaderChange(err) {
				if err := bo.Exec(dispatcherCtx, c.ServiceDiscovery.CheckMemberChanged); err != nil {
					select {
					case <-dispatcherCtx.Done():
						return
					default:
					}
				}
				// If the tso service server is changed, the call back will be triggered by CheckMemberChanged.
				// So we don't need to call updateTSOConnectionCtxs here.
			}
		}
	}
}

// chooseStream uses the reservoir sampling algorithm to randomly choose a connection.
// connectionCtxs will only have only one stream to choose when the TSO Follower Proxy is off.
func chooseStream(connectionCtxs *sync.Map) (connectionCtx *tsoConnectionContext) {
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
