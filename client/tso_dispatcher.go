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
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/client/timerpool"
	"go.uber.org/zap"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	tsLoopDCCheckInterval  = time.Minute
	defaultMaxTSOBatchSize = 10000 // should be higher if client is sending requests in burst
	retryInterval          = 500 * time.Millisecond
	maxRetryTimes          = 6
)

type tsoDispatcher struct {
	dispatcherCancel   context.CancelFunc
	tsoBatchController *tsoBatchController
}

func (td *tsoDispatcher) close() {
	td.dispatcherCancel()
	td.tsoBatchController.clear()
}

func (td *tsoDispatcher) push(request *tsoRequest) {
	td.tsoBatchController.tsoRequestCh <- request
}

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
			c.tsoDispatcher.Delete(dcLocation)
			dispatcher.(*tsoDispatcher).close()
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
	dc, forwardedHostTrim, addr, url string,
	updateAndClear func(newAddr string, connectionCtx *tsoConnectionContext)) {
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
	tsoBatchController := newTSOBatchController(
		make(chan *tsoRequest, defaultMaxTSOBatchSize*2),
		defaultMaxTSOBatchSize,
	)
	failpoint.Inject("shortDispatcherChannel", func() {
		tsoBatchController = newTSOBatchController(
			make(chan *tsoRequest, 1),
			defaultMaxTSOBatchSize,
		)
	})
	dispatcher := &tsoDispatcher{dispatcherCancel, tsoBatchController}

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
	ctx context.Context,
	dc string,
	tbc *tsoBatchController,
) {
	var (
		err       error
		streamURL string
		stream    tsoStream
		streamCtx context.Context
		cancel    context.CancelFunc
		// url -> connectionContext
		connectionCtxs sync.Map
	)
	// Clean up the connectionCtxs when the dispatcher exits.
	defer func() {
		log.Info("[tso] exit tso dispatcher", zap.String("dc-location", dc))
		// Cancel all connections.
		connectionCtxs.Range(func(_, cc any) bool {
			cc.(*tsoConnectionContext).cancel()
			return true
		})
		// Clear the tso batch controller.
		tbc.clear()
		c.wg.Done()
	}()
	// Daemon goroutine to update the connectionCtxs periodically and handle the `connectionCtxs` update event.
	go c.connectionCtxsUpdater(ctx, dc, &connectionCtxs)

	// Loop through each batch of TSO requests and send them for processing.
	streamLoopTimer := time.NewTimer(c.option.timeout)
	defer streamLoopTimer.Stop()
	bo := retry.InitialBackoffer(updateMemberBackOffBaseTime, updateMemberTimeout, updateMemberBackOffBaseTime)
tsoBatchLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Start to collect the TSO requests.
		maxBatchWaitInterval := c.option.getMaxTSOBatchWaitInterval()
		// Once the TSO requests are collected, must make sure they could be finished or revoked eventually,
		// otherwise the upper caller may get blocked on waiting for the results.
		if err = tbc.fetchPendingRequests(ctx, maxBatchWaitInterval); err != nil {
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
			connectionCtx := chooseStream(&connectionCtxs)
			if connectionCtx != nil {
				streamURL, stream, streamCtx, cancel = connectionCtx.streamURL, connectionCtx.stream, connectionCtx.ctx, connectionCtx.cancel
			}
			// Check stream and retry if necessary.
			if stream == nil {
				log.Info("[tso] tso stream is not ready", zap.String("dc", dc))
				if c.updateTSOConnectionCtxs(ctx, dc, &connectionCtxs) {
					continue streamChoosingLoop
				}
				timer := time.NewTimer(retryInterval)
				select {
				case <-ctx.Done():
					// Finish the collected requests if the context is canceled.
					tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(ctx.Err()))
					timer.Stop()
					return
				case <-streamLoopTimer.C:
					err = errs.ErrClientCreateTSOStream.FastGenByArgs(errs.RetryTimeoutErr)
					log.Error("[tso] create tso stream error", zap.String("dc-location", dc), errs.ZapError(err))
					c.svcDiscovery.ScheduleCheckMemberChanged()
					// Finish the collected requests if the stream is failed to be created.
					tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(err))
					timer.Stop()
					continue tsoBatchLoop
				case <-timer.C:
					timer.Stop()
					continue streamChoosingLoop
				}
			}
			select {
			case <-streamCtx.Done():
				log.Info("[tso] tso stream is canceled", zap.String("dc", dc), zap.String("stream-url", streamURL))
				// Set `stream` to nil and remove this stream from the `connectionCtxs` due to being canceled.
				connectionCtxs.Delete(streamURL)
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
		case <-ctx.Done():
			// Finish the collected requests if the context is canceled.
			tbc.finishCollectedRequests(0, 0, 0, errors.WithStack(ctx.Err()))
			return
		case tsDeadlineCh.(chan *deadline) <- dl:
		}
		// processRequests guarantees that the collected requests could be finished properly.
		err = c.processRequests(stream, dc, tbc)
		close(done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			c.svcDiscovery.ScheduleCheckMemberChanged()
			log.Error("[tso] getTS error after processing requests",
				zap.String("dc-location", dc),
				zap.String("stream-url", streamURL),
				zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
			// Set `stream` to nil and remove this stream from the `connectionCtxs` due to error.
			connectionCtxs.Delete(streamURL)
			cancel()
			stream = nil
			// Because ScheduleCheckMemberChanged is asynchronous, if the leader changes, we better call `updateMember` ASAP.
			if IsLeaderChange(err) {
				if err := bo.Exec(ctx, c.svcDiscovery.CheckMemberChanged); err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
				// Because the TSO Follower Proxy could be configured online,
				// If we change it from on -> off, background updateTSOConnectionCtxs
				// will cancel the current stream, then the EOF error caused by cancel()
				// should not trigger the updateTSOConnectionCtxs here.
				// So we should only call it when the leader changes.
				c.updateTSOConnectionCtxs(ctx, dc, &connectionCtxs)
			}
		}
	}
}

// updateTSOConnectionCtxs updates the `connectionCtxs` for the specified DC location regularly.
func (c *tsoClient) connectionCtxsUpdater(
	ctx context.Context,
	dc string,
	connectionCtxs *sync.Map,
) {
	log.Info("[tso] start tso connection contexts updater", zap.String("dc-location", dc))
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
		c.updateTSOConnectionCtxs(ctx, dc, connectionCtxs)
		select {
		case <-ctx.Done():
			log.Info("[tso] exit tso connection contexts updater", zap.String("dc-location", dc))
			return
		case <-c.option.enableTSOFollowerProxyCh:
			// TODO: implement TSO Follower Proxy support for the Local TSO.
			if dc != globalDCLocation {
				continue
			}
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
			// Triggered periodically when the TSO Follower Proxy is enabled.
		case <-c.updateTSOConnectionCtxsCh:
			// Triggered by the leader/follower change.
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
