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

package tso

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/batch"
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/client/pkg/utils/timerutil"
	"github.com/tikv/pd/client/pkg/utils/tsoutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

// deadline is used to control the TS request timeout manually,
// it will be sent to the `tsDeadlineCh` to be handled by the `watchTSDeadline` goroutine.
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
	timer := timerutil.GlobalTimerPool.Get(timeout)
	return &deadline{
		timer:  timer,
		done:   done,
		cancel: cancel,
	}
}

type tsoInfo struct {
	tsoServer           string
	reqKeyspaceGroupID  uint32
	respKeyspaceGroupID uint32
	respReceivedAt      time.Time
	physical            int64
	logical             int64
	sourceStreamID      string
}

type tsoServiceProvider interface {
	getOption() *opt.Option
	getServiceDiscovery() sd.ServiceDiscovery
	updateConnectionCtxs(ctx context.Context, connectionCtxs *sync.Map) bool
}

const dispatcherCheckRPCConcurrencyInterval = time.Second * 5

type tsoDispatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	provider tsoServiceProvider
	// URL -> *connectionContext
	connectionCtxs *sync.Map
	tsoRequestCh   chan *Request
	tsDeadlineCh   chan *deadline
	latestTSOInfo  atomic.Pointer[tsoInfo]
	// For reusing `*batchController` objects
	batchBufferPool *sync.Pool

	// For controlling amount of concurrently processing RPC requests.
	// A token must be acquired here before sending an RPC request, and the token must be put back after finishing the
	// RPC. This is used like a semaphore, but we don't use semaphore directly here as it cannot be selected with
	// other channels.
	tokenCh                  chan struct{}
	lastCheckConcurrencyTime time.Time
	tokenCount               int
	rpcConcurrency           int

	updateConnectionCtxsCh chan struct{}
}

func newTSODispatcher(
	ctx context.Context,
	maxBatchSize int,
	provider tsoServiceProvider,
) *tsoDispatcher {
	dispatcherCtx, dispatcherCancel := context.WithCancel(ctx)
	tsoRequestCh := make(chan *Request, maxBatchSize*2)
	failpoint.Inject("shortDispatcherChannel", func() {
		tsoRequestCh = make(chan *Request, 1)
	})

	// A large-enough capacity to hold maximum concurrent RPC requests. In our design, the concurrency is at most 16.
	const tokenChCapacity = 64
	tokenCh := make(chan struct{}, tokenChCapacity)

	td := &tsoDispatcher{
		ctx:            dispatcherCtx,
		cancel:         dispatcherCancel,
		provider:       provider,
		connectionCtxs: &sync.Map{},
		tsoRequestCh:   tsoRequestCh,
		tsDeadlineCh:   make(chan *deadline, tokenChCapacity),
		batchBufferPool: &sync.Pool{
			New: func() any {
				return batch.NewController[*Request](
					maxBatchSize*2,
					tsoRequestFinisher(0, 0, invalidStreamID),
					metrics.TSOBestBatchSize,
				)
			},
		},
		tokenCh:                tokenCh,
		updateConnectionCtxsCh: make(chan struct{}, 1),
	}
	go td.watchTSDeadline()
	return td
}

func (td *tsoDispatcher) watchTSDeadline() {
	log.Info("[tso] start tso deadline watcher")
	defer log.Info("[tso] exit tso deadline watcher")
	for {
		select {
		case d := <-td.tsDeadlineCh:
			select {
			case <-d.timer.C:
				log.Error("[tso] tso request is canceled due to timeout",
					errs.ZapError(errs.ErrClientGetTSOTimeout))
				d.cancel()
				timerutil.GlobalTimerPool.Put(d.timer)
			case <-d.done:
				timerutil.GlobalTimerPool.Put(d.timer)
			case <-td.ctx.Done():
				timerutil.GlobalTimerPool.Put(d.timer)
				return
			}
		case <-td.ctx.Done():
			return
		}
	}
}

func (td *tsoDispatcher) scheduleUpdateConnectionCtxs() {
	select {
	case td.updateConnectionCtxsCh <- struct{}{}:
	default:
	}
}

func (td *tsoDispatcher) revokePendingRequests(err error) {
	for range len(td.tsoRequestCh) {
		req := <-td.tsoRequestCh
		req.TryDone(err)
	}
}

func (td *tsoDispatcher) close() {
	td.cancel()
	tsoErr := errors.WithStack(errs.ErrClosing)
	td.revokePendingRequests(tsoErr)
}

func (td *tsoDispatcher) push(request *Request) {
	td.tsoRequestCh <- request
}

func (td *tsoDispatcher) handleDispatcher(wg *sync.WaitGroup) {
	var (
		ctx                = td.ctx
		provider           = td.provider
		svcDiscovery       = provider.getServiceDiscovery()
		option             = provider.getOption()
		connectionCtxs     = td.connectionCtxs
		tsoBatchController *batch.Controller[*Request]
	)

	log.Info("[tso] tso dispatcher created")
	// Clean up the connectionCtxs when the dispatcher exits.
	defer func() {
		log.Info("[tso] exit tso dispatcher")
		// Cancel all connections.
		connectionCtxs.Range(func(_, cc any) bool {
			cc.(*tsoConnectionContext).cancel()
			return true
		})
		if tsoBatchController != nil && tsoBatchController.GetCollectedRequestCount() != 0 {
			// If you encounter this failure, please check the stack in the logs to see if it's a panic.
			log.Fatal("batched tso requests not cleared when exiting the tso dispatcher loop", zap.Any("panic", recover()))
		}
		tsoErr := errors.WithStack(errs.ErrClosing)
		td.revokePendingRequests(tsoErr)
		wg.Done()
	}()
	// Daemon goroutine to update the connectionCtxs periodically and handle the `connectionCtxs` update event.
	go td.connectionCtxsUpdater()

	var (
		err       error
		streamCtx context.Context
		cancel    context.CancelFunc
		streamURL string
		stream    *tsoStream
	)
	// Loop through each batch of TSO requests and send them for processing.
	streamLoopTimer := time.NewTimer(option.Timeout)
	defer streamLoopTimer.Stop()

	// Create a not-started-timer to be used for collecting batches for concurrent RPC.
	batchingTimer := time.NewTimer(0)
	<-batchingTimer.C
	defer batchingTimer.Stop()

	bo := retry.InitialBackoffer(sd.UpdateMemberBackOffBaseTime, sd.UpdateMemberTimeout, sd.UpdateMemberBackOffBaseTime)
tsoBatchLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// In case error happens, the loop may continue without resetting `tsoBatchController` for retrying.
		if tsoBatchController == nil {
			tsoBatchController = td.batchBufferPool.Get().(*batch.Controller[*Request])
		}

		maxBatchWaitInterval := option.GetMaxTSOBatchWaitInterval()

		currentBatchStartTime := time.Now()
		// Update concurrency settings if needed.
		if err = td.checkTSORPCConcurrency(ctx, maxBatchWaitInterval, currentBatchStartTime); err != nil {
			// checkTSORPCConcurrency can only fail due to `ctx` being invalidated.
			log.Info("[tso] stop checking tso rpc concurrency configurations due to context canceled",
				zap.Error(err))
			return
		}

		// Start to collect the TSO requests.
		// Once the TSO requests are collected, must make sure they could be finished or revoked eventually,
		// otherwise the upper caller may get blocked on waiting for the results.
		if err = tsoBatchController.FetchPendingRequests(ctx, td.tsoRequestCh, td.tokenCh, maxBatchWaitInterval); err != nil {
			if err == context.Canceled {
				log.Info("[tso] stop fetching the pending tso requests due to context canceled")
			} else {
				log.Error("[tso] fetch pending tso requests error",
					zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
			}
			return
		}
		if maxBatchWaitInterval >= 0 {
			tsoBatchController.AdjustBestBatchSize()
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
		streamLoopTimer.Reset(option.Timeout)
		// Choose a stream to send the TSO gRPC request.
	streamChoosingLoop:
		for {
			connectionCtx := chooseStream(connectionCtxs)
			if connectionCtx != nil {
				streamCtx, cancel, streamURL, stream = connectionCtx.ctx, connectionCtx.cancel, connectionCtx.streamURL, connectionCtx.stream
			}
			// Check stream and retry if necessary.
			if stream == nil {
				log.Info("[tso] tso stream is not ready")
				if provider.updateConnectionCtxs(ctx, connectionCtxs) {
					continue streamChoosingLoop
				}
				timer := time.NewTimer(constants.RetryInterval)
				select {
				case <-ctx.Done():
					// Finish the collected requests if the context is canceled.
					td.cancelCollectedRequests(tsoBatchController, invalidStreamID, errors.WithStack(ctx.Err()))
					timer.Stop()
					return
				case <-streamLoopTimer.C:
					err = errs.ErrClientCreateTSOStream.FastGenByArgs(errs.RetryTimeoutErr)
					log.Error("[tso] create tso stream error", errs.ZapError(err))
					svcDiscovery.ScheduleCheckMemberChanged()
					// Finish the collected requests if the stream is failed to be created.
					td.cancelCollectedRequests(tsoBatchController, invalidStreamID, errors.WithStack(err))
					timer.Stop()
					continue tsoBatchLoop
				case <-timer.C:
					timer.Stop()
					continue streamChoosingLoop
				}
			}
			select {
			case <-streamCtx.Done():
				log.Info("[tso] tso stream is canceled", zap.String("stream-url", streamURL))
				// Set `stream` to nil and remove this stream from the `connectionCtxs` due to being canceled.
				connectionCtxs.Delete(streamURL)
				cancel()
				stream = nil
				continue
			default:
			}

			// Check if any error has occurred on this stream when receiving asynchronously.
			if err = stream.GetRecvError(); err != nil {
				exit := !td.handleProcessRequestError(ctx, bo, streamURL, cancel, err)
				stream = nil
				if exit {
					td.cancelCollectedRequests(tsoBatchController, invalidStreamID, errors.WithStack(ctx.Err()))
					return
				}
				continue
			}

			break streamChoosingLoop
		}

		noDelay := false
		failpoint.Inject("tsoDispatcherConcurrentModeNoDelay", func() {
			noDelay = true
		})

		// If concurrent RPC is enabled, the time for collecting each request batch is expected to be
		// estimatedRPCDuration / concurrency. Note the time mentioned here is counted from starting trying to collect
		// the batch, instead of the time when the first request arrives.
		// Here, if the elapsed time since starting collecting this batch didn't reach the expected batch time, then
		// continue collecting.
		if td.isConcurrentRPCEnabled() {
			estimatedLatency := stream.EstimatedRPCLatency()
			goalBatchTime := estimatedLatency / time.Duration(td.rpcConcurrency)

			failpoint.Inject("tsoDispatcherConcurrentModeAssertDelayDuration", func(val failpoint.Value) {
				if s, ok := val.(string); ok {
					expected, err := time.ParseDuration(s)
					if err != nil {
						panic(err)
					}
					if math.Abs(expected.Seconds()-goalBatchTime.Seconds()) > 1e-6 {
						log.Fatal("tsoDispatcher: trying to delay for unexpected duration for the batch", zap.Duration("goalBatchTime", goalBatchTime), zap.Duration("expectedBatchTime", expected))
					}
				} else {
					panic("invalid value for failpoint tsoDispatcherConcurrentModeAssertDelayDuration: expected string")
				}
			})

			waitTimerStart := time.Now()
			remainingBatchTime := goalBatchTime - waitTimerStart.Sub(currentBatchStartTime)
			if remainingBatchTime > 0 && !noDelay {
				if !batchingTimer.Stop() {
					select {
					case <-batchingTimer.C:
					default:
					}
				}
				batchingTimer.Reset(remainingBatchTime)

				err = tsoBatchController.FetchRequestsWithTimer(ctx, td.tsoRequestCh, batchingTimer)
				if err != nil {
					// There should not be other kinds of errors.
					log.Info("[tso] stop fetching the pending tso requests due to context canceled",
						zap.Error(err))
					td.cancelCollectedRequests(tsoBatchController, invalidStreamID, errors.WithStack(ctx.Err()))
					return
				}
			}
		}

		done := make(chan struct{})
		dl := newTSDeadline(option.Timeout, done, cancel)
		select {
		case <-ctx.Done():
			// Finish the collected requests if the context is canceled.
			td.cancelCollectedRequests(tsoBatchController, invalidStreamID, errors.WithStack(ctx.Err()))
			return
		case td.tsDeadlineCh <- dl:
		}
		// processRequests guarantees that the collected requests could be finished properly.
		err = td.processRequests(stream, tsoBatchController, done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err == nil {
			// A nil error returned by `processRequests` indicates that the request batch is started successfully.
			// In this case, the `tsoBatchController` will be put back to the pool when the request is finished
			// asynchronously (either successful or not). This infers that the current `tsoBatchController` object will
			// be asynchronously accessed after the `processRequests` call. As a result, we need to use another
			// `tsoBatchController` for collecting the next batch. Do to this, we set the `tsoBatchController` to nil so that
			// another one will be fetched from the pool at the beginning of the batching loop.
			// Otherwise, the `tsoBatchController` won't be processed in other goroutines concurrently, and it can be
			// reused in the next loop safely.
			tsoBatchController = nil
		} else {
			exit := !td.handleProcessRequestError(ctx, bo, streamURL, cancel, err)
			stream = nil
			if exit {
				return
			}
		}
	}
}

// handleProcessRequestError handles errors occurs when trying to process a TSO RPC request for the dispatcher loop.
// Returns true if the dispatcher loop is ok to continue. Otherwise, the dispatcher loop should be exited.
func (td *tsoDispatcher) handleProcessRequestError(ctx context.Context, bo *retry.Backoffer, streamURL string, streamCancelFunc context.CancelFunc, err error) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	svcDiscovery := td.provider.getServiceDiscovery()

	svcDiscovery.ScheduleCheckMemberChanged()
	log.Error("[tso] getTS error after processing requests",
		zap.String("stream-url", streamURL),
		zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
	// Set `stream` to nil and remove this stream from the `connectionCtxs` due to error.
	td.connectionCtxs.Delete(streamURL)
	streamCancelFunc()
	// Because ScheduleCheckMemberChanged is asynchronous, if the leader changes, we better call `updateMember` ASAP.
	if errs.IsLeaderChange(err) {
		if err := bo.Exec(ctx, svcDiscovery.CheckMemberChanged); err != nil {
			select {
			case <-ctx.Done():
				return false
			default:
			}
		}
		// Because the TSO Follower Proxy could be configured online,
		// If we change it from on -> off, background updateConnectionCtxs
		// will cancel the current stream, then the EOF error caused by cancel()
		// should not trigger the updateConnectionCtxs here.
		// So we should only call it when the leader changes.
		td.provider.updateConnectionCtxs(ctx, td.connectionCtxs)
	}

	return true
}

// updateConnectionCtxs updates the `connectionCtxs` regularly.
func (td *tsoDispatcher) connectionCtxsUpdater() {
	var (
		ctx            = td.ctx
		connectionCtxs = td.connectionCtxs
		provider       = td.provider
		option         = td.provider.getOption()
		updateTicker   = &time.Ticker{}
	)

	log.Info("[tso] start tso connection contexts updater")
	setNewUpdateTicker := func(ticker *time.Ticker) {
		if updateTicker.C != nil {
			updateTicker.Stop()
		}
		updateTicker = ticker
	}
	// Set to nil before returning to ensure that the existing ticker can be GC.
	defer setNewUpdateTicker(nil)

	for {
		provider.updateConnectionCtxs(ctx, connectionCtxs)
		select {
		case <-ctx.Done():
			log.Info("[tso] exit tso connection contexts updater")
			return
		case <-option.EnableTSOFollowerProxyCh:
			enableTSOFollowerProxy := option.GetEnableTSOFollowerProxy()
			log.Info("[tso] tso follower proxy status changed",
				zap.Bool("enable", enableTSOFollowerProxy))
			if enableTSOFollowerProxy && updateTicker.C == nil {
				// Because the TSO Follower Proxy is enabled,
				// the periodic check needs to be performed.
				setNewUpdateTicker(time.NewTicker(sd.MemberUpdateInterval))
			} else if !enableTSOFollowerProxy && updateTicker.C != nil {
				// Because the TSO Follower Proxy is disabled,
				// the periodic check needs to be turned off.
				setNewUpdateTicker(&time.Ticker{})
			} else {
				continue
			}
		case <-updateTicker.C:
			// Triggered periodically when the TSO Follower Proxy is enabled.
		case <-td.updateConnectionCtxsCh:
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

// processRequests sends the RPC request for the batch. It's guaranteed that after calling this function, requests
// in the batch must be eventually finished (done or canceled), either synchronously or asynchronously.
// `close(done)` will be called at the same time when finishing the requests.
// If this function returns a non-nil error, the requests will always be canceled synchronously.
func (td *tsoDispatcher) processRequests(
	stream *tsoStream, tbc *batch.Controller[*Request], done chan struct{},
) error {
	// `done` must be guaranteed to be eventually called.
	var (
		requests     = tbc.GetCollectedRequests()
		traceRegions = make([]*trace.Region, 0, len(requests))
		spans        = make([]opentracing.Span, 0, len(requests))
	)
	for _, req := range requests {
		traceRegions = append(traceRegions, trace.StartRegion(req.requestCtx, "pdclient.tsoReqSend"))
		if span := opentracing.SpanFromContext(req.requestCtx); span != nil && span.Tracer() != nil {
			spans = append(spans, span.Tracer().StartSpan("pdclient.processRequests", opentracing.ChildOf(span.Context())))
		}
	}
	defer func() {
		for i := len(spans) - 1; i >= 0; i-- {
			spans[i].Finish()
		}
		for i := len(traceRegions) - 1; i >= 0; i-- {
			traceRegions[i].End()
		}
	}()

	var (
		count              = int64(len(requests))
		svcDiscovery       = td.provider.getServiceDiscovery()
		clusterID          = svcDiscovery.GetClusterID()
		keyspaceID         = svcDiscovery.GetKeyspaceID()
		reqKeyspaceGroupID = svcDiscovery.GetKeyspaceGroupID()
	)

	// Load latest allocated ts for monotonicity assertion.
	tsoInfoBeforeReq := td.latestTSOInfo.Load()

	cb := func(result tsoRequestResult, reqKeyspaceGroupID uint32, err error) {
		// As golang doesn't allow double-closing a channel, here is implicitly a check that the callback
		// is never called twice or called while it's also being cancelled elsewhere.
		close(done)

		defer td.batchBufferPool.Put(tbc)
		if err != nil {
			td.cancelCollectedRequests(tbc, stream.streamID, err)
			return
		}

		curTSOInfo := &tsoInfo{
			tsoServer:           stream.getServerURL(),
			reqKeyspaceGroupID:  reqKeyspaceGroupID,
			respKeyspaceGroupID: result.respKeyspaceGroupID,
			respReceivedAt:      time.Now(),
			physical:            result.physical,
			logical:             result.logical,
			sourceStreamID:      stream.streamID,
		}
		// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
		firstLogical := result.logical - int64(result.count) + 1
		// Do the check before releasing the token.
		td.checkMonotonicity(tsoInfoBeforeReq, curTSOInfo, firstLogical)
		td.doneCollectedRequests(tbc, result.physical, firstLogical, stream.streamID)
	}

	err := stream.processRequests(
		clusterID, keyspaceID, reqKeyspaceGroupID,
		count, tbc.GetExtraBatchingStartTime(), cb)
	if err != nil {
		close(done)

		td.cancelCollectedRequests(tbc, stream.streamID, err)
		return err
	}
	return nil
}

func tsoRequestFinisher(physical, firstLogical int64, streamID string) batch.FinisherFunc[*Request] {
	return func(idx int, tsoReq *Request, err error) {
		// Retrieve the request context before the request is done to trace without race.
		requestCtx := tsoReq.requestCtx
		tsoReq.physical, tsoReq.logical = physical, firstLogical+int64(idx)
		tsoReq.streamID = streamID
		tsoReq.TryDone(err)
		trace.StartRegion(requestCtx, "pdclient.tsoReqDequeue").End()
	}
}

func (td *tsoDispatcher) cancelCollectedRequests(tbc *batch.Controller[*Request], streamID string, err error) {
	td.tokenCh <- struct{}{}
	tbc.FinishCollectedRequests(tsoRequestFinisher(0, 0, streamID), err)
}

func (td *tsoDispatcher) doneCollectedRequests(tbc *batch.Controller[*Request], physical, firstLogical int64, streamID string) {
	td.tokenCh <- struct{}{}
	tbc.FinishCollectedRequests(tsoRequestFinisher(physical, firstLogical, streamID), nil)
}

// checkMonotonicity checks whether the monotonicity of the TSO allocation is violated.
// It asserts (curTSOInfo, firstLogical) must be larger than lastTSOInfo, and updates td.latestTSOInfo if it grows.
//
// Note that when concurrent RPC is enabled, the lastTSOInfo may not be the latest value stored in td.latestTSOInfo
// field. Instead, it's the value that was loaded just before the current RPC request's beginning. The reason is,
// if two requests processing time has overlap, they don't have a strong order, and the later-finished one may be
// allocated later (with larger value) than another. We only need to guarantee request A returns larger ts than B
// if request A *starts* after request B *finishes*.
func (td *tsoDispatcher) checkMonotonicity(
	lastTSOInfo *tsoInfo, curTSOInfo *tsoInfo, firstLogical int64,
) {
	keyspaceID := td.provider.getServiceDiscovery().GetKeyspaceID()
	if lastTSOInfo != nil {
		if lastTSOInfo.respKeyspaceGroupID != curTSOInfo.respKeyspaceGroupID {
			log.Info("[tso] keyspace group changed",
				zap.Uint32("old-group-id", lastTSOInfo.respKeyspaceGroupID),
				zap.Uint32("new-group-id", curTSOInfo.respKeyspaceGroupID))
		}
		// The TSO we get is a range like [largestLogical-count+1, largestLogical], so we save the last TSO's largest logical
		// to compare with the new TSO's first logical. For example, if we have a TSO resp with logical 10, count 5, then
		// all TSOs we get will be [6, 7, 8, 9, 10]. latestTSOInfo.logical stores the logical part of the largest ts returned
		// last time.
		if tsoutil.TSLessEqual(curTSOInfo.physical, firstLogical, lastTSOInfo.physical, lastTSOInfo.logical) {
			log.Panic("[tso] timestamp fallback",
				zap.Uint32("keyspace", keyspaceID),
				zap.String("last-ts", fmt.Sprintf("(%d, %d)", lastTSOInfo.physical, lastTSOInfo.logical)),
				zap.String("cur-ts", fmt.Sprintf("(%d, %d)", curTSOInfo.physical, firstLogical)),
				zap.String("last-tso-server", lastTSOInfo.tsoServer),
				zap.String("cur-tso-server", curTSOInfo.tsoServer),
				zap.Uint32("last-keyspace-group-in-request", lastTSOInfo.reqKeyspaceGroupID),
				zap.Uint32("cur-keyspace-group-in-request", curTSOInfo.reqKeyspaceGroupID),
				zap.Uint32("last-keyspace-group-in-response", lastTSOInfo.respKeyspaceGroupID),
				zap.Uint32("cur-keyspace-group-in-response", curTSOInfo.respKeyspaceGroupID),
				zap.Time("last-response-received-at", lastTSOInfo.respReceivedAt),
				zap.Time("cur-response-received-at", curTSOInfo.respReceivedAt),
				zap.String("last-stream-id", lastTSOInfo.sourceStreamID),
				zap.String("cur-stream-id", curTSOInfo.sourceStreamID))
		}
	}

	if td.latestTSOInfo.CompareAndSwap(nil, curTSOInfo) {
		// If latestTSOInfo is missing, simply store it and exit.
		return
	}

	// Replace if we are holding a larger ts than that has been recorded.
	for {
		old := td.latestTSOInfo.Load()
		if tsoutil.TSLessEqual(curTSOInfo.physical, curTSOInfo.logical, old.physical, old.logical) {
			// The current one is large enough. Skip.
			break
		}
		if td.latestTSOInfo.CompareAndSwap(old, curTSOInfo) {
			// Successfully replaced.
			break
		}
	}
}

// checkTSORPCConcurrency checks configurations about TSO RPC concurrency, and adjust the token count if needed.
// Some other options (EnableTSOFollowerProxy and MaxTSOBatchWaitInterval) may affect the availability of concurrent
// RPC requests. As the dispatcher loop loads MaxTSOBatchWaitInterval in each single circle, pass it directly to this
// function. Other configurations will be loaded within this function when needed.
//
// Behavior of the function:
//   - As concurrent TSO RPC requests is an optimization aiming on the opposite purpose to that of EnableTSOFollowerProxy
//     and MaxTSOBatchWaitInterval, so once either EnableTSOFollowerProxy and MaxTSOBatchWaitInterval is enabled, the
//     concurrency will always be set to 1 no matter how the user configured it.
//   - Normally, this function takes effect in a limited frequency controlled by dispatcherCheckRPCConcurrencyInterval.
//     However, if the RPC concurrency is set to more than 1, and MaxTSOBatchWaitInterval is changed from disabled into
//     enabled (0 -> positive), this function takes effect immediately to disable concurrent RPC requests.
//   - After this function takes effect, the final decision of concurrency and token count will be set to
//     td.rpcConcurrency and td.tokenCount; and tokens available in td.tokenCh will also be adjusted.
func (td *tsoDispatcher) checkTSORPCConcurrency(ctx context.Context, maxBatchWaitInterval time.Duration, now time.Time) error {
	// If we currently enabled concurrent TSO RPC requests, but `maxBatchWaitInterval` is a positive value, it must
	// because that MaxTSOBatchWaitInterval is just enabled. In this case, disable concurrent TSO RPC requests
	// immediately, because MaxTSOBatchWaitInterval and concurrent RPC requests has opposite purpose.
	immediatelyUpdate := td.rpcConcurrency > 1 && maxBatchWaitInterval > 0

	// Allow always updating for test purpose.
	failpoint.Inject("tsoDispatcherAlwaysCheckConcurrency", func() {
		immediatelyUpdate = true
	})

	if !immediatelyUpdate && now.Sub(td.lastCheckConcurrencyTime) < dispatcherCheckRPCConcurrencyInterval {
		return nil
	}
	td.lastCheckConcurrencyTime = now

	newConcurrency := td.provider.getOption().GetTSOClientRPCConcurrency()
	if maxBatchWaitInterval > 0 || td.provider.getOption().GetEnableTSOFollowerProxy() {
		newConcurrency = 1
	}

	if newConcurrency == td.rpcConcurrency {
		return nil
	}

	log.Info("[tso] switching tso rpc concurrency", zap.Int("old", td.rpcConcurrency), zap.Int("new", newConcurrency))
	td.rpcConcurrency = newConcurrency

	// Find a proper token count.
	// When the concurrency is set to 1, there's only 1 token, which means only 1 RPC request can run at the same
	// time.
	// When the concurrency is set to more than 1, the time interval between sending two batches of requests is
	// controlled by an estimation of an average RPC duration. But as the duration of an RPC may jitter in the network,
	// and an RPC request may finish earlier or later. So we allow there to be the actual number of concurrent ongoing
	// request to be fluctuating. So in this case, the token count will be set to 2 times the expected concurrency.
	newTokenCount := newConcurrency
	if newConcurrency > 1 {
		newTokenCount = newConcurrency * 2
	}

	if newTokenCount > td.tokenCount {
		for td.tokenCount < newTokenCount {
			td.tokenCh <- struct{}{}
			td.tokenCount++
		}
	} else if newTokenCount < td.tokenCount {
		for td.tokenCount > newTokenCount {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-td.tokenCh:
			}
			td.tokenCount--
		}
	}
	return nil
}

func (td *tsoDispatcher) isConcurrentRPCEnabled() bool {
	return td.rpcConcurrency > 1
}
