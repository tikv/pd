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
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/client/timerpool"
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/zap"
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
	timer := timerpool.GlobalTimerPool.Get(timeout)
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
}

type tsoServiceProvider interface {
	getOption() *option
	getServiceDiscovery() ServiceDiscovery
	updateConnectionCtxs(ctx context.Context, dc string, connectionCtxs *sync.Map) bool
}

type tsoDispatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	dc     string

	provider tsoServiceProvider
	// URL -> *connectionContext
	connectionCtxs  *sync.Map
	batchController *tsoBatchController
	tsDeadlineCh    chan *deadline
	lastTSOInfo     *tsoInfo

	updateConnectionCtxsCh chan struct{}
}

func newTSODispatcher(
	ctx context.Context,
	dc string,
	maxBatchSize int,
	provider tsoServiceProvider,
) *tsoDispatcher {
	dispatcherCtx, dispatcherCancel := context.WithCancel(ctx)
	tsoBatchController := newTSOBatchController(
		make(chan *tsoRequest, maxBatchSize*2),
		maxBatchSize,
	)
	failpoint.Inject("shortDispatcherChannel", func() {
		tsoBatchController = newTSOBatchController(
			make(chan *tsoRequest, 1),
			maxBatchSize,
		)
	})
	td := &tsoDispatcher{
		ctx:                    dispatcherCtx,
		cancel:                 dispatcherCancel,
		dc:                     dc,
		provider:               provider,
		connectionCtxs:         &sync.Map{},
		batchController:        tsoBatchController,
		tsDeadlineCh:           make(chan *deadline, 1),
		updateConnectionCtxsCh: make(chan struct{}, 1),
	}
	go td.watchTSDeadline()
	return td
}

func (td *tsoDispatcher) watchTSDeadline() {
	log.Info("[tso] start tso deadline watcher", zap.String("dc-location", td.dc))
	defer log.Info("[tso] exit tso deadline watcher", zap.String("dc-location", td.dc))
	for {
		select {
		case d := <-td.tsDeadlineCh:
			select {
			case <-d.timer.C:
				log.Error("[tso] tso request is canceled due to timeout",
					zap.String("dc-location", td.dc), errs.ZapError(errs.ErrClientGetTSOTimeout))
				d.cancel()
				timerpool.GlobalTimerPool.Put(d.timer)
			case <-d.done:
				timerpool.GlobalTimerPool.Put(d.timer)
			case <-td.ctx.Done():
				timerpool.GlobalTimerPool.Put(d.timer)
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

func (td *tsoDispatcher) close() {
	td.cancel()
	td.batchController.clear()
}

func (td *tsoDispatcher) push(request *tsoRequest) {
	td.batchController.tsoRequestCh <- request
}

func (td *tsoDispatcher) handleDispatcher(wg *sync.WaitGroup) {
	var (
		ctx             = td.ctx
		dc              = td.dc
		provider        = td.provider
		svcDiscovery    = provider.getServiceDiscovery()
		option          = provider.getOption()
		connectionCtxs  = td.connectionCtxs
		batchController = td.batchController
	)
	log.Info("[tso] tso dispatcher created", zap.String("dc-location", dc))
	// Clean up the connectionCtxs when the dispatcher exits.
	defer func() {
		log.Info("[tso] exit tso dispatcher", zap.String("dc-location", dc))
		// Cancel all connections.
		connectionCtxs.Range(func(_, cc any) bool {
			cc.(*tsoConnectionContext).cancel()
			return true
		})
		// Clear the tso batch controller.
		batchController.clear()
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
	streamLoopTimer := time.NewTimer(option.timeout)
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
		maxBatchWaitInterval := option.getMaxTSOBatchWaitInterval()
		// Once the TSO requests are collected, must make sure they could be finished or revoked eventually,
		// otherwise the upper caller may get blocked on waiting for the results.
		if err = batchController.fetchPendingRequests(ctx, maxBatchWaitInterval); err != nil {
			// Finish the collected requests if the fetch failed.
			batchController.finishCollectedRequests(0, 0, 0, errors.WithStack(err))
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
			batchController.adjustBestBatchSize()
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
		streamLoopTimer.Reset(option.timeout)
		// Choose a stream to send the TSO gRPC request.
	streamChoosingLoop:
		for {
			connectionCtx := chooseStream(connectionCtxs)
			if connectionCtx != nil {
				streamCtx, cancel, streamURL, stream = connectionCtx.ctx, connectionCtx.cancel, connectionCtx.streamURL, connectionCtx.stream
			}
			// Check stream and retry if necessary.
			if stream == nil {
				log.Info("[tso] tso stream is not ready", zap.String("dc", dc))
				if provider.updateConnectionCtxs(ctx, dc, connectionCtxs) {
					continue streamChoosingLoop
				}
				timer := time.NewTimer(retryInterval)
				select {
				case <-ctx.Done():
					// Finish the collected requests if the context is canceled.
					batchController.finishCollectedRequests(0, 0, 0, errors.WithStack(ctx.Err()))
					timer.Stop()
					return
				case <-streamLoopTimer.C:
					err = errs.ErrClientCreateTSOStream.FastGenByArgs(errs.RetryTimeoutErr)
					log.Error("[tso] create tso stream error", zap.String("dc-location", dc), errs.ZapError(err))
					svcDiscovery.ScheduleCheckMemberChanged()
					// Finish the collected requests if the stream is failed to be created.
					batchController.finishCollectedRequests(0, 0, 0, errors.WithStack(err))
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
		dl := newTSDeadline(option.timeout, done, cancel)
		select {
		case <-ctx.Done():
			// Finish the collected requests if the context is canceled.
			batchController.finishCollectedRequests(0, 0, 0, errors.WithStack(ctx.Err()))
			return
		case td.tsDeadlineCh <- dl:
		}
		// processRequests guarantees that the collected requests could be finished properly.
		err = td.processRequests(stream, dc, td.batchController)
		close(done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			svcDiscovery.ScheduleCheckMemberChanged()
			log.Error("[tso] getTS error after processing requests",
				zap.String("dc-location", dc),
				zap.String("stream-url", streamURL),
				zap.Error(errs.ErrClientGetTSO.FastGenByArgs(err.Error())))
			// Set `stream` to nil and remove this stream from the `connectionCtxs` due to error.
			connectionCtxs.Delete(streamURL)
			cancel()
			stream = nil
			// Because ScheduleCheckMemberChanged is asynchronous, if the leader changes, we better call `updateMember` ASAP.
			if errs.IsLeaderChange(err) {
				if err := bo.Exec(ctx, svcDiscovery.CheckMemberChanged); err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
				// Because the TSO Follower Proxy could be configured online,
				// If we change it from on -> off, background updateConnectionCtxs
				// will cancel the current stream, then the EOF error caused by cancel()
				// should not trigger the updateConnectionCtxs here.
				// So we should only call it when the leader changes.
				provider.updateConnectionCtxs(ctx, dc, connectionCtxs)
			}
		}
	}
}

// updateConnectionCtxs updates the `connectionCtxs` for the specified DC location regularly.
func (td *tsoDispatcher) connectionCtxsUpdater() {
	var (
		ctx            = td.ctx
		dc             = td.dc
		connectionCtxs = td.connectionCtxs
		provider       = td.provider
		option         = td.provider.getOption()
		updateTicker   = &time.Ticker{}
	)

	log.Info("[tso] start tso connection contexts updater", zap.String("dc-location", dc))
	setNewUpdateTicker := func(ticker *time.Ticker) {
		if updateTicker.C != nil {
			updateTicker.Stop()
		}
		updateTicker = ticker
	}
	// Set to nil before returning to ensure that the existing ticker can be GC.
	defer setNewUpdateTicker(nil)

	for {
		provider.updateConnectionCtxs(ctx, dc, connectionCtxs)
		select {
		case <-ctx.Done():
			log.Info("[tso] exit tso connection contexts updater", zap.String("dc-location", dc))
			return
		case <-option.enableTSOFollowerProxyCh:
			// TODO: implement support of TSO Follower Proxy for the Local TSO.
			if dc != globalDCLocation {
				continue
			}
			enableTSOFollowerProxy := option.getEnableTSOFollowerProxy()
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

func (td *tsoDispatcher) processRequests(
	stream *tsoStream, dcLocation string, tbc *tsoBatchController,
) error {
	var (
		requests     = tbc.getCollectedRequests()
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
		for i := range spans {
			spans[i].Finish()
		}
		for i := range traceRegions {
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
	respKeyspaceGroupID, physical, logical, suffixBits, err := stream.processRequests(
		clusterID, keyspaceID, reqKeyspaceGroupID,
		dcLocation, count, tbc.batchStartTime)
	if err != nil {
		tbc.finishCollectedRequests(0, 0, 0, err)
		return err
	}
	curTSOInfo := &tsoInfo{
		tsoServer:           stream.getServerURL(),
		reqKeyspaceGroupID:  reqKeyspaceGroupID,
		respKeyspaceGroupID: respKeyspaceGroupID,
		respReceivedAt:      time.Now(),
		physical:            physical,
		logical:             logical,
	}
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	firstLogical := tsoutil.AddLogical(logical, -count+1, suffixBits)
	td.compareAndSwapTS(curTSOInfo, firstLogical)
	tbc.finishCollectedRequests(physical, firstLogical, suffixBits, nil)
	return nil
}

func (td *tsoDispatcher) compareAndSwapTS(
	curTSOInfo *tsoInfo, firstLogical int64,
) {
	if td.lastTSOInfo != nil {
		var (
			lastTSOInfo = td.lastTSOInfo
			dc          = td.dc
			physical    = curTSOInfo.physical
			keyspaceID  = td.provider.getServiceDiscovery().GetKeyspaceID()
		)
		if td.lastTSOInfo.respKeyspaceGroupID != curTSOInfo.respKeyspaceGroupID {
			log.Info("[tso] keyspace group changed",
				zap.String("dc-location", dc),
				zap.Uint32("old-group-id", lastTSOInfo.respKeyspaceGroupID),
				zap.Uint32("new-group-id", curTSOInfo.respKeyspaceGroupID))
		}
		// The TSO we get is a range like [largestLogical-count+1, largestLogical], so we save the last TSO's largest logical
		// to compare with the new TSO's first logical. For example, if we have a TSO resp with logical 10, count 5, then
		// all TSOs we get will be [6, 7, 8, 9, 10]. lastTSOInfo.logical stores the logical part of the largest ts returned
		// last time.
		if tsoutil.TSLessEqual(physical, firstLogical, lastTSOInfo.physical, lastTSOInfo.logical) {
			log.Panic("[tso] timestamp fallback",
				zap.String("dc-location", dc),
				zap.Uint32("keyspace", keyspaceID),
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
	}
	td.lastTSOInfo = curTSOInfo
}
