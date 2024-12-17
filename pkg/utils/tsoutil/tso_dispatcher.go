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

package tsoutil

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/timerutil"
)

const (
	maxMergeRequests = 10000
	// DefaultTSOProxyTimeout defines the default timeout value of TSP Proxying
	DefaultTSOProxyTimeout = 3 * time.Second
)

type tsoResp interface {
	GetTimestamp() *pdpb.Timestamp
}

// TSODispatcher dispatches the TSO requests to the corresponding forwarding TSO channels.
type TSODispatcher struct {
	tsoProxyHandleDuration prometheus.Histogram
	tsoProxyBatchSize      prometheus.Histogram

	// dispatchChs is used to dispatch different TSO requests to the corresponding forwarding TSO channels.
	dispatchChs sync.Map // Store as map[string]chan Request
}

// NewTSODispatcher creates and returns a TSODispatcher
func NewTSODispatcher(tsoProxyHandleDuration, tsoProxyBatchSize prometheus.Histogram) *TSODispatcher {
	tsoDispatcher := &TSODispatcher{
		tsoProxyHandleDuration: tsoProxyHandleDuration,
		tsoProxyBatchSize:      tsoProxyBatchSize,
	}
	return tsoDispatcher
}

// DispatchRequest is the entry point for dispatching/forwarding a tso request to the destination host
func (s *TSODispatcher) DispatchRequest(
	ctx context.Context,
	req Request,
	tsoProtoFactory ProtoFactory,
	doneCh <-chan struct{},
	errCh chan<- error,
	tsoPrimaryWatchers ...*etcdutil.LoopWatcher) {
	val, loaded := s.dispatchChs.LoadOrStore(req.getForwardedHost(), make(chan Request, maxMergeRequests))
	reqCh := val.(chan Request)
	if !loaded {
		tsDeadlineCh := make(chan *TSDeadline, 1)
		go s.dispatch(ctx, tsoProtoFactory, req.getForwardedHost(), req.getClientConn(), reqCh, tsDeadlineCh, doneCh, errCh, tsoPrimaryWatchers...)
		go WatchTSDeadline(ctx, tsDeadlineCh)
	}
	reqCh <- req
}

func (s *TSODispatcher) dispatch(
	ctx context.Context,
	tsoProtoFactory ProtoFactory,
	forwardedHost string,
	clientConn *grpc.ClientConn,
	tsoRequestCh <-chan Request,
	tsDeadlineCh chan<- *TSDeadline,
	doneCh <-chan struct{},
	errCh chan<- error,
	tsoPrimaryWatchers ...*etcdutil.LoopWatcher) {
	defer logutil.LogPanic()
	dispatcherCtx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	defer s.dispatchChs.Delete(forwardedHost)

	forwardStream, cancel, err := tsoProtoFactory.createForwardStream(ctx, clientConn)
	if err != nil || forwardStream == nil {
		log.Error("create tso forwarding stream error",
			zap.String("forwarded-host", forwardedHost),
			errs.ZapError(errs.ErrGRPCCreateStream, err))
		select {
		case <-dispatcherCtx.Done():
			return
		case _, ok := <-doneCh:
			if !ok {
				return
			}
		case errCh <- err:
			close(errCh)
			return
		}
	}
	defer cancel()

	requests := make([]Request, maxMergeRequests+1)
	needUpdateServicePrimaryAddr := len(tsoPrimaryWatchers) > 0 && tsoPrimaryWatchers[0] != nil
	for {
		select {
		case first := <-tsoRequestCh:
			pendingTSOReqCount := len(tsoRequestCh) + 1
			requests[0] = first
			for i := 1; i < pendingTSOReqCount; i++ {
				requests[i] = <-tsoRequestCh
			}
			done := make(chan struct{})
			dl := NewTSDeadline(DefaultTSOProxyTimeout, done, cancel)
			select {
			case tsDeadlineCh <- dl:
			case <-dispatcherCtx.Done():
				return
			}
			err = s.processRequests(forwardStream, requests[:pendingTSOReqCount])
			close(done)
			if err != nil {
				log.Error("proxy forward tso error",
					zap.String("forwarded-host", forwardedHost),
					errs.ZapError(errs.ErrGRPCSend, err))
				if needUpdateServicePrimaryAddr && strings.Contains(err.Error(), errs.NotLeaderErr) {
					tsoPrimaryWatchers[0].ForceLoad()
				}
				select {
				case <-dispatcherCtx.Done():
					return
				case _, ok := <-doneCh:
					if !ok {
						return
					}
				case errCh <- err:
					close(errCh)
					return
				}
			}
		case <-dispatcherCtx.Done():
			return
		}
	}
}

func (s *TSODispatcher) processRequests(forwardStream stream, requests []Request) error {
	// Merge the requests
	count := uint32(0)
	for _, request := range requests {
		count += request.getCount()
	}

	start := time.Now()
	resp, err := requests[0].process(forwardStream, count)
	if err != nil {
		return err
	}
	s.tsoProxyHandleDuration.Observe(time.Since(start).Seconds())
	s.tsoProxyBatchSize.Observe(float64(count))
	// Split the response
	ts := resp.GetTimestamp()
	physical, logical := ts.GetPhysical(), ts.GetLogical()
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	// This is different from the logic of client batch, for example, if we have a largest ts whose logical part is 10,
	// count is 5, then the splitting results should be 5 and 10.
	firstLogical := addLogical(logical, -int64(count))
	return s.finishRequest(requests, physical, firstLogical)
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64) int64 {
	return logical + count
}

func (*TSODispatcher) finishRequest(requests []Request, physical, firstLogical int64) error {
	countSum := int64(0)
	for i := range requests {
		newCountSum, err := requests[i].postProcess(countSum, physical, firstLogical)
		if err != nil {
			return err
		}
		countSum = newCountSum
	}
	return nil
}

// TSDeadline is used to watch the deadline of each tso request.
type TSDeadline struct {
	timer  *time.Timer
	done   chan struct{}
	cancel context.CancelFunc
}

// NewTSDeadline creates a new TSDeadline.
func NewTSDeadline(
	timeout time.Duration,
	done chan struct{},
	cancel context.CancelFunc,
) *TSDeadline {
	timer := timerutil.GlobalTimerPool.Get(timeout)
	return &TSDeadline{
		timer:  timer,
		done:   done,
		cancel: cancel,
	}
}

// WatchTSDeadline watches the deadline of each tso request.
func WatchTSDeadline(ctx context.Context, tsDeadlineCh <-chan *TSDeadline) {
	defer logutil.LogPanic()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case d := <-tsDeadlineCh:
			select {
			case <-d.timer.C:
				log.Error("tso proxy request processing is canceled due to timeout",
					errs.ZapError(errs.ErrProxyTSOTimeout))
				d.cancel()
				timerutil.GlobalTimerPool.Put(d.timer)
			case <-d.done:
				timerutil.GlobalTimerPool.Put(d.timer)
			case <-ctx.Done():
				timerutil.GlobalTimerPool.Put(d.timer)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
