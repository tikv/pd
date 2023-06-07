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
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

	ctx context.Context
	// dispatchChs is used to dispatch different TSO requests to the corresponding forwarding TSO channels.
	dispatchChs sync.Map // Store as map[string]chan Request (forwardedHost -> dispatch channel)
	// lastErrors is used to record the last error of each forwarding TSO channel.
	lastErrors sync.Map // Store as map[string]error (forwardedHost -> last error)
}

// NewTSODispatcher creates and returns a TSODispatcher
func NewTSODispatcher(
	ctx context.Context, tsoProxyHandleDuration, tsoProxyBatchSize prometheus.Histogram,
) *TSODispatcher {
	tsoDispatcher := &TSODispatcher{
		ctx:                    ctx,
		tsoProxyHandleDuration: tsoProxyHandleDuration,
		tsoProxyBatchSize:      tsoProxyBatchSize,
	}
	return tsoDispatcher
}

// GetAndDeleteLastError gets and deletes the last error of the forwarded host
func (s *TSODispatcher) GetAndDeleteLastError(forwardedHost string) error {
	if val, loaded := s.lastErrors.LoadAndDelete(forwardedHost); loaded {
		return val.(error)
	}
	return nil
}

// DispatchRequest is the entry point for dispatching/forwarding a tso request to the destination host
func (s *TSODispatcher) DispatchRequest(req Request, tsoProtoFactory ProtoFactory) {
	val, loaded := s.dispatchChs.LoadOrStore(req.getForwardedHost(), make(chan Request, maxMergeRequests))
	reqCh := val.(chan Request)
	if !loaded {
		go s.startDispatchLoop(req.getForwardedHost(), req.getClientConn(), reqCh, tsoProtoFactory)
	}
	reqCh <- req
}

// cleanup cleans up the pending requests for the forwarded host
func (s *TSODispatcher) cleanup(forwardedHost string, finalForwardErr error, pendingRequests []Request) {
	val, loaded := s.dispatchChs.LoadAndDelete(forwardedHost)
	if loaded {
		reqCh := val.(chan Request)
		waitingReqCount := len(reqCh)
		for i := 0; i < waitingReqCount; i++ {
			req := <-reqCh
			pendingRequests = append(pendingRequests, req)
		}
	}
	if finalForwardErr != nil {
		for _, pendingRequest := range pendingRequests {
			if pendingRequest != nil {
				pendingRequest.sendErrorResponseAsync(finalForwardErr)
			}
		}
	} else if len(pendingRequests) > 0 {
		log.Warn("the dispatch loop exited with pending requests unprocessed",
			zap.String("forwarded-host", forwardedHost),
			zap.Int("pending-requests-count", len(pendingRequests)))
	}
}

// startDispatchLoop starts the dispatch loop for the forwarded host
func (s *TSODispatcher) startDispatchLoop(
	forwardedHost string, clientConn *grpc.ClientConn,
	tsoRequestCh <-chan Request, tsoProtoFactory ProtoFactory,
) {
	defer logutil.LogPanic()
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// forwardErr indicates the failure in the forwarding stream which causes the dispatch loop to exit.
	var (
		forwardErr    error
		forwardStream stream
	)
	pendingRequests := make([]Request, maxMergeRequests+1)
	pendingTSOReqCount := 0

	log.Info("start the dispatch loop", zap.String("forwarded-host", forwardedHost))
	defer func() {
		log.Info("exiting from the dispatch loop. cleaning up the pending requests",
			zap.String("forwarded-host", forwardedHost))
		if forwardStream != nil {
			forwardStream.closeSend()
		}
		s.cleanup(forwardedHost, forwardErr, pendingRequests[:pendingTSOReqCount])
		log.Info("the dispatch loop exited", zap.String("forwarded-host", forwardedHost))
	}()

	forwardStream, _, forwardErr = tsoProtoFactory.createForwardStream(ctx, clientConn)
	if forwardErr != nil {
		log.Error("create tso forwarding stream error",
			zap.String("forwarded-host", forwardedHost),
			errs.ZapError(errs.ErrGRPCCreateStream, forwardErr))
		s.lastErrors.Store(forwardedHost, forwardErr)
		return
	}

	tsDeadlineCh := make(chan deadline, 1)
	go watchTSDeadline(ctx, forwardedHost, tsDeadlineCh)

	for {
		select {
		case first := <-tsoRequestCh:
			pendingTSOReqCount = len(tsoRequestCh) + 1
			pendingRequests[0] = first
			for i := 1; i < pendingTSOReqCount; i++ {
				pendingRequests[i] = <-tsoRequestCh
			}
			forwardErr = s.processRequestsWithDeadLine(
				ctx, tsDeadlineCh, forwardStream, pendingRequests[:pendingTSOReqCount], tsoProtoFactory)
			if forwardErr != nil {
				log.Error("proxy forward tso error",
					zap.String("forwarded-host", forwardedHost),
					errs.ZapError(errs.ErrGRPCSend, forwardErr))
				s.lastErrors.Store(forwardedHost, forwardErr)
				return
			}
			// All requests are processed successfully, reset this counter to avoid unnecessary cleanup.
			pendingTSOReqCount = 0
		case <-ctx.Done():
			return
		}
	}
}

func (s *TSODispatcher) processRequestsWithDeadLine(
	ctx context.Context, tsDeadlineCh chan<- deadline, forwardStream stream,
	requests []Request, tsoProtoFactory ProtoFactory,
) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	dl := deadline{
		timer:  time.After(DefaultTSOProxyTimeout),
		done:   done,
		cancel: cancel,
	}
	select {
	case tsDeadlineCh <- dl:
	case <-cctx.Done():
		return nil
	}
	err := s.processRequests(forwardStream, requests, tsoProtoFactory)
	close(done)
	return err
}

func (s *TSODispatcher) processRequests(forwardStream stream, requests []Request, tsoProtoFactory ProtoFactory) error {
	// Merge the requests
	count := uint32(0)
	for _, request := range requests {
		count += request.getCount()
	}

	start := time.Now()
	resp, err := requests[0].process(forwardStream, count, tsoProtoFactory)
	if err != nil {
		return err
	}
	s.tsoProxyHandleDuration.Observe(time.Since(start).Seconds())
	s.tsoProxyBatchSize.Observe(float64(count))
	// Split the response
	ts := resp.GetTimestamp()
	physical, logical, suffixBits := ts.GetPhysical(), ts.GetLogical(), ts.GetSuffixBits()
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	// This is different from the logic of client batch, for example, if we have a largest ts whose logical part is 10,
	// count is 5, then the splitting results should be 5 and 10.
	firstLogical := addLogical(logical, -int64(count), suffixBits)
	s.finishRequest(requests, physical, firstLogical, suffixBits)
	return nil
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64, suffixBits uint32) int64 {
	return logical + count<<suffixBits
}

func (s *TSODispatcher) finishRequest(requests []Request, physical, firstLogical int64, suffixBits uint32) {
	countSum := int64(0)
	for i := 0; i < len(requests); i++ {
		countSum = requests[i].sendResponseAsync(countSum, physical, firstLogical, suffixBits)
	}
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func watchTSDeadline(ctx context.Context, forwardedHost string, tsDeadlineCh <-chan deadline) {
	defer logutil.LogPanic()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Info("start to watch tso proxy request deadline", zap.String("forwarded-host", forwardedHost))
	defer func() {
		log.Info("tso proxy request deadline watch loop is closed",
			zap.String("forwarded-host", forwardedHost))
	}()

	for {
		select {
		case d := <-tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso proxy request processing is canceled due to timeout",
					errs.ZapError(errs.ErrProxyTSOTimeout))
				d.cancel()
			case <-d.done:
				continue
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func checkStream(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer logutil.LogPanic()

	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}
