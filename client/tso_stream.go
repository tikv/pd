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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/client/errs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TSO Stream Builder Factory

type tsoStreamBuilderFactory interface {
	makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder
}

type pdTSOStreamBuilderFactory struct{}

func (*pdTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &pdTSOStreamBuilder{client: pdpb.NewPDClient(cc), serverURL: cc.Target()}
}

type tsoTSOStreamBuilderFactory struct{}

func (*tsoTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &tsoTSOStreamBuilder{client: tsopb.NewTSOClient(cc), serverURL: cc.Target()}
}

// TSO Stream Builder

type tsoStreamBuilder interface {
	build(context.Context, context.CancelFunc, time.Duration) (*tsoStream, error)
}

type pdTSOStreamBuilder struct {
	serverURL string
	client    pdpb.PDClient
}

func (b *pdTSOStreamBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (*tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return newTSOStream(ctx, b.serverURL, pdTSOStreamAdapter{stream}), nil
	}
	return nil, err
}

type tsoTSOStreamBuilder struct {
	serverURL string
	client    tsopb.TSOClient
}

func (b *tsoTSOStreamBuilder) build(
	ctx context.Context, cancel context.CancelFunc, timeout time.Duration,
) (*tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return newTSOStream(ctx, b.serverURL, tsoTSOStreamAdapter{stream}), nil
	}
	return nil, err
}

func checkStreamTimeout(ctx context.Context, cancel context.CancelFunc, done chan struct{}, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		cancel()
	case <-ctx.Done():
	}
	<-done
}

type tsoRequestResult struct {
	physical, logical   int64
	count               uint32
	suffixBits          uint32
	respKeyspaceGroupID uint32
}

type grpcTSOStreamAdapter interface {
	Send(clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string,
		count int64) error
	Recv() (tsoRequestResult, error)
}

type pdTSOStreamAdapter struct {
	stream pdpb.PD_TsoClient
}

// Send implements the grpcTSOStreamAdapter interface.
func (s pdTSOStreamAdapter) Send(clusterID uint64, _, _ uint32, dcLocation string, count int64) error {
	req := &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}
	return s.stream.Send(req)
}

// Recv implements the grpcTSOStreamAdapter interface.
func (s pdTSOStreamAdapter) Recv() (tsoRequestResult, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return tsoRequestResult{}, err
	}
	return tsoRequestResult{
		physical:            resp.GetTimestamp().GetPhysical(),
		logical:             resp.GetTimestamp().GetLogical(),
		count:               resp.GetCount(),
		suffixBits:          resp.GetTimestamp().GetSuffixBits(),
		respKeyspaceGroupID: defaultKeySpaceGroupID,
	}, nil
}

type tsoTSOStreamAdapter struct {
	stream tsopb.TSO_TsoClient
}

// Send implements the grpcTSOStreamAdapter interface.
func (s tsoTSOStreamAdapter) Send(clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string, count int64) error {
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       clusterID,
			KeyspaceId:      keyspaceID,
			KeyspaceGroupId: keyspaceGroupID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}
	return s.stream.Send(req)
}

// Recv implements the grpcTSOStreamAdapter interface.
func (s tsoTSOStreamAdapter) Recv() (tsoRequestResult, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return tsoRequestResult{}, err
	}
	return tsoRequestResult{
		physical:            resp.GetTimestamp().GetPhysical(),
		logical:             resp.GetTimestamp().GetLogical(),
		count:               resp.GetCount(),
		suffixBits:          resp.GetTimestamp().GetSuffixBits(),
		respKeyspaceGroupID: resp.GetHeader().GetKeyspaceGroupId(),
	}, nil
}

type onFinishedCallback func(result tsoRequestResult, reqKeyspaceGroupID uint32, err error)

type batchedRequests struct {
	startTime          time.Time
	count              int64
	reqKeyspaceGroupID uint32
	callback           onFinishedCallback
}

// tsoStream represents an abstracted stream for requesting TSO.
// This type designed decoupled with users of this type, so tsoDispatcher won't be directly accessed here.
// Also in order to avoid potential memory allocations that might happen when passing closures as the callback,
// we instead use the `batchedRequestsNotifier` as the abstraction, and accepts generic type instead of dynamic interface
// type.
type tsoStream struct {
	serverURL string
	// The internal gRPC stream.
	//   - `pdpb.PD_TsoClient` for a leader/follower in the PD cluster.
	//   - `tsopb.TSO_TsoClient` for a primary/secondary in the TSO cluster.
	stream grpcTSOStreamAdapter
	// An identifier of the tsoStream object for metrics reporting and diagnosing.
	streamID string

	pendingRequests chan batchedRequests

	wg sync.WaitGroup

	// For syncing between sender and receiver to guarantee all requests are finished when closing.
	state          atomic.Int32
	stoppedWithErr atomic.Pointer[error]

	ongoingRequestCountGauge prometheus.Gauge
	ongoingRequests          atomic.Int32
}

const (
	streamStateIdle int32 = iota
	streamStateSending
	streamStateClosing
)

var streamIDAlloc atomic.Int32

const invalidStreamID = "<invalid>"

func newTSOStream(ctx context.Context, serverURL string, stream grpcTSOStreamAdapter) *tsoStream {
	streamID := fmt.Sprintf("%s-%d", serverURL, streamIDAlloc.Add(1))
	s := &tsoStream{
		serverURL:       serverURL,
		stream:          stream,
		streamID:        streamID,
		pendingRequests: make(chan batchedRequests, 64),

		ongoingRequestCountGauge: ongoingRequestCountGauge.WithLabelValues(streamID),
	}
	s.wg.Add(1)
	go s.recvLoop(ctx)
	return s
}

func (s *tsoStream) getServerURL() string {
	return s.serverURL
}

// ProcessRequests starts an RPC to get a batch of timestamps and synchronously waits for the result.
//
// This function is NOT thread-safe. Don't call this function concurrently in multiple goroutines. Neither should this
// function be called concurrently with ProcessRequestsAsync.
//
// WARNING: The caller is responsible to guarantee that when using this function, there must NOT be any unfinished
// async requests started by ProcessRequestsAsync. Otherwise, it might cause multiple goroutines calling `RecvMsg`
// of gRPC, which is unsafe.
//
// Calling this may implicitly switch the stream to sync mode.
func (s *tsoStream) ProcessRequests(
	clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string, count int64, batchStartTime time.Time,
) (tsoRequestResult, error) {
	start := time.Now()
	if err := s.stream.Send(clusterID, keyspaceID, keyspaceGroupID, dcLocation, count); err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return tsoRequestResult{}, err
	}
	tsoBatchSendLatency.Observe(time.Since(batchStartTime).Seconds())
	res, err := s.stream.Recv()
	duration := time.Since(start).Seconds()
	if err != nil {
		requestFailedDurationTSO.Observe(duration)
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return tsoRequestResult{}, err
	}
	requestDurationTSO.Observe(duration)
	tsoBatchSize.Observe(float64(count))

	if res.count != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return tsoRequestResult{}, err
	}

	respKeyspaceGroupID := res.respKeyspaceGroupID
	physical, logical, suffixBits := res.physical, res.logical, res.suffixBits
	return tsoRequestResult{
		physical:            physical,
		logical:             logical,
		count:               uint32(count),
		suffixBits:          suffixBits,
		respKeyspaceGroupID: respKeyspaceGroupID,
	}, nil
}

// ProcessRequestsAsync starts an RPC to get a batch of timestamps without waiting for the result. When the result is ready,
// it will be passed th `notifier.finish`.
//
// This function is NOT thread-safe. Don't call this function concurrently in multiple goroutines. Neither should this
// function be called concurrently with ProcessRequests.
//
// It's guaranteed that the `callback` will be called, but when the request is failed to be scheduled, the callback
// will be ignored.
//
// Calling this may implicitly switch the stream to async mode.
func (s *tsoStream) ProcessRequestsAsync(
	clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string, count int64, batchStartTime time.Time, callback onFinishedCallback,
) error {
	start := time.Now()

	// Check if the stream is closing or closed, in which case no more requests should be put in.
	// Note that the prevState should be restored very soon, as the receiver may check
	prevState := s.state.Swap(streamStateSending)
	switch prevState {
	case streamStateIdle:
		// Expected case
	case streamStateClosing:
		s.state.Store(prevState)
		err := s.GetRecvError()
		log.Info("sending to closed tsoStream", zap.String("stream", s.streamID), zap.Error(err))
		if err == nil {
			err = errors.WithStack(errs.ErrClientTSOStreamClosed)
		}
		return err
	case streamStateSending:
		log.Fatal("unexpected concurrent sending on tsoStream", zap.String("stream", s.streamID))
	default:
		log.Fatal("unknown tsoStream state", zap.String("stream", s.streamID), zap.Int32("state", prevState))
	}

	select {
	case s.pendingRequests <- batchedRequests{
		startTime:          start,
		count:              count,
		reqKeyspaceGroupID: keyspaceGroupID,
		callback:           callback,
	}:
	default:
		s.state.Store(prevState)
		return errors.New("unexpected channel full")
	}
	s.state.Store(prevState)

	if err := s.stream.Send(clusterID, keyspaceID, keyspaceGroupID, dcLocation, count); err != nil {
		// As the request is already put into `pendingRequests`, the request should finally be canceled by the recvLoop.
		// So do not return error here to avoid double-cancelling.
		log.Warn("failed to send RPC request through tsoStream", zap.String("stream", s.streamID), zap.Error(err))
		return nil
	}
	tsoBatchSendLatency.Observe(time.Since(batchStartTime).Seconds())
	s.ongoingRequestCountGauge.Set(float64(s.ongoingRequests.Add(1)))
	return nil
}

func (s *tsoStream) recvLoop(ctx context.Context) {
	var finishWithErr error
	var currentReq batchedRequests
	var hasReq bool

	defer func() {
		if r := recover(); r != nil {
			log.Fatal("tsoStream.recvLoop internal panic", zap.Stack("stacktrace"), zap.Any("panicMessage", r))
		}

		if finishWithErr == nil {
			// The loop must exit with a non-nil error (including io.EOF and context.Canceled). This should be
			// unreachable code.
			log.Fatal("tsoStream.recvLoop exited without error info", zap.String("stream", s.streamID))
		}

		if hasReq {
			// There's an unfinished request, cancel it, otherwise it will be blocked forever.
			currentReq.callback(tsoRequestResult{}, currentReq.reqKeyspaceGroupID, finishWithErr)
		}

		s.stoppedWithErr.Store(&finishWithErr)
		for !s.state.CompareAndSwap(streamStateIdle, streamStateClosing) {
			switch state := s.state.Load(); state {
			case streamStateIdle, streamStateSending:
				// streamStateSending should switch to streamStateIdle very quickly. Spin until successfully setting to
				// streamStateClosing.
				continue
			case streamStateClosing:
				log.Warn("unexpected double closing of tsoStream", zap.String("stream", s.streamID))
			default:
				log.Fatal("unknown tsoStream state", zap.String("stream", s.streamID), zap.Int32("state", state))
			}
		}

		log.Info("tsoStream.recvLoop ended", zap.String("stream", s.streamID), zap.Error(finishWithErr))

		close(s.pendingRequests)

		// Cancel remaining pending requests.
		for req := range s.pendingRequests {
			req.callback(tsoRequestResult{}, req.reqKeyspaceGroupID, errors.WithStack(finishWithErr))
		}

		s.wg.Done()
		s.ongoingRequests.Store(0)
		s.ongoingRequestCountGauge.Set(0)
	}()

recvLoop:
	for {
		// Try to load the corresponding `batchedRequests`. If `Recv` is successful, there must be a request pending
		// in the queue.
		select {
		case currentReq = <-s.pendingRequests:
			hasReq = true
		case <-ctx.Done():
			finishWithErr = ctx.Err()
			return
		}

		res, err := s.stream.Recv()

		durationSeconds := time.Since(currentReq.startTime).Seconds()

		if err != nil {
			// If a request is pending and error occurs, observe the duration it has cost.
			requestFailedDurationTSO.Observe(durationSeconds)
			if err == io.EOF {
				finishWithErr = errors.WithStack(errs.ErrClientTSOStreamClosed)
			} else {
				finishWithErr = errors.WithStack(err)
			}
			break recvLoop
		}

		latencySeconds := durationSeconds
		requestDurationTSO.Observe(latencySeconds)
		tsoBatchSize.Observe(float64(res.count))

		if res.count != uint32(currentReq.count) {
			finishWithErr = errors.WithStack(errTSOLength)
			break recvLoop
		}

		currentReq.callback(res, currentReq.reqKeyspaceGroupID, nil)
		// After finishing the requests, unset these variables which will be checked in the defer block.
		currentReq = batchedRequests{}
		hasReq = false

		s.ongoingRequestCountGauge.Set(float64(s.ongoingRequests.Add(-1)))
	}
}

// GetRecvError returns the error (if any) that has been encountered when receiving response asynchronously.
func (s *tsoStream) GetRecvError() error {
	perr := s.stoppedWithErr.Load()
	if perr == nil {
		return nil
	}
	return *perr
}

// WaitForClosed blocks until the stream is closed and the inner loop exits.
func (s *tsoStream) WaitForClosed() {
	s.wg.Wait()
}
